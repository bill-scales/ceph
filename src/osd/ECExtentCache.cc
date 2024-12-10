//
// Created by root on 10/17/24.
//

#include "ECExtentCache.h"

#include "ECUtil.h"

using namespace std;
using namespace ECUtil;

#define dout_context cct
#define dout_subsys ceph_subsys_osd

void ECExtentCache::Object::request(Op &op)
{
  uint64_t alignment = sinfo.get_chunk_size();
  extent_set eset = op.get_pin_eset(sinfo.get_chunk_size());

  for (auto &&[start, len]: eset ) {
    for (uint64_t to_pin = start; to_pin < start + len; to_pin += alignment) {
      if (!lines.contains(to_pin))
        lines.emplace(to_pin, Line(*this, to_pin));
      Line &l = lines.at(to_pin);
      ceph_assert(!l.in_lru);
      l.in_lru = false;
      l.ref_count++;
      op.lines.emplace_back(l);
    }
  }

  /* else add to read */
  if (op.reads) {
    for (auto &&[shard, eset]: *(op.reads)) {
      extent_set request = eset;
      if (cache.contains(shard)) {
        request.subtract(cache.get_extent_set(shard));
      }
      if (reading.contains(shard)) {
        request.subtract(reading.at(shard));
      }
      if (writing.contains(shard)) {
        request.subtract(writing.at(shard));
      }

      if (!request.empty()) {
        requesting[shard].insert(request);
      }
    }
  }

  // Store the set of writes we are doing in this IO after subtracting the previous set.
  // We require that the overlapping reads and writes in the requested IO are either read
  // or were written by a previous IO.
  writing.insert(op.writes);

  send_reads();
}

void ECExtentCache::Object::send_reads()
{
  if (!reading.empty() || requesting.empty())
    return; // Read busy

  reading.swap(requesting);
  pg.backend_read.backend_read(oid, reading, current_size);
}

uint64_t ECExtentCache::Object::read_done(shard_extent_map_t const &buffers)
{
  reading.clear();
  uint64_t size_change = insert(buffers);
  send_reads();
  return size_change;
}

uint64_t ECExtentCache::Object::insert(shard_extent_map_t const &buffers)
{
  uint64_t old_size = cache.size();
  cache.insert(buffers);
  writing.subtract(buffers.get_shard_extent_set());

  return cache.size() - old_size;
}

void ECExtentCache::Object::unpin(Op &op) {
  for ( auto &&l : op.lines) {
    ceph_assert(l.ref_count);
    if (!--l.ref_count) {
      erase_line(l);
    }
  }

  delete_maybe();
}

void ECExtentCache::Object::delete_maybe() const {
  if (lines.empty() && active_ios == 0) {
    pg.objects.erase(oid);
  }
}

uint64_t ECExtentCache::Object::erase_line(Line const &line) {
  uint64_t size_delta = cache.size();
  lines.erase(line.offset);
  size_delta -= cache.size();
  delete_maybe();
  return size_delta;
}

void ECExtentCache::cache_maybe_ready() const
{
  while (!waiting_ops.empty()) {
    OpRef op = waiting_ops.front();
    /* If reads_done finds all reads a recomplete it will call the completion
     * callback. Typically, this will cause the client to execute the
     * transaction and pop the front of waiting_ops.  So we abort if either
     * reads are not ready, or the client chooses not to complete the op
     */
    if (!op->complete_if_reads_cached() || op == waiting_ops.front())
      return;
  }
}

ECExtentCache::OpRef ECExtentCache::prepare(GenContextURef<shard_extent_map_t &> && ctx,
  hobject_t const &oid,
  std::optional<shard_extent_set_t> const &to_read,
  shard_extent_set_t const &write,
  uint64_t orig_size,
  uint64_t projected_size)
{

  if (!objects.contains(oid)) {
    objects.emplace(oid, Object(*this, oid));
  }
  OpRef op = std::make_shared<Op>(
    std::move(ctx), objects.at(oid), to_read, write, orig_size, projected_size);

  return op;
}

void ECExtentCache::read_done(hobject_t const& oid, shard_extent_map_t const&& update)
{
  objects.at(oid).read_done(update);
  cache_maybe_ready();
}

void ECExtentCache::write_done(OpRef const &op, shard_extent_map_t const && update)
{
  ceph_assert(op == waiting_ops.front());
  waiting_ops.pop_front();
  op->write_done(std::move(update));
}

uint64_t ECExtentCache::get_projected_size(hobject_t const &oid) const {
  return objects.at(oid).get_projected_size();
}

bool ECExtentCache::contains_object(hobject_t const &oid) const {
  return objects.contains(oid);
}

ECExtentCache::Op::~Op() {
  ceph_assert(object.active_ios > 0);
  object.active_ios--;
  ceph_assert(object.pg.active_ios > 0);
  object.pg.active_ios--;

  object.unpin(*this);
}

void ECExtentCache::on_change() {
  for (auto && op : waiting_ops) {
    op->cancel();
  }
  waiting_ops.clear();
  ceph_assert(objects.empty());
  ceph_assert(active_ios == 0);
}

void ECExtentCache::execute(OpRef &op) {
  op->request();
  waiting_ops.emplace_back(op);
  counter++;
  cache_maybe_ready();
}

bool ECExtentCache::idle() const
{
  return active_ios == 0;
}

int ECExtentCache::get_and_reset_counter()
{
  int ret = counter;
  counter = 0;
  return ret;
}

void ECExtentCache::LRU::inc_size(uint64_t _size) {
  ceph_assert(ceph_mutex_is_locked_by_me(mutex));
  size += _size;
}

void ECExtentCache::LRU::dec_size(uint64_t _size) {
  ceph_assert(size >= _size);
  size -= _size;
}

void ECExtentCache::LRU::free_to_size(uint64_t target_size) {
  while (target_size < size && !lru.empty())
  {
    Line l = lru.front();
    lru.pop_front();
    dec_size(l.object.erase_line(l));
  }
}

void ECExtentCache::LRU::free_maybe() {
  free_to_size(max_size);
}

void ECExtentCache::LRU::discard() {
  free_to_size(0);
}

extent_set ECExtentCache::Op::get_pin_eset(uint64_t alignment) const {
  extent_set eset = writes.get_extent_superset();
  if (reads) reads->get_extent_superset(eset);
  eset.align(alignment);

  return eset;
}

ECExtentCache::Op::Op(GenContextURef<shard_extent_map_t &> &&cache_ready_cb,
  Object &object,
  std::optional<shard_extent_set_t> const &to_read,
  shard_extent_set_t const &write,
  uint64_t orig_size,
  uint64_t projected_size) :
  object(object),
  reads(to_read),
  writes(write),
  projected_size(projected_size),
  cache_ready_cb(std::move(cache_ready_cb))
{
  object.active_ios++;
  object.pg.active_ios++;
  object.projected_size = projected_size;

  if (object.active_ios == 1)
    object.current_size = orig_size;
}