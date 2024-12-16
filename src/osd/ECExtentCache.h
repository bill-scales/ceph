//
// Created by root on 10/17/24.
//

#ifndef ECEXTENTCACHE_H
#define ECEXTENTCACHE_H

#include "ECUtil.h"

class ECExtentCache {
  class Address;
  class Line;
  class Object;
  typedef std::shared_ptr<Line> LineRef;
public:
  class LRU;
  class Op;
  typedef std::shared_ptr<Op> OpRef;
  struct BackendRead {
    virtual void backend_read(hobject_t oid, ECUtil::shard_extent_set_t const &request, uint64_t object_size) = 0;
    virtual ~BackendRead() = default;
  };

public:
  class LRU {
    std::list<LineRef> lru;
    uint64_t max_size = 0;
    uint64_t size = 0;
    ceph::mutex mutex = ceph::make_mutex("ECExtentCache::LRU");

    void free_maybe();
    void free_to_size(uint64_t target_size);
    void discard();
    void inc_size(uint64_t size);
    void dec_size(uint64_t size);
  public:
    explicit LRU(uint64_t max_size) : max_size(max_size) {}
  };

  class Op
  {
    friend class Object;
    friend class ECExtentCache;

    Object &object;
    std::optional<ECUtil::shard_extent_set_t> const reads;
    ECUtil::shard_extent_set_t const writes;
    bool complete = false;
    uint64_t projected_size = 0;
    GenContextURef<ECUtil::shard_extent_map_t &> cache_ready_cb;
    std::list<LineRef> lines;

    [[nodiscard]] extent_set get_pin_eset(uint64_t alignment) const;

  public:
    explicit Op(
      GenContextURef<ECUtil::shard_extent_map_t &> &&cache_ready_cb,
      Object &object,
      std::optional<ECUtil::shard_extent_set_t> const &to_read,
      ECUtil::shard_extent_set_t const &write,
      uint64_t orig_size,
      uint64_t projected_size);
      bool reading = false;
      bool read_done = false;
    ~Op();
    void cancel() { delete cache_ready_cb.release(); }
    ECUtil::shard_extent_set_t get_writes() { return writes; }
    [[nodiscard]] Object &get_object() const { return object; }

    bool complete_if_reads_cached()
    {
      if (!read_done) return false;
      auto result = object.get_cache(reads);
      complete = true;
        cache_ready_cb.release()->complete(result);
      return true;
    }

    void write_done(ECUtil::shard_extent_map_t const&& update) const
    {
      object.write_done(update, projected_size);
    }
  };

#define MIN_LINE_SIZE (32UL*1024UL)

private:
  class Object
  {
    friend class Op;
    friend class LRU;
    friend class Line;
    friend class ECExtentCache;

    ECExtentCache &pg;
    ECUtil::stripe_info_t const &sinfo;
    ECUtil::shard_extent_set_t requesting;
    ECUtil::shard_extent_set_t do_not_read;
    std::list<OpRef> reading_ops;
    std::list<OpRef> requesting_ops;
    std::map<uint64_t, std::weak_ptr<Line>> lines;
    int active_ios = 0;
    uint64_t current_size = 0;
    uint64_t projected_size = 0;
    uint64_t line_size = 0;
    bool reading = false;
    CephContext *cct;

    void request(OpRef &op);
    void send_reads();
    void unpin(Op &op);
    void delete_maybe() const;
    void erase_line(uint64_t offset);

  public:
    hobject_t oid;
    Object(ECExtentCache &pg, hobject_t const &oid, uint64_t size) : pg(pg), sinfo(pg.sinfo), cct(pg.cct), oid(oid), current_size(size), projected_size(size)
    {
      line_size = std::max(MIN_LINE_SIZE, pg.sinfo.get_chunk_size());
    }
    void insert(ECUtil::shard_extent_map_t const &buffers);
    void write_done(ECUtil::shard_extent_map_t const &buffers, uint64_t new_size);
    void read_done(ECUtil::shard_extent_map_t const &result);
    [[nodiscard]] uint64_t get_projected_size() const { return projected_size; }
    ECUtil::shard_extent_map_t get_cache(std::optional<ECUtil::shard_extent_set_t> const &set) const;
    uint64_t line_align(uint64_t line) const;
  };


  class Line
  {
  public:
    bool in_lru = false;
    uint64_t offset;
    ECUtil::shard_extent_map_t cache;
    Object &object;

    Line(Object &object, uint64_t offset) :
      offset(offset), cache(&object.pg.sinfo), object(object) {}

    ~Line()
    {
      object.erase_line(offset);
    }

    friend bool operator==(const Line& lhs, const Line& rhs)
    {
      return lhs.in_lru == rhs.in_lru
        && lhs.offset == rhs.offset
        && lhs.object.oid == rhs.object.oid;
    }

    friend bool operator!=(const Line& lhs, const Line& rhs)
    {
      return !(lhs == rhs);
    }
  };

  std::map<hobject_t, Object> objects;
  BackendRead &backend_read;
  LRU &lru;
  const ECUtil::stripe_info_t &sinfo;
  std::list<OpRef> waiting_ops;
  void cache_maybe_ready() const;
  int counter = 0;
  int active_ios = 0;
  CephContext* cct;

  OpRef prepare(GenContextURef<ECUtil::shard_extent_map_t &> &&ctx,
    hobject_t const &oid,
    std::optional<ECUtil::shard_extent_set_t> const &to_read,
    ECUtil::shard_extent_set_t const &write,
    uint64_t orig_size,
    uint64_t projected_size);

public:
  ~ECExtentCache()
  {
    // This should really only be needed in failed tests, as the PG should
    // clear up any IO before it gets destructed. However, here we make sure
    // to clean up any outstanding IO.
    on_change();
  }
  explicit ECExtentCache(BackendRead &backend_read,
    LRU &lru, const ECUtil::stripe_info_t &sinfo,
    CephContext *cct) :
    backend_read(backend_read),
    lru(lru),
    sinfo(sinfo),
    cct(cct) {}

  // Insert some data into the cache.
  void read_done(hobject_t const& oid, ECUtil::shard_extent_map_t const&& update);
  void write_done(OpRef const &op, ECUtil::shard_extent_map_t const&& update);
  void on_change();
  void on_change2();
  [[nodiscard]] bool contains_object(hobject_t const &oid) const;
  [[nodiscard]] uint64_t get_projected_size(hobject_t const &oid) const;

  template<typename CacheReadyCb>
  OpRef prepare(hobject_t const &oid,
    std::optional<ECUtil::shard_extent_set_t> const &to_read,
    ECUtil::shard_extent_set_t const &write,
    uint64_t orig_size,
    uint64_t projected_size,
    CacheReadyCb &&ready_cb) {

    GenContextURef<ECUtil::shard_extent_map_t &> ctx =
      make_gen_lambda_context<ECUtil::shard_extent_map_t &, CacheReadyCb>(
          std::forward<CacheReadyCb>(ready_cb));

    return prepare(std::move(ctx), oid, to_read, write, orig_size, projected_size);
  }

  void execute(OpRef &op);
  [[nodiscard]] bool idle() const;
  int get_and_reset_counter();

}; // ECExtentCaches

#endif //ECEXTENTCACHE_H
