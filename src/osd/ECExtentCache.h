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
    std::list<Line> lru;
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

    Object &object;
    std::optional<ECUtil::shard_extent_set_t> reads;
    ECUtil::shard_extent_set_t writes;
    bool complete = false;
    uint64_t projected_size = 0;
    GenContextURef<ECUtil::shard_extent_map_t &> cache_ready_cb;
    std::list<Line> lines;

    extent_set get_pin_eset(uint64_t alignment) const;

  public:
    explicit Op(
      GenContextURef<ECUtil::shard_extent_map_t &> &&cache_ready_cb,
      Object &object,
      std::optional<ECUtil::shard_extent_set_t> const &to_read,
      ECUtil::shard_extent_set_t const &write,
      uint64_t orig_size,
      uint64_t projected_size);
    ~Op();
    void cancel() { delete cache_ready_cb.release(); }
    ECUtil::shard_extent_set_t get_writes() { return writes; }
    Object &get_object() const { return object; }
    bool complete_if_reads_cached()
    {
      if (!object.cache.contains(reads)) return false;
      auto result = object.cache.intersect(reads);
      complete = true;
        cache_ready_cb.release()->complete(result);
      return true;
    }

    void write_done(ECUtil::shard_extent_map_t const&& update) const
    {
      object.insert(update);
      object.current_size = projected_size;
    }

    void request()
    {
      object.request(*this);
    }
  };

private:
  class Object
  {
    friend class Op;
    friend class LRU;

    ECExtentCache &pg;
    ECUtil::stripe_info_t const &sinfo;
    ECUtil::shard_extent_set_t requesting;
    ECUtil::shard_extent_set_t reading;
    ECUtil::shard_extent_set_t writing;
    ECUtil::shard_extent_map_t cache;
    std::map<uint64_t, Line> lines;
    int active_ios = 0;
    uint64_t projected_size = 0;
    uint64_t current_size = 0;
    CephContext *cct;

    void request(Op &op);
    void send_reads();
    void unpin(Op &op);
    void delete_maybe() const;
    uint64_t erase_line(Line const &l);

  public:
    hobject_t oid;
    Object(ECExtentCache &pg, hobject_t const &oid) : pg(pg), sinfo(pg.sinfo), cache(&pg.sinfo), cct(pg.cct), oid(oid) {}
    uint64_t insert(ECUtil::shard_extent_map_t const &buffers);
    uint64_t read_done(ECUtil::shard_extent_map_t const &result);
    uint64_t get_projected_size() const { return projected_size; }
  };


  class Line
  {
  public:
    bool in_lru = false;
    int ref_count = 0;
    uint64_t offset;
    Object &object;

    Line(Object &object, uint64_t offset) : offset(offset) , object(object) {}

    friend bool operator==(const Line& lhs, const Line& rhs)
    {
      return lhs.in_lru == rhs.in_lru
        && lhs.ref_count == rhs.ref_count
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
  bool contains_object(hobject_t const &oid) const;
  uint64_t get_projected_size(hobject_t const &oid) const;

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
