// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSKY <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_ASYNC_STACK_H
#define CEPH_MSG_ASYNC_STACK_H

#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "include/spinlock.h"
#include "msg/async/Event.h"
#include "msg/msg_types.h"

#ifdef WITH_CRIMSON
#include "crimson/common/perf_counters_collection.h"
#else
#include "common/perf_counters_collection.h"
#endif

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

class Worker;
class ConnectedSocketImpl {
 public:
  virtual ~ConnectedSocketImpl() {}
  virtual int is_connected() = 0;
  virtual ssize_t read(char*, size_t) = 0;
  virtual ssize_t send(ceph::buffer::list &bl, bool more) = 0;
  virtual void shutdown() = 0;
  virtual void close() = 0;
  virtual int fd() const = 0;
  virtual void set_priority(int sd, int prio, int domain) = 0;
};

class ConnectedSocket;
struct SocketOptions {
  bool nonblock = true;
  bool nodelay = true;
  int rcbuf_size = 0;
  int priority = -1;
  entity_addr_t connect_bind_addr;
};

/// \cond internal
class ServerSocketImpl {
 public:
  unsigned addr_type; ///< entity_addr_t::TYPE_*
  unsigned addr_slot; ///< position of our addr in myaddrs().v
  ServerSocketImpl(unsigned type, unsigned slot)
    : addr_type(type), addr_slot(slot) {}
  virtual ~ServerSocketImpl() {}
  virtual int accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w) = 0;
  virtual void abort_accept() = 0;
  /// Get file descriptor
  virtual int fd() const = 0;
};
/// \endcond

/// \addtogroup networking-module
/// @{

/// A TCP (or other stream-based protocol) connection.
///
/// A \c ConnectedSocket represents a full-duplex stream between
/// two endpoints, a local endpoint and a remote endpoint.
class ConnectedSocket {
  std::unique_ptr<ConnectedSocketImpl> _csi;

 public:
  /// Constructs a \c ConnectedSocket not corresponding to a connection
  ConnectedSocket() {};
  /// \cond internal
  explicit ConnectedSocket(std::unique_ptr<ConnectedSocketImpl> csi)
      : _csi(std::move(csi)) {}
  /// \endcond
   ~ConnectedSocket() {
    if (_csi)
      _csi->close();
  }
  /// Moves a \c ConnectedSocket object.
  ConnectedSocket(ConnectedSocket&& cs) = default;
  /// Move-assigns a \c ConnectedSocket object.
  ConnectedSocket& operator=(ConnectedSocket&& cs) = default;

  int is_connected() {
    return _csi->is_connected();
  }
  /// Read the input stream with copy.
  ///
  /// Copy an object returning data sent from the remote endpoint.
  ssize_t read(char* buf, size_t len) {
    return _csi->read(buf, len);
  }
  /// Gets the output stream.
  ///
  /// Gets an object that sends data to the remote endpoint.
  ssize_t send(ceph::buffer::list &bl, bool more) {
    return _csi->send(bl, more);
  }
  /// Disables output to the socket.
  ///
  /// Current or future writes that have not been successfully flushed
  /// will immediately fail with an error.  This is useful to abort
  /// operations on a socket that is not making progress due to a
  /// peer failure.
  void shutdown() {
    return _csi->shutdown();
  }
  /// Disables input from the socket.
  ///
  /// Current or future reads will immediately fail with an error.
  /// This is useful to abort operations on a socket that is not making
  /// progress due to a peer failure.
  void close() {
    _csi->close();
    _csi.reset();
  }

  /// Get file descriptor
  int fd() const {
    return _csi->fd();
  }

  void set_priority(int sd, int prio, int domain) {
    _csi->set_priority(sd, prio, domain);
  }

  explicit operator bool() const {
    return _csi.get();
  }
};
/// @}

/// \addtogroup networking-module
/// @{

/// A listening socket, waiting to accept incoming network connections.
class ServerSocket {
  std::unique_ptr<ServerSocketImpl> _ssi;
 public:
  /// Constructs a \c ServerSocket not corresponding to a connection
  ServerSocket() {}
  /// \cond internal
  explicit ServerSocket(std::unique_ptr<ServerSocketImpl> ssi)
      : _ssi(std::move(ssi)) {}
  ~ServerSocket() {
    if (_ssi)
      _ssi->abort_accept();
  }
  /// \endcond
  /// Moves a \c ServerSocket object.
  ServerSocket(ServerSocket&& ss) = default;
  /// Move-assigns a \c ServerSocket object.
  ServerSocket& operator=(ServerSocket&& cs) = default;

  /// Accepts the next connection to successfully connect to this socket.
  ///
  /// \Accepts a \ref ConnectedSocket representing the connection, and
  ///          a \ref entity_addr_t describing the remote endpoint.
  int accept(ConnectedSocket *sock, const SocketOptions &opt, entity_addr_t *out, Worker *w) {
    return _ssi->accept(sock, opt, out, w);
  }

  /// Stops any \ref accept() in progress.
  ///
  /// Current and future \ref accept() calls will terminate immediately
  /// with an error.
  void abort_accept() {
    _ssi->abort_accept();
    _ssi.reset();
  }

  /// Get file descriptor
  int fd() const {
    return _ssi->fd();
  }

  /// get listen/bind addr
  unsigned get_addr_slot() {
    return _ssi->addr_slot;
  }

  explicit operator bool() const {
    return _ssi.get();
  }
};
/// @}

class NetworkStack;

enum {
  l_msgr_first = 94000,
  l_msgr_recv_messages,
  l_msgr_send_messages,
  l_msgr_recv_bytes,
  l_msgr_send_bytes,
  l_msgr_created_connections,
  l_msgr_active_connections,

  l_msgr_running_total_time,
  l_msgr_running_send_time,
  l_msgr_running_recv_time,
  l_msgr_running_fast_dispatch_time,

  l_msgr_send_messages_queue_lat,
  l_msgr_handle_ack_lat,

  l_msgr_recv_encrypted_bytes,
  l_msgr_send_encrypted_bytes,

  l_msgr_last,
};

enum {
  l_msgr_labeled_first = l_msgr_last + 1,

  l_msgr_connection_ready_timeouts,
  l_msgr_connection_idle_timeouts,

  l_msgr_labeled_last,
};

class Worker;

/**
 * Event callback dispatched to a retiring worker's center by release_worker()
 * when the last connection reference is dropped.  Because it is enqueued via
 * dispatch_event_external() it is processed in FIFO order, after any
 * C_clean_handler callbacks that were already queued — so all connection
 * cleanup events fire before the worker thread exits.
 */
class C_worker_done : public EventCallback {
  Worker *worker;
 public:
  explicit C_worker_done(Worker *w) : worker(w) {}
  void do_request(uint64_t) override;
};

class Worker {
  std::mutex init_lock;
  std::condition_variable init_cond;
  bool init = false;

 public:
  bool done = false;

  /**
   * Set to true by NetworkStack when this worker is being retired (i.e. it
   * is no longer eligible to accept new connections).  The worker thread's
   * poll loop will exit as soon as both retiring==true AND references==0,
   * ensuring every in-flight connection event has been processed before the
   * thread terminates.
   *
   * Written under pool_spin by the resizing thread; read lock-free by the
   * worker thread, so it must be atomic.
   */
  std::atomic<bool> retiring{false};

  friend class C_worker_done;

  CephContext *cct;
  std::shared_ptr<PerfCounters> perf_logger;
  std::shared_ptr<PerfCounters> perf_labeled_logger;
  unsigned id;

  std::atomic_uint references;
  EventCenter center;

  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  Worker(CephContext *c, unsigned worker_id)
    : cct(c), id(worker_id), references(0), center(c) {
    char name[128];
    char name_prefix[] = "AsyncMessenger::Worker";
    sprintf(name, "%s-%u", name_prefix, id);

    // initialize perf_logger
    PerfCountersBuilder plb(cct, name, l_msgr_first, l_msgr_last);

    plb.add_u64_counter(l_msgr_recv_messages, "msgr_recv_messages", "Network received messages");
    plb.add_u64_counter(l_msgr_send_messages, "msgr_send_messages", "Network sent messages");
    plb.add_u64_counter(l_msgr_recv_bytes, "msgr_recv_bytes", "Network received bytes", NULL, 0, unit_t(UNIT_BYTES));
    plb.add_u64_counter(l_msgr_send_bytes, "msgr_send_bytes", "Network sent bytes", NULL, 0, unit_t(UNIT_BYTES));
    plb.add_u64(l_msgr_active_connections, "msgr_active_connections", "Active connection number");
    plb.add_u64_counter(l_msgr_created_connections, "msgr_created_connections", "Created connection number");

    plb.add_time(l_msgr_running_total_time, "msgr_running_total_time", "The total time of thread running");
    plb.add_time(l_msgr_running_send_time, "msgr_running_send_time", "The total time of message sending");
    plb.add_time(l_msgr_running_recv_time, "msgr_running_recv_time", "The total time of message receiving");
    plb.add_time(l_msgr_running_fast_dispatch_time, "msgr_running_fast_dispatch_time", "The total time of fast dispatch");

    plb.add_time_avg(l_msgr_send_messages_queue_lat, "msgr_send_messages_queue_lat", "Network sent messages lat");
    plb.add_time_avg(l_msgr_handle_ack_lat, "msgr_handle_ack_lat", "Connection handle ack lat");

    plb.add_u64_counter(l_msgr_recv_encrypted_bytes, "msgr_recv_encrypted_bytes", "Network received encrypted bytes", NULL, 0, unit_t(UNIT_BYTES));
    plb.add_u64_counter(l_msgr_send_encrypted_bytes, "msgr_send_encrypted_bytes", "Network sent encrypted bytes", NULL, 0, unit_t(UNIT_BYTES));

    perf_logger.reset(plb.create_perf_counters());
    cct->get_perfcounters_collection()->add(perf_logger.get());

    // Add labeled perfcounters
    std::string labels = ceph::perf_counters::key_create(
        name_prefix, {{"id", std::to_string(id)}});
    PerfCountersBuilder plb_labeled(
        cct, labels, l_msgr_labeled_first,
        l_msgr_labeled_last);

    plb_labeled.add_u64_counter(
        l_msgr_connection_ready_timeouts, "msgr_connection_ready_timeouts",
        "Number of not yet ready connections declared as dead", NULL,
        PerfCountersBuilder::PRIO_USEFUL);
    plb_labeled.add_u64_counter(
        l_msgr_connection_idle_timeouts, "msgr_connection_idle_timeouts",
        "Number of connections closed due to idleness", NULL,
        PerfCountersBuilder::PRIO_USEFUL);

    perf_labeled_logger.reset(plb_labeled.create_perf_counters());
    cct->get_perfcounters_collection()->add(perf_labeled_logger.get());
  }
  virtual ~Worker() {
    // Remove from the collection so they stop being visible in admin socket
    // dumps.  The PerfCounters objects themselves may live longer if any
    // AsyncConnection still holds a shared_ptr reference; the memory is
    // freed when the last reference is released.
    if (perf_logger)
      cct->get_perfcounters_collection()->remove(perf_logger.get());
    if (perf_labeled_logger)
      cct->get_perfcounters_collection()->remove(perf_labeled_logger.get());
  }

  virtual int listen(entity_addr_t &addr, unsigned addr_slot,
                     const SocketOptions &opts, ServerSocket *) = 0;
  virtual int connect(const entity_addr_t &addr,
                      const SocketOptions &opts, ConnectedSocket *socket) = 0;
  virtual void destroy() {}

  virtual void initialize() {}
  PerfCounters *get_perf_counter() { return perf_logger.get(); }
  PerfCounters *get_labeled_perf_counter() { return perf_labeled_logger.get(); }
  // Shared ownership: AsyncConnection holds these to keep the counters alive
  // past Worker deletion (which happens when a worker is reaped).
  std::shared_ptr<PerfCounters> get_perf_counter_shared() { return perf_logger; }
  std::shared_ptr<PerfCounters> get_labeled_perf_counter_shared() { return perf_labeled_logger; }
  void release_worker() {
    int oldref = references.fetch_sub(1);
    ceph_assert(oldref > 0);
    // If this was the last reference and the worker is retiring, signal it to
    // stop by dispatching a done-sentinel to its own event center.  The
    // sentinel is enqueued AFTER any C_clean_handler already in the queue, so
    // the worker processes all pending cleanup events before it exits.
    if (oldref == 1 && retiring.load()) {
      center.dispatch_event_external(new C_worker_done(this));
    }
  }
  void init_done() {
    init_lock.lock();
    init = true;
    init_cond.notify_all();
    init_lock.unlock();
  }
  bool is_init() {
    std::lock_guard<std::mutex> l(init_lock);
    return init;
  }
  void wait_for_init() {
    std::unique_lock<std::mutex> l(init_lock);
    while (!init)
      init_cond.wait(l);
  }
  void reset() {
    init_lock.lock();
    init = false;
    init_cond.notify_all();
    init_lock.unlock();
    done = false;
    retiring.store(false);
  }
};

class NetworkStack {
  ceph::spinlock pool_spin;
  bool started = false;

  /**
   * Target number of active workers.  Workers with index >= target_num_workers
   * are "retiring": get_worker() will not assign new connections to them.
   * Guarded by pool_spin.
   */
  unsigned target_num_workers = 0;

  /**
   * Workers that have finished their thread (references dropped to zero while
   * retiring==true) and are waiting to be joined and deleted.
   * Guarded by pool_spin.
   */
  std::vector<Worker*> finished_workers;

  std::function<void ()> add_thread(Worker* w);

  /**
   * Join and delete every worker in finished_workers, removing them from
   * workers[].  Must be called with pool_spin held.
   */
  void _reap_finished_workers();

  virtual Worker* create_worker(CephContext *c, unsigned i) = 0;
  virtual void rename_thread(unsigned id) {
    static constexpr int TASK_COMM_LEN = 16;
    char tp_name[TASK_COMM_LEN];
    sprintf(tp_name, "msgr-worker-%u", id);
    ceph_pthread_setname(tp_name);
  }

 protected:
  CephContext *cct;
  std::vector<Worker*> workers;
  std::string type; ///< transport type string (posix, rdma, smc, …)

  explicit NetworkStack(CephContext *c);
 public:
  NetworkStack(const NetworkStack &) = delete;
  NetworkStack& operator=(const NetworkStack &) = delete;
  virtual ~NetworkStack() {
    // stop() should have been called before destruction and will have cleared
    // workers[].  Handle the case where it was not called (e.g. in tests).
    for (auto &&w : finished_workers)
      delete w;
    for (auto &&w : workers)
      delete w;
  }

  static std::shared_ptr<NetworkStack> create(
    CephContext *c, const std::string &type);

  // backend need to override this method if backend doesn't support shared
  // listen table.
  // For example, posix backend has in kernel global listen table. If one
  // thread bind a port, other threads also aware this.
  // But for dpdk backend, we maintain listen table in each thread. So we
  // need to let each thread do binding port.
  virtual bool support_local_listen_table() const { return false; }
  virtual bool nonblock_connect_need_writable_event() const { return true; }

  void start();
  void stop();
  virtual Worker *get_worker();
  Worker *get_worker(unsigned worker_id) {
    return workers[worker_id];
  }
  void drain();

  /**
   * Return the number of active (non-retiring) workers.
   * Guarded by pool_spin; safe to call from any thread.
   */
  unsigned get_num_worker() const {
    return target_num_workers;
  }

  /**
   * Return the total number of worker slots (active + retiring).
   * Retiring workers are still in workers[] but are no longer assigned new
   * connections.  This is used internally and by callers that need to iterate
   * all workers.
   */
  unsigned get_num_worker_slots() const {
    return workers.size();
  }

  /**
   * Dynamically resize the worker pool.
   *
   * May only be called after start().
   *
   * Growing: new worker threads are created and started immediately.
   *
   * Shrinking: workers with index >= n are marked as retiring so that no new
   * connections are assigned to them.  Each caller (AsyncMessenger) is
   * responsible for closing its own connections that are on retiring workers;
   * once a retiring worker's reference count reaches zero its thread exits
   * and the worker is queued for lazy reaping.  Reaping (join + delete) happens
   * the next time set_num_workers() is called by any messenger, and also
   * during stop().
   *
   * Because set_num_workers() is idempotent (a second call with the same n
   * is a no-op once the target is already set), it is safe for multiple
   * AsyncMessenger instances sharing the same stack to each call it.
   *
   * @param n  Desired total number of active workers.  Must be >= 1.
   */
  void set_num_workers(unsigned n);

  /**
   * Called by a retiring worker's thread just before it terminates.
   * Moves the worker to finished_workers so it can be reaped by the next
   * set_num_workers() or stop() call.
   */
  void worker_finished(Worker *w);

  // direct is used in tests only
  virtual void spawn_worker(std::function<void ()> &&) = 0;
  virtual void join_worker(unsigned i) = 0;

  virtual bool is_ready() { return true; };
  virtual void ready() { };
};

#endif //CEPH_MSG_ASYNC_STACK_H
