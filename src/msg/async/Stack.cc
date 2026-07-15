// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "Stack.h"
#include "include/compat.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "PosixStack.h"
#ifdef HAVE_RDMA
#include "rdma/RDMAStack.h"
#endif
#ifdef HAVE_DPDK
#include "dpdk/DPDKStack.h"
#endif

#include "common/dout.h"
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << "stack "

void C_worker_done::do_request(uint64_t)
{
  // This callback runs on the worker's own thread, in FIFO order after all
  // previously-queued events (including C_clean_handler callbacks posted by
  // _stop()).  Setting done=true here causes the poll loop to exit cleanly
  // after this callback returns.
  worker->done = true;
  delete this;
}

std::function<void ()> NetworkStack::add_thread(Worker* w)
{
  return [this, w]() {
      rename_thread(w->id);
      const unsigned EventMaxWaitUs = 30000000;
      w->center.set_owner();
      ldout(cct, 10) << __func__ << " starting" << dendl;
      w->initialize();
      w->init_done();
      while (!w->done) {
        ldout(cct, 30) << __func__ << " calling event process" << dendl;

        ceph::timespan dur;
        int r = w->center.process_events(EventMaxWaitUs, &dur);
        if (r < 0) {
          ldout(cct, 20) << __func__ << " process events failed: "
                         << cpp_strerror(errno) << dendl;
          // TODO do something?
        }
        w->perf_logger->tinc(l_msgr_running_total_time, dur);
      }
      // Drain any external events that were enqueued in the window between
      // C_worker_done::do_request() setting done=true and this thread exiting
      // the poll loop.  In particular, migrate_worker() checks done==false
      // and then calls submit_to() (which enqueues a C_submit_event and
      // blocks waiting for it).  If done transitions to true between the
      // check and the enqueue, the C_submit_event would sit in the queue
      // forever and the caller would deadlock.  Processing the queue one
      // final time here ensures those synchronous submits are serviced.
      w->center.process_events(0, nullptr);
      // Capture retiring flag BEFORE reset() clears it.
      const bool was_retiring = w->retiring.load();
      ldout(cct, 10) << __func__ << " worker " << w->id << " thread exiting"
                     << " retiring=" << was_retiring
                     << " references=" << w->references.load() << dendl;
      w->reset();
      w->destroy();
      // If this worker was retired (not stopped via stop()), hand it off to the
      // NetworkStack for lazy reaping (join + delete) by the next caller of
      // set_num_workers() or stop().
      if (was_retiring) {
        worker_finished(w);
      }
  };
}

std::shared_ptr<NetworkStack> NetworkStack::create(CephContext *c,
						   const std::string &t)
{
  std::shared_ptr<NetworkStack> stack = nullptr;
  if (t == "posix")
    stack.reset(new PosixNetworkStack(c, false));
  else if (t == "smc")
    stack.reset(new PosixNetworkStack(c, true));
#ifdef HAVE_RDMA
  else if (t == "rdma")
    stack.reset(new RDMAStack(c));
#endif
#ifdef HAVE_DPDK
  else if (t == "dpdk")
    stack.reset(new DPDKStack(c));
#endif

  if (stack == nullptr) {
    lderr(c) << __func__ << " ms_async_transport_type " << t <<
    " is not supported! " << dendl;
    ceph_abort();
    return nullptr;
  }

  stack->type = t;

  unsigned num_workers = c->_conf->ms_async_op_threads;
  ceph_assert(num_workers > 0);
  if (num_workers >= EventCenter::MAX_EVENTCENTER) {
    ldout(c, 0) << __func__ << " max thread limit is "
                  << EventCenter::MAX_EVENTCENTER << ", switching to this now. "
                  << "Higher thread values are unnecessary and currently unsupported."
                  << dendl;
    num_workers = EventCenter::MAX_EVENTCENTER;
  }
  for (unsigned worker_id = 0; worker_id < num_workers; ++worker_id) {
    Worker *w = stack->create_worker(c, worker_id);
    int ret = w->center.init(EventCenter::INIT_EVENT_NUMBER, worker_id, t);
    if (ret)
      throw std::system_error(-ret, std::generic_category());
    stack->workers.push_back(w);
  }
  return stack;
}

NetworkStack::NetworkStack(CephContext *c)
  : target_num_workers(0), cct(c)
{}

void NetworkStack::start()
{
  std::unique_lock<decltype(pool_spin)> lk(pool_spin);
  if (started) {
    return ;
  }

  for (Worker* worker : workers) {
    if (worker->is_init())
      continue;
    spawn_worker(add_thread(worker));
  }
  target_num_workers = workers.size();
  started = true;
  lk.unlock();

  for (Worker* worker : workers) {
    worker->wait_for_init();
  }
}

Worker* NetworkStack::get_worker()
{
  ldout(cct, 30) << __func__ << dendl;

   // start with some reasonably large number
  unsigned min_load = std::numeric_limits<int>::max();
  Worker* current_best = nullptr;

  pool_spin.lock();
  // Only consider the first target_num_workers entries; workers beyond that
  // index are retiring and must not receive new connections.
  unsigned active = target_num_workers;
  for (unsigned i = 0; i < active && i < workers.size(); ++i) {
    Worker* worker = workers[i];
    unsigned worker_load = worker->references.load();
    if (worker_load < min_load) {
      current_best = worker;
      min_load = worker_load;
    }
  }

  pool_spin.unlock();
  ceph_assert(current_best);
  ++current_best->references;
  return current_best;
}

void NetworkStack::worker_finished(Worker *w)
{
  std::lock_guard lk(pool_spin);
  ldout(cct, 10) << __func__ << " worker " << w->id << " queued for reap"
                 << dendl;
  finished_workers.push_back(w);
}

// Must be called with pool_spin held.
// Joins and deletes any workers that have finished their threads.
// Also removes them from the workers[] vector.
void NetworkStack::_reap_finished_workers()
{
  if (finished_workers.empty())
    return;

  for (Worker *w : finished_workers) {
    ldout(cct, 10) << __func__ << " reaping worker " << w->id << dendl;
    // Find its position in workers[] so join_worker() gets the right index.
    for (unsigned i = 0; i < workers.size(); ++i) {
      if (workers[i] == w) {
        join_worker(i);         // joins the thread, erases threads[i]
        workers.erase(workers.begin() + i);
        break;
      }
    }
    delete w;
  }
  finished_workers.clear();
}

void NetworkStack::stop()
{
  std::lock_guard lk(pool_spin);

  // Reap any workers that retired while we were running.
  _reap_finished_workers();

  // Stop and join all remaining workers in reverse order, erasing each
  // from workers[] as we go so that the destructor cannot double-delete.
  while (!workers.empty()) {
    unsigned i = workers.size() - 1;
    Worker *w = workers[i];
    w->done = true;
    w->center.wakeup();
    join_worker(i);   // joins the thread and erases threads[i]
    workers.erase(workers.begin() + i);
    delete w;
  }
  target_num_workers = 0;
  started = false;
}

void NetworkStack::set_num_workers(unsigned n)
{
  ceph_assert(n >= 1);
  ceph_assert(started);

  // Determine what action to take while holding the lock, then release it
  // before doing the slow work (spawning/waiting for threads).
  enum { NONE, GROW, SHRINK } action;
  unsigned old_target;
  unsigned cur_slots;  // workers.size() at decision time (incl. retiring)

  {
    std::lock_guard lk(pool_spin);

    // Reap any workers whose threads have already finished.
    _reap_finished_workers();

    old_target = target_num_workers;
    cur_slots  = workers.size();

    if (n == old_target) {
      return;
    } else if (n > old_target) {
      action = GROW;
    } else {
      action = SHRINK;
      // Lower the target immediately so get_worker() stops assigning to
      // workers[n..].  The retiring flags are also set here so there is no
      // window between releasing the lock and the loop below.
      target_num_workers = n;
      for (unsigned i = n; i < workers.size(); ++i) {
        Worker *w = workers[i];
        if (!w->retiring.load()) {
          ldout(cct, 10) << __func__ << " retiring worker " << w->id << dendl;
          w->retiring.store(true);
          // If references is already 0, no future release_worker() will
          // dispatch C_worker_done (release_worker only dispatches it when
          // it drops references 1→0 while retiring==true).  We must dispatch
          // it here instead so the worker thread can exit.
          //
          // There is no double-dispatch race: release_worker() does an
          // atomic fetch_sub and only dispatches when oldref==1, meaning it
          // was the thread that made references reach 0.  If references is
          // still 0 after we store retiring=true, it means every
          // release_worker() that could ever fire for this worker already
          // completed *before* retiring became true, so none of them
          // dispatched C_worker_done.  We are therefore the sole dispatcher.
          if (w->references.load() == 0) {
            w->center.dispatch_event_external(new C_worker_done(w));
          } else {
            // references > 0: the last release_worker() will observe
            // retiring==true and dispatch C_worker_done itself.
            w->center.wakeup();  // wake the poll loop so it re-checks retiring+refs
          }
        }
      }
    }
  }

  if (action == GROW) {
    // Add workers cur_slots..n-1.  Clamp against the hard ceiling.
    if (n >= EventCenter::MAX_EVENTCENTER) {
      ldout(cct, 0) << __func__ << " requested " << n
                    << " workers but max is " << EventCenter::MAX_EVENTCENTER
                    << ", clamping" << dendl;
      n = EventCenter::MAX_EVENTCENTER;
    }

    for (unsigned worker_id = cur_slots; worker_id < n; ++worker_id) {
      Worker *w = create_worker(cct, worker_id);
      int ret = w->center.init(EventCenter::INIT_EVENT_NUMBER, worker_id, type);
      if (ret) {
        lderr(cct) << __func__ << " center init failed for worker " << worker_id
                   << ": " << cpp_strerror(-ret) << dendl;
        delete w;
        break;
      }
      {
        std::lock_guard<decltype(pool_spin)> lk(pool_spin);
        workers.push_back(w);
        spawn_worker(add_thread(w));
        // Advance the target as each worker comes online.
        target_num_workers = workers.size();
      }
      w->wait_for_init();
      ldout(cct, 10) << __func__ << " added worker " << worker_id << dendl;
    }
  }
  // SHRINK: already done inside the lock above.
}

class C_drain : public EventCallback {
  ceph::mutex drain_lock = ceph::make_mutex("C_drain::drain_lock");
  ceph::condition_variable drain_cond;
  unsigned drain_count;

 public:
  explicit C_drain(size_t c)
      : drain_count(c) {}
  void do_request(uint64_t id) override {
    std::lock_guard l{drain_lock};
    drain_count--;
    if (drain_count == 0) drain_cond.notify_all();
  }
  void wait() {
    std::unique_lock l{drain_lock};
    drain_cond.wait(l, [this] { return drain_count == 0; });
  }
};

void NetworkStack::drain()
{
  ldout(cct, 30) << __func__ << " started." << dendl;
  pthread_t cur = pthread_self();
  pool_spin.lock();
  // Only drain active (non-retiring) workers.  Retiring workers may have
  // already exited or be in the process of exiting; sending them a drain
  // event could block indefinitely.
  unsigned active = target_num_workers;
  C_drain drain(active);
  for (unsigned i = 0; i < active && i < workers.size(); ++i) {
    ceph_assert(cur != workers[i]->center.get_owner());
    workers[i]->center.dispatch_event_external(EventCallbackRef(&drain));
  }
  pool_spin.unlock();
  drain.wait();
  ldout(cct, 30) << __func__ << " end." << dendl;
}
