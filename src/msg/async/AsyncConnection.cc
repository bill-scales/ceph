// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "AsyncConnection.h"

#include <fmt/core.h>

#include <unistd.h>

#include "common/ceph_strings.h"
#include "common/ceph_time.h"
#include "common/iso_8601.h"
#include "common/tcp_info.h"
#include "include/Context.h"
#include "include/msgr.h"
#include "include/random.h"
#include "common/errno.h"
#include "AsyncMessenger.h"

#include "ProtocolV1.h"
#include "ProtocolV2.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "common/EventTrace.h"

// Constant to limit starting sequence number to 2^31.  Nothing special about it, just a big number.  PLR
#define SEQ_MASK  0x7fffffff

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix _conn_prefix(_dout)
std::ostream& AsyncConnection::_conn_prefix(std::ostream *_dout) {
  return *_dout << "-- " << async_msgr->get_myaddrs() << " >> "
		<< *peer_addrs << " conn(" << this
		<< (msgr2 ? " msgr2=" : " legacy=")
		<< protocol.get()
		<< " " << ceph_con_mode_name(protocol->auth_meta->con_mode)
                << " :" << port
                << " s=" << get_state_name(state)
                << " l=" << policy.lossy
                << ").";
}

// Notes:
// 1. Don't dispatch any event when closed! It may cause AsyncConnection alive even if AsyncMessenger dead

const uint32_t AsyncConnection::TCP_PREFETCH_MIN_SIZE = 512;

class C_time_wakeup : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_time_wakeup(AsyncConnectionRef c): conn(c) {}
  void do_request(uint64_t fd_or_id) override {
    conn->wakeup_from(fd_or_id);
  }
};

class C_handle_read : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_handle_read(AsyncConnectionRef c): conn(c) {}
  void do_request(uint64_t fd_or_id) override {
    conn->process();
  }
};

class C_handle_write : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_handle_write(AsyncConnectionRef c): conn(c) {}
  void do_request(uint64_t fd) override {
    conn->handle_write();
  }
};

class C_handle_write_callback : public EventCallback {
  AsyncConnectionRef conn;

public:
  explicit C_handle_write_callback(AsyncConnectionRef c) : conn(c) {}
  void do_request(uint64_t fd) override { conn->handle_write_callback(); }
};

class C_clean_handler : public EventCallback {
  AsyncConnectionRef conn;
 public:
  explicit C_clean_handler(AsyncConnectionRef c): conn(c) {}
  void do_request(uint64_t id) override {
    conn->cleanup();
    delete this;
  }
};

class C_tick_wakeup : public EventCallback {
  AsyncConnectionRef conn;

 public:
  explicit C_tick_wakeup(AsyncConnectionRef c): conn(c) {}
  void do_request(uint64_t fd_or_id) override {
    conn->tick(fd_or_id);
  }
};


AsyncConnection::AsyncConnection(CephContext *cct, AsyncMessenger *m, DispatchQueue *q,
                                 Worker *w, bool m2, bool local)
  : Connection(cct, m),
    delay_state(NULL), async_msgr(m), conn_id(q->get_id()),
    logger_ref(w->get_perf_counter_shared()),
    labeled_logger_ref(w->get_labeled_perf_counter_shared()),
    logger(logger_ref.get()),
    labeled_logger(labeled_logger_ref.get()),
    state(STATE_NONE), port(-1),
    dispatch_queue(q), recv_buf(NULL),
    recv_max_prefetch(std::max<int64_t>(msgr->cct->_conf->ms_tcp_prefetch_max_size, TCP_PREFETCH_MIN_SIZE)),
    recv_start(0), recv_end(0),
    last_active(ceph::coarse_mono_clock::now()),
    connect_timeout_us(cct->_conf->ms_connection_ready_timeout*1000*1000),
    inactive_timeout_us(cct->_conf->ms_connection_idle_timeout*1000*1000),
    msgr2(m2), state_offset(0),
    worker(w), center(&w->center),read_buffer(nullptr)
{
#ifdef UNIT_TESTS_BUILT
  this->interceptor = m->interceptor;
#endif
  read_handler = new C_handle_read(this);
  write_handler = new C_handle_write(this);
  write_callback_handler = new C_handle_write_callback(this);
  wakeup_handler = new C_time_wakeup(this);
  tick_handler = new C_tick_wakeup(this);
  // double recv_max_prefetch see "read_until"
  recv_buf = new char[2*recv_max_prefetch];
  if (local) {
    protocol = std::unique_ptr<Protocol>(new LoopbackProtocolV1(this));
  } else if (m2) {
    protocol = std::unique_ptr<Protocol>(new ProtocolV2(this));
  } else {
    protocol = std::unique_ptr<Protocol>(new ProtocolV1(this));
  }
  logger->inc(l_msgr_created_connections);
}

AsyncConnection::~AsyncConnection()
{
  if (recv_buf)
    delete[] recv_buf;
  ceph_assert(!delay_state);
}

int AsyncConnection::get_con_mode() const
{
  return protocol->get_con_mode();
}

bool AsyncConnection::is_msgr2() const
{
  return protocol->proto_type == 2;
}

void AsyncConnection::maybe_start_delay_thread()
{
  if (!delay_state) {
    async_msgr->cct->_conf.with_val<std::string>(
      "ms_inject_delay_type",
      [this](const std::string& s) {
	if (s.find(ceph_entity_type_name(peer_type)) != std::string::npos) {
	  ldout(msgr->cct, 1) << __func__ << " setting up a delay queue"
			      << dendl;
	  delay_state = new DelayedDelivery(async_msgr, center, dispatch_queue,
					    conn_id);
	}
      });
  }
}


ssize_t AsyncConnection::read(unsigned len, char *buffer,
                              std::function<void(char *, ssize_t)> callback) {
  ldout(async_msgr->cct, 20) << __func__
                             << (pendingReadLen ? " continue" : " start")
                             << " len=" << len << dendl;
  ssize_t r = read_until(len, buffer);
  if (r > 0) {
    readCallback = callback;
    pendingReadLen = len;
    read_buffer = buffer;
  }
  return r;
}

// Because this func will be called multi times to populate
// the needed buffer, so the passed in bufferptr must be the same.
// Normally, only "read_message" will pass existing bufferptr in
//
// And it will uses readahead method to reduce small read overhead,
// "recv_buf" is used to store read buffer
//
// return the remaining bytes, 0 means this buffer is finished
// else return < 0 means error
ssize_t AsyncConnection::read_until(unsigned len, char *p)
{
  ldout(async_msgr->cct, 25) << __func__ << " len is " << len << " state_offset is "
                             << state_offset << dendl;

  if (async_msgr->cct->_conf->ms_inject_socket_failures && cs) {
    if (rand() % async_msgr->cct->_conf->ms_inject_socket_failures == 0) {
      ldout(async_msgr->cct, 0) << __func__ << " injecting socket failure" << dendl;
      cs.shutdown();
    }
  }

  ssize_t r = 0;
  uint64_t left = len - state_offset;
  if (recv_end > recv_start) {
    uint64_t to_read = std::min<uint64_t>(recv_end - recv_start, left);
    memcpy(p, recv_buf+recv_start, to_read);
    recv_start += to_read;
    left -= to_read;
    ldout(async_msgr->cct, 25) << __func__ << " got " << to_read << " in buffer "
                               << " left is " << left << " buffer still has "
                               << recv_end - recv_start << dendl;
    if (left == 0) {
      state_offset = 0;
      return 0;
    }
    state_offset += to_read;
  }

  recv_end = recv_start = 0;
  /* nothing left in the prefetch buffer */
  if (left > (uint64_t)recv_max_prefetch) {
    /* this was a large read, we don't prefetch for these */
    do {
      r = read_bulk(p+state_offset, left);
      ldout(async_msgr->cct, 25) << __func__ << " read_bulk left is " << left << " got " << r << dendl;
      if (r < 0) {
        ldout(async_msgr->cct, 1) << __func__ << " read failed" << dendl;
        return -1;
      } else if (r == static_cast<int>(left)) {
        state_offset = 0;
        return 0;
      }
      state_offset += r;
      left -= r;
    } while (r > 0);
  } else {
    do {
      r = read_bulk(recv_buf+recv_end, recv_max_prefetch);
      ldout(async_msgr->cct, 25) << __func__ << " read_bulk recv_end is " << recv_end
                                 << " left is " << left << " got " << r << dendl;
      if (r < 0) {
        ldout(async_msgr->cct, 1) << __func__ << " read failed" << dendl;
        return -1;
      }
      recv_end += r;
      if (r >= static_cast<int>(left)) {
        recv_start = len - state_offset;
        memcpy(p+state_offset, recv_buf, recv_start);
        state_offset = 0;
        return 0;
      }
      left -= r;
    } while (r > 0);
    memcpy(p+state_offset, recv_buf, recv_end-recv_start);
    state_offset += (recv_end - recv_start);
    recv_end = recv_start = 0;
  }
  ldout(async_msgr->cct, 25) << __func__ << " need len " << len << " remaining "
                             << len - state_offset << " bytes" << dendl;
  return len - state_offset;
}

/* return -1 means `fd` occurs error or closed, it should be closed
 * return 0 means EAGAIN or EINTR */
ssize_t AsyncConnection::read_bulk(char *buf, unsigned len)
{
  ssize_t nread;
 again:
  nread = cs.read(buf, len);
  if (nread < 0) {
    if (nread == -EAGAIN) {
      nread = 0;
    } else if (nread == -EINTR) {
      goto again;
    } else {
      ldout(async_msgr->cct, 1) << __func__ << " reading from fd=" << cs.fd()
                          << " : "<< nread << " " << strerror(nread) << dendl;
      return -1;
    }
  } else if (nread == 0) {
    ldout(async_msgr->cct, 1) << __func__ << " peer close file descriptor "
                              << cs.fd() << dendl;
    return -1;
  }
  return nread;
}

ssize_t AsyncConnection::write(ceph::buffer::list &bl,
                               std::function<void(ssize_t)> callback,
                               bool more) {

    std::unique_lock<std::mutex> l(write_lock);
    outgoing_bl.claim_append(bl);
    ssize_t r = _try_send(more);
    if (r > 0) {
      writeCallback = std::move(callback);
    }
    return r;
}

// return the remaining bytes, it may larger than the length of ptr
// else return < 0 means error
ssize_t AsyncConnection::_try_send(bool more)
{
  if (async_msgr->cct->_conf->ms_inject_socket_failures && cs) {
    if (rand() % async_msgr->cct->_conf->ms_inject_socket_failures == 0) {
      ldout(async_msgr->cct, 0) << __func__ << " injecting socket failure" << dendl;
      cs.shutdown();
    }
  }

  if (!center->in_thread()) {
    // The connection was migrated to a new worker between when this write
    // event was enqueued and now.  Re-dispatch to the correct center so
    // the send is retried on the right thread.
    center->dispatch_event_external(write_handler);
    return 0;
  }
  ldout(async_msgr->cct, 25) << __func__ << " cs.send " << outgoing_bl.length()
                             << " bytes" << dendl;
  // network block would make ::send return EAGAIN, that would make here looks
  // like do not call cs.send() and r = 0
  ssize_t r = 0;
  if (likely(!inject_network_congestion())) {
    r = cs.send(outgoing_bl, more);
  }
  if (r < 0) {
    ldout(async_msgr->cct, 1) << __func__ << " send error: " << cpp_strerror(r) << dendl;
    return r;
  }

  ldout(async_msgr->cct, 10) << __func__ << " sent bytes " << r
                             << " remaining bytes " << outgoing_bl.length() << dendl;

  if (!open_write && is_queued()) {
    center->create_file_event(cs.fd(), EVENT_WRITABLE, write_handler);
    open_write = true;
  }

  if (open_write && !is_queued()) {
    center->delete_file_event(cs.fd(), EVENT_WRITABLE);
    open_write = false;
    if (writeCallback) {
      center->dispatch_event_external(write_callback_handler);
    }
  }

  return outgoing_bl.length();
}

void AsyncConnection::inject_delay() {
  if (async_msgr->cct->_conf->ms_inject_internal_delays) {
    ldout(async_msgr->cct, 10) << __func__ << " sleep for " <<
      async_msgr->cct->_conf->ms_inject_internal_delays << dendl;
    utime_t t;
    t.set_from_double(async_msgr->cct->_conf->ms_inject_internal_delays);
    t.sleep();
  }
}

bool AsyncConnection::inject_network_congestion() const {
  return (async_msgr->cct->_conf->ms_inject_network_congestion > 0 &&
	  rand() % async_msgr->cct->_conf->ms_inject_network_congestion != 0);
}

void AsyncConnection::process() {
  std::lock_guard<std::mutex> l(lock);
  last_active = ceph::coarse_mono_clock::now();
  recv_start_time = ceph::mono_clock::now();

  ldout(async_msgr->cct, 20) << __func__ << dendl;

  switch (state) {
    case STATE_NONE: {
      ldout(async_msgr->cct, 20) << __func__ << " enter none state" << dendl;
      return;
    }
    case STATE_CLOSED: {
      ldout(async_msgr->cct, 20) << __func__ << " socket closed" << dendl;
      return;
    }
    case STATE_CONNECTING: {
      ceph_assert(!policy.server);

      // clear timer (if any) since we are connecting/re-connecting
      if (last_tick_id) {
        center->delete_time_event(last_tick_id);
      }
      last_connect_started = ceph::coarse_mono_clock::now();
      last_tick_id = center->create_time_event(
          connect_timeout_us, tick_handler);

      if (cs) {
        center->delete_file_event(cs.fd(), EVENT_READABLE | EVENT_WRITABLE);
        cs.close();
      }

      SocketOptions opts;
      opts.priority = async_msgr->get_socket_priority();
      if (async_msgr->cct->_conf->mon_use_min_delay_socket) {
          if (async_msgr->get_mytype() == CEPH_ENTITY_TYPE_MON &&
              peer_is_mon()) {
            opts.priority = SOCKET_PRIORITY_MIN_DELAY;
          }
      }
      opts.connect_bind_addr = msgr->get_myaddrs().front();
      ssize_t r = worker->connect(target_addr, opts, &cs);
      if (r < 0) {
        protocol->fault();
        return;
      }

      center->create_file_event(cs.fd(), EVENT_READABLE, read_handler);
      state = STATE_CONNECTING_RE;
    }
    case STATE_CONNECTING_RE: {
      ssize_t r = cs.is_connected();
      if (r < 0) {
        ldout(async_msgr->cct, 1) << __func__ << " reconnect failed to "
                                  << target_addr << dendl;
        if (r == -ECONNREFUSED) {
          ldout(async_msgr->cct, 2)
              << __func__ << " connection refused!" << dendl;
          dispatch_queue->queue_refused(this);
        }
        protocol->fault();
        return;
      } else if (r == 0) {
        ldout(async_msgr->cct, 10)
            << __func__ << " nonblock connect inprogress" << dendl;
        if (async_msgr->get_stack()->nonblock_connect_need_writable_event()) {
          center->create_file_event(cs.fd(), EVENT_WRITABLE,
                                    read_handler);
        }
        logger->tinc(l_msgr_running_recv_time,
               ceph::mono_clock::now() - recv_start_time);
        return;
      }

      center->delete_file_event(cs.fd(), EVENT_WRITABLE);
      ldout(async_msgr->cct, 10)
          << __func__ << " connect successfully, ready to send banner" << dendl;
      state = STATE_CONNECTION_ESTABLISHED;
      break;
    }

    case STATE_ACCEPTING: {
      center->create_file_event(cs.fd(), EVENT_READABLE, read_handler);
      state = STATE_CONNECTION_ESTABLISHED;
      if (async_msgr->cct->_conf->mon_use_min_delay_socket) {
        if (async_msgr->get_mytype() == CEPH_ENTITY_TYPE_MON &&
            peer_is_mon()) {
          cs.set_priority(cs.fd(), SOCKET_PRIORITY_MIN_DELAY,
                          target_addr.get_family());
        }
      }
      break;
    }

    case STATE_CONNECTION_ESTABLISHED: {
      if (pendingReadLen) {
        ssize_t r = read(*pendingReadLen, read_buffer, readCallback);
        if (r <= 0) { // read all bytes, or an error occured
          pendingReadLen.reset();
          char *buf_tmp = read_buffer;
          read_buffer = nullptr;
          readCallback(buf_tmp, r);
        }
	logger->tinc(l_msgr_running_recv_time,
	    ceph::mono_clock::now() - recv_start_time);
        return;
      }
      break;
    }
  }

  protocol->read_event();

  logger->tinc(l_msgr_running_recv_time,
               ceph::mono_clock::now() - recv_start_time);
}

bool AsyncConnection::is_connected() {
  return protocol->is_connected();
}

void AsyncConnection::connect(const entity_addrvec_t &addrs, int type,
                              entity_addr_t &target) {

  std::lock_guard<std::mutex> l(lock);
  set_peer_type(type);
  set_peer_addrs(addrs);
  policy = msgr->get_policy(type);
  target_addr = target;
  _connect();
}

void AsyncConnection::_connect()
{
  ldout(async_msgr->cct, 10) << __func__ << dendl;

  state = STATE_CONNECTING;
  protocol->connect();
  // rescheduler connection in order to avoid lock dep
  // may called by external thread(send_message)
  center->dispatch_event_external(read_handler);
}

void AsyncConnection::accept(ConnectedSocket socket,
			     const entity_addr_t &listen_addr,
			     const entity_addr_t &peer_addr)
{
  ldout(async_msgr->cct, 10) << __func__ << " sd=" << socket.fd()
			     << " listen_addr " << listen_addr
			     << " peer_addr " << peer_addr << dendl;
  ceph_assert(socket.fd() >= 0);

  std::lock_guard<std::mutex> l(lock);
  cs = std::move(socket);
  socket_addr = listen_addr;
  target_addr = peer_addr; // until we know better
  state = STATE_ACCEPTING;
  protocol->accept();
  // rescheduler connection in order to avoid lock dep
  center->dispatch_event_external(read_handler);
}

int AsyncConnection::send_message(Message *m)
{
  FUNCTRACE(async_msgr->cct);
  lgeneric_subdout(async_msgr->cct, ms,
		   1) << "-- " << async_msgr->get_myaddrs() << " --> "
		      << get_peer_addrs() << " -- "
		      << *m << " -- " << m << " con "
		      << this
		      << dendl;

  if (is_blackhole()) {
    lgeneric_subdout(async_msgr->cct, ms, 0) << __func__ << ceph_entity_type_name(peer_type)
      << " blackhole " << *m << dendl;
    m->put();
    return 0;
  }

  // optimistic think it's ok to encode(actually may broken now)
  if (!m->get_priority())
    m->set_priority(async_msgr->get_default_send_priority());

  m->get_header().src = async_msgr->get_myname();
  m->set_connection(this);

#if defined(WITH_EVENTTRACE)
  if (m->get_type() == CEPH_MSG_OSD_OP)
    OID_EVENT_TRACE_WITH_MSG(m, "SEND_MSG_OSD_OP_BEGIN", true);
  else if (m->get_type() == CEPH_MSG_OSD_OPREPLY)
    OID_EVENT_TRACE_WITH_MSG(m, "SEND_MSG_OSD_OPREPLY_BEGIN", true);
#endif

  if (is_loopback) { //loopback connection
    ldout(async_msgr->cct, 20) << __func__ << " " << *m << " local" << dendl;
    std::lock_guard<std::mutex> l(write_lock);
    if (protocol->is_connected()) {
      dispatch_queue->local_delivery(m, m->get_priority());
    } else {
      ldout(async_msgr->cct, 10) << __func__ << " loopback connection closed."
                                 << " Drop message " << m << dendl;
      m->put();
    }
    return 0;
  }

  // we don't want to consider local message here, it's too lightweight which
  // may disturb users
  logger->inc(l_msgr_send_messages);

  protocol->send_message(m);
  return 0;
}

entity_addr_t AsyncConnection::_infer_target_addr(const entity_addrvec_t& av)
{
  // pick the first addr of the same address family as socket_addr.  it could be
  // an any: or v2: addr, we don't care.  it should not be a v1 addr.
  for (auto& i : av.v) {
    if (i.is_legacy()) {
      continue;
    }
    if (i.get_family() == socket_addr.get_family()) {
      ldout(async_msgr->cct,10) << __func__ << " " << av << " -> " << i << dendl;
      return i;
    }
  }
  ldout(async_msgr->cct,10) << __func__ << " " << av << " -> nothing to match "
			    << socket_addr << dendl;
  return {};
}

void AsyncConnection::fault()
{
  shutdown_socket();
  open_write = false;

  // queue delayed items immediately
  if (delay_state)
    delay_state->flush();

  recv_start = recv_end = 0;
  state_offset = 0;
  outgoing_bl.clear();
}

void AsyncConnection::_stop() {
  writeCallback = {};
  dispatch_queue->discard_queue(conn_id);
  async_msgr->unregister_conn(this);

  state = STATE_CLOSED;
  open_write = false;

  state_offset = 0;
  // Enqueue the cleanup callback BEFORE releasing the worker reference.
  // release_worker() may dispatch C_worker_done (the sentinel that exits
  // the poll loop) when this is the last connection on a retiring worker.
  // C_worker_done must be the last item in the external_events queue so
  // that the worker thread processes this C_clean_handler before it exits.
  // If we called release_worker() first, C_worker_done would land in the
  // queue before C_clean_handler, and the worker would exit with
  // C_clean_handler still pending — leaving a stale event in the dead
  // EventCenter's queue that triggers an in_thread() assert when the
  // EventCenter destructor tries to drain it on the wrong thread.
  center->dispatch_event_external(EventCallbackRef(new C_clean_handler(this)));
  worker->release_worker();
}

void AsyncConnection::migrate_worker(Worker *new_worker)
{
  ceph_assert(!is_loopback);

  EventCenter *old_center = center;   // safe to read outside lock: only we mutate it
  Worker      *old_worker  = worker;

  // -----------------------------------------------------------------------
  // Step 1: drain all pending events on the old worker and swap the
  //         worker/center pointers.
  //
  // The pointer swap MUST happen atomically with event draining, to close
  // the following race:
  //
  //   T1 (any thread): dispatch_event_external(write_handler) via the old
  //     center — write_handler lands in old center's external_events.
  //   T2 (migrate caller): swap center → new worker.
  //   Old worker: fires write_handler → write_event() → write_message()
  //     asserts center->in_thread(); but center now points to new worker
  //     → assert fires on the wrong thread.
  //
  // There are two cases depending on whether the old worker thread is still
  // running:
  //
  // Case A — old worker is STILL RUNNING (retiring=true, references>0, or
  //   just not yet exited):
  //     We submit a synchronous lambda to the old worker's center.
  //     submit_to() is ordered after all previously-enqueued external events
  //     (FIFO queue), so by the time the lambda runs, all write_handler /
  //     read_handler callbacks that were already enqueued have been processed.
  //     We perform the pointer swap inside the lambda.  From that point on,
  //     any dispatch_event_external(write_handler) will land on the new center.
  //
  // Case B — old worker thread has ALREADY EXITED (done=true):
  //     The thread is gone; nobody is processing that center's queue.
  //     No new event callbacks can fire on the old center.  We can perform
  //     the pointer swap directly here — no submit_to() needed or safe.
  //     (Calling submit_to() on a dead center would deadlock because nobody
  //     would ever process the lambda and signal the waiting condvar.)
  //
  // In both cases we also unregister any file/time events from the old center
  // so that if the old center is somehow drained later it sees nothing for
  // this connection.
  // -----------------------------------------------------------------------

  // closed tracks whether the connection was already STATE_CLOSED/STATE_NONE
  // at the time the old-worker work was done, so we know not to double-
  // release old_worker (which _stop() already released).
  bool closed = false;

  // do_old_worker_work performs the event cleanup and pointer swap.
  // on_old_thread=true  → called on the old worker's thread (Case A):
  //   delete_file_event/delete_time_event are safe (they assert in_thread()).
  // on_old_thread=false → old worker thread is dead (Case B):
  //   skip those calls; the old center will never process events again.
  auto do_old_worker_work = [&](bool on_old_thread) {
    std::lock_guard<std::mutex> l(lock);

    if (state == STATE_CLOSED || state == STATE_NONE) {
      // Connection already stopped; _stop() already released old_worker.
      new_worker->release_worker();
      closed = true;
      return;
    }

    // Remove file/time events from the old center so it no longer delivers
    // notifications.  Only do this when we are actually on the old worker's
    // thread; delete_file_event/delete_time_event assert in_thread().
    // When the old thread is dead, these events will never fire anyway.
    if (on_old_thread) {
      if (cs) {
        old_center->delete_file_event(cs.fd(), EVENT_READABLE | EVENT_WRITABLE);
      }
      for (auto id : register_time_events)
        old_center->delete_time_event(id);
      if (last_tick_id)
        old_center->delete_time_event(last_tick_id);
    }
    open_write = false;
    register_time_events.clear();
    last_tick_id = 0;

    // Swap the worker/center pointers.  After this, any subsequent
    // dispatch_event_external(write_handler) goes to the new center.
    worker = new_worker;
    center = &new_worker->center;
    if (delay_state)
      delay_state->set_center(center);

    logger_ref         = new_worker->get_perf_counter_shared();
    labeled_logger_ref = new_worker->get_labeled_perf_counter_shared();
    logger         = logger_ref.get();
    labeled_logger = labeled_logger_ref.get();
  };

  if (old_worker->done) {
    // Case B: old worker thread is dead — run inline, skipping event-center
    // operations that require in_thread().
    do_old_worker_work(false);
  } else {
    // Case A: old worker thread is alive — submit synchronously so we are
    // ordered after any in-flight write_handler / read_handler events.
    old_center->submit_to(old_center->get_id(),
                          [&]() { do_old_worker_work(true); }, false);
  }

  if (closed) {
    // old_worker was already released by _stop(); new_worker was released
    // inside do_old_worker_work().
    return;
  }

  // Release the OLD worker's reference now that we no longer use it.
  old_worker->release_worker();

  // -----------------------------------------------------------------------
  // Step 2: on the NEW worker's thread — re-register file/time events so
  //         the new center starts delivering events.
  //
  // center now points to new_worker's EventCenter (swapped in Step 1).
  // -----------------------------------------------------------------------
  center->submit_to(
    center->get_id(),
    [this]() {
      std::lock_guard<std::mutex> l(lock);

      if (state == STATE_CLOSED || state == STATE_NONE) {
        return;   // closed between step 1 and step 2
      }

      if (cs) {
        center->create_file_event(cs.fd(), EVENT_READABLE, read_handler);

        if (is_queued()) {
          center->create_file_event(cs.fd(), EVENT_WRITABLE, write_handler);
          open_write = true;
        }
      }

      if (!is_connected()) {
        last_tick_id = center->create_time_event(connect_timeout_us, tick_handler);
      } else if (inactive_timeout_us > 0) {
        last_tick_id = center->create_time_event(inactive_timeout_us, tick_handler);
      }
    },
    false /* synchronous */);
}

bool AsyncConnection::is_queued() const {
  return outgoing_bl.length();
}

void AsyncConnection::shutdown_socket() {
  for (auto &&t : register_time_events) center->delete_time_event(t);
  register_time_events.clear();
  if (last_tick_id) {
    center->delete_time_event(last_tick_id);
    last_tick_id = 0;
  }
  if (cs) {
    center->delete_file_event(cs.fd(), EVENT_READABLE | EVENT_WRITABLE);
    cs.shutdown();
    cs.close();
  }
}

void AsyncConnection::DelayedDelivery::do_request(uint64_t id)
{
  Message *m = nullptr;
  {
    std::lock_guard<std::mutex> l(delay_lock);
    register_time_events.erase(id);
    if (stop_dispatch)
      return ;
    if (delay_queue.empty())
      return ;
    m = delay_queue.front();
    delay_queue.pop_front();
  }
  if (msgr->ms_can_fast_dispatch(m)) {
    dispatch_queue->fast_dispatch(m);
  } else {
    dispatch_queue->enqueue(m, m->get_priority(), conn_id);
  }
}

void AsyncConnection::DelayedDelivery::discard() {
  stop_dispatch = true;
  center->submit_to(center->get_id(),
                    [this]() mutable {
                      std::lock_guard<std::mutex> l(delay_lock);
                      while (!delay_queue.empty()) {
                        Message *m = delay_queue.front();
                        dispatch_queue->dispatch_throttle_release(
                            m->get_dispatch_throttle_size());
                        m->put();
                        delay_queue.pop_front();
                      }
                      for (auto i : register_time_events)
                        center->delete_time_event(i);
                      register_time_events.clear();
                      stop_dispatch = false;
                    },
                    true);
}

void AsyncConnection::DelayedDelivery::flush() {
  stop_dispatch = true;
  center->submit_to(
      center->get_id(), [this] () mutable {
    std::lock_guard<std::mutex> l(delay_lock);
    while (!delay_queue.empty()) {
      Message *m = delay_queue.front();
      if (msgr->ms_can_fast_dispatch(m)) {
        dispatch_queue->fast_dispatch(m);
      } else {
        dispatch_queue->enqueue(m, m->get_priority(), conn_id);
      }
      delay_queue.pop_front();
    }
    for (auto i : register_time_events)
      center->delete_time_event(i);
    register_time_events.clear();
    stop_dispatch = false;
  }, true);
}

void AsyncConnection::send_keepalive()
{
  protocol->send_keepalive();
}

void AsyncConnection::mark_down()
{
  ldout(async_msgr->cct, 1) << __func__ << dendl;
  std::lock_guard<std::mutex> l(lock);
  protocol->stop();
}

void AsyncConnection::handle_write()
{
  ldout(async_msgr->cct, 10) << __func__ << dendl;
  // A write_handler event may have been dispatched to the old worker's center
  // just before a migration swapped center to a new worker.  The old worker's
  // thread will dequeue and fire this callback even though center now belongs
  // to the new worker.  Detect this case (in_thread() is false after the
  // pointer swap) and re-dispatch to the now-correct center so the write runs
  // on the right thread.  This mirrors the same guard in _try_send().
  if (!center->in_thread()) {
    ldout(async_msgr->cct, 10) << __func__
      << " not on owning thread after migration, re-dispatching" << dendl;
    center->dispatch_event_external(write_handler);
    return;
  }
  protocol->write_event();
}

void AsyncConnection::handle_write_callback() {
  std::lock_guard<std::mutex> l(lock);
  last_active = ceph::coarse_mono_clock::now();
  recv_start_time = ceph::mono_clock::now();
  write_lock.lock();
  if (writeCallback) {
    auto callback = std::move(writeCallback);
    write_lock.unlock();
    callback(0);
    return;
  }
  write_lock.unlock();
}

void AsyncConnection::stop(bool queue_reset) {
  lock.lock();
  bool need_queue_reset = (state != STATE_CLOSED) && queue_reset;
  protocol->stop();
  lock.unlock();
  if (need_queue_reset) dispatch_queue->queue_reset(this);
}

void AsyncConnection::cleanup() {
  shutdown_socket();
  delete read_handler;
  delete write_handler;
  delete write_callback_handler;
  delete wakeup_handler;
  delete tick_handler;
  if (delay_state) {
    delete delay_state;
    delay_state = NULL;
  }
}

void AsyncConnection::wakeup_from(uint64_t id)
{
  lock.lock();
  register_time_events.erase(id);
  lock.unlock();
  process();
}

void AsyncConnection::tick(uint64_t id)
{
  auto now = ceph::coarse_mono_clock::now();
  ldout(async_msgr->cct, 20) << __func__ << " last_id=" << last_tick_id
                             << " last_active=" << last_active << dendl;
  std::lock_guard<std::mutex> l(lock);
  last_tick_id = 0;
  if (!is_connected()) {
    if (connect_timeout_us <=
        (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>
          (now - last_connect_started).count()) {
      ldout(async_msgr->cct, 1) << __func__ << " see no progress in more than "
                                << connect_timeout_us
                                << " us during connecting to "
                                << target_addr << ", fault."
                                << dendl;
      protocol->fault();
      labeled_logger->inc(l_msgr_connection_ready_timeouts);
    } else {
      last_tick_id = center->create_time_event(connect_timeout_us, tick_handler);
    }
  } else {
    auto idle_period = std::chrono::duration_cast<std::chrono::microseconds>
      (now - last_active).count();
    if (inactive_timeout_us < (uint64_t)idle_period) {
      ldout(async_msgr->cct, 1) << __func__ << " idle (" << idle_period
                                << ") for more than " << inactive_timeout_us
                                << " us, fault."
                                << dendl;
      protocol->fault();
      labeled_logger->inc(l_msgr_connection_idle_timeouts);
    } else {
      last_tick_id = center->create_time_event(inactive_timeout_us, tick_handler);
    }
  }
}

void AsyncConnection::dump(Formatter *f, bool tcp_info) {
  std::lock_guard<std::mutex> l(lock);

  f->open_object_section("async_connection");
  f->dump_string("state", get_state_name(state));
  f->dump_unsigned("messenger_nonce", async_msgr->get_nonce());

  f->open_object_section("status");
  f->dump_bool("connected", is_connected());
  f->dump_bool("loopback", is_loopback);
  f->close_section();  // status

  if (cs) {
    f->dump_int("socket_fd", cs.fd());
    if (!tcp_info || !dump_tcp_info(cs.fd(), f)) {
      f->dump_null("tcp_info");
    }
  } else {
    f->dump_null("socket_fd");
    f->dump_null("tcp_info");
  }
  f->dump_int("conn_id", conn_id);

  f->open_object_section("peer");
  f->dump_object("entity_name", get_peer_entity_name());
  f->dump_string("type", ceph_entity_type_name(get_peer_type()));
  f->dump_int("id", get_peer_id());
  f->dump_int("global_id", get_peer_global_id());
  f->open_object_section("addr");
  peer_addrs->dump(f);
  f->close_section();  // addr
  f->close_section();  // peer

  f->dump_string("last_connect_started_ago",
                 ceph::timespan_str(ceph::coarse_mono_clock::now() -
                                    last_connect_started));
  f->dump_string(
      "last_active_ago",
      fmt::format("{}", ceph::timespan_str(ceph::coarse_mono_clock::now() -
                                           last_active)));
  f->dump_string("recv_start_time_ago",
                 fmt::format("{}", ceph::timespan_str(ceph::mono_clock::now() -
                                                      recv_start_time)));
  f->dump_unsigned("last_tick_id", last_tick_id);
  f->dump_object("socket_addr", socket_addr);
  f->dump_object("target_addr", target_addr);
  f->dump_int("port", port);
  f->open_object_section("protocol");
  if (protocol) {
    protocol->dump(f);
  } else {
    f->dump_null("null");
  }
  f->close_section();  // protocol
  f->dump_int("worker_id", worker ? worker->id : -1);
  f->close_section();  // async_connection
}
