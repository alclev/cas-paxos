#include "mu_squared.h"

#include "util.h"

using namespace paxos_st;

MuSquared::MuSquared(CasPaxos&& paxos,
                     std::vector<std::pair<uint32_t, uint8_t*>> proposals)
    : CasPaxos(std::move(paxos)),
      proposals_(std::move(proposals)),
      lease_established_(false),
      threads_alive_(true),
      op_counter_(0) {
  per_thread_latencies_.resize(system_size_);
  for (auto& v : per_thread_latencies_) {
    v.reserve(1 << 20);  // 1M samples per thread
  }
  shard_size_ = key_range_ / system_size_;
  // Each node will only propose keys withing shard_start
  // and shard_start + shard_size
  for (int i = 0; i < system_size_; ++i) {
    lease_boundaries_.push_back(i * shard_size_);
  }
  lease_boundaries_.push_back(key_range_);

  // Initialize queues
  for (int i = 0; i < system_size_; ++i) {
    commit_queues_.push_back(std::make_unique<MPSCQueue<OP, kQueueSize>>());
  }
  forward_queue_ = std::make_unique<MPSCQueue<OP, kQueueSize>>();

  threads_alive_.store(true, std::memory_order_release);
  ack_.store(0, std::memory_order_release);
  // forwarder_thread_ = std::thread(&MuSquared::Forwarder, this);
  // forward_handler_thread_ = std::thread(&MuSquared::ForwardHandler, this);
  poller_ = std::thread(&MuSquared::Poller, this);

  cached_laddr_.offset = host_id_ * kSlotSize;
  // the raddr and wr_id should be arbitrary here, as its rewritten in fast path
  romulus::WorkRequest::BuildWrite(cached_laddr_, cached_raddrs_[0], host_id_,
                                   &cached_write_, true);
  romulus::WorkRequest::BuildCAS(cached_laddr_, cached_raddrs_[0], 0, 0,
                                 host_id_, &cached_cas_);

  // set up lease connections
  for (int i = 0; i < system_size_; ++i) {
    if (i == host_id_)
      forwarding_conns_.push_back(nullptr);
    else
      // NB: this hardcodes forwarding conn to the last qp
      forwarding_conns_.push_back(remote_conns_[i][num_qps_ - 1]);
  }
  // first system size entries a reserved for the lease
  fuo_.store(system_size_, std::memory_order_release);

  // Init for prepare phase
  curr_promise_ballot_ = host_id_ + 1;
  for (uint32_t i = 0; i < system_size_; ++i) {
    expected_[i] = State(i + 1, kNullBallot, kNullValue);
    done_[i] = false;
    swap_[i] = State(curr_promise_ballot_, 0, Value(0));
    // state_[i] = State();
  }
  ROMULUS_ASSERT(kForwardRingSize % 2 == 0,
                 "Forward ring size must be multiple of 2.");
}

void MuSquared::DrainCQ() {
  ibv_wc wc;
  auto cq_raw = remote_conns_[0][0]->GetCQ();
  while (ibv_poll_cq(cq_raw, 1, &wc) > 0) {
    _mm_pause();
  }
}

void MuSquared::LeasePropose(uint32_t len, uint8_t* buf, bool is_lease) {
  if (!is_lease) {
    auto kv = *reinterpret_cast<KVPair<uint16_t>*>(buf);
    auto& key = kv.key;
    // ROMULUS_DEBUG("[LEASE PROPOSE] Received lease proposal for key {}", key);
    // Calculate whose lease the key belongs to
    int target = target_id(key, shard_size_, system_size_);
    auto fuo = fuo_.fetch_add(1, std::memory_order_acq_rel);
    std::atomic<int> op_acks{0};
    auto op = OP{len, *reinterpret_cast<uint32_t*>(buf),
                 static_cast<uint32_t>(fuo), &op_acks};

    DrainCQ();

    if (target == host_id_) {
      // load the staging buffer for the write
      *reinterpret_cast<State*>(cached_laddr_.addr + cached_laddr_.offset) =
          State(0, 0, Value(op.raw));

      auto start = std::chrono::high_resolution_clock::now();
      FastCommit(op.fuo);
      auto end = std::chrono::high_resolution_clock::now();
      op_counter_++;
      double latency =
          std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
              .count();
      per_thread_latencies_[host_id_].push_back(latency);
      // ROMULUS_INFO("Queue push time={}us, commit work time={}us, total
      // latency={}us",
      //              std::chrono::duration_cast<std::chrono::microseconds>(t1 -
      //              start).count(),
      //              std::chrono::duration_cast<std::chrono::microseconds>(end
      //              - t1).count(), latency);
    } else {
      // Forwarding branch
      // ROMULUS_DEBUG(
      //     "[LEASE PROPOSE] Key {} outside our lease boundary, forwarding",
      //     key);

      return;

      while (!forward_queue_->push(op)) {
        // theoretically, queue could be full, tell cpu to backoff
        _mm_pause();
      }
    }
  } else {
    Value v = Value(*reinterpret_cast<uint32_t*>(buf));
    ROMULUS_ASSERT(LeasePrepare(host_id_),
                   "Failed to prepare for lease proposal");
    ROMULUS_ASSERT(LeasePromise(host_id_, v),
                   "Failed to accept lease proposal");
    lease_established_.store(true, std::memory_order_release);
  }
}

void MuSquared::Warmup() {
  for (int i = 0; i < (int)kNumWarmupIters; ++i) {
    // Post the CAS
    // auto start = std::chrono::high_resolution_clock::now();
    ack_.store(0, std::memory_order_release);
    for (int n = 0; n < system_size_; ++n) {
      auto& conn = cached_conns_[n];
      auto& raddr = cached_raddrs_[n];
      raddr.addr_info.offset = host_id_ * kSlotSize;
      cached_laddr_.offset = n * kSlotSize;
      conn->CompareAndSwap(cached_laddr_, raddr, 0, 0,
                           reinterpret_cast<uint64_t>(&ack_));
    }
    // auto t1 = std::chrono::high_resolution_clock::now();
    while (ack_.load(std::memory_order_acquire) < system_size_) {
      _mm_pause();
    }
    // auto end = std::chrono::high_resolution_clock::now();

    // ROMULUS_INFO(
    //     "Warmup iteration {}: Post={} ns Poll={} ns", i,
    //     std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - start)
    //         .count(),
    //     std::chrono::duration_cast<std::chrono::nanoseconds>(end - t1)
    //         .count());
    std::this_thread::sleep_for(std::chrono::microseconds(50));
  }
}

bool MuSquared::LeasePrepare(uint32_t offset) {
  // auto start = std::chrono::high_resolution_clock::now();
  State* curr_proposal = &proposed_state_[offset];
  uint32_t done_count = 0;
  curr_proposal->SetPromiseBallot(curr_promise_ballot_);
  // int rounds = 0;

  for (uint32_t i = 0; i < system_size_; ++i) {
    expected_[i] = State();
    done_[i] = false;
    swap_[i] = State(curr_promise_ballot_, 0, Value(0));
  }
  // auto end_init = std::chrono::high_resolution_clock::now();
  // Retry until a quroum succeeds and the local log is written to. Making
  while (done_count < quorum_) {
    ++wr_id_;
    int posted = 0;
    // Post loop
    // auto t0 = std::chrono::high_resolution_clock::now();
    ack_.store(0, std::memory_order_release);
    for (uint32_t i = 0; i < system_size_; ++i) {
      if (done_[i]) continue;
      // if (detected_[i]) {
      //   done_[i] = true;
      //   ++done_count;
      //   continue;
      // }
      // Post a request
      auto& conn = cached_conns_[i];
      auto& raddr = cached_raddrs_[i];
      raddr.addr_info.offset = offset * kSlotSize;
      auto& laddr = cached_laddr_;
      laddr.offset = i * kSlotSize;
      ROMULUS_DEBUG("Prepare CAS: laddr={} raddr={} expected={} swap={}", raddr.addr_info.addr + raddr.addr_info.offset, laddr.addr + laddr.offset, expected_[i].raw,
                    swap_[i].raw);
      conn->CompareAndSwap(laddr, raddr, expected_[i].raw, swap_[i].raw,
                           reinterpret_cast<uint64_t>(&ack_));
      ++posted;
    }
    // auto t1 = std::chrono::high_resolution_clock::now();
    while (ack_.load(std::memory_order_acquire) < posted) {
      _mm_pause();
    }
    // auto t2 = std::chrono::high_resolution_clock::now();

    for (uint32_t i = 0; i < system_size_; ++i) {
      if (done_[i]) continue;
      State observed =
          *reinterpret_cast<State*>(cached_laddr_.addr + i * kSlotSize);
      if (observed.raw == expected_[i].raw) {
        expected_[i] = swap_[i];
        done_[i] = true;
        ++done_count;
      } else {
        expected_[i] = observed;
        swap_[i] = State(curr_promise_ballot_, observed.GetBallot(),
                         observed.GetValue());
      }
    }
    // rounds++;
    // auto t3 = std::chrono::high_resolution_clock::now();
    // ROMULUS_INFO(
    //     "Round {}: Init: {} us Post time = {} us Poll time = {} us Process time = {} us",
    //     rounds,
    //     std::chrono::duration_cast<std::chrono::microseconds>(end_init - start)
    //         .count(),
    //       std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count(),
    //     std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count(),
    //     std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2).count());
  }
  // ROMULUS_INFO("Lease prepare phase took {} rounds", rounds);

  return true;
}

bool MuSquared::LeasePromise(uint32_t offset, Value& v) {
  // auto start = std::chrono::high_resolution_clock::now();
  State* curr_proposal = &proposed_state_[offset];
  Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
  uint32_t done_count = 0;

  // Metadata to indicate to indicate a SUCCESSFUL cas for the given
  // acceptor
  // Metadata to indicate whether a CAS for a given acceptor has been POLLED
  for (uint32_t i = 0; i < system_size_; ++i) {
    // expected_[i] = State();
    done_[i] = false;
  }

  // We install the chose value if it hasn't already been set
  if (curr_proposal->GetBallot() == 0) {
    curr_proposal->SetProposal(curr_promise_ballot, v);
  }

  // ROMULUS_DEBUG("<Promise> slot={}, state={}", offset,
  //               proposed_state_[offset].ToString());

  // Retry until a quroum succeeds and the local log is written to. Making
  // sure that we write to the local log allows a follower to be certain
  // that if the slot is filled that the value is committed.
  int posted = 0;
  // int rounds = 0;
  // auto end_init = std::chrono::high_resolution_clock::now();
  while (done_count < quorum_) {
    // auto t0 = std::chrono::high_resolution_clock::now();
    // Post CAS ops.
    posted = 0;
    ack_.store(0, std::memory_order_release);
    for (uint32_t i = 0; i < system_size_; ++i) {
      // Already succeeded.
      if (done_[i]) continue;
      // if (detected_[i]) {
      //   done_[i] = true;
      //   ++done_count;
      //   continue;
      // }
      // Post a request
      auto& conn = cached_conns_[i];
      auto& raddr = cached_raddrs_[i];
      raddr.addr_info.offset = offset * kSlotSize;
      auto& laddr = cached_laddr_;
      laddr.offset = i * kSlotSize;
      ROMULUS_DEBUG("Promise CAS: laddr={} raddr={} expected={} swap={}", raddr.addr_info.addr + raddr.addr_info.offset, laddr.addr + laddr.offset, expected_[i].raw,
                    swap_[i].raw);
      conn->CompareAndSwap(laddr, raddr, expected_[i].raw, curr_proposal->raw,
                           reinterpret_cast<uint64_t>(&ack_));
      ++posted;
    }
    // auto t1 = std::chrono::high_resolution_clock::now();
    while (ack_.load(std::memory_order_acquire) < posted) {
      _mm_pause();
    }
    // auto t2 = std::chrono::high_resolution_clock::now();

    for (uint32_t i = 0; i < system_size_; ++i) {
      if (done_[i]) continue;
      // This will the result of the previous CAS
      cached_laddr_.offset = i * kSlotSize;
      State observed =
          *reinterpret_cast<State*>(cached_laddr_.addr + cached_laddr_.offset);
      // State observed = *c->scratch_state;

      if (expected_[i].raw == observed.raw) {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS success! observed={}, expected={}, "
        //     "curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        // CAS succeeded. Done.
        done_[i] = true;
        ++done_count;
      } else if (observed.GetPromiseBallot() > curr_promise_ballot) {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS failure Case 1: Seen higher ballot, abort."
        //     "observed={}, expected={}, curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        // We will make an assumption the that
        stable_leader_ = true;
        return false;
      } else {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS failure Case 2: Ballot still good. retry."
        //     "observed={}, expected={}, curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        expected_[i] = observed;
      }
    }
    // auto t3 = std::chrono::high_resolution_clock::now();
    ++wr_id_;
    // ++rounds;
    // ROMULUS_INFO(
    //     "Round {}: Init: {} us Post time = {} us Poll time = {} us Process time = {} us",
    //     rounds,
    //     std::chrono::duration_cast<std::chrono::microseconds>(end_init - start)
    //         .count(),
    //     std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count(),
    //     std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count(),
    //     std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2).count());
  }

  log_[offset] = *curr_proposal;
  // ROMULUS_INFO("Lease promise phase took {} rounds", rounds);
  return true;
}

void MuSquared::StartCommitThreads() {
  // Launch fast commit handlers
  // for (int tid = 0; tid < system_size_; ++tid) {
  //   if (tid == host_id_) continue;
  //   commit_posters_.emplace_back(&MuSquared::FastCommit, this, tid);
  // }
  // commit_poller_ = std::thread(&MuSquared::CommitPoller, this);
}

void MuSquared::FastCommit(uint32_t offset) {
  for (int i = 0; i < system_size_; ++i) {
    auto& conn = cached_conns_[i];

    auto& raddr = cached_raddrs_[i];
    raddr.addr_info.offset = (offset & (kRingSize - 1)) * kSlotSize;

    cached_write_.remote_addr(raddr);
    ROMULUS_ASSERT(conn->Post(&cached_write_, 1), "Failed to post");
  }
  auto shared_cq = remote_conns_[0][0]->GetCQ();
  int completions = 0;
  ibv_wc wcs[MAX_POLL_CHUNK];
  while (completions < quorum_) {
    int n = ibv_poll_cq(shared_cq, system_size_, wcs);
    if (n > 0) {
      completions += n;
    }
  }
}


void MuSquared::FastCommit(uint32_t offset) {
  std::atomic<int> ack{0};
  for (int i = 0; i < system_size_; ++i) {
    auto& conn = cached_conns_[i];

    auto& raddr = cached_raddrs_[i];
    raddr.addr_info.offset = (offset & (kRingSize - 1)) * kSlotSize;

    cached_write_.remote_addr(raddr)->wr_id(reinterpret_cast<uint64_t>(&ack));
    ROMULUS_ASSERT(conn->Post(&cached_write_, 1), "Failed to post");
  }
  while(ack.load(std::memory_order_acquire) < system_size_) {
    _mm_pause();
  }
}

void MuSquared::Poller() {
  pin_thread_to_core(8);
  auto shared_cq = remote_conns_[0][0]->GetCQ();
  std::vector<ibv_wc> wcs(system_size_);
  while (threads_alive_.load(std::memory_order_acquire) && !lease_established_.load(std::memory_order_acquire)) {
    int n = ibv_poll_cq(shared_cq, system_size_, wcs.data());
    for (int i = 0; i < n; ++i) {
      if (wcs[i].status != IBV_WC_SUCCESS) {
        ROMULUS_DEBUG("Work request (wc={}) failed: {}", wcs[i].wr_id,
                      ibv_wc_status_str(wcs[i].status));
      }
      // Otherwise, pull ack* out of wr_id and increment
      auto* ack = reinterpret_cast<std::atomic<int>*>(wcs[i].wr_id);
      if (ack) ack->fetch_add(1, std::memory_order_relaxed);
    }
  }
}

uint64_t MuSquared::GetTotalOps() { return op_counter_; }

std::vector<double> MuSquared::AggregateLatencies() {
  std::vector<double> all_lats;
  // for (const auto& thread_lats : per_thread_latencies_) {
  //   all_lats.insert(all_lats.end(), thread_lats.begin(), thread_lats.end());
  // }
  return per_thread_latencies_[host_id_];  // only return local latencies for
                                           // now
}

void MuSquared::Forwarder() {
  // reserved this scratch region for forwarding
  auto laddr =
      memblock_.GetAddrInfo(kScratchRegionId + "_" + std::to_string(4));
  laddr.length = kSlotSize;
  uint32_t local_write_idx = 0;
  while (threads_alive_.load(std::memory_order_acquire)) {
    OP req;
    if (forward_queue_->pop(req)) {
      auto kv = *reinterpret_cast<KVPair<uint16_t>*>(&req.raw);
      auto key = kv.key;
      // ROMULUS_DEBUG("[FORWARDER] Received lease proposal to forward for key
      // {}",
      //               key);
      auto target = target_id(key, shard_size_, system_size_);
      auto& conn = forwarding_conns_[target];
      ROMULUS_ASSERT(conn != nullptr,
                     "Forwarding connection to target_id {} is null, cannot "
                     "forward lease proposal",
                     target);
      auto& raddr = remote_addrs_[target][kForwarderRegionId];
      // The lease regions is a series of continguous rings, each
      // kForwardRingSize long
      // Know that this is a multiple of 2, we can use bitwise AND to wrap
      // around the ring
      raddr.addr_info.offset = (host_id_ * kForwardRingSize +
                                (local_write_idx & (kForwardRingSize - 1))) *
                               kSlotSize;
      laddr.offset = kSlotSize * host_id_;
      raddr.addr_info.length = kSlotSize;
      *reinterpret_cast<State*>(laddr.addr + laddr.offset) =
          State(req.fuo, 0, Value(req.raw));

      uint64_t wr_id = (static_cast<uint64_t>(host_id_) << 48) |
                       (static_cast<uint64_t>(target) << 32);
      ROMULUS_ASSERT(conn->Write(laddr, raddr, wr_id),
                     "Failed to post forward request write");
      auto res = conn->ProcessCompletions(1);
      ROMULUS_ASSERT(
          res == 1,
          "Failed to process completion for forward request write, got {}",
          res);
      local_write_idx++;
    } else {
      _mm_pause();
      continue;
    }
  }
}

void MuSquared::ForwardHandler() {
  // reserved this scratch region for forwarding
  auto laddr =
      memblock_.GetAddrInfo(kScratchRegionId + "_" + std::to_string(3));
  laddr.length = kSlotSize;
  romulus::ReliableConnection* loopback_conn;
  try {
    loopback_conn = remote_conns_[host_id_].at(3);
  } catch (const std::out_of_range& e) {
    ROMULUS_FATAL("Loopback connection for forward handler not found: {}",
                  e.what());
    return;
  }
  auto& raddr = remote_addrs_[host_id_][kForwarderRegionId];
  raddr.addr_info.length = kSlotSize;

  std::vector<uint32_t> read_idx(system_size_, 0);

  while (threads_alive_.load(std::memory_order_acquire)) {
    bool found_any = false;

    for (int n = 0; n < system_size_; ++n) {
      if (n == host_id_) continue;

      // compute offset into sender n's ring at current read index
      uint32_t slot =
          (n * kForwardRingSize + (read_idx[n] & (kForwardRingSize - 1)));
      raddr.addr_info.offset = slot * kSlotSize;
      laddr.offset = n * kSlotSize;
      *reinterpret_cast<State*>(laddr.addr + laddr.offset) = State();

      uint64_t wr_id = (static_cast<uint64_t>(host_id_) << 48) |
                       (static_cast<uint64_t>(n) << 32);
      ROMULUS_ASSERT(loopback_conn->Read(laddr, raddr, wr_id),
                     "Failed to post forward request read");
      ROMULUS_ASSERT(loopback_conn->ProcessCompletions(1) == 1,
                     "Failed to process completion for forward request read");

      State s = *reinterpret_cast<State*>(laddr.addr + laddr.offset);
      if (s.raw == 0) continue;
      // got a hit

      found_any = true;
      // push into commit queues
      uint32_t raw_val = s.GetValue().raw();
      uint32_t fuo = s.GetBallot();
      for (int i = 0; i < system_size_; ++i) {
        if (i == host_id_) continue;
        while (
            !commit_queues_[i]->push({sizeof(Value), raw_val, fuo, nullptr})) {
          _mm_pause();
        }
      }

      // clear the slot so we don't reprocess
      // write zero back to remote slot
      *reinterpret_cast<State*>(laddr.addr + laddr.offset) = State();
      ROMULUS_ASSERT(loopback_conn->Write(laddr, raddr, wr_id),
                     "Failed to clear forward request slot");
      ROMULUS_ASSERT(loopback_conn->ProcessCompletions(1) == 1,
                     "Failed to process completion for forward request clear");

      read_idx[n]++;
    }
    if (!found_any) {
      _mm_pause();
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
  }
}

void MuSquared::Cleanup() {
  ROMULUS_INFO("Cleaning up...");
  threads_alive_.store(false, std::memory_order_release);
  poller_.join();
  // for (auto& t : commit_posters_) {
  //   t.join();
  // }
  // forwarder_thread_.join();
  // forward_handler_thread_.join();
}
