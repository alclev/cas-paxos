#include "mu_squared.h"

MuSquared::MuSquared(std::shared_ptr<romulus::ArgMap> args,
                     uint64_t system_size,
                     std::shared_ptr<romulus::Device> device)
    : args_(args),
      id_(args->uget(NODE_ID)),
      hostname_(args->sget(HOSTNAME)),
      system_size_(system_size),
      quorum_(system_size / 2 + 1),
      num_shards_(args->uget(NUM_SHARDS)),
      fuos_(num_shards_, 0),
      capacity_(args->uget(CAPACITY)),
      pipeline_depth_(args->uget(PIPELINE_DEPTH)),
      local_ballot_(0),
      device_(std::move(device)),
      num_shared_cq_(args->uget(NUM_SHARED_CQ)),
      num_qps_(args->uget(NUM_QP)) {
  // Define seed for the workload as static partition based on node id
  uint64_t key_range = args_->uget(KEY_RANGE);
  shard_size_ = key_range / num_shards_;

  shard_ranges_.resize(num_shards_);
  for (uint64_t i = 0; i < num_shards_; ++i) {
    shard_ranges_[i] = {i * shard_size_, (i == num_shards_ - 1)
                                             ? key_range - 1
                                             : (i + 1) * shard_size_ - 1};
  }

  for (uint64_t i = 0; i < num_shards_; ++i) {
    if (i % system_size_ == id_) {
      my_shards_.push_back(i);
      ROMULUS_INFO("Node {} is responsible for shard {}: {} - {}", id_, i,
                   shard_ranges_[i].first, shard_ranges_[i].second);
    }
  }

  // Generate phased workload
  ROMULUS_INFO("Generating workload...");
  proposals_.reserve(mu_squared::kNumProposals);
  WorkloadConfig config{mu_squared::kNumProposals, args_->uget(TXN_SIZE),
                        key_range, capacity_};
  // Now add the proposals that will trigger the lease acquisition, namely for
  // the next shard in the ring
  for (auto& curr : my_shards_) {
    auto wg_primary = WorkloadGenerator::generate<int>(
        config, shard_ranges_[curr].first, shard_ranges_[curr].second);
    auto next = (curr + 1) % num_shards_;
    auto wg_next = WorkloadGenerator::generate<int>(
        config, shard_ranges_[next].first, shard_ranges_[next].second);
    // This will make a new workload where 25% of the proposals will trigger
    // lease acquisition on the next shard, while the rest will be for the
    // current shard. This should
    auto combined = Disperse(wg_primary, wg_next, 0.75);
    proposals_.insert(proposals_.end(), combined.begin(), combined.end());
  }

  // Initialize metrics
  metrics_ = RawMetrics{0, {}};
}

void MuSquared::RemoteDump() {
  ROMULUS_INFO("Remote dump initiated...");
  // landing space for the reads
  auto laddr = memblock_.GetAddrInfo(mu_squared::kScratchRegionId);
  laddr.length = mu_squared::kSlotSize;
  laddr.offset = mu_squared::kSlotSize * id_;
  romulus::WorkRequest read;

  std::stringstream ss;
  for (int n = 0; n < (int)system_size_; ++n) {
    ss << "+----------------NODE " << n << "----------------+\n";

    auto conn = remote_conns_[n][0];
    auto conn_raw = conn->GetCQ();

    // Dump proposed region
    auto raddr = remote_addrs_[n][mu_squared::kProposedRegionId];
    raddr.addr_info.length = mu_squared::kSlotSize;

    for (int i = 0; i < (int)num_shards_; ++i) {
      raddr.addr_info.offset = mu_squared::kSlotSize * i;
      romulus::WorkRequest::BuildRead(laddr, raddr, 0, &read);
      ROMULUS_ASSERT(conn->Post(&read, 1), "Failed to post read in mem dump.");
      ibv_wc wc;
      while (true) {
        int n = ibv_poll_cq(conn_raw, 1, &wc);
        if (n < 0) ROMULUS_FATAL("Dump: Error in polling");
        if (n) break;  // success
      }
      State p = *reinterpret_cast<State*>(laddr.addr + laddr.offset);
      ss << "[PROPOSED] Shard id: " << i << " State: " << p.ToString() << "\n";
    }

    // Dump log region
    for (int s = 0; s < (int)num_shards_; ++s) {
      raddr =
          remote_addrs_[n][mu_squared::kLogRegionId + "_" + std::to_string(s)];
      raddr.addr_info.length = mu_squared::kSlotSize;

      for (int i = 0; i < (int)capacity_; ++i) {
        raddr.addr_info.offset = mu_squared::kSlotSize * i;
        romulus::WorkRequest::BuildRead(laddr, raddr, 0, &read);
        ROMULUS_ASSERT(conn->Post(&read, 1),
                       "Failed to post read in mem dump.");
        ibv_wc wc;
        while (true) {
          int n = ibv_poll_cq(conn_raw, 1, &wc);
          if (n < 0) ROMULUS_FATAL("Dump: Error in polling");
          if (n > 0 && wc.status == IBV_WC_SUCCESS) break;  // success
        }
        State p = *reinterpret_cast<State*>(laddr.addr + laddr.offset);
        ss << "[LOG] Shard id: " << s << " Offset: " << i
           << " State: " << p.ToString() << "\n";
      }
    }

    // Dump lease region
    raddr = remote_addrs_[n][mu_squared::kLeaseRegionId];
    raddr.addr_info.length = mu_squared::kSlotSize;

    for (int i = 0; i < (int)num_shards_; ++i) {
      raddr.addr_info.offset = mu_squared::kSlotSize * i;
      romulus::WorkRequest::BuildRead(laddr, raddr, 0, &read);
      ROMULUS_ASSERT(conn->Post(&read, 1), "Failed to post read in mem dump.");
      ibv_wc wc;
      while (true) {
        int n = ibv_poll_cq(conn_raw, 1, &wc);
        if (n < 0) ROMULUS_FATAL("Dump: Error in polling");
        if (n > 0 && wc.status == IBV_WC_SUCCESS) break;  // success
      }
      State l = *reinterpret_cast<State*>(laddr.addr + laddr.offset);
      ss << "[LEASE] Shard id: " << i << " Lease: " << l.ToString() << "\n";
    }
  }
  ss << std::endl;
  ROMULUS_INFO("Memory Dump:\n{}", ss.str());
}

void MuSquared::Propose(txn_t<int>& txn) {
  ROMULUS_ASSERT(!txn.keys.empty(), "Transaction must have at least one key.");
  // For now, hardcoding the first key as I use one key per transaction
  uint64_t target_shard = SelectShard(txn.keys.front());
  ROMULUS_DEBUG("Proposing transaction with key {} for shard {}",
                txn.keys.front(), target_shard);
  // Perform quorum READ on lease table
  std::vector<bool> done(system_size_, false);
  uint64_t done_count = 0;
  while (done_count < quorum_) {
    // Post CAS ops.
    int posted = 0;
    for (uint32_t n = 0; n < system_size_; ++n) {
      if (done[n]) continue;

      auto& conn = cached_conns_[n];
      auto& raddr = cached_raddrs_[n];
      raddr.addr_info.offset = target_shard * mu_squared::kSlotSize;
      auto& laddr = cached_laddr_;
      laddr.offset = n * mu_squared::kSlotSize;
      romulus::WorkRequest read_wr;
      romulus::WorkRequest::BuildRead(
          laddr, raddr, wr_id_t(id_, target_shard, n).raw, &read_wr);
      ROMULUS_ASSERT(conn->Post(&read_wr, 1),
                     "Failed to post read in quorum read.");
      ++posted;
    }
    auto conn_raw = remote_conns_[0][0]->GetCQ();

    std::vector<ibv_wc> wc(posted);
    int total = 0;
    while (total < posted) {
      int n = ibv_poll_cq(conn_raw, posted - total, wc.data() + total);
      if (n < 0) ROMULUS_FATAL("Propose Quorum Read: Error in polling");
      total += n;
    }

    for (int i = 0; i < total; ++i) {
      if (wc[i].status != IBV_WC_SUCCESS) continue;
      uint64_t wr_id = wc[i].wr_id;
      uint64_t extracted_id = wr_id & 0xFFFFFFFF;
      done[extracted_id] = true;
      done_count++;
    }
  }

  // DrainCQ();

  // Already reached a quorum, find State with highest ballot num
  Ballot winning_ballot = 0;
  uint64_t winning_idx = 0;
  for (int n = 0; n < (int)system_size_; ++n) {
    if (done[n]) {
      cached_laddr_.offset = n * mu_squared::kSlotSize;
      State observed =
          *reinterpret_cast<State*>(cached_laddr_.addr + cached_laddr_.offset);
      if (observed.GetBallot() > winning_ballot) {
        winning_ballot = observed.GetBallot();
        winning_idx = n;
      }
    }
  }

  cached_laddr_.offset = winning_idx * mu_squared::kSlotSize;
  State entry_read =
      *reinterpret_cast<State*>(cached_laddr_.addr + cached_laddr_.offset);
  uint32_t observed_id = entry_read.GetValue().raw();
  ROMULUS_DEBUG("Current lease owner for shard {}: {}", target_shard,
                observed_id);
  // If I have the lease --> Fast Commit
  if (observed_id == id_) {
    FastCommit(target_shard, txn);
    // DrainCQ();
    return;  // Fast commit done
  } else {
    // Try to acquire lease and try again
    AcquireLease(target_shard);
    Propose(txn);
  }
}

bool MuSquared::AcquireLease(uint64_t shard_id) {
  ROMULUS_DEBUG("Attempting to acquire lease for shard {}", shard_id);
  bool success = false;
  while (!success) {
    bool ok = Prepare(shard_id);
    // DrainCQ();
    if (ok) {
      ROMULUS_DEBUG("Prepare succeeded for shard {}", shard_id);
    } else {
      ROMULUS_INFO("Prepare failed for shard {}. Aborting and retrying...",
                   shard_id);
      std::this_thread::sleep_for(
          std::chrono::microseconds(mu_squared::kMaxStartingBackoff));
      continue;
    }

    ok = Promise(shard_id, Value(id_));
    // DrainCQ();
    if (ok) {
      ROMULUS_DEBUG("Successfully acquired lease for shard {}", shard_id);
      // Freshly acquired lease needs a FUO scan
      needs_fuo_scan_.insert(shard_id);
      return true;
    } else {
      ROMULUS_INFO("Promise failed for shard {}. Aborting and retrying...",
                   shard_id);
      std::this_thread::sleep_for(
          std::chrono::microseconds(mu_squared::kMaxStartingBackoff));
    }
  }
  return false;
}

bool MuSquared::Prepare(uint32_t shard_id) {
  State* curr_proposal = &proposed_state_[shard_id];
  Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
  if (local_ballot_ == 0) {
    local_ballot_ = MakeBallot(1);
  }
  curr_promise_ballot = std::max(curr_promise_ballot, local_ballot_);
  curr_proposal->SetPromiseBallot(curr_promise_ballot);

  auto backoff =
      std::chrono::nanoseconds(std::rand() % mu_squared::kMaxStartingBackoff);

  std::vector<State> swap(system_size_,
                          State(curr_promise_ballot, 0, Value(0)));
  std::vector<State> state(system_size_, State());
  expected_ = std::vector<State>(system_size_, State());
  std::vector<bool> done(system_size_, false);
  uint32_t done_count = 0;

  uint64_t wr_id_base =
      (static_cast<uint64_t>(id_) << 32) | static_cast<uint64_t>(shard_id);
  uint32_t cached_offset = shard_id * mu_squared::kSlotSize;
  while (done_count < quorum_) {
    // Post CAS ops.
    int posted = 0;
    for (uint32_t n = 0; n < system_size_; ++n) {
      if (done[n]) continue;

      uint64_t wr_id = wr_id_base | static_cast<uint64_t>(n);

      auto& conn = cached_conns_[n];
      auto& raddr = cached_raddrs_[n];
      raddr.addr_info.offset = cached_offset;
      auto& laddr = cached_laddr_;
      laddr.offset = n * mu_squared::kSlotSize;

      conn->CompareAndSwap(laddr, raddr, expected_[n].raw, swap[n].raw, wr_id);
      ++posted;
    }
    remote_conns_[0][0]->ProcessCompletions(posted);

    // Check return value of CAS.
    bool need_bump = false;
    Ballot observed_max_ballot = curr_promise_ballot;
    // uint32_t winning_index = 0;

    for (uint32_t n = 0; n < system_size_; ++n) {
      if (done[n]) continue;
      cached_laddr_.offset = n * mu_squared::kSlotSize;
      State observed =
          *reinterpret_cast<State*>(cached_laddr_.addr + cached_laddr_.offset);

      if (observed.raw == expected_[n].raw) {
        ROMULUS_DEBUG("Prepare: cas success");
        state[n] = observed;
        done[n] = true;
        ++done_count;
      } else {
        ROMULUS_DEBUG(
            "Prepare: cas failed but promise ballot still good. observed={}, "
            "expected={}",
            observed.ToString(), expected_[n].ToString());
        expected_[n] = observed;
        swap[n] = State(curr_promise_ballot, observed.GetBallot(),
                        observed.GetValue());
        state[n] = observed;
        if (observed.GetPromiseBallot() > curr_promise_ballot) {
          need_bump = true;
          observed_max_ballot =
              std::max(observed_max_ballot, observed.GetPromiseBallot());
          // winning_index = n;
        }
      }
    }
    // Handle ballot bump after processing all completions
    if (need_bump) {
      ROMULUS_DEBUG("Prepare: cas failed. abort and bump.");
      Ballot unique_ballot = BumpBallot(observed_max_ballot);
      ROMULUS_ASSERT(unique_ballot > observed_max_ballot,
                     "GlobalBallot did not exceed observed promise ballot");
      curr_promise_ballot = unique_ballot;
      curr_proposal->SetPromiseBallot(unique_ballot);
      // reset metadata
      done_count = 0;
      std::fill(done.begin(), done.end(), false);
      std::fill(expected_.begin(), expected_.end(), State());
      std::fill(swap.begin(), swap.end(),
                State(curr_promise_ballot, 0, Value(0)));
      // perform backoff
      backoff = DoBackoff(backoff);
    }
  }
  // Reduce over the quorum: adopt highest accepted proposal
  Ballot best_ballot = 0;
  Value best_value = Value(0);

  for (uint32_t i = 0; i < system_size_; ++i) {
    if (!done[i]) continue;
    // we reduce over the state vector
    if (state[i].GetBallot() > best_ballot) {
      best_ballot = state[i].GetBallot();
      best_value = state[i].GetValue();
    }
  }
  ROMULUS_DEBUG("Prepared slot: shard_id={}, state={}", shard_id,
                curr_proposal->ToString());

  // seed expected for promise phase
  expected_ = std::move(swap);
  return true;
}

bool MuSquared::Promise(uint32_t shard_id, Value v) {
  State* curr_proposal = &proposed_state_[shard_id];
  Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
  uint32_t done_count = 0;

  // expected already set from prepare
  std::vector<bool> done(system_size_, false);

  // We install the chose value
  curr_proposal->SetProposal(curr_promise_ballot, v);

  uint32_t cached_offset = shard_id * mu_squared::kSlotSize;
  uint64_t wr_id_base =
      (static_cast<uint64_t>(id_) << 32) | static_cast<uint64_t>(shard_id);

  // Retry until a quroum succeeds and the local log is written to. Making
  // sure that we write to the local log allows a follower to be certain
  // that if the slot is filled that the value is committed.
  int posted = 0;
  while (done_count < quorum_) {
    // Post CAS ops.
    posted = 0;
    for (uint32_t n = 0; n < system_size_; ++n) {
      // Already succeeded.
      if (done[n]) continue;

      // Post a request
      auto& conn = cached_conns_[n];
      auto& raddr = cached_raddrs_[n];
      raddr.addr_info.offset = cached_offset;
      auto& laddr = cached_laddr_;
      laddr.offset = n * mu_squared::kSlotSize;

      uint64_t wr_id = wr_id_base | static_cast<uint64_t>(n);

      conn->CompareAndSwap(laddr, raddr, expected_[n].raw, curr_proposal->raw,
                           wr_id);
      ++posted;
    }

    // Shared cq batch poll
    remote_conns_[0][0]->ProcessCompletions(posted);

    for (uint32_t n = 0; n < system_size_; ++n) {
      if (done[n]) continue;
      // This will the result of the previous CAS
      cached_laddr_.offset = n * mu_squared::kSlotSize;
      State observed =
          *reinterpret_cast<State*>(cached_laddr_.addr + cached_laddr_.offset);
      // State observed = *c->scratch_state;

      if (expected_[n].raw == observed.raw) {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS success! observed={}, expected={}, "
        //     "curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        // CAS succeeded. Done.
        done[n] = true;
        ++done_count;
      } else if (observed.GetPromiseBallot() > curr_promise_ballot) {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS failure Case 1: Seen higher ballot, abort."
        //     "observed={}, expected={}, curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        return false;
      } else {
        // ROMULUS_DEBUG(
        //     "<Promise> CAS failure Case 2: Ballot still good. retry."
        //     "observed={}, expected={}, curr_proposal={}",
        //     observed.ToString(), expected_[i].ToString(),
        //     curr_proposal->ToString());
        expected_[n] = observed;
      }
    }
  }

  return true;
}

void MuSquared::FastCommit(uint64_t shard_id, txn_t<int>& txn) {
  ROMULUS_DEBUG("Entering the fast path...");

  if (needs_fuo_scan_.count(shard_id)) {
    ROMULUS_DEBUG("Performing FUO scan for shard {}", shard_id);
    auto raddr = remote_addrs_[id_][GenLogID(shard_id)];
    raddr.addr_info.length = mu_squared::kSlotSize;
    auto laddr = cached_laddr_;
    laddr.length = mu_squared::kSlotSize;
    laddr.offset = mu_squared::kSlotSize * id_;
    auto conn = cached_conns_[id_];  // loopback
    romulus::WorkRequest read_wr;
    uint64_t fuo = 0;
    for (int log_slot = 0; log_slot < (int)capacity_; ++log_slot) {
      raddr.addr_info.offset = log_slot * mu_squared::kSlotSize;
      conn->Read(laddr, raddr, wr_id_t(id_, shard_id, log_slot).raw);
      ROMULUS_ASSERT(conn->ProcessCompletions(1) == 1,
                     "Failed to process completion in FUO scan.");

      // Process the read data
      State observed = *reinterpret_cast<State*>(laddr.addr + laddr.offset);
      // ROMULUS_DEBUG("[FUO SCAN] observed={}", observed.ToString());
      if (observed == State()) {
        fuo = log_slot;
        break;
      }
    }
    ROMULUS_DEBUG("FUO scan complete for shard {}: FUO={}", shard_id, fuo);
    fuos_[shard_id] = fuo;
    needs_fuo_scan_.erase(shard_id);
  }

  // assuming one kv pair
  int val = txn.values.front();
  State commit_val;
  commit_val.SetValue(Value(static_cast<uint32_t>(val)));

  auto& fuo = fuos_[shard_id];
  if (fuo >= capacity_) {
    ROMULUS_FATAL("FUO overflow for shard {}", shard_id);
  }

  auto& laddr = cached_laddr_;
  laddr.length = mu_squared::kSlotSize;
  laddr.offset = mu_squared::kSlotSize * id_;
  // Load the staging buffer
  *reinterpret_cast<State*>(laddr.addr + laddr.offset) = commit_val;

  // Perform quorum WRITE to the appropriate log
  std::vector<bool> done(system_size_, false);
  uint64_t done_count = 0;
  while (done_count < quorum_) {
    // Post writes
    int posted = 0;
    for (uint32_t n = 0; n < system_size_; ++n) {
      if (done[n]) continue;

      auto& conn = cached_conns_[n];
      auto& raddr = remote_addrs_[n][GenLogID(shard_id)];
      raddr.addr_info.length = mu_squared::kSlotSize;
      raddr.addr_info.offset = fuo * mu_squared::kSlotSize;

      romulus::WorkRequest write_wr;
      romulus::WorkRequest::BuildWrite(
          laddr, raddr, wr_id_t(id_, shard_id, n).raw, &write_wr);
      ROMULUS_ASSERT(conn->Post(&write_wr, 1),
                     "Failed to post write in fast path.");
      ++posted;
    }
    auto conn_raw = remote_conns_[0][0]->GetCQ();

    std::vector<ibv_wc> wc(posted);
    int total = 0;
    while (total < posted) {
      int n = ibv_poll_cq(conn_raw, posted - total, wc.data() + total);
      if (n < 0) ROMULUS_FATAL("Fast path write: Error in polling");
      total += n;
    }

    for (int i = 0; i < total; ++i) {
      if (wc[i].status != IBV_WC_SUCCESS) continue;
      uint64_t wr_id = wc[i].wr_id;
      uint64_t extracted_id = wr_id & 0xFFFFFFFF;
      done[extracted_id] = true;
      done_count++;
    }
  }
  fuo++;
  ROMULUS_DEBUG("Fast commit complete for shard {}. FUO now at {}", shard_id,
                fuo);
}

std::string MuSquared::GenLogID(uint64_t shard_id) {
  return mu_squared::kLogRegionId + "_" + std::to_string(shard_id);
}

uint64_t MuSquared::SelectShard(int key) {
  uint64_t id = static_cast<uint64_t>(key) / shard_size_;
  return std::min(id, num_shards_ - 1);
}

Ballot MuSquared::MakeBallot(uint32_t round) {
  uint32_t b = round * system_size_ + id_;
  ROMULUS_ASSERT(b <= std::numeric_limits<uint16_t>::max(),
                 "Ballot overflow: round={}, system_size={}", round,
                 system_size_);
  return static_cast<Ballot>(b);
}

Ballot MuSquared::BumpBallot(Ballot observed_ballot) {
  auto observed_round = observed_ballot / system_size_;
  auto my_round = local_ballot_ / system_size_;
  auto next_round = std::max(observed_round + 1, my_round + 1);

  auto new_ballot = MakeBallot(next_round);
  local_ballot_ = static_cast<Ballot>(new_ballot);
  return local_ballot_;
}

void MuSquared::Warmup() {}

void MuSquared::Sync() { conn_manager_->arrive_strict_barrier(); }

void MuSquared::DrainCQ() {
  ibv_wc wc;
  auto cq_raw = remote_conns_[0][0]->GetCQ();
  while (ibv_poll_cq(cq_raw, 1, &wc) > 0) {
    uint64_t wr_id_raw = wc.wr_id;
    wr_id_t wr_id(wr_id_raw);
    ROMULUS_DEBUG("[DRAIN] Straggler: {} Shard: {} Epoch: {}", wr_id.GetID(),
                  wr_id.GetShardID(), wr_id.GetEpoch());
  }
}

RawMetrics MuSquared::GetStats() { return metrics_; }

std::vector<txn_t<int>> MuSquared::GetProposals() { return proposals_; }

void MuSquared::Cleanup() { ROMULUS_INFO("Cleaning up..."); }
