#include "velos_squared.h"

VelosSquared::VelosSquared(std::shared_ptr<romulus::ArgMap> args,
                           uint64_t system_size,
                           std::shared_ptr<romulus::Device> device)
    : args_(args), id_(args->uget(NODE_ID)), hostname_(args->sget(HOSTNAME)),
      system_size_(system_size), quorum_(system_size / 2 + 1),
      num_shards_(args->uget(NUM_SHARDS)), capacity_(args->uget(CAPACITY)),
      pipeline_depth_(args->uget(PIPELINE_DEPTH)),
      no_outliers_(args_->bget(NO_OUTLIERS)), local_ballot_(0), wr_id_(0),
      device_(std::move(device)), raw_(nullptr),
      failure_detector_running_(true), prepare_running_(true) {

  // Define seed for the workload as static partition based on node id
  uint64_t key_range = args_->uget(KEY_RANGE);
  shard_size_ = key_range / num_shards_;
  // Assigning static ranges to each node in the system
  std::vector<std::pair<uint64_t, uint64_t>> shard_ranges(num_shards_);
  for (uint64_t i = 0; i < num_shards_; ++i) {
    shard_ranges[i] = {i * shard_size_, (i == num_shards_ - 1)
                                          ? key_range - 1
                                          : (i + 1) * shard_size_ - 1};
  }
  // Among these ranges, we decide which shards based on id
  for (uint64_t i = 0; i < num_shards_; ++i) {
    if (i % system_size_ == id_) {
      my_shards_.push_back(i);
      ROMULUS_INFO("Node {} is responsible for shard {}: {} - {}", id_, i,
                   shard_ranges[i].first, shard_ranges[i].second);
    }
  }

  // Generate phased workload
  ROMULUS_INFO("Generating workload...");
  proposals_.reserve(velos_squared::kNumProposals);
  WorkloadConfig config{velos_squared::kNumProposals, args_->uget(TXN_SIZE),
                        key_range, capacity_};
  // Now add the proposals that will trigger contention, namely for the next
  // shard in the ring
  std::vector<txn_t<int>> outliers;
  outliers.reserve(my_shards_.size());

  // Construct strided workload
  for (auto &curr : my_shards_) {
    auto wg_primary = WorkloadGenerator::generate<int>(
      config, shard_ranges[curr].first, shard_ranges[curr].second);
    proposals_.insert(proposals_.end(), wg_primary.begin(), wg_primary.end());
    // If we want outliers, insert them in strides alternating with the other
    if (!no_outliers_) {
      auto next = (curr + 1) % num_shards_;
      auto wg_next = WorkloadGenerator::generate<int>(
        config, shard_ranges[next].first, shard_ranges[next].second);
      proposals_.insert(proposals_.end(), wg_next.begin(), wg_next.end());
    }
  }

  fuos_ = std::make_unique<std::atomic<uint64_t>[]>(num_shards_);
  prep_offsets_ = std::make_unique<std::atomic<uint64_t>[]>(num_shards_);
  prom_offsets_ = std::make_unique<std::atomic<uint64_t>[]>(num_shards_);
  want_prep_ = std::make_unique<std::atomic<bool>[]>(num_shards_);
  // initialize member as s,n matrix
  preprepare_expected_ = std::vector<std::vector<State>>(
    num_shards_, std::vector<State>(system_size_));
  preprepare_done_ = std::vector<std::vector<bool>>(
    num_shards_, std::vector<bool>(system_size_));

  for (uint64_t s = 0; s < num_shards_; ++s) {
    fuos_[s].store(0, std::memory_order_relaxed);
    prep_offsets_[s].store(0, std::memory_order_relaxed);
    prom_offsets_[s].store(0, std::memory_order_relaxed);
    want_prep_[s].store(false, std::memory_order_relaxed);

    for (uint64_t n = 0; n < system_size_; ++n) {
      preprepare_expected_[s][n] = State();
      preprepare_done_[s][n] = false;
    }
  }
  wr_ids_.assign(num_shards_, 0);
  expected_.assign(system_size_, State());
  done_.assign(system_size_, false);
  detected_.assign(system_size_, false);
}

VelosSquared::~VelosSquared() { Shutdown(); }

void VelosSquared::SpawnThreads() {
  prepare_th_ = std::thread(&VelosSquared::PrepareHandler, this);
  fd_thread_ = std::thread(&VelosSquared::FailureDetector, this);
}

void VelosSquared::Propose(uint64_t target_shard, Value &v, uint32_t depth) {
  ROMULUS_ASSERT(depth < velos_squared::kMaxProposeDepth,
                 "Propose recursion bound exceeded.");
  // Keep the preparer ahead of us
  if (prep_offsets_[target_shard].load(std::memory_order_acquire) -
        prom_offsets_[target_shard].load(std::memory_order_relaxed) <=
      velos_squared::kPrepareLow)
    while (!prep_queue_.enqueue(prep_req_t{target_shard, false}))
      _mm_pause();
  // If we are able to succesfully write to a quorum of logs, then return true,
  // otherwise return false and we need to re-prepare under a higher ballot
  bool ok = pipeline_depth_ == 1 ? Promise_Single(target_shard, v, depth)
                                 : Promise_Pipe(target_shard, v, depth);
  if (!ok) {
    ROMULUS_DEBUG(
      "Could not get quorum of promises for shard {} at depth {}. Retrying"
      "path...",
      target_shard, depth);
    // Trigger the want prep flag on the shard of interest
    want_prep_[target_shard].store(true, std::memory_order_release);
    while (!prep_queue_.enqueue(prep_req_t{target_shard, true}))
      _mm_pause();
    // Block until the PrepareHandler flips back to false
    while (want_prep_[target_shard].load(std::memory_order_acquire))
      _mm_pause();
    RandomBackoff(1, velos_squared::kMaxStartingBackoff);
    Propose(target_shard, v, depth + 1);
  }
}

void VelosSquared::PrepareHandler() {

  pin_thread_to_core(2);

  prep_req_t req;
  while (prepare_running_.load(std::memory_order_acquire)) {
    if (prep_queue_.dequeue(req)) {
      // A reset rewinds to the promise watermark and bumps the ballot, since
      // everything prepared under the old ballot has been overtaken
      if (req.reset) {
        prep_offsets_[req.shard_id].store(
          prom_offsets_[req.shard_id].load(std::memory_order_acquire),
          std::memory_order_release);
        local_ballot_ = MakeBallot(local_ballot_ / system_size_ + 1);
      }
      Prepare(req.shard_id);
      if (req.reset)
        want_prep_[req.shard_id].store(false, std::memory_order_release);
    } else {
      _mm_pause();
      // if (!first && empty_counter % 1000000 == 0) {
      //   ROMULUS_DEBUG("[PrepareHandler] Empty queue for {} iterations. Stopping.", empty_counter);
      //   prepare_running_.store(false, std::memory_order_release);
      // }
        
    }
  }
}

bool VelosSquared::Prepare(uint64_t target_shard) {
  uint64_t fuo = 0;
  std::vector<State> swap(system_size_);
  std::vector<State> state(system_size_);
  auto &expected = preprepare_expected_[target_shard];
  auto &done = preprepare_done_[target_shard];

  auto laddr = prep_ctx_.laddr_;
  laddr.length = velos_squared::kSlotSize;

  // Prepare up to a window ahead of the promise watermark
  while ((fuo = prep_offsets_[target_shard].load(std::memory_order_acquire)) <
           capacity_ &&
         fuo < prom_offsets_[target_shard].load(std::memory_order_acquire) +
                 velos_squared::kPrepareWindow) {
    State *curr_proposal = &proposed_state_[target_shard * capacity_ + fuo];
    Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
    if (local_ballot_ == 0) {
      local_ballot_ = MakeBallot(1);
    }
    curr_promise_ballot = std::max(curr_promise_ballot, local_ballot_);
    curr_proposal->SetPromiseBallot(curr_promise_ballot);

    uint32_t done_count = 0;

    std::fill(done.begin(), done.end(), false);
    std::fill(expected.begin(), expected.end(), State());
    std::fill(swap.begin(), swap.end(),
              State(curr_promise_ballot, 0, Value(0)));
    std::fill(state.begin(), state.end(), State());

    while (done_count < quorum_) {
      uint64_t wr_id_base =
        (static_cast<uint64_t>(++wr_ids_[target_shard]) << 48) |
        (static_cast<uint64_t>(id_) << 32);
      // Post CAS ops.
      int posted = 0;
      for (uint32_t n = 0; n < system_size_; ++n) {
        if (done[n])
          continue;

        uint64_t wr_id = wr_id_base | static_cast<uint64_t>(n);

        auto &conn = prep_ctx_.conns_[n];
        auto &raddr = prep_ctx_.raddrs_mat_[n][target_shard];
        raddr.addr_info.offset = fuo * velos_squared::kSlotSize;
        raddr.addr_info.length = velos_squared::kSlotSize;

        laddr.offset = n * velos_squared::kSlotSize;

        conn->CompareAndSwap(laddr, raddr, expected[n].raw, swap[n].raw, wr_id);
        posted++;
      }

      auto conn_raw = prep_ctx_.conns_[id_]->GetCQ();
      std::vector<ibv_wc> wc(posted);
      int total = 0;
      while (total < posted) {
        int n = ibv_poll_cq(conn_raw, posted - total, wc.data() + total);
        if (n < 0)
          ROMULUS_FATAL("Prepare: Error in polling");
        total += n;
      }

      // Check return value of CAS.
      bool need_bump = false;
      Ballot observed_max_ballot = curr_promise_ballot;

      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done[i])
          continue;
        laddr.offset = i * velos_squared::kSlotSize;
        State observed = *reinterpret_cast<State *>(laddr.addr + laddr.offset);

        if (observed.raw == expected[i].raw) {
          state[i] = observed;
          done[i] = true;
          ++done_count;
        } else {
          expected[i] = observed;
          swap[i] = State(curr_promise_ballot, observed.GetBallot(),
                          observed.GetValue());
          state[i] = observed;
          if (observed.GetPromiseBallot() > curr_promise_ballot) {
            need_bump = true;
            observed_max_ballot =
              std::max(observed_max_ballot, observed.GetPromiseBallot());
          }
        }
      }
      // Handle ballot bump after processing all completions. A quorum must be
      // promised under one ballot, so restart the slot
      if (need_bump) {
        local_ballot_ = MakeBallot(observed_max_ballot / system_size_ + 1);
        curr_promise_ballot = local_ballot_;
        curr_proposal->SetPromiseBallot(curr_promise_ballot);
        done_count = 0;
        std::fill(done.begin(), done.end(), false);
        for (uint32_t i = 0; i < system_size_; ++i) {
          swap[i] = State(curr_promise_ballot, expected[i].GetBallot(),
                          expected[i].GetValue());
        }
      }
    }
    // Reduce over the quorum: adopt highest accepted proposal
    for (uint32_t i = 0; i < system_size_; ++i) {
      if (state[i].GetBallot() > curr_proposal->GetBallot()) {
        curr_proposal->SetProposal(state[i].GetBallot(), state[i].GetValue());
      }
    }
    ROMULUS_DEBUG("Prepared slot: shard={}, prep_offset={}, state={}",
                  target_shard, fuo, curr_proposal->ToString());

    prep_offsets_[target_shard].fetch_add(1, std::memory_order_release);
  }

  return true;
}

bool VelosSquared::Promise_Single(uint64_t target_shard, Value &v,
                                  uint32_t attempt) {
  ROMULUS_ASSERT(prom_offsets_[target_shard].load(std::memory_order_acquire) <
                   capacity_,
                 "Log exhausted on shard {}", target_shard);
  // Wait until the preparer has prepared the next slot
  while (prom_offsets_[target_shard].load(std::memory_order_acquire) >=
         prep_offsets_[target_shard].load(std::memory_order_acquire)) {
    _mm_pause();
  }
  uint64_t prom = prom_offsets_[target_shard].load(std::memory_order_relaxed);

  State *curr_proposal = &proposed_state_[target_shard * capacity_ + prom];
  Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
  uint32_t done_count = 0;
  // Snapshot before mutation: this is what prepare left on the acceptors
  State prepared = *curr_proposal;
  for (uint32_t i = 0; i < system_size_; ++i) {
    expected_[i] = prepared;
    done_[i] = false;
  }
  // We install the chosen value if it hasn't already been set
  if (curr_proposal->GetBallot() == 0) {
    curr_proposal->SetProposal(curr_promise_ballot, v);
  }

  // Cached values
  uint32_t cached_offset = prom * velos_squared::kSlotSize;
  auto laddr = cons_ctx_.laddr_;
  laddr.length = velos_squared::kSlotSize;
  // Retry until a quroum succeeds and the local log is written to. Making
  // sure that we write to the local log allows a follower to be certain
  // that if the slot is filled that the value is committed.
  int posted = 0;
  while (done_count < quorum_) {
    uint64_t wr_id_base = (static_cast<uint64_t>(++wr_id_) << 48) |
                          (static_cast<uint64_t>(id_) << 32);
    // Post CAS ops.
    posted = 0;
    for (uint32_t i = 0; i < system_size_; ++i) {
      // Already succeeded.
      if (done_[i])
        continue;
      // if (detected_[i]) {
      //   done_[i] = true;
      //   ++done_count;
      //   continue;
      // }
      // Post a request
      auto &conn = cons_ctx_.conns_[i];
      auto &raddr = cons_ctx_.raddrs_mat_[i][target_shard];
      raddr.addr_info.offset = cached_offset;
      raddr.addr_info.length = velos_squared::kSlotSize;
      laddr.offset = i * velos_squared::kSlotSize;

      uint64_t wr_id = wr_id_base | static_cast<uint64_t>(i);

      conn->CompareAndSwap(laddr, raddr, expected_[i].raw, curr_proposal->raw,
                           wr_id);
      ++posted;
    }

    // Shared cq batch poll
    auto conn_raw = cons_ctx_.conns_[id_]->GetCQ();
    std::vector<ibv_wc> wc(posted);
    int total = 0;
    while(total < posted) {
      int n = ibv_poll_cq(conn_raw, posted - total, wc.data() + total);
      if (n < 0)
        ROMULUS_FATAL("Promise: Error in polling");
      total += n;
    }

    for (uint32_t i = 0; i < system_size_; ++i) {
      if (done_[i])
        continue;
      // This will the result of the previous CAS
      laddr.offset = i * velos_squared::kSlotSize;
      State observed = *reinterpret_cast<State *>(laddr.addr + laddr.offset);

      if (expected_[i].raw == observed.raw) {
        // CAS succeeded. Done.
        done_[i] = true;
        ++done_count;
      } else if (observed.GetPromiseBallot() > curr_promise_ballot) {
        // Seen higher ballot, abort. Caller re-prepares
        return false;
      } else {
        expected_[i] = observed;
      }
    }
  }

  log_[target_shard * capacity_ + prom] = *curr_proposal;
  ROMULUS_DEBUG("<Promise> Promised value: {} on shard {} slot {}", v.raw(),
                target_shard, prom);
  prom_offsets_[target_shard].fetch_add(1, std::memory_order_release);
  return true;
}

bool VelosSquared::Promise_Pipe(uint64_t target_shard, Value &v,
                                uint32_t attempt) {
  // TODO
  return Promise_Single(target_shard, v, attempt);
}

void VelosSquared::FailureDetector() {
  ROMULUS_INFO("[Failure Detector] Pinning to core 4...");
  pin_thread_to_core(4);

  // while (failure_detector_running_.load(std::memory_order_acquire)) {
  // }
}

Ballot VelosSquared::MakeBallot(uint32_t round) {
  uint32_t b = round * system_size_ + id_;
  ROMULUS_ASSERT(b <= std::numeric_limits<uint16_t>::max(),
                 "Ballot overflow: round={}, system_size={}", round,
                 system_size_);
  return static_cast<Ballot>(b);
}

std::string VelosSquared::GenLogID(uint64_t shard_id) {
  return velos_squared::kLogRegionId + "_" + std::to_string(shard_id);
}

uint64_t VelosSquared::SelectShard(int key) {
  uint64_t id = static_cast<uint64_t>(key) / shard_size_;
  return std::min(id, num_shards_ - 1);
}

void VelosSquared::Warmup() {
  const int num_warmup_iters = 1e4;

  auto laddr = memblock_.GetAddrInfo(velos_squared::kLogRegionId);
  laddr.length = velos_squared::kSlotSize;
  // TODO
}

void VelosSquared::Sync() { conn_manager_->arrive_strict_barrier(); }

void VelosSquared::DrainCQ(ibv_cq *cq_raw) {
  ibv_wc wc;
  while (ibv_poll_cq(cq_raw, 1, &wc) > 0) {
    uint64_t wr_id = wc.wr_id;
    ROMULUS_DEBUG("[DRAIN] Straggler: {} ", wr_id);
  }
}

void VelosSquared::Reset(uint64_t shard_id) {
  ResetLog(shard_id);
  fuos_[shard_id] = 0;
  prep_offsets_[shard_id] = 0;
  prom_offsets_[shard_id] = 0;
}

void VelosSquared::ResetLog(uint64_t shard_id) {
  auto raddr = memblock_.GetAddrInfo(GenLogID(shard_id));
  std::memset((void *)(raddr.addr + raddr.offset), 0,
              capacity_ * velos_squared::kSlotSize);
  std::memset((void *)&proposed_state_[shard_id * capacity_], 0,
              capacity_ * velos_squared::kSlotSize);
}

std::vector<txn_t<int>> VelosSquared::GetProposals() { return proposals_; }

void VelosSquared::Shutdown() {
  ROMULUS_INFO("Shutting down...");
  failure_detector_running_.store(false, std::memory_order_release);
  prepare_running_.store(false, std::memory_order_release);

  if (fd_thread_.joinable())
    fd_thread_.join();

  if (prepare_th_.joinable())
    prepare_th_.join();
}