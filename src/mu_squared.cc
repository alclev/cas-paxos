#include "mu_squared.h"

MuSquared::MuSquared(std::shared_ptr<romulus::ArgMap> args,
                     uint64_t system_size,
                     std::shared_ptr<romulus::Device> device)
    : args_(args), id_(args->uget(NODE_ID)), hostname_(args->sget(HOSTNAME)),
      system_size_(system_size), quorum_(system_size / 2 + 1),
      num_shards_(args->uget(NUM_SHARDS)), capacity_(args->uget(CAPACITY)),
      pipeline_depth_(args->uget(PIPELINE_DEPTH)), req_epoch_(0),
      need_fuo_scan_(false), device_(std::move(device)),
      num_handlers_(args_->uget(NUM_HANDLERS)) {
  ROMULUS_ASSERT(num_handlers_ > 0, "Num handlers must be at least 1");
  // At most, we only need a handler per shard or else there will be no work for
  // the remaining threads
  num_handlers_ = std::min(num_handlers_, num_shards_);
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
  std::vector<txn_t<int>> outliers;
  outliers.reserve(my_shards_.size());

  for (auto &curr : my_shards_) {
    auto wg_primary = WorkloadGenerator::generate<int>(
        config, shard_ranges_[curr].first, shard_ranges_[curr].second);

    proposals_.insert(proposals_.end(), wg_primary.begin(), wg_primary.end());

    auto next = (curr + 1) % num_shards_;
    auto wg_next = WorkloadGenerator::generate<int>(
        config, shard_ranges_[next].first, shard_ranges_[next].second);

    outliers.push_back(wg_next.back());
  }

  // Insert outlier at the end of workload
  proposals_.push_back(outliers.back()); // arbitrary at the moment

  want_perms_ = std::make_unique<std::atomic<bool>[]>(num_shards_);
  perm_acks_ = std::make_unique<std::atomic<bool>[]>(num_shards_);

  fuos_ = std::make_unique<std::atomic<uint64_t>[]>(num_shards_);
  for (uint64_t s = 0; s < num_shards_; ++s)
    fuos_[s].store(0, std::memory_order_relaxed);
}

MuSquared::~MuSquared() { Shutdown(); }

void MuSquared::SpawnThreads() {
  ROMULUS_DEBUG("Spawning {} permission handler threads...", num_handlers_);
  for (int p = 0; p < (int)num_handlers_; ++p) {
    perm_threads_.emplace_back(&MuSquared::PermHandler, this, p);
  }

  fd_thread_ = std::thread(&MuSquared::FailureDetector, this);
}

void MuSquared::Propose(uint64_t target_shard, txn_t<int> &txn,
                        uint32_t depth) {
  ROMULUS_ASSERT(depth < mu_squared::kMaxProposeDepth,
                 "Propose recursion bound exceeded.");
  // If we are able to succesfully write to a quorum of logs, then return true,
  // otherwise return false and we need to acquire the lease
  if (!FastCommit(target_shard, txn)) {
    ROMULUS_DEBUG(
        "No existing permissions for shard {}. Entering lease acquisition "
        "path...",
        target_shard);
    // Trigger the want perm flag on the shard of interest
    want_perms_[target_shard].store(true, std::memory_order_release);
    // Block until one of the PermHandlers flips back to false
    while (want_perms_[target_shard].load(std::memory_order_acquire))
      _mm_pause();
    // Check the result of the operation. If unsuccessful then backoff and try
    // again
    if (!perm_acks_[target_shard].load(std::memory_order_relaxed))
      RandomBackoff(1, mu_squared::kMaxStartingBackoff);
    Propose(target_shard, txn, depth + 1);
  }
}

// Writes to peer's perm_req regions to signify our request to the rest of the
// system Then, we block until we receive a quorum of grant messages in return
bool MuSquared::AcquirePermissions(
    uint64_t shard_id, std::vector<std::pair<uint64_t, PermCtx>> &owned) {
  // Incremement the permissions sequence counter on every round of permission
  // requests
  perm_req_t req{req_epoch_.fetch_add(1, std::memory_order_relaxed) + 1};
  uint64_t slot = id_ * num_shards_ + shard_id;

  auto laddr = memblock_.GetAddrInfo(mu_squared::kPermRequesterScratchRegionId);
  laddr.length = mu_squared::kSlotSize;
  laddr.offset = mu_squared::kSlotSize * shard_id;
  *reinterpret_cast<perm_req_t *>(laddr.addr + laddr.offset) = req;
  // Populate before with snapshot before posting
  auto grant_addr = memblock_.GetAddrInfo(mu_squared::kPermGrantRegionId);
  auto *grants = reinterpret_cast<volatile uint64_t *>(grant_addr.addr +
                                                       grant_addr.offset);
  std::vector<uint64_t> before(system_size_);
  for (uint32_t n = 0; n < system_size_; ++n)
    before[n] = grants[n * num_shards_ + shard_id];

  int posted = 0;
  for (uint32_t n = 0; n < system_size_; ++n) {
    auto raddr = remote_addrs_[n][mu_squared::kPermReqRegionId];
    raddr.addr_info.length = mu_squared::kSlotSize;
    raddr.addr_info.offset = slot * mu_squared::kSlotSize;
    auto conn = perm_handler_ctx_.conns_mat_[n][SelectHandler(shard_id)];

    bool ok = conn->Write(laddr, raddr, wr_id_t(id_, shard_id, n).raw);
    bool polled = ok && conn->ProcessCompletions(1) == 1;
    if (!polled) {
      ROMULUS_DEBUG("[Requester] request to node {} failed", n);
      continue;
    }
    ++posted;
  }

  if (static_cast<uint32_t>(posted) < quorum_) {
    ROMULUS_INFO("PermReq: only {}/{} requests delivered for shard {}", posted,
                 quorum_, shard_id);
    return false;
  }

  ROMULUS_DEBUG("[ReqPerm] Successfully posted/polled {} perm requests",
                posted);

  // Block until quorum of grants
  std::vector<bool> acked(system_size_, false);
  uint32_t acks = 0;
  auto timeout = std::chrono::steady_clock::now() +
                 std::chrono::milliseconds(mu_squared::kPermTimeout_ms);

  while (acks < quorum_) {
    if (std::chrono::steady_clock::now() > timeout) {
      ROMULUS_INFO("PermReq timed out: shard {} got {}/{}", shard_id, acks,
                   quorum_);
      return false;
    }

    // This MUST be here in order to prevent deadlock and continue serving
    // requests from peers while we block on our own requests
    for (auto &[s, ctx2] : owned)
      HandleRequests(s, ctx2);

    for (uint32_t n = 0; n < system_size_; ++n) {
      if (acked[n])
        continue;
      uint64_t raw = grants[n * num_shards_ + shard_id];
      // No change, continue scanning
      if (raw == before[n])
        continue;
      // grant detected, see if it is from old leader
      perm_grant_t grant(raw);
      ROMULUS_DEBUG("[ReqPerm] grant from {}: raw={:#x} owner={} cycled={} "
                    "epoch={} fuo={}",
                    n, grant.raw_, grant.IsPrevOwner(), grant.Cycled(),
                    grant.Epoch(), grant.FUO());

      if (grant.IsPrevOwner())
        fuos_[shard_id] = grant.FUO();

      if (grant.Cycled() ||
          replication_ctx_.conns_mat_[n][shard_id]->InErrorState())
        replication_ctx_.conns_mat_[n][shard_id]->Reconnect(
            mu_squared::kFullPermission);

      acked[n] = true;
      ++acks;
    }
  }

  return true;
}

bool MuSquared::FastCommit(uint64_t shard_id, txn_t<int> &txn) {
  // Check if a repair is needed for this column's QP's
  if (want_perms_[shard_id].load(std::memory_order_acquire))
    return false;

  ROMULUS_DEBUG("Entering the fast path...");
  // assuming one kv pair
  int val = txn.values.front();
  State commit_val;
  commit_val.SetValue(Value(static_cast<uint32_t>(val)));

  if (fuos_[shard_id] >= capacity_) {
    ROMULUS_DEBUG("Hit capacity for shard {}. Resetting...", shard_id);
    Reset(shard_id);
  }

  auto &laddr = replication_ctx_.laddr_;
  laddr.length = mu_squared::kSlotSize;
  laddr.offset = mu_squared::kSlotSize * id_;
  // Load the staging buffer
  *reinterpret_cast<State *>(laddr.addr + laddr.offset) = commit_val;

  // Perform quorum WRITE to the appropriate log
  std::vector<bool> done(system_size_, false);
  uint64_t done_count = 0;
  while (done_count < quorum_) {
    // Post writes
    int posted = 0;
    bool post_failed = false;
    for (uint32_t n = 0; n < system_size_; ++n) {
      // Skip if already counted or is dead
      if (done[n] || replication_ctx_.conns_mat_[n][shard_id]->InErrorState())
        continue;

      auto &conn = replication_ctx_.conns_mat_[n][shard_id];
      auto &raddr = replication_ctx_.raddrs_mat_[n][shard_id];

      raddr.addr_info.length = mu_squared::kSlotSize;
      raddr.addr_info.offset = fuos_[shard_id] * mu_squared::kSlotSize;

      if (!conn->Write(laddr, raddr, wr_id_t(n, shard_id, 0).raw)) {
        ROMULUS_DEBUG("[FAST PATH] post failed, shard {} node {}", shard_id, n);
        post_failed = true;
        break;
      }
      ++posted;
    }

    if (posted == 0)
      return false;

    // shared cq
    auto &conn = replication_ctx_.conns_mat_.front()[shard_id];
    auto conn_raw = conn->GetCQ(); // shared cq

    std::vector<ibv_wc> wc(posted);

    int total = 0;
    while (total < posted) {
      int n = ibv_poll_cq(conn_raw, posted - total, wc.data() + total);
      if (n < 0)
        ROMULUS_FATAL("Fast path write: Error in polling");
      total += n;
    }
    bool needs_reconnect = false;
    for (int i = 0; i < total; ++i) {
      wr_id_t wr_id = wc[i].wr_id;
      if (wc[i].status == IBV_WC_SUCCESS) {
        done[wr_id.GetID()] = true;
        ++done_count;
        continue;
      }
      ROMULUS_DEBUG("[FAST PATH] shard {} node {}: {}", shard_id, wr_id.GetID(),
                    ibv_wc_status_str(wc[i].status));
      replication_ctx_.conns_mat_[wr_id.GetID()][shard_id]->SetErrorState(true);
      needs_reconnect = true;
    }
    // Reached a QP in error state
    if (needs_reconnect)
      want_perms_[shard_id].store(true, std::memory_order_release);
    // Successfully committed
    if (done_count >= quorum_)
      break;
    // There is no possibility for progress, abort
    if (needs_reconnect || post_failed)
      return false;
  }
  fuos_[shard_id]++;
  ROMULUS_DEBUG("Fast commit complete for shard {}. FUO now at {}", shard_id,
                fuos_[shard_id].load());
  return true;
}

// Scan perm req region for valid incoming requests, execute the permission
// change, and ack on completion
void MuSquared::HandleRequests(uint64_t shard_id, PermCtx &ctx) {
  for (uint32_t n = 0; n < system_size_; ++n) {
    uint64_t raw = ctx.req_raw[n * num_shards_ + shard_id];
    if (raw == mu_squared::kPermNull || raw == ctx.last_req[n])
      continue;

    ROMULUS_DEBUG("[HandleRequests] request from node {} for shard {}", n,
                  shard_id);
    uint64_t owner_id = ctx.current_owner;

    // revoke from the old owner
    if (owner_id != mu_squared::kNoOwner && owner_id != n) {
      auto *old_conn = replication_ctx_.conns_mat_[owner_id][shard_id];
      if (old_conn->ApplyPermissions(mu_squared::kNoPermission) ==
          romulus::ReliableConnection::PermResult::Failed) {
        ROMULUS_DEBUG("[PermHandler] revoke from node {} shard {} failed",
                      owner_id, shard_id);
        // must abort
        continue;
      }
    }
    // grant to the requester
    auto *new_conn = replication_ctx_.conns_mat_[n][shard_id];
    romulus::ReliableConnection::PermResult r =
        new_conn->ApplyPermissions(mu_squared::kFullPermission);
    if (r == romulus::ReliableConnection::PermResult::Failed) {
      ROMULUS_DEBUG("[PermHandler] grant to node {} shard {} failed", n,
                    shard_id);
      continue;
    }
    // construct the grant
    perm_grant_t grant_msg;
    grant_msg.SetEpoch(ctx.grant_epoch++);
    grant_msg.SetCycled(r == romulus::ReliableConnection::PermResult::Cycled);
    if (owner_id == id_) {
      grant_msg.SetOwner(true);
      grant_msg.SetFUO(fuos_[shard_id]);
    } else {
      grant_msg.SetOwner(false);
    }

    // load staging buffer
    *reinterpret_cast<uint64_t *>(ctx.laddr.addr + ctx.laddr.offset) =
        grant_msg.raw_;

    // RDMA-write the ack
    auto *ack_conn = perm_handler_ctx_.conns_mat_[n][SelectHandler(shard_id)];
    auto grant_raddr = remote_addrs_[n][mu_squared::kPermGrantRegionId];
    grant_raddr.addr_info.length = mu_squared::kSlotSize;
    grant_raddr.addr_info.offset =
        (id_ * num_shards_ + shard_id) * mu_squared::kSlotSize;

    if (!ack_conn->Write(ctx.laddr, grant_raddr,
                         wr_id_t(id_, shard_id, n).raw) ||
        ack_conn->ProcessCompletions(1) != 1) {
      ROMULUS_DEBUG("[PermHandler] ack to node {} shard {} failed", n,
                    shard_id);
      continue;
    }

    ctx.current_owner = n;
    ctx.last_req[n] = raw;
  }
}

void MuSquared::PermHandler(uint64_t tid) {
  uint16_t core = 1 + tid;
  ROMULUS_DEBUG("[Permission Handler] Pinning to core {}...", core);
  pin_thread_to_core(core);

  auto raddr = memblock_.GetAddrInfo(mu_squared::kPermReqRegionId);

  std::vector<std::pair<uint64_t, PermCtx>> owned;
  for (uint64_t s = 0; s < num_shards_; ++s) {
    if (SelectHandler(s) != tid)
      continue;
    PermCtx ctx;
    ctx.last_req.assign(system_size_, mu_squared::kPermNull);
    ctx.current_owner = s % system_size_;
    ctx.laddr = memblock_.GetAddrInfo(mu_squared::kPermHandlerScratchRegionId);
    ctx.laddr.offset = mu_squared::kSlotSize * s;
    ctx.laddr.length = mu_squared::kSlotSize;
    ctx.req_raw =
        reinterpret_cast<volatile uint64_t *>(raddr.addr + raddr.offset);
    owned.emplace_back(s, std::move(ctx));
  }

  while (perm_handler_running_.load(std::memory_order_acquire)) {
    for (auto &[s, ctx] : owned)
      HandleRequests(s, ctx);

    for (auto &[s, ctx] : owned) {
      if (!want_perms_[s].load(std::memory_order_acquire))
        continue;
      perm_acks_[s].store(AcquirePermissions(s, owned),
                          std::memory_order_relaxed);
      want_perms_[s].store(false, std::memory_order_release);
    }
    _mm_pause();
  }
}

uint64_t MuSquared::Acquire_FUO() { return 0; }

void MuSquared::FailureDetector() {
  uint16_t core = 1 + num_handlers_;
  ROMULUS_INFO("[Failure Detector] Pinning to core {}...", core);

  pin_thread_to_core(core);

  // while (failure_detector_running_.load(std::memory_order_acquire)) {
  // }
}

std::string MuSquared::GenLogID(uint64_t shard_id) {
  return mu_squared::kLogRegionId + "_" + std::to_string(shard_id);
}

uint64_t MuSquared::SelectHandler(uint64_t s) const {
  return (s / system_size_) % num_handlers_;
}

uint64_t MuSquared::SelectShard(int key) {
  uint64_t id = static_cast<uint64_t>(key) / shard_size_;
  return std::min(id, num_shards_ - 1);
}

void MuSquared::Warmup() {
  const int num_warmup_iters = 1e4;

  auto laddr = memblock_.GetAddrInfo(mu_squared::kPermRequesterScratchRegionId);
  laddr.length = mu_squared::kSlotSize;

  for (int i = 0; i < num_warmup_iters; ++i) {

    for (int n = 0; n < (int)system_size_; ++n) {

      for (int offset = 0; offset < (int)num_shards_; ++offset) {
        laddr.offset = mu_squared::kSlotSize * offset;
        *reinterpret_cast<uint64_t *>(laddr.addr + laddr.offset) = 0;
        auto raddr = remote_addrs_[n][mu_squared::kPermReqRegionId];
        raddr.addr_info.length = mu_squared::kSlotSize;

        raddr.addr_info.offset = (id_ * num_shards_ + offset) * mu_squared::kSlotSize;
        auto conn = perm_handler_ctx_.conns_mat_[n][SelectHandler(offset)];
        ROMULUS_ASSERT(conn->Write(laddr, raddr, wr_id_t(0).raw),
                       "Failed to write in warmup");
        ROMULUS_ASSERT(conn->ProcessCompletions(1) == 1,
                       "Failed to poll in warmup");
      }
    }
  }
}

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

void MuSquared::Reset(uint64_t shard_id) {
  ResetLogs(shard_id);
  fuos_[shard_id] = 0;
}

void MuSquared::ResetLogs(uint64_t shard_id) {
  auto raddr = memblock_.GetAddrInfo(GenLogID(shard_id));
  std::memset((void *)(raddr.addr + raddr.offset), 0,
              capacity_ * mu_squared::kSlotSize);
}

std::vector<txn_t<int>> MuSquared::GetProposals() { return proposals_; }

void MuSquared::Cleanup() { ROMULUS_INFO("Cleaning up..."); }

void MuSquared::Shutdown() {
  ROMULUS_INFO("Shutting down...");
  failure_detector_running_.store(false, std::memory_order_release);
  perm_handler_running_.store(false, std::memory_order_release);

  for (auto &t : perm_threads_) {
    if (t.joinable()) {
      t.join();
    }
  }

  if (fd_thread_.joinable())
    fd_thread_.join();
}