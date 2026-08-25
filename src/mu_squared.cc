#include "mu_squared.h"

MuSquared::MuSquared(std::shared_ptr<romulus::ArgMap> args,
                     uint64_t system_size,
                     std::shared_ptr<romulus::Device> device)
    : args_(args), id_(args->uget(NODE_ID)), hostname_(args->sget(HOSTNAME)),
      system_size_(system_size), quorum_(system_size / 2 + 1),
      num_shards_(args->uget(NUM_SHARDS)), capacity_(args->uget(CAPACITY)),
      pipeline_depth_(args->uget(PIPELINE_DEPTH)), req_epoch_(0), fuo_(0),
      need_fuo_scan_(false), device_(std::move(device)),
      num_shared_cq_(args->uget(NUM_SHARED_CQ)) {
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
}

MuSquared::~MuSquared() { Shutdown(); }

void MuSquared::SpawnThreads() {
  for (int s = 0; s < (int)num_shards_; ++s) {
    perm_threads_.emplace_back(&MuSquared::PermHandler, this, s);
  }
  for (int n = 0; n < (int)system_size_; ++n) {
    fd_threads_.emplace_back(&MuSquared::FailureDetector, this, n);
  }
}

void MuSquared::Propose(txn_t<int> &txn, uint32_t depth) {
  ROMULUS_ASSERT(!txn.keys.empty(), "Transaction must have at least one key.");
  if (depth >= mu_squared::kMaxProposeDepth) {
    ROMULUS_FATAL("Propose recursion bound exceeded on key {}",
                  txn.keys.front());
  }

  uint64_t target_shard = SelectShard(txn.keys.front());
  // If we are able to succesfully write to a quorum of logs, then return true,
  // otherwise return false and we need to acquire the lease
  bool committed = FastCommit(target_shard, txn);
  if (!committed) {
    ROMULUS_DEBUG(
        "No existing permissions for shard {}. Entering lease acquisition "
        "path...",
        target_shard);

    if (RequestPermissions(target_shard)) {
      ROMULUS_DEBUG("Successfully acquired lease for shard {}", target_shard);
      // Remote side has been reset via reqpermissions, now we need to put local
      // side of qp in RTR
      for (uint32_t n = 0; n < system_size_; ++n)
        replication_ctx_.conns_mat_[n][target_shard]->Reconnect();

    } else {
      ROMULUS_DEBUG("Failed to acquire lease for shard {}", target_shard);
    }
    // At this point, we have conceptually acquired the lease for this shard id
    // Now try again, increasing depth which is really just the number of
    // attempts for this txn
    RandomBackoff(1, mu_squared::kMaxStartingBackoff);
    Propose(txn, depth + 1);
  }
  // Otherwise, we committed successfully and we move on
}

bool MuSquared::RequestPermissions(uint64_t shard_id) {
  // Incremement the permissions sequence counter on every round of permission
  // requests
  perm_req_t req{++req_epoch_};
  uint64_t slot = id_ * num_shards_ + shard_id;

  auto laddr = memblock_.GetAddrInfo(mu_squared::kPermRequesterScratchRegionId);
  laddr.length = mu_squared::kSlotSize;
  laddr.offset = 0;
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
    auto conn = perm_handler_ctx_.conns_[n];

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
    for (uint32_t n = 0; n < system_size_; ++n) {
      if (acked[n])
        continue;
      uint64_t raw = grants[n * num_shards_ + shard_id];
      // No change, continue scanning
      if (raw == before[n])
        continue;
      // grant detected, see if it is from old leader
      perm_grant_t grant(raw);
      ROMULUS_DEBUG(
          "[ReqPerm] grant from {}: raw={:#x} owner={} epoch={} fuo={}", n,
          grant.raw_, grant.IsPrevOwner(), grant.Epoch(), grant.FUO());
      // reset my fuo to reflect this
      if (grant.IsPrevOwner())
        fuo_ = grant.FUO();
      acked[n] = true;
      ++acks;
    }

    if (std::chrono::steady_clock::now() > timeout) {
      ROMULUS_INFO("PermReq timed out: shard {} got {}/{}", shard_id, acks,
                   quorum_);
      return false;
    }
  }
  return true;
}

bool MuSquared::FastCommit(uint64_t shard_id, txn_t<int> &txn) {
  ROMULUS_DEBUG("Entering the fast path...");
  // assuming one kv pair
  int val = txn.values.front();
  State commit_val;
  commit_val.SetValue(Value(static_cast<uint32_t>(val)));

  if (fuo_ >= capacity_) {
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
    for (uint32_t n = 0; n < system_size_; ++n) {
      if (done[n])
        continue;

      auto &conn = replication_ctx_.conns_mat_[n][shard_id];
      auto &raddr = replication_ctx_.raddrs_mat_[n][shard_id];

      raddr.addr_info.length = mu_squared::kSlotSize;
      raddr.addr_info.offset = fuo_ * mu_squared::kSlotSize;

      bool committed = conn->Write(laddr, raddr, wr_id_t(n, shard_id, 0).raw);
      if (!committed) {
        ROMULUS_DEBUG(
            "[FAST PATH] Failed to commit value at shard id {} on node {}",
            shard_id, n);
        return false;
      }
      ++posted;
    }

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
      needs_reconnect = true;
    }

    if (needs_reconnect) {
      for (uint32_t n = 0; n < system_size_; ++n)
        replication_ctx_.conns_mat_[n][shard_id]->Reconnect();
      return false;
    }
  }
  fuo_++;
  ROMULUS_DEBUG("Fast commit complete for shard {}. FUO now at {}", shard_id,
                fuo_);
  return true;
}

void MuSquared::PermHandler(uint64_t shard_id) {
  auto laddr = memblock_.GetAddrInfo(mu_squared::kPermHandlerScratchRegionId);
  laddr.offset = mu_squared::kSlotSize * shard_id;
  laddr.length = mu_squared::kSlotSize;

  auto raddr = memblock_.GetAddrInfo(mu_squared::kPermReqRegionId);
  auto *raddr_raw =
      reinterpret_cast<volatile uint64_t *>(raddr.addr + raddr.offset);

  std::vector<uint64_t> last_req(system_size_, mu_squared::kPermNull);
  uint64_t current_owner = mu_squared::kNoOwner;

  uint64_t grant_epoch = 0;

  while (perm_handler_running_.load(std::memory_order_acquire)) {

    for (int n = 0; n < (int)system_size_; ++n) {
      // node-major layout
      uint64_t raw = raddr_raw[n * num_shards_ + shard_id];

      if (raw == mu_squared::kPermNull || raw == last_req[n])
        continue;

      // at this point we can assume that the request is valid, that is
      // not null and strictly greater than the previous epoch
      // first we revoke the old permissions assuming a legit request
      ROMULUS_DEBUG("[PermHandler] Received request from node {} for shard {}",
                    n, shard_id);
      uint64_t owner_id = current_owner;

      if (owner_id != mu_squared::kNoOwner && (int)owner_id != n) {
        auto *old_conn = replication_ctx_.conns_mat_[owner_id][shard_id];
        if (!old_conn->ChangePermissions(LOCAL_READ | LOCAL_WRITE)) {
          // must abort for correctness
          ROMULUS_DEBUG("[PermHandler] revoke from node {} shard {} failed",
                        owner_id, shard_id);
          continue;
        }

      } else {
        // Either old owner is null or the incoming request is for an already
        // existing permissions in place.
        ROMULUS_DEBUG("Skipping revokation of old permissions... ");
      }

      // then we grant permissions to the new requester
      auto *new_conn = replication_ctx_.conns_mat_[n][shard_id];
      ROMULUS_ASSERT(
          new_conn->ChangePermissions(LOCAL_READ | LOCAL_WRITE | REMOTE_READ |
                                      REMOTE_WRITE | REMOTE_ATOMIC),
          "[PERM HANDLER] Failed to grant new permissions for shard {}",
          shard_id);
      ROMULUS_DEBUG("[PERM HANDLER] Granted permission to node {} for shard {}",
                    n, shard_id);

      // Ack to indicate successful completion of request
      auto *ack_conn = perm_handler_ctx_.conns_[n];
      perm_grant_t grant_msg;
      grant_msg.SetEpoch(grant_epoch++);

      // Note: embed fuo in grant message & load staging
      if (owner_id != mu_squared::kNoOwner && owner_id == id_) {
        grant_msg.SetOwner(true);
        grant_msg.SetFUO(fuo_);
      } else {
        // If we are not the old leader, embed seq number for liveness
        grant_msg.SetOwner(false);
      }
      // load in the staging buffer
      *reinterpret_cast<uint64_t *>(laddr.addr + laddr.offset) = grant_msg.raw_;

      auto grant_raddr = remote_addrs_[n][mu_squared::kPermGrantRegionId];
      grant_raddr.addr_info.length = mu_squared::kSlotSize;
      grant_raddr.addr_info.offset =
          (id_ * num_shards_ + shard_id) * mu_squared::kSlotSize;

      if (!ack_conn->Write(laddr, grant_raddr, wr_id_t(id_, shard_id, n).raw)) {
        ROMULUS_DEBUG("[PermHandler] ack post to node {} shard {} failed", n,
                      shard_id);
        continue;
      }
      if (ack_conn->ProcessCompletions(1) != 1) {
        ROMULUS_DEBUG("[PermHandler] ack completion to node {} shard {} failed",
                      n, shard_id);
        continue;
      }
      current_owner = n;
      last_req[n] = raw;
    }
    _mm_pause();
  }
}

uint64_t MuSquared::Acquire_FUO() { return 0; }

void MuSquared::FailureDetector(uint64_t target_node) {
  while (failure_detector_running_.load(std::memory_order_acquire)) {
  }
}

std::string MuSquared::GenLogID(uint64_t shard_id) {
  return mu_squared::kLogRegionId + "_" + std::to_string(shard_id);
}

uint64_t MuSquared::SelectShard(int key) {
  uint64_t id = static_cast<uint64_t>(key) / shard_size_;
  return std::min(id, num_shards_ - 1);
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

void MuSquared::Reset(uint64_t shard_id) {
  ResetLogs(shard_id);
  fuo_ = 0;
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

  for (auto &t : fd_threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
}