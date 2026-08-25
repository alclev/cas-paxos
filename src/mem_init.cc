#include "mu_squared.h"

void MuSquared::Init(std::string_view dev_name, int dev_port,
                     std::unique_ptr<romulus::ConnectionRegistry> registry,
                     std::unordered_map<uint64_t, std::string> mach_map) {
  // Set up remotely accessible memory.
  ROMULUS_DEBUG("Initializing CAS-based Paxos");
  ROMULUS_DEBUG("Quorum size: {}", quorum_);
  ROMULUS_DEBUG("Opening device with name {} on port {}", dev_name, dev_port);
  ROMULUS_ASSERT(device_->Open(dev_name, dev_port), "Failed to open device.");
  device_->AllocatePd(mu_squared::kPdId);

  ROMULUS_INFO("Registering remotely accessible memory");

  uint64_t scratch_len = system_size_;
  uint64_t proposal_len = num_shards_;
  uint64_t fd_local_len = (system_size_ - 1);
  uint64_t fd_remote_len = (system_size_ - 1);
  uint64_t perm_handler_scratch_len = num_shards_; // just one slot
  uint64_t perm_requester_scratch_len = 1; // just one slot
  uint64_t perm_req_len = system_size_ * num_shards_;
  uint64_t perm_grant_len = system_size_ * num_shards_;

  // capacity is per-shard
  uint64_t log_len = capacity_ * num_shards_;

  // NB: the following configuration assumes STANDALONE
  std::size_t remote_len = scratch_len + proposal_len + log_len + fd_local_len +
                           fd_remote_len + perm_handler_scratch_len + perm_requester_scratch_len + perm_req_len +
                           perm_grant_len;

  raw_ =
      new romulus::APArray<State, mu_squared::kSlotSize, CACHE_PREFETCH_SIZE>(
          remote_len);
  std::memset(raw_->Get(), 0, raw_->GetTotalBytes());

  // ============================================================================
  //   Memory Layout
  //   n = system_size   s = num_shards_   c = capacity   k = kSlotSize
  // ============================================================================
  //
  //   +-------------+
  //   |   scratch   |  : n * k
  //   +-------------+
  //   |  proposed   |  : s * k
  //   +-------------+
  //   |    log_0    |  : c * k
  //   +-------------+
  //   |    log_1    |  : c * k
  //   +-------------+
  //   |      :      |
  //   +-------------+
  //   |  log_{s-1}  |  : c * k
  //   +-------------+
  //   |    FD_l     |  : (n-1) * k      local FD region
  //   +-------------+
  //   |    FD_r     |  : (n-1) * k      remote FD region
  //   +-------------+
  //   |  p_scratch  |  : k * s
  //   +-------------+
  //   |  perm_req   |  : n * s * k  inbound requests, polled locally
  //   +-------------+
  //   | perm_grant  |  : n * s * k  inbound acks, polled locally
  //   +-------------+
  //
  // ============================================================================

  // Set up local view of log memory
  uint64_t off = 0;
  scratch_ = romulus::APArraySlice(raw_, off, off + scratch_len);
  off += scratch_len;
  proposed_state_ = romulus::APArraySlice(raw_, off, off + proposal_len);
  off += proposal_len;
  log_ = romulus::APArraySlice(raw_, off, off + log_len);

  // Constructing the memblock
  auto pd = device_->GetPd(mu_squared::kPdId);
  memblock_ = romulus::MemBlock(mu_squared::kBlockId, pd,
                                reinterpret_cast<uint8_t *>(raw_->Get()),
                                remote_len * mu_squared::kSlotSize);

  // Registering memblock regions
  uint64_t current_offset = 0;
  memblock_.RegisterMemRegion(mu_squared::kScratchRegionId, current_offset,
                              scratch_len * mu_squared::kSlotSize);
  current_offset += scratch_len * mu_squared::kSlotSize;
  memblock_.RegisterMemRegion(mu_squared::kProposedRegionId, current_offset,
                              proposal_len * mu_squared::kSlotSize);
  current_offset += proposal_len * mu_squared::kSlotSize;
  for (int i = 0; i < (int)num_shards_; ++i) {
    memblock_.RegisterMemRegion(GenLogID(i), current_offset,
                                capacity_ * mu_squared::kSlotSize);
    current_offset += capacity_ * mu_squared::kSlotSize;
  }
  memblock_.RegisterMemRegion(mu_squared::kFDLocalRegionId, current_offset,
                              fd_local_len * mu_squared::kSlotSize);
  current_offset += fd_local_len * mu_squared::kSlotSize;
  memblock_.RegisterMemRegion(mu_squared::kFDRemoteRegionId, current_offset,
                              fd_remote_len * mu_squared::kSlotSize);
  current_offset += fd_remote_len * mu_squared::kSlotSize;
    memblock_.RegisterMemRegion(mu_squared::kPermHandlerScratchRegionId,
                              current_offset,
                              perm_handler_scratch_len * mu_squared::kSlotSize);
  current_offset += perm_handler_scratch_len * mu_squared::kSlotSize;
  memblock_.RegisterMemRegion(mu_squared::kPermRequesterScratchRegionId,
                              current_offset,
                              perm_requester_scratch_len * mu_squared::kSlotSize);
  current_offset += perm_requester_scratch_len * mu_squared::kSlotSize;
  memblock_.RegisterMemRegion(mu_squared::kPermReqRegionId, current_offset,
                              perm_req_len * mu_squared::kSlotSize);
  current_offset += perm_req_len * mu_squared::kSlotSize;
  memblock_.RegisterMemRegion(mu_squared::kPermGrantRegionId, current_offset,
                              perm_grant_len * mu_squared::kSlotSize);
  current_offset += perm_grant_len * mu_squared::kSlotSize;

  // Define number of QP's **for this version**
  // (n * s) + (2 * n) - 1
  num_qps_ = num_shards_ +
             3; // 1 primary, s replication, 1 FD per peer, 1 perm per peer
  num_shared_cq_ = num_shards_ + 2; // 1 primary, s replication have shared cq
  // Shared cq mapping
  // QP 0: Primary consensus logic -- shared CQ -- idx 0
  // QP 1..s: Replication logic    -- shared CQ -- idx 1..s
  // QP s+1: perm ack              -- shared CQ -- idx s+1
  // QP s+2: FD                -- NOT shared CQ -- idx s+2

  // Register memory and connect to other nodes
  registry_ = std::move(registry);
  conn_manager_ = std::make_unique<romulus::ConnectionManager>(
      hostname_, registry_.get(), id_, system_size_, num_qps_, num_shared_cq_);

  // Barrier
  conn_manager_->arrive_strict_barrier();

  ROMULUS_DEBUG("Attemping to register memory...");
  bool register_ok = conn_manager_->Register(*device_, memblock_);

  // Barrier
  conn_manager_->arrive_strict_barrier();

  ROMULUS_DEBUG("Attemping to connect to remote peers...");
  bool connect_ok = conn_manager_->Connect(memblock_);

  // Barrier
  conn_manager_->arrive_strict_barrier();

  ROMULUS_ASSERT(register_ok && connect_ok,
                 "Failed to register or connect log memory");
  // At this point, we need to cache the connections and addresses
  romulus::RemoteAddr remote_addr;
  std::vector<std::string> regions;
  regions.insert(regions.end(),
                 {mu_squared::kScratchRegionId, mu_squared::kProposedRegionId,
                  mu_squared::kFDLocalRegionId, mu_squared::kFDRemoteRegionId,
                  mu_squared::kPermHandlerScratchRegionId,
                  mu_squared::kPermRequesterScratchRegionId,
                  mu_squared::kPermReqRegionId,
                  mu_squared::kPermGrantRegionId});
  for (int i = 0; i < (int)num_shards_; ++i) {
    regions.push_back(mu_squared::kLogRegionId + "_" + std::to_string(i));
  }

  for (auto &m : mach_map) {
    // <region_id, remote_addr>
    std::unordered_map<std::string, romulus::RemoteAddr> tmp_addrs;
    // We need to account for all the remotely visible regions
    for (auto &r : regions) {
      // If the machine id maps to **this** node, then this will represent the
      // loopback addr
      conn_manager_->GetRemoteAddr(m.first, mu_squared::kBlockId, r,
                                   &remote_addr);
      tmp_addrs.emplace(r, remote_addr);
    }
    // <machine_id, map<region, remote_addr>>
    remote_addrs_.emplace(m.first, tmp_addrs);
    // Note that having available multiple QP's does not do much unless
    // there is concurrent access to them
    std::vector<romulus::ReliableConnection *> conns;
    // here, the 0th index **is the loopback**
    if (m.second == hostname_) {
      uint64_t num_loopback = num_qps_;
      for (int q = 0; q < (int)num_loopback; ++q) {
        uint64_t loopback_id = (q == 0) ? 0 : kLoopback - q;
        auto conn = conn_manager_->GetConnection(m.first, loopback_id);
        conns.push_back(conn);
#ifdef MEMDUMP
        ROMULUS_DEBUG("Loopback #{} qp: {} cq: {}", q,
                      reinterpret_cast<uintptr_t>(conn->GetQP()),
                      reinterpret_cast<uintptr_t>(conn->GetCQ()));
#endif
      }
    } else {
      for (int q = 1; q < (int)num_qps_ + 1; ++q) {
        auto conn = conn_manager_->GetConnection(m.first, q);
        conns.push_back(conn);
        // ROMULUS_DEBUG("Remote conn (node, qp) = ({}, {}) qp: {} cq: {}",
        //               m.first, q, reinterpret_cast<uintptr_t>(conn->GetQP()),
        //               reinterpret_cast<uintptr_t>(conn->GetCQ()));
      }
    }
    remote_conns_.emplace(m.first, conns);
  }
  // Initialize the perm_req and perm_grant regions to have kPermNull in
  // every slot
  auto perm_laddr = memblock_.GetAddrInfo(mu_squared::kPermHandlerScratchRegionId);
  perm_laddr.length = mu_squared::kSlotSize;
  perm_laddr.offset = 0;
  // load the staging buffer
  *reinterpret_cast<uint64_t *>(perm_laddr.addr + perm_laddr.offset) =
      mu_squared::kPermNull;

  for (int target = 0; target < (int)system_size_; ++target) {
    // Target node address & connection information
    auto *conn = remote_conns_[target].front();
    auto req_base = remote_addrs_[target][mu_squared::kPermReqRegionId];
    auto grant_base = remote_addrs_[target][mu_squared::kPermGrantRegionId];
    req_base.addr_info.length = mu_squared::kSlotSize;
    grant_base.addr_info.length = mu_squared::kSlotSize;

    // Target nnode's request and grant matrices
    for (int n = 0; n < (int)system_size_; ++n) {
      for (int s = 0; s < (int)num_shards_; ++s) {
        int slot = (n * num_shards_) + s;
        auto req_raddr = req_base, grant_raddr = grant_base;
        req_raddr.addr_info.offset = slot * mu_squared::kSlotSize;
        grant_raddr.addr_info.offset = slot * mu_squared::kSlotSize;

        // Write and poll for perm reg
        ROMULUS_ASSERT(conn->Write(perm_laddr, req_raddr, 0),
                       "Error intializing req_raddr region");
        ROMULUS_ASSERT(conn->ProcessCompletions(1) == 1,
                       "Error polling req_raddr region");
        // Write and poll for grant reg
        ROMULUS_ASSERT(conn->Write(perm_laddr, grant_raddr, 0),
                       "Error polling grant_raddr region");
        ROMULUS_ASSERT(conn->ProcessCompletions(1) == 1,
                       "Error polling grant_raddr region");
      }
    }
  }
  ROMULUS_DEBUG(
      "Succesfully intialized all slots in perm_req and perm_grant with "
      "sentinal value: {}",
      mu_squared::kPermNull);

  // Initialize all RDMA contexts
  cons_ctx_.conns_.resize(system_size_);
  cons_ctx_.proposed_.resize(system_size_);
  cons_ctx_.log_mat_.resize(system_size_,
                            std::vector<romulus::RemoteAddr>(num_shards_));

  replication_ctx_.conns_mat_.resize(
      system_size_, std::vector<romulus::ReliableConnection *>(num_shards_));
  replication_ctx_.raddrs_mat_.resize(
      system_size_, std::vector<romulus::RemoteAddr>(num_shards_));

  perm_handler_ctx_.conns_.resize(system_size_);
  perm_handler_ctx_.req_raddrs_.resize(system_size_);
  perm_handler_ctx_.grant_raddrs_.resize(system_size_);

  fd_ctx_.conns_.resize(system_size_, nullptr);
  fd_ctx_.raddrs_.resize(system_size_);

  // Can share scratch, both context are on same thread
  cons_ctx_.laddr_ = memblock_.GetAddrInfo(mu_squared::kScratchRegionId);
  replication_ctx_.laddr_ = memblock_.GetAddrInfo(mu_squared::kScratchRegionId);
  perm_handler_ctx_.laddr_ =
      memblock_.GetAddrInfo(mu_squared::kPermHandlerScratchRegionId);
  fd_ctx_.laddr_ = memblock_.GetAddrInfo(mu_squared::kFDLocalRegionId);

  for (int s = 0; s < (int)num_shards_; ++s) {

    for (int n = 0; n < (int)system_size_; ++n) {
      // row, col = node, shard
      auto conn = remote_conns_[n][1 + s];
#ifdef MEMDUMP
      ROMULUS_DEBUG("Conn mat ({},{}) : QP: {} CQ: {}", n, s,
                    reinterpret_cast<uintptr_t>(conn->GetQP()),
                    reinterpret_cast<uintptr_t>(conn->GetCQ()));
#endif
      replication_ctx_.conns_mat_[n][s] = conn;
      replication_ctx_.raddrs_mat_[n][s] = remote_addrs_[n][GenLogID(s)];
      cons_ctx_.log_mat_[n][s] = remote_addrs_[n][GenLogID(s)];
    }
  }

  for (int n = 0; n < (int)system_size_; ++n) {
    // primary
    auto &conn = remote_conns_[n][0];
#ifdef MEMDUMP
    ROMULUS_DEBUG("Cons (node {}) : QP: {} CQ: {}", n,
                  reinterpret_cast<uintptr_t>(conn->GetQP()),
                  reinterpret_cast<uintptr_t>(conn->GetCQ()));
#endif
    cons_ctx_.conns_[n] = conn;
    cons_ctx_.proposed_[n] = remote_addrs_[n][mu_squared::kProposedRegionId];

    // perm handler
    conn = remote_conns_[n][1 + num_shards_];
#ifdef MEMDUMP
    ROMULUS_DEBUG("PermHandler (node {}) : QP: {} CQ: {}", n,
                  reinterpret_cast<uintptr_t>(conn->GetQP()),
                  reinterpret_cast<uintptr_t>(conn->GetCQ()));
#endif
    perm_handler_ctx_.conns_[n] = conn;
    perm_handler_ctx_.req_raddrs_[n] =
        remote_addrs_[n][mu_squared::kPermReqRegionId];
    perm_handler_ctx_.grant_raddrs_[n] =
        remote_addrs_[n][mu_squared::kPermGrantRegionId];

    // failure detector
    if (n == (int)id_)
      continue;
    conn = remote_conns_[n][2 + num_shards_];
#ifdef MEMDUMP
    ROMULUS_DEBUG("FailureDetector (node {}) : QP: {} CQ: {}", n,
                  reinterpret_cast<uintptr_t>(conn->GetQP()),
                  reinterpret_cast<uintptr_t>(conn->GetCQ()));
#endif
    fd_ctx_.conns_[n] = conn;
    fd_ctx_.raddrs_[n] = remote_addrs_[n][mu_squared::kFDRemoteRegionId];
  }

  // QP mappings
  // n = system_size   s = num_shards_   c = capacity   k = kSlotSize
  //
  // --- QP counts ---
  // Primary concensus: n
  // Replication      : n * s
  // Perm Acks        : n
  // FD               : n - 1
  //
  // -----------------------------

  // Initialize permissions for replication QP's with only local rw access
  for (int n = 0; n < (int)system_size_; ++n) {
    for (int s = 0; s < (int)num_shards_; ++s) {
      auto &c = replication_ctx_.conns_mat_[n][s];
      c->ChangePermissions(LOCAL_READ | LOCAL_WRITE);
    }
  }

  // Finally, barrier
  conn_manager_->arrive_strict_barrier();
}