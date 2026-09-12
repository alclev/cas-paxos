#ifdef MU_SQUARED
#include "mu_squared.h"
#else
#include "velos_squared.h"
#endif

#ifdef MU_SQUARED
void MuSquared::Init(std::string_view dev_name, int dev_port,
                     std::unique_ptr<romulus::ConnectionRegistry> registry,
                     std::unordered_map<uint64_t, std::string> mach_map) {
  // Set up remotely accessible memory.
  ROMULUS_DEBUG("Initializing memory for Mu^2");
  ROMULUS_DEBUG("Quorum size: {}", quorum_);
  ROMULUS_DEBUG("Opening device with name {} on port {}", dev_name, dev_port);
  ROMULUS_ASSERT(device_->Open(dev_name, dev_port), "Failed to open device.");
  device_->AllocatePd(mu_squared::kPdId);

  ROMULUS_INFO("Registering remotely accessible memory");

  uint64_t scratch_len = system_size_ * pipeline_depth_;
  uint64_t proposal_len = num_shards_;
  uint64_t fd_local_len = (system_size_ - 1);
  uint64_t fd_remote_len = (system_size_ - 1);
  uint64_t perm_handler_scratch_len = num_shards_;   // one slot per shard
  uint64_t perm_requester_scratch_len = num_shards_; // one slot per shard
  uint64_t perm_req_len = system_size_ * num_shards_;
  uint64_t perm_grant_len = system_size_ * num_shards_;

  // capacity is per-shard
  uint64_t log_len = capacity_ * num_shards_;

  // NB: the following configuration assumes STANDALONE
  std::size_t remote_len = scratch_len + proposal_len + log_len + fd_local_len +
                           fd_remote_len + perm_handler_scratch_len +
                           perm_requester_scratch_len + perm_req_len +
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
  //   |  r_scratch  |  : k * s
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
  memblock_.RegisterMemRegion(
    mu_squared::kPermRequesterScratchRegionId, current_offset,
    perm_requester_scratch_len * mu_squared::kSlotSize);
  current_offset += perm_requester_scratch_len * mu_squared::kSlotSize;
  memblock_.RegisterMemRegion(mu_squared::kPermReqRegionId, current_offset,
                              perm_req_len * mu_squared::kSlotSize);
  current_offset += perm_req_len * mu_squared::kSlotSize;
  memblock_.RegisterMemRegion(mu_squared::kPermGrantRegionId, current_offset,
                              perm_grant_len * mu_squared::kSlotSize);
  current_offset += perm_grant_len * mu_squared::kSlotSize;

  // Define number of QP's **for this version**
  num_qps_ = num_shards_ + num_handlers_ + 2;
  num_shared_cq_ = num_shards_ + num_handlers_ + 1;

  // Shared cq mapping
  // QP 0            : primary consensus           -- shared CQ 0
  // QP 1 .. s       : replication, shard 0..s-1   -- shared CQ 1..s
  // QP s+1 .. s+p   : perm handler, handler 0..p-1 -- shared CQ s+1..s+p
  // QP s+p+1        : failure detector            -- NOT shared

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
  auto req_laddr = memblock_.GetAddrInfo(mu_squared::kPermReqRegionId);
  auto grant_laddr = memblock_.GetAddrInfo(mu_squared::kPermGrantRegionId);

  // Both matrices are locally polled, so they are primed with local stores
  for (uint64_t slot = 0; slot < perm_req_len; ++slot) {
    *reinterpret_cast<uint64_t *>(req_laddr.addr + req_laddr.offset +
                                  slot * mu_squared::kSlotSize) =
      mu_squared::kPermNull;
    *reinterpret_cast<uint64_t *>(grant_laddr.addr + grant_laddr.offset +
                                  slot * mu_squared::kSlotSize) =
      mu_squared::kPermNull;
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

  perm_handler_ctx_.conns_mat_.resize(
    system_size_, std::vector<romulus::ReliableConnection *>(num_handlers_));
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

  for (int h = 0; h < (int)num_handlers_; ++h) {

    for (int n = 0; n < (int)system_size_; ++n) {
      // row, col = node, handler
      auto conn = remote_conns_[n][1 + num_shards_ + h];
#ifdef MEMDUMP
      ROMULUS_DEBUG("Perm mat ({},{}) : QP: {} CQ: {}", n, h,
                    reinterpret_cast<uintptr_t>(conn->GetQP()),
                    reinterpret_cast<uintptr_t>(conn->GetCQ()));
#endif
      perm_handler_ctx_.conns_mat_[n][h] = conn;
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
    perm_handler_ctx_.req_raddrs_[n] =
      remote_addrs_[n][mu_squared::kPermReqRegionId];
    perm_handler_ctx_.grant_raddrs_[n] =
      remote_addrs_[n][mu_squared::kPermGrantRegionId];

    // failure detector
    if (n == (int)id_)
      continue;
    conn = remote_conns_[n][1 + num_shards_ + num_handlers_];
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
  // Perm Acks        : n * p
  // FD               : n - 1
  //
  // Thread counts
  // Primary consensus: 1
  // Permission handlers: p
  // Failure Detector: 1
  // Total: p + 2 threads
  // -----------------------------

  // Initialize permissions for replication QP's: the owner of each shard holds
  // write access from the outset, every other node only local rw access
  for (int n = 0; n < (int)system_size_; ++n) {
    for (int s = 0; s < (int)num_shards_; ++s) {
      auto &c = replication_ctx_.conns_mat_[n][s];
      c->ApplyPermissions(n == (int)(s % system_size_)
                              ? mu_squared::kFullPermission
                              : mu_squared::kNoPermission);
      // c->ApplyPermissions(mu_squared::kNoPermission);
    }
  }

  // Finally, barrier
  conn_manager_->arrive_strict_barrier();
}

#else

void VelosSquared::Init(std::string_view dev_name, int dev_port,
                        std::unique_ptr<romulus::ConnectionRegistry> registry,
                        std::unordered_map<uint64_t, std::string> mach_map) {
  // Set up remotely accessible memory.
  ROMULUS_DEBUG("Initializing memory for Velos^2");
  ROMULUS_DEBUG("Quorum size: {}", quorum_);
  ROMULUS_DEBUG("Opening device with name {} on port {}", dev_name, dev_port);
  ROMULUS_ASSERT(device_->Open(dev_name, dev_port), "Failed to open device.");
  device_->AllocatePd(velos_squared::kPdId);

  ROMULUS_INFO("Registering remotely accessible memory");

  uint64_t scratch_len = num_shards_ * system_size_ * pipeline_depth_;
  uint64_t pre_scratch_len = system_size_;
  uint64_t fd_local_len = (system_size_ - 1);
  uint64_t fd_remote_len = (system_size_ - 1);

  // capacity is per-shard. proposed mirrors the log since every slot is
  // prepared ahead of time
  uint64_t proposal_len = capacity_ * num_shards_;
  uint64_t log_len = capacity_ * num_shards_;

  // NB: the following configuration assumes STANDALONE
  std::size_t remote_len = scratch_len + proposal_len + log_len +
                           pre_scratch_len + fd_local_len + fd_remote_len;

  raw_ =
    new romulus::APArray<State, velos_squared::kSlotSize, CACHE_PREFETCH_SIZE>(
      remote_len);
  std::memset(raw_->Get(), 0, raw_->GetTotalBytes());

  // ============================================================================
  //   Memory Layout
  //   n = system_size   s = num_shards_   c = capacity   k = kSlotSize
  //   d = pipeline_depth_
  // ============================================================================
  //
  //   +-------------+
  //   |   scratch   |  : n * d * k
  //   +-------------+
  //   |  proposed   |  : s * c * k      local only, shard-major
  //   +-------------+
  //   |    log_0    |  : c * k
  //   +-------------+
  //   |    log_1    |  : c * k
  //   +-------------+
  //   |      :      |
  //   +-------------+
  //   |  log_{s-1}  |  : c * k
  //   +-------------+
  //   | pre_scratch |  : n * k
  //   +-------------+
  //   |    FD_l     |  : (n-1) * k      local FD region
  //   +-------------+
  //   |    FD_r     |  : (n-1) * k      remote FD region
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
  auto pd = device_->GetPd(velos_squared::kPdId);
  memblock_ = romulus::MemBlock(velos_squared::kBlockId, pd,
                                reinterpret_cast<uint8_t *>(raw_->Get()),
                                remote_len * velos_squared::kSlotSize);
  // Registering memblock regions
  uint64_t current_offset = 0;
  memblock_.RegisterMemRegion(velos_squared::kScratchRegionId, current_offset,
                              scratch_len * velos_squared::kSlotSize);
  current_offset += scratch_len * velos_squared::kSlotSize;
  memblock_.RegisterMemRegion(velos_squared::kProposedRegionId, current_offset,
                              proposal_len * velos_squared::kSlotSize);
  current_offset += proposal_len * velos_squared::kSlotSize;
  for (int i = 0; i < (int)num_shards_; ++i) {
    memblock_.RegisterMemRegion(GenLogID(i), current_offset,
                                capacity_ * velos_squared::kSlotSize);
    current_offset += capacity_ * velos_squared::kSlotSize;
  }
  memblock_.RegisterMemRegion(velos_squared::kPreScratchRegionId,
                              current_offset,
                              pre_scratch_len * velos_squared::kSlotSize);
  current_offset += pre_scratch_len * velos_squared::kSlotSize;
  memblock_.RegisterMemRegion(velos_squared::kFDLocalRegionId, current_offset,
                              fd_local_len * velos_squared::kSlotSize);
  current_offset += fd_local_len * velos_squared::kSlotSize;
  memblock_.RegisterMemRegion(velos_squared::kFDRemoteRegionId, current_offset,
                              fd_remote_len * velos_squared::kSlotSize);
  current_offset += fd_remote_len * velos_squared::kSlotSize;

  // Define number of QP's **for this version**
  num_qps_ = 3;
  num_shared_cq_ = 2;

  // Shared cq mapping
  // QP 0     : primary consensus           -- shared CQ 0
  // QP 1     : async prepreparation        -- shared CQ 1
  // QP 2     : failure detector            -- NOT shared

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
  regions.insert(
    regions.end(),
    {velos_squared::kScratchRegionId, velos_squared::kProposedRegionId,
     velos_squared::kFDLocalRegionId, velos_squared::kFDRemoteRegionId});
  for (int i = 0; i < (int)num_shards_; ++i) {
    regions.push_back(velos_squared::kLogRegionId + "_" + std::to_string(i));
  }

  for (auto &m : mach_map) {
    // <region_id, remote_addr>
    std::unordered_map<std::string, romulus::RemoteAddr> tmp_addrs;
    // We need to account for all the remotely visible regions
    for (auto &r : regions) {
      // If the machine id maps to **this** node, then this will represent the
      // loopback addr
      conn_manager_->GetRemoteAddr(m.first, velos_squared::kBlockId, r,
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

  // Initialize all RDMA contexts. Each thread owns its own copy of the log
  // addrs since the offset is mutated per CAS
  cons_ctx_.conns_.resize(system_size_);
  cons_ctx_.raddrs_mat_.resize(system_size_,
                               std::vector<romulus::RemoteAddr>(num_shards_));
  prep_ctx_.conns_.resize(system_size_);
  prep_ctx_.raddrs_mat_.resize(system_size_,
                               std::vector<romulus::RemoteAddr>(num_shards_));

  fd_ctx_.conns_.resize(system_size_, nullptr);
  fd_ctx_.raddrs_.resize(system_size_);

  cons_ctx_.laddr_ = memblock_.GetAddrInfo(velos_squared::kScratchRegionId);
  prep_ctx_.laddr_ = memblock_.GetAddrInfo(velos_squared::kPreScratchRegionId);
  fd_ctx_.laddr_ = memblock_.GetAddrInfo(velos_squared::kFDLocalRegionId);

  for (int n = 0; n < (int)system_size_; ++n) {
    // primary
    cons_ctx_.conns_[n] = remote_conns_[n][0];
    // prepreparation
    prep_ctx_.conns_[n] = remote_conns_[n][1];
    for (int s = 0; s < (int)num_shards_; ++s) {
      cons_ctx_.raddrs_mat_[n][s] = remote_addrs_[n][GenLogID(s)];
      prep_ctx_.raddrs_mat_[n][s] = remote_addrs_[n][GenLogID(s)];
    }

    // failure detector
    if (n == (int)id_)
      continue;
    fd_ctx_.conns_[n] = remote_conns_[n][2];
    fd_ctx_.raddrs_[n] = remote_addrs_[n][velos_squared::kFDRemoteRegionId];
  }
#ifdef MEMDUMP
  // Dump every context's QP/CQ so the shared-CQ mapping is verifiable
  ROMULUS_INFO("=== Context dump: node {} of {} ===", id_, system_size_);
  ROMULUS_INFO("num_qps_={} num_shared_cq_={}", num_qps_, num_shared_cq_);
  for (int n = 0; n < (int)system_size_; ++n) {
    ROMULUS_INFO(
      "cons_ctx_[{}]{} QP={:#x} CQ={:#x}", n, (n == (int)id_) ? " LOOPBACK" : "",
      reinterpret_cast<uintptr_t>(cons_ctx_.conns_[n]->GetQP()),
      reinterpret_cast<uintptr_t>(cons_ctx_.conns_[n]->GetCQ()));
  }
  for (int n = 0; n < (int)system_size_; ++n) {
    ROMULUS_INFO(
      "prep_ctx_[{}]{} QP={:#x} CQ={:#x}", n, (n == (int)id_) ? " LOOPBACK" : "",
      reinterpret_cast<uintptr_t>(prep_ctx_.conns_[n]->GetQP()),
      reinterpret_cast<uintptr_t>(prep_ctx_.conns_[n]->GetCQ()));
  }
  for (int n = 0; n < (int)system_size_; ++n) {
    if (fd_ctx_.conns_[n] == nullptr) {
      ROMULUS_INFO("fd_ctx_[{}] null", n);
      continue;
    }
    ROMULUS_INFO("fd_ctx_[{}] QP={:#x} CQ={:#x}", n,
                 reinterpret_cast<uintptr_t>(fd_ctx_.conns_[n]->GetQP()),
                 reinterpret_cast<uintptr_t>(fd_ctx_.conns_[n]->GetCQ()));
  }
  // A shared CQ must hold every completion the poller waits on, and the two
  // phases must not land on the same one
  std::set<uintptr_t> cons_cqs, prep_cqs;
  for (int n = 0; n < (int)system_size_; ++n) {
    cons_cqs.insert(reinterpret_cast<uintptr_t>(cons_ctx_.conns_[n]->GetCQ()));
    prep_cqs.insert(reinterpret_cast<uintptr_t>(prep_ctx_.conns_[n]->GetCQ()));
  }
  ROMULUS_INFO("distinct cons CQs={} prep CQs={}", cons_cqs.size(),
               prep_cqs.size());
  for (auto &c : cons_cqs) {
    if (prep_cqs.count(c))
      ROMULUS_INFO("!! CQ {:#x} shared between cons and prep", c);
  }
  // Local view of the remotely accessible regions
  auto dump = [&](const std::string &r) {
    auto a = memblock_.GetAddrInfo(r);
    ROMULUS_INFO("region {:<24} addr={:#x} offset={} len={}", r, a.addr,
                 a.offset, a.length);
  };
  dump(velos_squared::kScratchRegionId);
  dump(velos_squared::kProposedRegionId);
  dump(velos_squared::kPreScratchRegionId);
  dump(velos_squared::kFDLocalRegionId);
  dump(velos_squared::kFDRemoteRegionId);
  for (int s = 0; s < (int)num_shards_; ++s)
    dump(GenLogID(s));
  // Remote log bases as seen from here: the stride decides atomic lock-table
  // collisions at the responder
  for (int s = 0; s < (int)num_shards_; ++s) {
    ROMULUS_INFO("raddr log_{} node0 addr={:#x} rkey={}", s,
                 cons_ctx_.raddrs_mat_[0][s].addr_info.addr,
                 cons_ctx_.raddrs_mat_[0][s].addr_info.key);
  }
  ROMULUS_INFO("=== End context dump ===");
#endif
  // QP mappings
  // n = system_size   s = num_shards_   c = capacity   k = kSlotSize
  //
  // --- QP counts ---
  // Primary concensus: n
  // Prepreparation   : n
  // FD               : n - 1
  //
  // Thread counts
  // Primary consensus: 1
  // Prepare handler: 1
  // Failure Detector: 1
  // Total: 3 threads
  // -----------------------------

  // Finally, barrier
  conn_manager_->arrive_strict_barrier();
}

#endif