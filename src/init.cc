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
  uint64_t num_shards = args_->uget(NUM_SHARDS);

  uint64_t scratch_len = system_size_ * mu_squared::kSlotSize;
  uint64_t proposal_len = num_shards * mu_squared::kSlotSize;
  uint64_t lease_len = num_shards * mu_squared::kSlotSize;

  // capacity is per-shard
  uint64_t log_len = capacity_ * mu_squared::kSlotSize * num_shards;

  // NB: the following configuration assumes STANDALONE
  std::size_t remote_len = scratch_len + proposal_len + lease_len + log_len;

  raw_ = new romulus::APArray<State, mu_squared::kSlotSize,
                              CACHE_PREFETCH_SIZE>(remote_len);
  std::memset(raw_->Get(), 0, raw_->GetTotalBytes());

  // Set up local view of log memory
  scratch_ = romulus::APArraySlice(raw_, 0, scratch_len);
  proposed_state_ =
      romulus::APArraySlice(raw_, scratch_len, scratch_len + proposal_len);
  lease_table_ = romulus::APArraySlice(raw_, scratch_len + proposal_len,
                                       scratch_len + proposal_len + lease_len);
  log_ =
      romulus::APArraySlice(raw_, scratch_len + proposal_len + lease_len,
                            scratch_len + proposal_len + lease_len + log_len);

  // Constructing the memblock
  auto pd = device_->GetPd(mu_squared::kPdId);
  memblock_ =
      romulus::MemBlock(mu_squared::kBlockId, pd,
                        reinterpret_cast<uint8_t*>(raw_->Get()), remote_len);

  // Registering memblock regions
  int current_offset = 0;
  memblock_.RegisterMemRegion(mu_squared::kScratchRegionId, current_offset,
                              scratch_len);
  current_offset += scratch_len;
  memblock_.RegisterMemRegion(mu_squared::kProposedRegionId, current_offset,
                              proposal_len);
  current_offset += proposal_len;
  memblock_.RegisterMemRegion(mu_squared::kLeaseRegionId, current_offset,
                              lease_len);
  current_offset += lease_len;
  for (int i = 0; i < (int)num_shards; ++i) {
    memblock_.RegisterMemRegion(
        mu_squared::kLogRegionId + "_" + std::to_string(i), current_offset,
        capacity_ * mu_squared::kSlotSize);
    current_offset += capacity_ * mu_squared::kSlotSize;
  }

  // Register memory and connect to other nodes
  registry_ = std::move(registry);
  conn_manager_ = std::make_unique<romulus::ConnectionManager>(
      hostname_, registry_.get(), id_, system_size_, num_qps_, num_shared_cq_);

  // Reusuing the barrier object here- it is just a counter
  registry_->Register<Barrier>("paxos_epoch", Barrier());

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
                  mu_squared::kLeaseRegionId});
  for (int i = 0; i < (int)num_shards; ++i) {
    regions.push_back(mu_squared::kLogRegionId + "_" + std::to_string(i));
  }

  for (auto& m : mach_map) {
    // <region_id, remote_addr>
    std::unordered_map<std::string, romulus::RemoteAddr> tmp_addrs;
    // We need to account for all the remotely visible regions
    for (auto& r : regions) {
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
    std::vector<romulus::ReliableConnection*> conns;
    // here, the 0th index **is the loopback**
    if (m.second == hostname_) {
      conns.push_back(conn_manager_->GetConnection(m.first, 0));
      conns.push_back(conn_manager_->GetConnection(
          m.first, std::numeric_limits<uint64_t>::max()));
      conns.push_back(conn_manager_->GetConnection(
          m.first, std::numeric_limits<uint64_t>::max() - 1));
      conns.push_back(conn_manager_->GetConnection(
          m.first, std::numeric_limits<uint64_t>::max() - 2));

      // ROMULUS_DEBUG("Loopback #1 cq: {}",
      //               reinterpret_cast<uintptr_t>(conns[0]->GetCQ()));
      // ROMULUS_DEBUG("Loopback #2 cq: {}",
      //               reinterpret_cast<uintptr_t>(conns[1]->GetCQ()));
    } else {
      for (int q = 1; q < (int)num_qps_ + 1; ++q) {
        auto conn = conn_manager_->GetConnection(m.first, q);
        conns.push_back(conn);
      }
    }
    remote_conns_.emplace(m.first, conns);
  }
  // Initialize our lease space to the sentinal value
  auto conn = remote_conns_[id_][0];  // loopback
  auto raddr = remote_addrs_[id_][mu_squared::kLeaseRegionId];
  raddr.addr_info.length = mu_squared::kSlotSize;

  // To be outside of the range of id's to we don't interfer with P1
  uint64_t sentinal = system_size_ + 1; 

  auto laddr = memblock_.GetAddrInfo(mu_squared::kScratchRegionId);
  laddr.length = mu_squared::kSlotSize;
  laddr.offset = mu_squared::kSlotSize * id_;
  *reinterpret_cast<uint64_t*>(laddr.addr + laddr.offset) = sentinal;
  romulus::WorkRequest write;

  // Post num_shards number of WRs
  for (int s_id = 0; s_id < (int)num_shards_; ++s_id) {
    raddr.addr_info.offset = mu_squared::kSlotSize * s_id;
    romulus::WorkRequest::BuildWrite(laddr, raddr, 0, &write);
    ROMULUS_ASSERT(conn->Post(&write, 1), "Failed to post write in musq ctor.");
  }

  auto conn_raw = conn->GetCQ();
  std::vector<ibv_wc> wcs(num_shards_);
  int polled = 0;
  while (polled < (int)num_shards_) {
    int n = ibv_poll_cq(conn_raw, num_shards_ - polled, wcs.data() + polled);
    if (n < 0) {
      ROMULUS_FATAL("Error in polling lease memory initialization");
      break;
    }
    polled += n;
  }
  ROMULUS_INFO("Successfully initialized lease memory");

  // Define cached conns, raddr, and laddr for hot path access
  cached_conns_.resize(system_size_);
  cached_raddrs_.resize(system_size_);
  for (int n = 0; n < (int)system_size_; ++n) {
    cached_conns_[n] = remote_conns_[n][0];
    auto raddr = remote_addrs_[n][mu_squared::kLeaseRegionId];
    raddr.addr_info.length = mu_squared::kSlotSize;
    cached_raddrs_[n] = raddr;
  }
  cached_laddr_ = memblock_.GetAddrInfo(mu_squared::kScratchRegionId);
  cached_laddr_.length = mu_squared::kSlotSize;

  // Finally, barrier
  conn_manager_->arrive_strict_barrier();
}