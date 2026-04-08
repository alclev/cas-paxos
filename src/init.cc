#include "cas_paxos_st.h"

using namespace paxos_st;

void CasPaxos::Init(std::string_view dev_name, int dev_port,
                    std::unique_ptr<romulus::ConnectionRegistry> registry,
                    std::unordered_map<uint64_t, std::string> mach_map) {
  // Set up remotely accessible memory.
  ROMULUS_DEBUG("Initializing CAS-based Paxos");
  ROMULUS_DEBUG("Quorum size: {}", quorum_);
  ROMULUS_DEBUG("Opening device with name {} on port {}", dev_name, dev_port);
  ROMULUS_ASSERT(device_.Open(dev_name, dev_port), "Failed to open device.");
  device_.AllocatePd(kPdId);

  ROMULUS_INFO("Registering remotely accessible memory");
  uint64_t scratch_len = system_size_ * kSlotSize;
  uint64_t extra_scratch_len = system_size_ * kSlotSize;
  uint64_t proposal_len = capacity_ * kSlotSize;
  uint64_t log_len = capacity_ * kSlotSize;
  uint64_t leader_len = kSlotSize;
  uint64_t heartbeat_len = kSlotSize;

  ROMULUS_ASSERT(buf_size_ % kSlotSize == 0,
                 "Buf size not being a multiple of {} is not supported!",
                 kSlotSize);
  ROMULUS_ASSERT(num_qps_ > 1, "This experiment requires at least 2 qp's.");

  // NB: the following configuration assumes STANDALONE
  std::size_t remote_len = scratch_len + extra_scratch_len + proposal_len +
                           log_len + leader_len + heartbeat_len;
  raw_ =
      new romulus::APArray<State, kSlotSize, CACHE_PREFETCH_SIZE>(remote_len);
  std::memset(raw_->Get(), 0, raw_->GetTotalBytes());

  // Constructing the memblock
  auto pd = device_.GetPd(kPdId);
  memblock_ = romulus::MemBlock(
      kBlockId, pd, reinterpret_cast<uint8_t*>(raw_->Get()), remote_len);

  // Registering memblock regions
  memblock_.RegisterMemRegion(kScratchRegionId, 0, scratch_len);
  memblock_.RegisterMemRegion(kScratchExtraRegionId, scratch_len,
                              extra_scratch_len);
  memblock_.RegisterMemRegion(kProposedRegionId,
                              scratch_len + extra_scratch_len, proposal_len);
  memblock_.RegisterMemRegion(
      kLogRegionId, (scratch_len + extra_scratch_len + proposal_len), log_len);
  memblock_.RegisterMemRegion(
      kLeaderRegionId, scratch_len + extra_scratch_len + proposal_len + log_len,
      leader_len);
  memblock_.RegisterMemRegion(
      kHeartBeatRegionId,
      scratch_len + extra_scratch_len + proposal_len + log_len + leader_len,
      heartbeat_len);

  // Optionally set up AParray for fast access to local views of log memory
#ifndef STANDALONE
  // Not implemented...
#endif

  // Set up local view of log memory
  scratch_ = romulus::APArraySlice(raw_, 0, scratch_len);
  proposed_state_ =
      romulus::APArraySlice(raw_, scratch_len + extra_scratch_len,
                            scratch_len + extra_scratch_len + proposal_len);
  log_ = romulus::APArraySlice(
      raw_, scratch_len + extra_scratch_len + proposal_len,
      scratch_len + extra_scratch_len + proposal_len + log_len);

  // Initialize proposed state (remote peers read this). +1 because 0 is a
  // special value in the state.
  ROMULUS_ASSERT(kSlotSize == sizeof(State),
                 "kSlotSize != sizeof(State) not supported.");
  for (uint32_t i = 0; i < capacity_; ++i) {
    proposed_state_[i] = State(host_id_ + 1, kNullBallot, kNullValue);
    log_[i] = State(0, kNullBallot, kNullValue);
  }

  // Register memory and connect to other nodes
  registry_ = std::move(registry);
  conn_manager_ = std::make_unique<romulus::ConnectionManager>(
      hostname_, registry_.get(), host_id_, system_size_, num_qps_);

  // Reusuing the barrier object here- it is just a counter
  registry_->Register<Barrier>("paxos_epoch", Barrier());

  // Barrier
  conn_manager_->arrive_strict_barrier();

  ROMULUS_DEBUG("Attemping to register memory...");
  bool register_ok = conn_manager_->Register(device_, memblock_);

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
  std::vector<std::string> regions = {kScratchRegionId,  kScratchExtraRegionId,
                                      kProposedRegionId, kLogRegionId,
                                      kLeaderRegionId,   kHeartBeatRegionId};
  for (auto& m : mach_map) {
    // <region_id, remote_addr>
    std::unordered_map<std::string, romulus::RemoteAddr> tmp_addrs;
    // We need to account for all the remotely visible regions
    for (auto& r : regions) {
      // If the machine id maps to **this** node, then this will represent the
      // loopback addr
      conn_manager_->GetRemoteAddr(m.first, kBlockId, r, &remote_addr);
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
      ROMULUS_DEBUG("Loopback #1 cq: {}",
                    reinterpret_cast<uintptr_t>(conns[0]->GetCQ()));
      ROMULUS_DEBUG("Loopback #2 cq: {}",
                    reinterpret_cast<uintptr_t>(conns[1]->GetCQ()));
    } else {
      for (int q = 1; q < (int)num_qps_ + 1; ++q) {
        auto conn = conn_manager_->GetConnection(m.first, q);
        conns.push_back(conn);
      }
    }
    remote_conns_.emplace(m.first, conns);
  }
  // Initialize the contexts with the cached addresses
  contexts_.reserve(system_size_);
  for (int i = 0; i < system_size_; ++i) {
    contexts_.push_back(new RemoteContext());
    RemoteContext* context = contexts_[i];
    context->proposed_state = &proposed_state_[0];

    context->conn = remote_conns_[i].front();
    context->scratch_laddr = memblock_.GetAddrInfo(kScratchRegionId);
    context->scratch_laddr.offset = kSlotSize * i;
    context->scratch_laddr.length = sizeof(State);

    context->scratch_state = reinterpret_cast<State*>(
        context->scratch_laddr.addr + context->scratch_laddr.offset);

    context->log_raddr = remote_addrs_[i][kLogRegionId];
    context->log_raddr.addr_info.length = sizeof(State);

    context->proposal_raddr = remote_addrs_[i][kProposedRegionId];
    context->proposal_raddr.addr_info.length = sizeof(State);
  }
  // Optionally dump the contents of our cached maps...
#ifdef SYSDUMP
  // Dump the remote addresses
  for (auto& m : remote_addrs_) {
    for (auto& r : m.second) {
      if (m.first == host_id_)
        ROMULUS_INFO("[MAP] Machine={} (loopback)\tRegion={}\tAddr={:x}",
                     m.first, r.first, r.second.addr_info.addr);
      else
        ROMULUS_INFO("[MAP] Machine={}\tRegion={}\tAddr={:x}", m.first, r.first,
                     r.second.addr_info.addr);
    }
  }
  // Dump the connections
  for (auto& c : remote_conns_) {
    if (c.second.size() == 1) {
      ROMULUS_INFO("[MAP] Machine={}\tConnection={:x} (loopback)", c.first,
                   reinterpret_cast<uintptr_t>(c.second.front()));
    } else {
      for (int q = 0; q < (int)c.second.size(); ++q) {
        ROMULUS_INFO("[MAP] Machine={}\tConnection={:x}", c.first,
                     reinterpret_cast<uintptr_t>(c.second[q]));
      }
    }
  }
#endif
  // Finally, barrier
  conn_manager_->arrive_strict_barrier();
}