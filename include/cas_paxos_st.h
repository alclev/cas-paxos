#pragma once

#include <aparray.h>
#include <romulus/common.h>
#include <romulus/connection_manager.h>
#include <romulus/device.h>
#include <romulus/romulus.h>
#include <state.h>

#include <cassert>
#include <memory>
#include <numeric>

// Compile time configurations for testing different optimizations
// #define DYNO_NO_WRITEBUF
// #define DYNO_NO_SENDALL

namespace paxos_st {

// IDs used when registering RDMA connections.
const std::string kRegistryName = "CasPaxos";
const std::string kPdId = "PdId";
const std::string kBlockId = "LogBlock";
const std::string kScratchRegionId = "ScratchRegion";
const std::string kProposedRegionId = "ProposedRegion";
const std::string kLogRegionId = "LogRegion";
const std::string kBufRegionPrefix = "BufRegion";
const std::string kLeaderRegionId = "LeaderRegion";

const std::string kLocalId = "StagingBlock";
const std::string kLocalWriteId = "LocalWriteRegion";
const std::string kLocalReadId = "LocalReadRegion";
const std::string kLocalCasId = "LocalCasRegion";

constexpr uint32_t kSlotSize = sizeof(State);
constexpr uint32_t kNumBufSlots = 50000000;
constexpr std::chrono::seconds kTimeout(10);
constexpr uint16_t kNullBallot = std::numeric_limits<uint16_t>::min();
constexpr uint32_t kMaxStartingBackoff = 3600;

struct RemoteContext {
  State* scratch_state;
  State* proposed_state;

  romulus::ReliableConnection* conn;

  // Log related info
  romulus::AddrInfo scratch_laddr;
  romulus::RemoteAddr proposal_raddr;
  romulus::RemoteAddr log_raddr;

  // Head of WR chain
  romulus::WorkRequest wr;
  RemoteContext() {}
  RemoteContext(const RemoteContext& c)
      : scratch_state(c.scratch_state),
        proposed_state(c.proposed_state),
        conn(c.conn),
        scratch_laddr(c.scratch_laddr),
        proposal_raddr(c.proposal_raddr),
        log_raddr(c.log_raddr),
        wr(c.wr) {}
};

namespace {  // namespace anonymous

template <typename Rep, typename Period>
inline std::chrono::duration<Rep, Period> DoBackoff(
    std::chrono::duration<Rep, Period> backoff) {
  ROMULUS_DEBUG(
      "Backing off for {} ms",
      std::chrono::duration_cast<std::chrono::milliseconds>(backoff).count());
  // Backoff if an abort was triggered this round.
  auto start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start < backoff);
  return backoff * 2;
}
// Return the next higher unique ballot calculated by offsetting for this host
// into the next chunk of ballots to use. If the peer ballot is lower than the
// local ballot, then return the current ballot.
inline uint32_t NextBallot(uint32_t local_ballot, uint32_t peer_ballot,
                           uint8_t host_id, uint32_t sys_size) {
  if (local_ballot < peer_ballot) {
    return ((((peer_ballot - 1) / sys_size) + 1) * sys_size) + (host_id + 1);
  } else {
    return local_ballot;
  }
}

bool PollCompletionsOnce(romulus::ReliableConnection* c, uint64_t wr_id) {
  return c->TryProcessOutstanding() > 0 && c->CheckCompletionsForId(wr_id);
}

}  // namespace

class CasPaxos : public Paxos {
 public:
  CasPaxos(std::shared_ptr<romulus::ArgMap> args,
           std::vector<std::string> peers, uint8_t transport_flag)
      : args_(args),
        system_size_(peers.size() + 1),
        capacity_(args->uget(romulus::CAPACITY)),
        wr_id_(0),
        log_offset_(0),
        is_leader_(false),
        stable_leader_(args->bget(romulus::STABLE_LEADER)),
        multi_paxos_opt_(args->bget(romulus::MULTIPAX_OPT)),
        hostname_(args->sget(romulus::HOSTNAME)),
        host_id_(args->uget(romulus::NODE_ID)),
        quorum_(romulus::GetQuorum(peers.size() + 1)),
        peers_(peers),
        device_(transport_flag),
        buf_size_(args->uget(romulus::BUF_SIZE)),
        num_qps_(args->uget(romulus::NUM_QP)) {}

  ~CasPaxos() {
    delete raw_;
    SyncNodes();
  }

  void Init(std::string_view dev_name, int dev_port,
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
    uint64_t proposal_len = capacity_ * kSlotSize;
    uint64_t log_len = capacity_ * kSlotSize;
    uint64_t leader_len = sizeof(State) * kSlotSize;

    ROMULUS_ASSERT(buf_size_ % kSlotSize == 0,
                   "Buf size not being a multiple of {} is not supported!",
                   kSlotSize);
    // NB: the following configuration assumes STANDALONE
    std::size_t remote_len = scratch_len + proposal_len + log_len + leader_len;
    raw_ =
        new romulus::APArray<State, kSlotSize, CACHE_PREFETCH_SIZE>(remote_len);
    std::memset(raw_->Get(), 0, raw_->GetTotalBytes());

    // Constructing the memblock
    auto pd = device_.GetPd(kPdId);
    memblock_ = romulus::MemBlock(
        kBlockId, pd, reinterpret_cast<uint8_t*>(raw_->Get()), remote_len);

    // Registering memblock regions
    memblock_.RegisterMemRegion(kScratchRegionId, 0, scratch_len);
    memblock_.RegisterMemRegion(kProposedRegionId, scratch_len, proposal_len);
    memblock_.RegisterMemRegion(kLogRegionId, (scratch_len + proposal_len),
                                log_len);
    memblock_.RegisterMemRegion(
        kLeaderRegionId, scratch_len + proposal_len + log_len, leader_len);

    // Optionally set up AParray for fast access to local views of log memory
#ifndef STANDALONE
    // Not implemented...
#endif

    // Set up local view of log memory
    scratch_ = romulus::APArraySlice(raw_, 0, scratch_len);
    proposed_state_ =
        romulus::APArraySlice(raw_, scratch_len, scratch_len + proposal_len);
    log_ = romulus::APArraySlice(raw_, scratch_len + proposal_len,
                                 scratch_len + proposal_len + log_len);
    leader_ = romulus::APArraySlice(
        raw_, scratch_len + proposal_len + log_len,
        scratch_len + proposal_len + log_len + leader_len);

    // Initialize proposed state (remote peers read this). +1 because 0 is a
    // special value in the state.
    ROMULUS_ASSERT(kSlotSize == sizeof(State),
                   "kSlotSize != sizeof(State) not supported.");
    for (uint32_t i = 0; i < capacity_; ++i) {
      proposed_state_[i] = State(host_id_ + 1, kNullBallot, kNullBallot);
      log_[i] = State(0, kNullBallot, kNullBallot);
    }

    // Register memory and connect to other nodes
    registry_ = std::move(registry);
    conn_manager_ = std::make_unique<romulus::ConnectionManager>(
        hostname_, registry_.get(), host_id_, system_size_, num_qps_);

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
    std::vector<std::string> regions = {kScratchRegionId, kProposedRegionId,
                                        kLogRegionId, kLeaderRegionId};
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

      context->conn = remote_conns_[host_id_].front();
      context->scratch_laddr = memblock_.GetAddrInfo(kScratchRegionId);
      context->scratch_laddr.addr += kSlotSize * i;  // Offset into scratch mem
      context->scratch_laddr.length = sizeof(State);
      context->scratch_state =
          reinterpret_cast<State*>(context->scratch_laddr.addr);

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
          ROMULUS_INFO("[MAP] Machine={}\tRegion={}\tAddr={:x}", m.first,
                       r.first, r.second.addr_info.addr);
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

  void Reset() override {
    log_offset_ = 0;
    buf_offset_ = 0;
  }

  void Propose([[maybe_unused]] uint32_t len,
               [[maybe_unused]] uint8_t* buf) override {
#ifndef STANDALONE
// TODO: commit logic
#endif
    Value v;
    v.SetId(host_id_);
    v.SetOffset(buf_offset_);
    ProposeInternal(v);
  }

  void CatchUp() override {
    if (is_leader_) return;
    ROMULUS_DEBUG("<CatchUp> Catching up");
    while (TryCatchUp()) {
      ROMULUS_COUNTER_INC("skipped");
    }
  }
  void SyncNodes() override {
    ROMULUS_DEBUG("Syncing nodes");
    conn_manager_->arrive_barrier_timeout();
    ROMULUS_DEBUG("Nodes synced");
  }
  void CleanUp() override {
    SyncNodes();
    ROMULUS_COUNTER_ACC("p1_aborts");
    ROMULUS_COUNTER_ACC("p2_aborts");
    ROMULUS_COUNTER_ACC("attempts");
    ROMULUS_COUNTER_ACC("skipped");
    ROMULUS_COUNTER_ACC("proposed");
    ROMULUS_INFO("!> p1_aborts={}", ROMULUS_COUNTER_GET("p1_aborts"));
    ROMULUS_INFO("!> p2_aborts={}", ROMULUS_COUNTER_GET("p2_aborts"));
    ROMULUS_INFO("!> attempts={}", ROMULUS_COUNTER_GET("attempts"));
    ROMULUS_INFO("!> skipped={}", ROMULUS_COUNTER_GET("skipped"));
    ROMULUS_INFO("!> proposed={}", ROMULUS_COUNTER_GET("proposed"));
    CatchUp();
    DumpLog();
  }

 private:
  void Failure() {
    ROMULUS_INFO("Failure detected. Undergoing leader election...");
    is_leader_ = false;
    stable_leader_ = false;
    // TODO
    // conn_manager_->arrive_strict_barrier();
    // LeaderChange();
    // conn_manager_->arrive_strict_barrier();
    // // Catch up on previously committed values.
    // CatchUp();
    // conn_manager_->arrive_strict_barrier();
  }

  // Params: node_id of the new leader
  void LeaderChange(State* new_leader) {
    ROMULUS_INFO("Electing new leader {}", new_leader->GetValue().id());
    // Barrier
    conn_manager_->arrive_strict_barrier();
    uint32_t ok_count = 0;
    std::vector<bool> ok(system_size_);
    // If we are the leader, then we write to the leader slot
    if (is_leader_) {
      ROMULUS_DEBUG("IM THE LEADER");
      // Write the new leader to the leader slot
      leader_[0] = *new_leader;

      auto laddr = memblock_.GetAddrInfo(kScratchRegionId);
      auto* laddr_raw = reinterpret_cast<uint8_t*>(laddr.addr);

      for (int i = 0; i < system_size_; i++) {
        auto* c = contexts_[i];
        laddr.offset = i * kSlotSize;
        laddr.length = kSlotSize;
        *reinterpret_cast<State*>(laddr_raw + laddr.offset) = *new_leader;

        auto raddr = remote_addrs_[i][kLeaderRegionId];
        raddr.addr_info.offset = 0;
        raddr.addr_info.length = kSlotSize;
        // We hardcode using the first qp, unless there is an explicit reason to
        // iterate through them on a single thread
        c->conn = remote_conns_[i].front();
        // NB: Here, we can consider chaining the WR's and passing them to a
        // single connection but the problem here is that we are interfacing
        // with multiple peers, so we cannot, rely on a single QP, therefore we
        // will have to post one
        // if (i != 0) {
        //   outstanding_[i - 1].append(&outstanding_[i]);
        // }
        romulus::WorkRequest::BuildWrite(laddr, raddr, wr_id_, &c->wr);
        ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                       "Error posting in leader change.");
      }

      while (ok_count < system_size_) {
        for (uint32_t i = 0; i < system_size_; ++i) {
          if (ok[i]) continue;
          auto* c = contexts_[i];
          ok[i] = PollCompletionsOnce(c->conn, wr_id_) > 0;
          if (ok[i]) ++ok_count;
        }
      }
    }
    // Barrier
    conn_manager_->arrive_strict_barrier();
    // Now we read from our slot
    if (!is_leader_) {
      // Landing space for the reads
      // we only need to read once from our own slot
      auto* c = contexts_[host_id_];
      auto laddr = memblock_.GetAddrInfo(kScratchRegionId);
      laddr.offset = host_id_ * kSlotSize;
      laddr.length = kSlotSize;

      auto raddr = remote_addrs_[host_id_][kLeaderRegionId];
      raddr.addr_info.offset = 0;
      raddr.addr_info.length = kSlotSize;
      // Should be loopback
      c->conn = remote_conns_[host_id_].front();

      romulus::WorkRequest::BuildRead(laddr, raddr, wr_id_, &c->wr);
      ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                     "Error reading in leader change.");
      // Only expecting a single completion
      while (!PollCompletionsOnce(c->conn, wr_id_));

      for (int i = 0; i < system_size_; ++i) {
        auto laddr_raw = reinterpret_cast<uint64_t*>(laddr.addr + laddr.offset);
        ROMULUS_DEBUG("[Leader READ] {:x}", *laddr_raw);
      }
    }
    wr_id_++;
    conn_manager_->arrive_strict_barrier();
  }

#if 0
  //+ Prepare work request then post with first request.
  uint32_t PrepareWrite(uint32_t len, uint8_t* buf) {
    ROMULUS_ASSERT(buf_offset_ < buf_chunk_size_,
                   "Buffer offset out of bounds! actual={}, max={}",
                   buf_offset_, buf_chunk_size_);
    // Write the buffer into the local copy of this node's buffer.
    *reinterpret_cast<uint32_t*>(&buf_[buf_offset_]) = len;
    std::memcpy(&buf_[buf_offset_ + sizeof(uint32_t)], buf, len);
    auto total_len = len + sizeof(uint32_t);

    // Propagate the local copy to all other peers.
    RemoteContext* c;
    for (uint32_t i = 0; i < system_size_; ++i) {
      if (i == host_id_) continue;  // Already written.
      c = contexts_[i];

      if (c->buf_laddr.addr != reinterpret_cast<uint64_t>(buf_)) {
        ROMULUS_FATAL("Bad address! expected={}, actual={:x}",
                      reinterpret_cast<uint64_t>(buf_),
                      reinterpret_cast<uint64_t>(c->buf_laddr.addr));
      }
      if (c->buf_laddr.offset != buf_offset_) {
        ROMULUS_FATAL("Bad offset! expected={}, actual={}", buf_offset_,
                      c->buf_laddr.offset);
      }

      // Stage the buf request.
      c->buf_laddr.length = total_len;
      c->buf_raddr.addr_info.length = total_len;
      c->post_buf_wr = true;  // Needs to be posted.
      romulus::WorkRequest::BuildWrite(c->buf_laddr, c->buf_raddr, wr_id_,
                                       &c->buf_wr);
      c->buf_wr.unsignaled();  // Does not need a completion since we always
                               // to another op after.
      c->buf_raddr.addr_info.offset += total_len;
      c->buf_laddr.offset += total_len;
    }
    ++wr_id_;
    return total_len;
  }
#endif

  // Repeated attempt to propose the given value until it is successfully
  // committed. Initially, try to update the log by repeatedly calling
  // TryCatchUp until it returns false, which indicates that the log offset is
  // at an entry that was not committed yet. Then, update the ballot to be the
  // next highest unique ballot number for that slot. Next, prepare peers by
  // attempting to CAS in the newly chosen ballot. Finally, commit the value
  // by writing to a quorum of nodes. During this process it is possible that
  // a previously proposed value is adopted, in which case the cycle will
  // repeat in an attempt to commit the provided value. If the prepare phase
  // is successful, then the node considers itself the leader. If it remains
  // the leader then future ballot updates and prepare phases will be skipped.
  void ProposeInternal([[maybe_unused]] Value v) {
    ROMULUS_DEBUG("<InlineProposal> Starting.");
    auto backoff = std::chrono::nanoseconds(std::rand() % kMaxStartingBackoff);
    bool ok = false, done = false, skip = false;
    State* result = nullptr;
    while (!done) {
      ROMULUS_COUNTER_INC("attempts");
      // Skip preparation if we are the leader -- key multi-paxos optimization
      if (multi_paxos_opt_) {
        // Proceed with the multipaxos optimization...
        if (stable_leader_) {
          // Skip prepare
          skip = true;
        } else {
          // Leader failure or beginning execution -- need to run prepare...
          skip = false;
          auto new_leader = Prepare();
          // Upon success, rc will encode the node id of the new leader
          is_leader_ = (new_leader->GetValue().id() == host_id_);
          LeaderChange(new_leader);

          // Once leader data has propogated, we assume stable leader again
          stable_leader_ = true;
        }
      } else {
        // Otherwise, we do the prepare every round
        result = Prepare();
        LeaderChange(result);
      }

      if (result != nullptr || skip) {
        ok = Promise(v);
        if (ok) {
          auto committed = log_[log_offset_].GetValue();
          if (committed == v) {
            // Given value was committed. Done.
            ROMULUS_DEBUG(
                "Proposed slot committed: value=({}, {}), log_offset={}",
                v.id(), v.offset(), log_offset_);
#ifndef STANDALONE
            // TODO: commit logic
#endif
            is_leader_ = true;
            done = true;
          } else {
            // Commit an existing value.
            ROMULUS_DEBUG("Slot committed: value=({}, {}), log_offset={}",
                          committed.id(), committed.offset(), log_offset_);
            is_leader_ = false;
          }
          ++log_offset_;
        } else {
          ROMULUS_COUNTER_INC("p2_aborts");
        }
      } else {
        ROMULUS_COUNTER_INC("p1_aborts");
      }

      // If aborted, backoff.
      if (!ok) {
        is_leader_ = false;
        backoff = DoBackoff(backoff);
      }
    }
    ROMULUS_COUNTER_INC("proposed");
  }

  /// @brief Issues RMDA-reads to all replicas, busy-waits untall all reads
  /// complete. Scan's scratch buffers for quorum agreement -- if committed,
  /// ammend local log with RDMA CAS
  /// @return bool
  bool TryCatchUp() {
    ROMULUS_DEBUG("<TryCatchUp> Catching up slot: {}", log_offset_);

    // Post READs to all remote peers.
    RemoteContext* c;
    std::vector<bool> ok;
    uint32_t ok_count = 0;
    ok.resize(system_size_);
    for (uint32_t i = 0; i < system_size_; ++i) {
      ok[i] = false;
      c = contexts_[i];
      c->log_raddr.addr_info.offset = log_offset_ * kSlotSize;
      romulus::WorkRequest::BuildRead(c->scratch_laddr, c->log_raddr, wr_id_,
                                      &c->wr);
      // StageLogRequest(c);
      ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                     "<TryCatchUp> Failed to post requests.");
    }

    // Wait for a response from all nodes.
    //+ Timeout if this takes too long.
    while (ok_count < system_size_) {
      for (uint32_t i = 0; i < system_size_; ++i) {
        if (ok[i]) continue;
        c = contexts_[i];
        ok[i] = PollCompletionsOnce(c->conn, wr_id_);
        if (ok[i]) ++ok_count;
      }
    }

    // Compare returned values to determine if this slot has been committed
    // already.
    RemoteContext *c_i, *c_j;
    uint32_t num_agreed = 0;
    Value accepted_val;
    for (uint32_t i = 0; i < quorum_; ++i) {
      c_i = contexts_[i];
      if (!ok[i] || c_i->scratch_state->GetBallot() == kNullBallot) continue;
      num_agreed = 1;
      accepted_val = c_i->scratch_state->GetValue();
      for (uint32_t j = i + 1; j < system_size_ && num_agreed < quorum_; ++j) {
        c_j = contexts_[j];
        if (!ok[j] || c_j->scratch_state->GetBallot() == kNullBallot) continue;
        if (c_j->scratch_state->GetValue() == accepted_val) {
          ++num_agreed;
        }
      }

      // This slot is committed.
      if (num_agreed >= quorum_) {
        // If the local log does not reflect the accepted value then CAS it in.
        if (log_[log_offset_].GetValue() != accepted_val) {
          auto loopback_context = contexts_[host_id_];
          loopback_context->log_raddr.addr_info.offset =
              log_offset_ * kSlotSize;
          romulus::WorkRequest::BuildCAS(
              loopback_context->scratch_laddr, loopback_context->log_raddr,
              log_[log_offset_].raw, c_i->scratch_state->raw, wr_id_,
              &loopback_context->wr);
          // StageLogRequest(loopback_context);
          ROMULUS_ASSERT(
              loopback_context->conn->Post(&loopback_context->wr, 1),
              "<TryCatchUp> Failed to post requests when updating local slot.");

          // Only expect a single outstanding completion.
          while (!PollCompletionsOnce(loopback_context->conn, wr_id_));
        }
        ROMULUS_DEBUG("<TryCatchUp> Caught up: log_offset={}, state={}",
                      log_offset_, log_[log_offset_].ToString());
        ++log_offset_;
        ++wr_id_;
        return true;
      }
    }
    ++wr_id_;
    ROMULUS_DEBUG("<TryCatchUp> Failed. log_offset={}", log_offset_);
    return false;
  }

  // Deprecated. With the new design we pull the metadata the prepare round
  // or global leader slot
#if 0
  void UpdateBallot() {
    // Skip if we are the leader.
    if (is_leader_) return;
    ROMULUS_DEBUG("Updating ballot");

    // Post READs to all remote peers.
    RemoteContext* c;
    std::vector<bool> ok;
    uint32_t ok_count = 0;
    ok.resize(system_size_);
    for (uint32_t i = 0; i < system_size_; ++i) {
      ok[i] = false;
      c = contexts_[i];
      c->proposal_raddr.addr_info.offset = log_offset_ * kSlotSize;
      romulus::WorkRequest::BuildRead(c->scratch_laddr, c->proposal_raddr,
                                      wr_id_, &c->log_wr);
      StageLogRequest(c);
      ROMULUS_ASSERT(PostRequests(c),
                     "<UpdateBallot> Failed to post requests.");
    }

    // Get the responses.
    while (ok_count < system_size_) {
      for (uint32_t i = 0; i < system_size_; ++i) {
        if (ok[i]) continue;
        c = contexts_[i];
        ok[i] = PollCompletionsOnce(c, wr_id_) > 0;
        if (ok[i]) ++ok_count;
      }
    }

    // Update the current max ballot.
    auto* curr_proposal = &proposed_state_[log_offset_];
    for (uint32_t i = 0; i < system_size_; ++i) {
      if (!ok[i]) continue;
      c = contexts_[i];
      //+ Avoid writing every time?
      curr_proposal->SetMaxBallot(NextBallot(curr_proposal->GetMaxBallot(),
                                             c->scratch_state->GetMaxBallot(),
                                             host_id_, system_size_));
    }
    ROMULUS_DEBUG("<UpdateBallot> Updated ballot: ballot={}",
                  curr_proposal->GetMaxBallot());
    ++wr_id_;
  }
#endif

  State* Prepare() {
    State* curr_proposal = &proposed_state_[log_offset_];
    std::vector<State> expected, swap;
    std::vector<bool> ok, done;
    uint32_t done_count = 0;
    RemoteContext* c;

    ok.resize(system_size_);
    done.resize(system_size_);
    expected.resize(system_size_);
    swap.resize(system_size_);
    for (uint32_t i = 0; i < system_size_; ++i) {
      expected[i] = State();
      swap[i] = State(curr_proposal->GetMaxBallot(), kNullBallot, kNullBallot);
      ok[i] = false;
      done[i] = false;
    }
    // Retry until a quroum succeeds.
    while (done_count < quorum_) {
      uint32_t ok_count = 0;
      // Post CAS ops.
      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done[i]) {
          ++ok_count;
        } else {
          ok[i] = false;
          c = contexts_[i];
          c->log_raddr.addr_info.offset = log_offset_ * kSlotSize;
          romulus::WorkRequest::BuildCAS(c->scratch_laddr, c->log_raddr,
                                         expected[i].raw, swap[i].raw, wr_id_,
                                         &c->wr);
          ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                         "<Prepare> Failed when posting requests.");
          // StageLogRequest(c);
          // ROMULUS_ASSERT(PostRequests(c),
          //                "<Prepare> Failed when posting requests.");
        }
      }

      // Poll for completions
      while (ok_count < system_size_) {
        for (uint32_t i = 0; i < system_size_; ++i) {
          if (done[i] || ok[i]) continue;
          c = contexts_[i];
          ok[i] = PollCompletionsOnce(c->conn, wr_id_);
          if (ok[i]) ++ok_count;
        }
      }

      // Check return value of CAS.
      auto* curr_proposal = &proposed_state_[log_offset_];
      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done[i] || !ok[i]) continue;
        c = contexts_[i];
        if (c->scratch_state->raw == expected[i].raw) {
          // CAS succeeded. Done.
          ++done_count;
          done[i] = true;
        } else if (c->scratch_state->GetMaxBallot() <=
                   curr_proposal->GetMaxBallot()) {
          // CAS failed but ballot is still good. Update proposal and retry.
          swap[i].SetProposal(c->scratch_state->GetBallot(),
                              c->scratch_state->GetValue());
          expected[i] = *(c->scratch_state);
          ROMULUS_DEBUG("<Promise> Updating: swap={}, expected={}",
                        swap[i].ToString(), expected[i].ToString());
        } else {
          // CAS failed and higher ballot. Abort.
          ROMULUS_DEBUG("Failed: state={}", c->scratch_state->ToString());
          return nullptr;
        }
      }
      ++wr_id_;
    }

    // Update current proposal to reflect the highest balloted proposal.
    for (uint32_t i = 0; i < system_size_; ++i) {
      if (swap[i].GetBallot() > curr_proposal->GetBallot()) {
        curr_proposal->SetProposal(swap[i].GetBallot(), swap[i].GetValue());
      }
    }
    ROMULUS_DEBUG("<Prepare> Prepared slot: log_offset={}, state={}",
                  log_offset_, curr_proposal->ToString());
    return curr_proposal;
  }

  bool Promise([[maybe_unused]] Value v) {
    State* curr_proposal = &proposed_state_[log_offset_];
    std::vector<State> expected;
    std::vector<bool> ok, done;
    uint32_t done_count = 0;
    RemoteContext* c;

    ok.resize(system_size_);
    done.resize(system_size_);
    expected.resize(system_size_);
    for (uint32_t i = 0; i < system_size_; ++i) {
      expected[i] = State();
      done[i] = false;
    }

    // Set the value if the prepare phase found no accepted value. Otherwise,
    // update the ballot.
    if (curr_proposal->GetBallot() == kNullBallot) {
      curr_proposal->SetProposal(curr_proposal->GetMaxBallot(), v);
    }

    ROMULUS_DEBUG("<Promise> slot={}, state={}", log_offset_,
                  proposed_state_[log_offset_].ToString());

    // Retry until a quroum succeeds and the local log is written to. Making
    // sure that we write to the local log allows a follower to be certain
    // that if the slot is filled that the value is committed.
    while (done_count < quorum_) {
      // Post CAS ops.
      uint32_t ok_count = 0;
      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done[i]) {
          // Already succeeded.
          ++ok_count;
        } else {
          // Post a request
          ok[i] = false;
          c = contexts_[i];
          c->log_raddr.addr_info.offset = log_offset_ * kSlotSize;
          romulus::WorkRequest::BuildCAS(c->scratch_laddr, c->log_raddr,
                                         expected[i].raw, curr_proposal->raw,
                                         wr_id_, &c->wr);
          ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                         "<Promise> Failed when posting requests.");
        }
      }

      // Poll for completions
      while (ok_count < system_size_) {
        for (uint32_t i = 0; i < system_size_; ++i) {
          if (done[i] || ok[i]) continue;
          c = contexts_[i];
          ok[i] = PollCompletionsOnce(c->conn, wr_id_);
          if (ok[i]) ++ok_count;
        }
      }

      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done[i] || !ok[i]) continue;
        c = contexts_[i];
        if (expected[i].raw == c->scratch_state->raw) {
          // CAS succeeded. Done.
          ++done_count;
          done[i] = true;

        } else if (c->scratch_state->GetMaxBallot() <=
                   curr_proposal->GetMaxBallot()) {
          // CAS failed but ballot is still good. Retry because we already ran
          // the prepare phase.
          expected[i] = *(c->scratch_state);
          ROMULUS_DEBUG("<Promise> Updating expected: expected={}",
                        expected[i].ToString());
        } else {
          // CAS failed on first try (as leader) or a higher ballot is found.
          ROMULUS_DEBUG("<Promise> Failed: state={}",
                        c->scratch_state->ToString());
          return false;
        }
      }
      ++wr_id_;
    }
    return true;
  }

  bool isLeaderStable() override { return stable_leader_; }

  bool isLeader() override { return stable_leader_; }

  void DumpLog() {
#if 0
    for (uint32_t i = 0; i < log_offset_; ++i) {
      ROMULUS_DEBUG("log[{}]: {}", i, log_[i].ToString());
    }
#endif
  }

  // Member variables.
 private:
  // Global arg map
  std::shared_ptr<romulus::ArgMap> args_;

  // Total number of nodes in the system.
  const uint8_t system_size_;

  // Number of slots in the log.
  const uint32_t capacity_;

  // Local view of remotely accessible memory.
  romulus::APArray<State, kSlotSize, CACHE_PREFETCH_SIZE>* raw_;
  romulus::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> scratch_;
  romulus::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> proposed_state_;
  romulus::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> log_;
  romulus::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> leader_;

  // Buffer fields for commit
  [[maybe_unused]] uint8_t* buf_;
  [[maybe_unused]] uint32_t buf_offset_ = 0;
  [[maybe_unused]] uint64_t buf_chunk_size_;

  uint64_t wr_id_ = 0;
  uint32_t log_offset_ = 0;

  // Whether this node thinks its the leader.
  bool is_leader_;
  // Whether there exists a stable leader
  bool stable_leader_;
  // True if multi-paxos optimization has been toggled
  bool multi_paxos_opt_;

  // Hostname used during registration to connect with peers.
  std::string hostname_;

  // The node id of this node.
  uint8_t host_id_;

  // Number of acknowledgements required to reach a quorum.
  const uint8_t quorum_;

  // Names of remote peers.
  std::vector<std::string> peers_;

  std::unordered_map<uint64_t,
                     std::unordered_map<std::string, romulus::RemoteAddr>>
      remote_addrs_;
  std::unordered_map<uint64_t, std::vector<romulus::ReliableConnection*>>
      remote_conns_;

  std::vector<RemoteContext*> contexts_;

  // RDMA related members.
  romulus::Device device_;
  std::unique_ptr<romulus::ConnectionManager> conn_manager_;
  std::unique_ptr<romulus::ConnectionRegistry> registry_;
  romulus::MemBlock memblock_;
  uint64_t buf_size_;
  uint64_t num_qps_;
};

}  // namespace paxos_st