#pragma once

#include <aparray.h>
#include <immintrin.h>
#include <romulus/common.h>
#include <romulus/connection_manager.h>
#include <romulus/device.h>
#include <romulus/romulus.h>
#include <state.h>

#include <cassert>
#include <memory>
#include <numeric>

#define BATCH_SIZE 16

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
constexpr auto kTimeout = std::chrono::nanoseconds(500'000'000);
constexpr uint16_t kNullBallot = 0;
constexpr uint32_t kNullValue = std::numeric_limits<uint32_t>::max();
constexpr uint32_t kShutdown = std::numeric_limits<uint32_t>::max() - 1;
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
  std::string ToString() const {
    std::ostringstream oss;
    oss << "RemoteContext{"
        << "scratch_state=" << static_cast<const void*>(scratch_state) << ", "
        << "proposed_state=" << static_cast<const void*>(proposed_state) << ", "
        << "conn=" << static_cast<const void*>(conn) << ", "
        << "scratch_laddr={addr=" << reinterpret_cast<void*>(scratch_laddr.addr)
        << ", offset=" << scratch_laddr.offset
        << ", length=" << scratch_laddr.length << "}, "
        << "proposal_raddr={addr="
        << reinterpret_cast<void*>(proposal_raddr.addr_info.addr)
        << ", offset=" << proposal_raddr.addr_info.offset
        << ", length=" << proposal_raddr.addr_info.length << "}, "
        << "log_raddr={addr="
        << reinterpret_cast<void*>(log_raddr.addr_info.addr)
        << ", offset=" << log_raddr.addr_info.offset
        << ", length=" << log_raddr.addr_info.length << "}"
        << "}";
    return oss.str();
  }
};

namespace {  // namespace anonymous

template <typename Rep, typename Period>
inline std::chrono::duration<Rep, Period> DoBackoff(
    std::chrono::duration<Rep, Period> backoff) {
  ROMULUS_DEBUG(
      "Backing off for {} us",
      std::chrono::duration_cast<std::chrono::microseconds>(backoff).count());

  if (backoff < std::chrono::microseconds(50)) {
    // if our backoff is small, we invoke pause instruction instead of heavier
    // system call
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < backoff) {
      _mm_pause();
    }
  } else {
    std::this_thread::sleep_for(backoff);
  }

  return std::min(backoff * 2, kTimeout);
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
        preprepare_offset_(0),
        is_leader_(false),
        stable_leader_(args->bget(romulus::STABLE_LEADER)),
        multi_paxos_opt_(args->bget(romulus::MULTIPAX_OPT)),
        hostname_(args->sget(romulus::HOSTNAME)),
        host_id_(args->uget(romulus::NODE_ID)),
        quorum_(romulus::GetQuorum(peers.size() + 1)),
        peers_(peers),
        device_(transport_flag),
        buf_size_(args->uget(romulus::BUF_SIZE)),
        num_qps_(args->uget(romulus::NUM_QP)) {
    expected_.resize(system_size_);
    done_.resize(system_size_);
    polled_.resize(system_size_);
    wr_ids_.resize(system_size_);
  }

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
    uint64_t leader_len = kSlotSize;

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
    Value v;
#ifndef STANDALONE
    v.SetId(host_id_);
    v.SetOffset(buf_offset_);
#endif
    v = *reinterpret_cast<uint32_t*>(buf);
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
    conn_manager_->arrive_strict_barrier();
    ROMULUS_DEBUG("Nodes synced.");
  }
  void CleanUp() override {
    if (is_leader_) {
      State shutdown_msg(0, 0, Value(kShutdown));
      BroadcastLeader(&shutdown_msg);
    }
    CatchUp();
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

  bool BroadcastLeader(State* new_leader) {
    uint32_t ok_count = 0;
    std::vector<bool> ok(system_size_);
    auto laddr = memblock_.GetAddrInfo(kScratchRegionId);
    auto* laddr_raw = reinterpret_cast<uint8_t*>(laddr.addr);

    // Write the new leader meta-data to everyone's slot, including my own
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
    return true;
  }

  State ReadLeaderSlot() {
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
    ROMULUS_ASSERT(c->conn->Post(&c->wr, 1), "Error reading in leader change.");
    // Only expecting a single completion
    while (!PollCompletionsOnce(c->conn, wr_id_));

    State new_leader = *reinterpret_cast<State*>(laddr.addr + laddr.offset);
    ROMULUS_DEBUG("[Leader READ] {}", new_leader.ToString());
    return new_leader;
  }

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
  void ProposeInternal(Value& v) {
    ROMULUS_DEBUG("<ProposeInternal> Starting.");
    auto backoff = std::chrono::nanoseconds(std::rand() % kMaxStartingBackoff);
    bool done = false;
    State* curr_proposal;

    while (!done) {
      bool ok = false;
      ROMULUS_COUNTER_INC("attempts");

      if (multi_paxos_opt_) {
        ROMULUS_DEBUG(
            "Multi-Paxos optimization enabled. Skipping prepare phase...");

        if (!stable_leader_) {
          // Leader election - need to run prepare
          curr_proposal = Prepare();

          Ballot winning_ballot = curr_proposal->GetPromiseBallot();
          uint32_t leader_id = winning_ballot % system_size_;
          is_leader_ = (leader_id == host_id_);

          if (is_leader_) {
            BroadcastLeader(curr_proposal);
          } else {
            leader_ = ReadLeaderSlot();
          }
          stable_leader_ = true;

          // If we lost election, exit and let test loop handle it
          if (!is_leader_) {
            return;
          }
        } else {
          // Multi-paxos optimization on, and we are **stable**
          curr_proposal = &proposed_state_[log_offset_];
        }
      } else {
        // Non-Multi-Paxos: prepare every round
        curr_proposal = Prepare();
        ROMULUS_DEBUG("Node {} completed prepare with curr_proposal={}",
                      host_id_, curr_proposal->ToString());
        if (curr_proposal != nullptr) {
          Ballot winning_ballot = curr_proposal->GetPromiseBallot();
          uint32_t leader_id = winning_ballot % system_size_;
          is_leader_ = (leader_id == host_id_);
        }
      }

      // At this point, we should be the leader (or non-multipaxos with
      // curr_proposal)
      if ((multi_paxos_opt_ && is_leader_) ||
          (!multi_paxos_opt_ && curr_proposal)) {
        ROMULUS_DEBUG("Entering promise phase...");
        ok = Promise(v);
        if (ok) {
          auto committed = log_[log_offset_].GetValue();
          if (committed == v) {
            ROMULUS_DEBUG(
                "Proposed slot committed: value=({}, {}), log_offset={}",
                v.id(), v.offset(), log_offset_);
            is_leader_ = true;
            done = true;
          } else {
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

      // If aborted, backoff and retry
      if (!ok) {
        is_leader_ = false;
        if (multi_paxos_opt_ && stable_leader_) {
          // We lost leadership
          return;
        }
        if (!multi_paxos_opt_) {
          stable_leader_ = false;
        }
        backoff = DoBackoff(backoff);
      }
    }
    ROMULUS_COUNTER_INC("proposed");
  }

  /// @brief Issues RMDA-reads to all replicas, busy-waits until all reads
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
        // If the local log does not reflect the accepted value then CAS it
        // in.
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
              "<TryCatchUp> Failed to post requests when updating local "
              "slot.");

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

  State* Prepare(bool prepreparation = false) override {
    if (prepreparation) {
      fuo_ = preprepare_offset_;
    } else {
      fuo_ = log_offset_;
    }

    State* curr_proposal = &proposed_state_[fuo_];
    Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
    if (local_ballot_ == 0) {
      local_ballot_ = MakeBallot(1);
    }
    curr_promise_ballot = std::max(curr_promise_ballot, local_ballot_);
    curr_proposal->SetPromiseBallot(curr_promise_ballot);

    auto backoff = std::chrono::nanoseconds(std::rand() % kMaxStartingBackoff);
    RemoteContext* c;

    std::fill(expected_.begin(), expected_.end(), State());
    std::fill(done_.begin(), done_.end(), false);
    std::fill(polled_.begin(), polled_.end(), false);

    std::vector<State> swap(system_size_);
    std::vector<State> state(system_size_);
    uint32_t done_count = 0;

    // Init
    for (uint32_t i = 0; i < system_size_; ++i) {
      // expected[i] = State();
      swap[i] = State(curr_promise_ballot, 0, Value(0));
      state[i] = State();
    }

    while (done_count < quorum_) {
      std::fill(polled_.begin(), polled_.end(), false);
      ++wr_id_;

      uint32_t posted = 0;
      // Post CAS ops.
      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done_[i]) continue;
        c = contexts_[i];
        c->log_raddr.addr_info.offset = fuo_ * kSlotSize;
        uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                         (static_cast<uint64_t>(host_id_) << 32) |
                         static_cast<uint64_t>(i);
        wr_ids_.at(i) = wr_id;
        romulus::WorkRequest::BuildCAS(c->scratch_laddr, c->log_raddr,
                                       expected_[i].raw, swap[i].raw, wr_id,
                                       &c->wr);
        ROMULUS_DEBUG(
            "Prepare CAS: scratch_laddr={}, log_raddr={}, expected_i={}, "
            "swap_i={}, wr_id={}",
            c->scratch_laddr.addr, c->log_raddr.addr_info.addr,
            expected_[i].raw, swap[i].raw, wr_id);
        ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                       "<Prepare> Failed when posting requests.");
        ++posted;
      }

      // Poll for completions
      uint32_t completions = 0;
      while (completions < posted) {
        for (uint32_t i = 0; i < system_size_; ++i) {
          if (done_[i] || polled_[i]) continue;
          if (PollCompletionsOnce(contexts_[i]->conn, wr_ids_[i])) {
            polled_[i] = true;
            ++completions;
          }
        }
      }

      // Check return value of CAS.
      bool need_bump = false;
      Ballot observed_max_ballot = curr_promise_ballot;
      uint32_t winning_index = 0;

      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done_[i] || !polled_[i]) continue;
        State observed = *contexts_[i]->scratch_state;

        if (observed.raw == expected_[i].raw) {
          ROMULUS_DEBUG("Prepare: cas success");
          state[i] = observed;
          done_[i] = true;
          ++done_count;
        } else {
          expected_[i] = observed;
          swap[i] = State(curr_promise_ballot, observed.GetBallot(),
                          observed.GetValue());
          state[i] = observed;
          if (observed.GetPromiseBallot() > curr_promise_ballot) {
            need_bump = true;
            observed_max_ballot =
                std::max(observed_max_ballot, observed.GetPromiseBallot());
            winning_index = i;
          }
        }
      }
      // Handle ballot bump after processing all completions
      if (need_bump) {
        if (multi_paxos_opt_) {
          // In multi-paxos we give up -- let them be the leader
          ROMULUS_DEBUG(
              "Prepare: detected higher ballot, yielding to other leader");
          is_leader_ = false;
          // failure condition
          proposed_state_[fuo_] = state[winning_index];
          return &proposed_state_[fuo_];
        }
        ROMULUS_DEBUG("Prepare: cas failed. abort and bump.");
        Ballot unique_ballot = BumpBallot(observed_max_ballot);
        ROMULUS_ASSERT(unique_ballot > observed_max_ballot,
                       "GlobalBallot did not exceed observed promise ballot");

        curr_promise_ballot = unique_ballot;
        curr_proposal->SetPromiseBallot(unique_ballot);

        done_count = 0;
        std::fill(done_.begin(), done_.end(), false);

        for (uint32_t j = 0; j < system_size_; ++j) {
          expected_[j] = State();
          swap[j] = State(curr_promise_ballot, 0, Value(0));

          backoff = DoBackoff(backoff);
        }
      }
    }
    // Reduce over the quorum: adopt highest accepted proposal
    Ballot best_ballot = 0;
    Value best_value = Value(0);

    for (uint32_t i = 0; i < system_size_; ++i) {
      if (!done_[i]) continue;
      // we reduce over the state vector
      if (state[i].GetBallot() > best_ballot) {
        best_ballot = state[i].GetBallot();
        best_value = state[i].GetValue();
      }
    }
    ROMULUS_DEBUG("Prepared slot: log_offset={}, state={}", fuo_,
                  curr_proposal->ToString());
    if (prepreparation) {
      preprepare_offset_++;
    }
    return curr_proposal;
  }

  bool Promise(Value v, bool prepreparation = false) override {
    // std::chrono::_V2::steady_clock::time_point t0, t1, t2, t3, t4;
    // t0 = std::chrono::steady_clock::now();

    State* curr_proposal = &proposed_state_[log_offset_];
    Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
    RemoteContext* c;
    uint32_t done_count = 0;

    // Metadata to indicate to indicate a SUCCESSFUL cas for the given
    // acceptor
    // Metadata to indicate whether a CAS for a given acceptor has been POLLED
    std::fill(expected_.begin(), expected_.end(), State());
    std::fill(done_.begin(), done_.end(), false);
    std::fill(polled_.begin(), polled_.end(), false);

    // We install the chose value if it hasn't already been set
    if (curr_proposal->GetBallot() == 0) {
      curr_proposal->SetProposal(curr_promise_ballot, v);
    }

    ROMULUS_DEBUG("<Promise> slot={}, state={}", log_offset_,
                  proposed_state_[log_offset_].ToString());

    // Retry until a quroum succeeds and the local log is written to. Making
    // sure that we write to the local log allows a follower to be certain
    // that if the slot is filled that the value is committed.
    while (done_count < quorum_) {
      // t1 = std::chrono::steady_clock::now();
      // Post CAS ops.
      uint32_t posted = 0;
      for (uint32_t i = 0; i < system_size_; ++i) {
        // Already succeeded.
        if (done_[i]) continue;
        // Post a request
        polled_[i] = false;
        c = contexts_[i];
        c->log_raddr.addr_info.offset = log_offset_ * kSlotSize;

        uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                         (static_cast<uint64_t>(host_id_) << 32) |
                         static_cast<uint64_t>(i);
        wr_ids_.at(i) = wr_id;
        romulus::WorkRequest::BuildCAS(c->scratch_laddr, c->log_raddr,
                                       expected_[i].raw, curr_proposal->raw,
                                       wr_id, &c->wr);
        ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                       "<Promise> Failed when posting requests.");
        ++posted;
      }
      // t2 = std::chrono::steady_clock::now();
      // Poll for completions
      uint32_t completions = 0;
      while (completions < posted) {
        for (uint32_t i = 0; i < system_size_; ++i) {
          if (done_[i] || polled_[i]) continue;
          c = contexts_[i];
          if (PollCompletionsOnce(c->conn, wr_ids_[i])) {
            polled_[i] = true;
            ++completions;
          }
        }
      }
      // t3 = std::chrono::steady_clock::now();

      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done_[i] || !polled_[i]) continue;
        c = contexts_[i];
        // This will the result of the previous CAS
        State observed = *c->scratch_state;

        if (expected_[i].raw == observed.raw) {
          ROMULUS_DEBUG(
              "<Promise> CAS success! observed={}, expected={}, "
              "curr_proposal={}",
              observed.ToString(), expected_[i].ToString(),
              curr_proposal->ToString());
          // CAS succeeded. Done.
          done_[i] = true;
          ++done_count;
        } else if (observed.GetPromiseBallot() > curr_promise_ballot) {
          ROMULUS_DEBUG(
              "<Promise> CAS failure Case 1: Seen higher ballot, abort."
              "observed={}, expected={}, curr_proposal={}",
              observed.ToString(), expected_[i].ToString(),
              curr_proposal->ToString());
          // We will make an assumption the that
          stable_leader_ = true;
          return false;
        } else {
          ROMULUS_DEBUG(
              "<Promise> CAS failure Case 2: Ballot still good. retry."
              "observed={}, expected={}, curr_proposal={}",
              observed.ToString(), expected_[i].ToString(),
              curr_proposal->ToString());
          expected_[i] = observed;
        }
      }
      // t4 = std::chrono::steady_clock::now();
      ++wr_id_;
    }
    // if (log_offset_ % 200) {
    //   ROMULUS_INFO(
    //       "Promise: init={}ns post={}ns poll={}ns check={}ns total={}ns",
    //       (t1 - t0).count(), (t2 - t1).count(), (t3 - t2).count(),
    //       (t4 - t3).count(), (t4 - t0).count());
    // }
    log_[log_offset_] = *curr_proposal;
    if(prepreparation){
      ++log_offset_;
    }
    return true;
  }

  // decode the ballot (round * sys_size + host_id)
  // Note, the division gets rid of the host_id constant
  Ballot BumpBallot(Ballot observed_ballot) {
    auto observed_round = observed_ballot / system_size_;
    auto my_round = local_ballot_ / system_size_;
    auto next_round = std::max(observed_round + 1, my_round + 1);

    auto new_ballot = MakeBallot(next_round);
    local_ballot_ = static_cast<Ballot>(new_ballot);
    return local_ballot_;
  }

  Ballot MakeBallot(uint32_t round) {
    uint32_t b = round * system_size_ + host_id_;
    ROMULUS_ASSERT(b <= std::numeric_limits<uint16_t>::max(),
                   "Ballot overflow: round={}, system_size={}", round,
                   system_size_);
    return static_cast<Ballot>(b);
  }

  bool isLeaderStable() override { return stable_leader_; }

  bool isLeader() override { return is_leader_; }

  inline Ballot GlobalBallot() {
    uint64_t epoch = 0;
    registry_->Fetch_and_Add("paxos_epoch", 1, &epoch);
    // This allows for a maximum of 15 proposers
    ROMULUS_ASSERT(epoch < (1ULL << 12), "Ballot overflow");
    epoch = (epoch << 4) | host_id_;
    // truncate the first 48 bits of the word
    return static_cast<Ballot>(epoch);
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
  State leader_;

  // Buffer fields for commit
  [[maybe_unused]] uint8_t* buf_;
  [[maybe_unused]] uint32_t buf_offset_ = 0;
  [[maybe_unused]] uint64_t buf_chunk_size_;

  uint64_t wr_id_ = 0;
  uint32_t log_offset_ = 0;
  uint32_t preprepare_offset_ = 0;
  uint32_t fuo_ = 0;

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

  Ballot local_ballot_ = 0;

  std::vector<State> expected_;
  // Metadata to indicate to indicate a SUCCESSFUL cas for the given
  // acceptor
  std::vector<bool> done_;
  // Metadata to indicate whether a CAS for a given acceptor has been POLLED
  std::vector<bool> polled_;
  std::vector<uint64_t> wr_ids_;

  // RDMA related members.
  romulus::Device device_;
  std::unique_ptr<romulus::ConnectionManager> conn_manager_;
  std::unique_ptr<romulus::ConnectionRegistry> registry_;
  romulus::MemBlock memblock_;
  uint64_t buf_size_;
  uint64_t num_qps_;
};

}  // namespace paxos_st