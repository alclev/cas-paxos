#pragma once

#include <aparray.h>
#include <immintrin.h>
#include <romulus/common.h>
#include <romulus/connection_manager.h>
#include <romulus/device.h>
#include <romulus/romulus.h>
#include <state.h>

#include <atomic>
#include <cassert>
#include <memory>
#include <numeric>

#define BATCH_SIZE 16

// IDs used when registering RDMA connections.
const std::string kRegistryName = "Velos";
const std::string kPdId = "PdId";
const std::string kBlockId = "LogBlock";
const std::string kScratchPrepareId = "PrepareScratch";
const std::string kScratchPromiseId = "PromiseScratch";
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
constexpr uint16_t kNullBallot = 0;
constexpr uint32_t kNullValue = std::numeric_limits<uint32_t>::max();
constexpr uint32_t kShutdown = std::numeric_limits<uint32_t>::max() - 1;
constexpr uint32_t kMaxStartingBackoff = 3600;
// Upper bound on pipeline depth; 8 bits of the wr_id encode the window slot.
constexpr uint32_t kMaxPipeDepth = 255;

struct RemoteContext {
  State *scratch_state;
  State *proposed_state;

  romulus::ReliableConnection *conn;

  // Log related info
  romulus::AddrInfo scratch_laddr;
  romulus::RemoteAddr proposal_raddr;
  romulus::RemoteAddr log_raddr;

  // Head of WR chain
  romulus::WorkRequest wr;
  RemoteContext() {}
  RemoteContext(const RemoteContext &c)
      : scratch_state(c.scratch_state), proposed_state(c.proposed_state),
        conn(c.conn), scratch_laddr(c.scratch_laddr),
        proposal_raddr(c.proposal_raddr), log_raddr(c.log_raddr), wr(c.wr) {}
  std::string ToString() const {
    std::ostringstream oss;
    oss << "RemoteContext{"
        << "scratch_state=" << static_cast<const void *>(scratch_state) << ", "
        << "proposed_state=" << static_cast<const void *>(proposed_state)
        << ", "
        << "conn=" << static_cast<const void *>(conn) << ", "
        << "scratch_laddr={addr="
        << reinterpret_cast<void *>(scratch_laddr.addr)
        << ", offset=" << scratch_laddr.offset
        << ", length=" << scratch_laddr.length << "}, "
        << "proposal_raddr={addr="
        << reinterpret_cast<void *>(proposal_raddr.addr_info.addr)
        << ", offset=" << proposal_raddr.addr_info.offset
        << ", length=" << proposal_raddr.addr_info.length << "}, "
        << "log_raddr={addr="
        << reinterpret_cast<void *>(log_raddr.addr_info.addr)
        << ", offset=" << log_raddr.addr_info.offset
        << ", length=" << log_raddr.addr_info.length << "}"
        << "}";
    return oss.str();
  }
};

class Velos {
public:
  explicit Velos(std::shared_ptr<romulus::ArgMap> args,
                 std::vector<std::string> peers, uint8_t transport_flag)
      : args_(args), system_size_(peers.size() + 1),
        capacity_(args->uget(romulus::CAPACITY)), wr_id_(0), prepare_offset_(0),
        promise_offset_(0), is_leader_(false),
        stable_leader_(args->bget(romulus::STABLE_LEADER)),
        hostname_(args->sget(romulus::HOSTNAME)),
        host_id_(args->uget(romulus::NODE_ID)),
        quorum_(romulus::GetQuorum(peers.size() + 1)), peers_(peers),
        device_(transport_flag), buf_size_(args->uget(romulus::BUF_SIZE)),
        num_qps_(args->uget(romulus::NUM_QP)),
        num_shared_cq_(args->uget(romulus::NUM_SHARED_CQ)),
        pipe_depth_(args->uget(romulus::OUTSTANDING_REQS)) {}

  ~Velos() {
    delete raw_;
    SyncNodes();
  }

  void Init(std::string_view dev_name, int dev_port,
            std::unique_ptr<romulus::ConnectionRegistry> registry,
            std::unordered_map<uint64_t, std::string> mach_map) {
    // Set up remotely accessible memory.
    ROMULUS_DEBUG("Initializing Velos...");
    ROMULUS_DEBUG("Quorum size: {}", quorum_);
    ROMULUS_DEBUG("Opening device with name {} on port {}", dev_name, dev_port);
    ROMULUS_ASSERT(device_.Open(dev_name, dev_port), "Failed to open device.");
    device_.AllocatePd(kPdId);

    ROMULUS_INFO("Registering remotely accessible memory");
    ROMULUS_ASSERT(pipe_depth_ >= 1 && pipe_depth_ <= kMaxPipeDepth,
                   "pipe_depth={} out of range [1,{}]", pipe_depth_,
                   kMaxPipeDepth);
    // Layout: [prep scratch | pipe scratch | proposed | log]
    //   prep scratch: n slots      -- Prepare() (background thread)
    //   pipe scratch: n * D slots  -- Promise_Single()/Promise_Pipe(); slot
    //                                 k = d * n + i holds the CAS result from
    //                                 replica i for window position d
    uint64_t prep_scratch_len = system_size_ * kSlotSize;
    uint64_t pipe_scratch_len = system_size_ * pipe_depth_ * kSlotSize;
    uint64_t scratch_len = prep_scratch_len + pipe_scratch_len;
    uint64_t proposal_len = capacity_ * kSlotSize;
    uint64_t log_len = capacity_ * kSlotSize;

    ROMULUS_ASSERT(buf_size_ % kSlotSize == 0,
                   "Buf size not being a multiple of {} is not supported!",
                   kSlotSize);
    // Two QPs per peer on two distinct CQs: the prepare thread and the
    // promise thread must never poll the same CQ.
    num_qps_ = 2;
    num_shared_cq_ = 0;

    // NB: the following configuration assumes STANDALONE
    std::size_t remote_len = scratch_len + proposal_len + log_len;
    raw_ =
      new romulus::APArray<State, kSlotSize, CACHE_PREFETCH_SIZE>(remote_len);
    std::memset(raw_->Get(), 0, raw_->GetTotalBytes());

    // Constructing the memblock
    auto pd = device_.GetPd(kPdId);
    memblock_ = romulus::MemBlock(
      kBlockId, pd, reinterpret_cast<uint8_t *>(raw_->Get()), remote_len);

    // Registering memblock regions
    memblock_.RegisterMemRegion(kScratchPrepareId, 0, prep_scratch_len);
    memblock_.RegisterMemRegion(kScratchPromiseId, prep_scratch_len,
                                pipe_scratch_len);
    memblock_.RegisterMemRegion(kProposedRegionId, scratch_len, proposal_len);
    memblock_.RegisterMemRegion(kLogRegionId, scratch_len + proposal_len,
                                log_len);

    // Optionally set up AParray for fast access to local views of log memory
#ifndef STANDALONE
    // Not implemented...
#endif

    // Set up local view of log memory (offsets match the regions above, so
    // log_[] IS the RDMA-visible log).
    scratch_ = romulus::APArraySlice(raw_, prep_scratch_len, scratch_len);
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
      hostname_, registry_.get(), host_id_, system_size_, num_qps_,
      num_shared_cq_);

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
    std::vector<std::string> regions = {kScratchPrepareId, kScratchPromiseId,
                                        kProposedRegionId, kLogRegionId};
    for (auto &m : mach_map) {
      // <region_id, remote_addr>
      std::unordered_map<std::string, romulus::RemoteAddr> tmp_addrs;
      // We need to account for all the remotely visible regions
      for (auto &r : regions) {
        // If the machine id maps to **this** node, then this will represent the
        // loopback addr
        conn_manager_->GetRemoteAddr(m.first, kBlockId, r, &remote_addr);
        tmp_addrs.emplace(r, remote_addr);
      }
      // <machine_id, map<region, remote_addr>>
      remote_addrs_.emplace(m.first, tmp_addrs);
      // Note that having available multiple QP's does not do much unless
      // there is concurrent access to them
      std::vector<romulus::ReliableConnection *> conns;
      // here, the 0th index **is the loopback**
      if (m.second == hostname_) {
        for (uint64_t q = 0; q < num_qps_; ++q) {
          uint64_t loopback_id = (q == 0) ? 0 : romulus::kLoopback - q;
          auto conn = conn_manager_->GetConnection(m.first, loopback_id);
          ROMULUS_ASSERT(conn != nullptr, "Missing loopback conn q={} id={}", q,
                         loopback_id);
          conns.push_back(conn);
        }
      } else {
        for (int q = 1; q < (int)num_qps_ + 1; ++q) {
          auto conn = conn_manager_->GetConnection(m.first, q);
          ROMULUS_ASSERT(conn != nullptr, "Missing conn to {} q={}", m.first,
                         q);
          conns.push_back(conn);
        }
      }
      remote_conns_.emplace(m.first, conns);
    }
    // Initialize the contexts with the cached addresses
    prepare_contexts_.reserve(system_size_);
    for (int i = 0; i < system_size_; ++i) {
      prepare_contexts_.push_back(new RemoteContext());
      RemoteContext *context = prepare_contexts_[i];
      context->proposed_state = &proposed_state_[0];
      // This represents the first loopback that we will use for prepare
      context->conn = remote_conns_[i].front();
      context->scratch_laddr = memblock_.GetAddrInfo(kScratchPrepareId);
      context->scratch_laddr.offset = kSlotSize * i;
      context->scratch_laddr.length = sizeof(State);

      context->scratch_state = reinterpret_cast<State *>(
        context->scratch_laddr.addr + context->scratch_laddr.offset);

      context->log_raddr = remote_addrs_[i][kLogRegionId];
      context->log_raddr.addr_info.length = sizeof(State);

      context->proposal_raddr = remote_addrs_[i][kProposedRegionId];
      context->proposal_raddr.addr_info.length = sizeof(State);
    }

    promise_contexts_.reserve(system_size_);
    for (int i = 0; i < system_size_; ++i) {
      promise_contexts_.push_back(new RemoteContext());
      RemoteContext *context = promise_contexts_[i];
      context->proposed_state = &proposed_state_[0];
      // This represents the second loopback connection that we will use for
      // promise
      context->conn = remote_conns_[i][1];

      context->scratch_laddr = memblock_.GetAddrInfo(kScratchPromiseId);
      context->scratch_laddr.offset = kSlotSize * i;
      context->scratch_laddr.length = sizeof(State);

      context->scratch_state = reinterpret_cast<State *>(
        context->scratch_laddr.addr + context->scratch_laddr.offset);

      context->log_raddr = remote_addrs_[i][kLogRegionId];
      context->log_raddr.addr_info.length = sizeof(State);

      context->proposal_raddr = remote_addrs_[i][kProposedRegionId];
      context->proposal_raddr.addr_info.length = sizeof(State);
    }

    // Pipelined-promise state. Promise_Single() uses row d=0 of the pipe
    // scratch through promise_contexts_; Promise_Pipe() addresses rows
    // directly via pipe_laddr_.
    pipe_laddr_ = memblock_.GetAddrInfo(kScratchPromiseId);
    pipe_laddr_.length = kSlotSize;
    pipe_expected_.resize(pipe_depth_);
    pipe_vals_.resize(pipe_depth_);
    // Optionally dump the contents of our cached maps...
#ifdef SYSDUMP
    // Dump the remote addresses
    for (auto &m : remote_addrs_) {
      for (auto &r : m.second) {
        if (m.first == host_id_)
          ROMULUS_INFO("[MAP] Machine={} (loopback)\tRegion={}\tAddr={:x}",
                       m.first, r.first, r.second.addr_info.addr);
        else
          ROMULUS_INFO("[MAP] Machine={}\tRegion={}\tAddr={:x}", m.first,
                       r.first, r.second.addr_info.addr);
      }
    }
    // Dump the connections
    for (auto &c : remote_conns_) {
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

  void Reset() {
    prepare_offset_ = 0;
    promise_offset_ = 0;
    pipe_outstanding_ = 0;
  }

  void Propose(Value &v) {
    //     Value v;
    // #ifndef STANDALONE
    //     v.SetId(host_id_);
    //     v.SetOffset(buf_offset_);
    // #endif
    //     v = *reinterpret_cast<uint32_t *>(buf);
    //     ProposeInternal(v);
    if (pipe_depth_ == 1)
      Promise_Single(v);
    else
      Promise_Pipe(v);
  }

  void CatchUp() {
    if (is_leader_)
      return;
    ROMULUS_DEBUG("<CatchUp> Catching up");
    while (TryCatchUp()) {
      ROMULUS_COUNTER_INC("skipped");
    }
  }
  void SyncNodes() {
    ROMULUS_DEBUG("Syncing nodes");
    conn_manager_->arrive_strict_barrier();
    ROMULUS_DEBUG("Nodes synced.");
  }
  void CleanUp() {
    ROMULUS_COUNTER_ACC("p1_aborts");
    ROMULUS_COUNTER_ACC("p2_aborts");
    ROMULUS_COUNTER_ACC("attempts");
    ROMULUS_COUNTER_ACC("skipped");
    ROMULUS_COUNTER_ACC("proposed");
    ROMULUS_COUNTER_ACC("pipe_fallback");
    ROMULUS_INFO("!> p1_aborts={}", ROMULUS_COUNTER_GET("p1_aborts"));
    ROMULUS_INFO("!> p2_aborts={}", ROMULUS_COUNTER_GET("p2_aborts"));
    ROMULUS_INFO("!> attempts={}", ROMULUS_COUNTER_GET("attempts"));
    ROMULUS_INFO("!> skipped={}", ROMULUS_COUNTER_GET("skipped"));
    ROMULUS_INFO("!> proposed={}", ROMULUS_COUNTER_GET("proposed"));
    ROMULUS_INFO("!> pipe_fallback={}", ROMULUS_COUNTER_GET("pipe_fallback"));
  }

  State *Prepare() {
    const uint32_t prep_slot = prepare_offset_.load(std::memory_order_relaxed);
    if (prep_slot >= capacity_)
      return nullptr; // log full; don't run off the end of proposed_state_
    State *curr_proposal = &proposed_state_[prep_slot];
    Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
    if (local_ballot_ == 0) {
      local_ballot_ = MakeBallot(1);
    }
    curr_promise_ballot = std::max(curr_promise_ballot, local_ballot_);
    curr_proposal->SetPromiseBallot(curr_promise_ballot);

    auto backoff = std::chrono::nanoseconds(std::rand() % kMaxStartingBackoff);
    RemoteContext *c;

    std::vector<State> expected(system_size_);
    std::vector<bool> done(system_size_);
    std::vector<bool> polled(system_size_, false);
    std::vector<State> swap(system_size_);
    std::vector<State> state(system_size_);
    std::vector<uint64_t> wr_ids(system_size_);
    uint32_t done_count = 0;

    // Init. `expected` must equal what Init() wrote into the log slot so the
    // uncontended CAS succeeds on the first round.
    for (uint32_t i = 0; i < system_size_; ++i) {
      expected[i] = State(0, kNullBallot, kNullValue);
      swap[i] = State(curr_promise_ballot, 0, Value(0));
      state[i] = State();
    }

    while (done_count < quorum_) {
      if (dump_requested_.load(std::memory_order_relaxed))
        return nullptr;
      std::fill(polled.begin(), polled.end(), false);
      ++wr_id_;

      uint32_t posted = 0;
      // Post CAS ops.
      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done[i])
          continue;
        c = prepare_contexts_[i];
        c->log_raddr.addr_info.offset = prep_slot * kSlotSize;
        uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                         (static_cast<uint64_t>(host_id_) << 32) |
                         static_cast<uint64_t>(i);
        wr_ids.at(i) = wr_id;
        romulus::WorkRequest::BuildCAS(c->scratch_laddr, c->log_raddr,
                                       expected[i].raw, swap[i].raw, wr_id,
                                       &c->wr);
        ROMULUS_DEBUG(
          "Prepare CAS: scratch_laddr={}, log_raddr={}, expected_i={}, "
          "swap_i={}, wr_id={}",
          c->scratch_laddr.addr, c->log_raddr.addr_info.addr, expected[i].raw,
          swap[i].raw, wr_id);
        ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                       "<Prepare> Failed when posting requests.");
        ++posted;
      }

      // Poll for completions
      uint32_t completions = 0;
      while (completions < posted) {
        for (uint32_t i = 0; i < system_size_; ++i) {
          if (done[i] || polled[i])
            continue;
          if (PollCompletionsOnce(prepare_contexts_[i]->conn, wr_ids[i])) {
            polled[i] = true;
            ++completions;
          }
        }
      }

      // Check return value of CAS.
      bool need_bump = false;
      Ballot observed_max_ballot = curr_promise_ballot;
      [[maybe_unused]] uint32_t winning_index = 0;

      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done[i] || !polled[i])
          continue;
        State observed = *prepare_contexts_[i]->scratch_state;

        if (observed.raw == expected[i].raw) {
          ROMULUS_DEBUG("Prepare: Cas success observed = expected = {}",
                        observed.raw);
          state[i] = observed;
          done[i] = true;
          ++done_count;
        } else {
          ROMULUS_DEBUG("Prepare: Cas failed: observed={}, expected={}",
                        observed.raw, expected[i].raw);
          expected[i] = observed;
          swap[i] = State(curr_promise_ballot, observed.GetBallot(),
                          observed.GetValue());
          state[i] = observed;
          if (observed.GetPromiseBallot() > curr_promise_ballot) {
            ROMULUS_DEBUG(
              "Prepare: observed higher ballot, bump and try again...");
            need_bump = true;
            observed_max_ballot =
              std::max(observed_max_ballot, observed.GetPromiseBallot());
            winning_index = i;
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

        done_count = 0;
        std::fill(done.begin(), done.end(), false);

        for (uint32_t j = 0; j < system_size_; ++j) {
          expected[j] = State();
          swap[j] = State(curr_promise_ballot, 0, Value(0));

          backoff = DoBackoff(backoff);
        }
      }
    }
    // Reduce over the quorum: adopt highest accepted proposal
    Ballot best_ballot = 0;
    Value best_value = Value(0);

    for (uint32_t i = 0; i < system_size_; ++i) {
      if (!done[i])
        continue;
      // we reduce over the state vector
      if (state[i].GetBallot() > best_ballot) {
        best_ballot = state[i].GetBallot();
        best_value = state[i].GetValue();
      }
    }
    if (best_ballot > 0) {
      curr_proposal->SetProposal(best_ballot, best_value);
    } else {
      curr_proposal->SetBallot(0);
      curr_proposal->SetValue(Value(0));
    }

    ROMULUS_DEBUG("Prepared slot: log_offset={}, state={}", prep_slot,
                  curr_proposal->ToString());

    // Publish: proposed_state_[prep_slot] is complete before the promise
    // thread can observe prepare_offset_ > prep_slot.
    prepare_offset_.store(prep_slot + 1, std::memory_order_release);
    return curr_proposal;
  }

  bool Promise_Single(Value &v) {
    ROMULUS_ASSERT(promise_offset_ <
                     prepare_offset_.load(std::memory_order_acquire),
                   "<Promise> slot {} not prepared.", promise_offset_);
    // std::chrono::_V2::steady_clock::time_point t0, t1, t2, t3, t4;
    // t0 = std::chrono::steady_clock::now();

    State *curr_proposal = &proposed_state_[promise_offset_];
    Ballot curr_promise_ballot = curr_proposal->GetPromiseBallot();
    RemoteContext *c;
    uint32_t done_count = 0;

    // `expected` must equal what Prepare() CAS'd into the slot so the
    // uncontended CAS succeeds on the first round.
    std::vector<State> expected(system_size_,
                                State(curr_promise_ballot, 0, Value(0)));
    std::vector<bool> done(system_size_, false);
    std::vector<bool> polled(system_size_, false);
    std::vector<uint64_t> wr_ids(system_size_);

    // We install the chose value if it hasn't already been set
    if (curr_proposal->GetBallot() == 0) {
      curr_proposal->SetProposal(curr_promise_ballot, v);
    }

    ROMULUS_DEBUG("<Promise> slot={}, state={}", promise_offset_,
                  proposed_state_[promise_offset_].ToString());

    // Retry until a quroum succeeds and the local log is written to. Making
    // sure that we write to the local log allows a follower to be certain
    // that if the slot is filled that the value is committed.
    while (done_count < quorum_) {
      if (dump_requested_.load(std::memory_order_relaxed))
        return false;
      // t1 = std::chrono::steady_clock::now();
      // Post CAS ops.
      uint32_t posted = 0;
      for (uint32_t i = 0; i < system_size_; ++i) {
        // Already succeeded.
        if (done[i])
          continue;
        // Post a request
        polled[i] = false;
        c = promise_contexts_[i];
        c->log_raddr.addr_info.offset = promise_offset_ * kSlotSize;

        uint64_t wr_id = (static_cast<uint64_t>(wr_id_) << 48) |
                         (static_cast<uint64_t>(host_id_) << 32) |
                         static_cast<uint64_t>(i);
        wr_ids.at(i) = wr_id;
        romulus::WorkRequest::BuildCAS(c->scratch_laddr, c->log_raddr,
                                       expected[i].raw, curr_proposal->raw,
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
          if (done[i] || polled[i])
            continue;
          c = promise_contexts_[i];
          if (PollCompletionsOnce(c->conn, wr_ids[i])) {
            polled[i] = true;
            ++completions;
          }
        }
      }
      // t3 = std::chrono::steady_clock::now();

      for (uint32_t i = 0; i < system_size_; ++i) {
        if (done[i] || !polled[i])
          continue;
        c = promise_contexts_[i];
        // This will the result of the previous CAS
        State observed = *c->scratch_state;

        if (expected[i].raw == observed.raw) {
          ROMULUS_DEBUG("<Promise> CAS success! observed={}, expected={}, "
                        "curr_proposal={}",
                        observed.ToString(), expected[i].ToString(),
                        curr_proposal->ToString());
          // CAS succeeded. Done.
          done[i] = true;
          ++done_count;
        } else if (observed.GetPromiseBallot() > curr_promise_ballot) {
          ROMULUS_DEBUG(
            "<Promise> CAS failure Case 1: Seen higher ballot, abort."
            "observed={}, expected={}, curr_proposal={}",
            observed.ToString(), expected[i].ToString(),
            curr_proposal->ToString());
          // We will make an assumption the that
          stable_leader_ = true;
          return false;
        } else {
          ROMULUS_DEBUG(
            "<Promise> CAS failure Case 2: Ballot still good. retry."
            "observed={}, expected={}, curr_proposal={}",
            observed.ToString(), expected[i].ToString(),
            curr_proposal->ToString());
          expected[i] = observed;
        }
      }
      // t4 = std::chrono::steady_clock::now();
      ++wr_id_;
    }
    // if (promise_offset_ % 200) {
    //   ROMULUS_INFO(
    //       "Promise: init={}ns post={}ns poll={}ns check={}ns total={}ns",
    //       (t1 - t0).count(), (t2 - t1).count(), (t3 - t2).count(),
    //       (t4 - t3).count(), (t4 - t0).count());
    // }
    log_[promise_offset_] = *curr_proposal;
    promise_offset_++;
    return true;
  }

  // Fast path only. Each call posts the Phase-2 CAS for ONE slot to every
  // replica. Nothing is polled until pipe_depth_ slots are outstanding; the
  // D-th call drains all of them, quorum-checks in order, and commits. A slot
  // short of quorum falls back to Promise_Single() (serial retry loop).
  // Fast path only. Each call posts the Phase-2 CAS for ONE slot to every
  // replica. Nothing is polled until pipe_depth_ slots are outstanding; the
  // D-th call drains all of them, quorum-checks in order, and commits. A slot
  // short of quorum falls back to Promise_Single() (serial retry loop).
  bool Promise_Pipe(Value &v) {
    const uint32_t n = system_size_;
    const uint32_t d = pipe_outstanding_; // position in the window
    const uint32_t slot = promise_offset_ + d;

    ROMULUS_ASSERT(slot < capacity_, "Promise exhausted log.");
    // The log is prepared in full before the first Propose(), so this holds
    // on arrival. It replaced a spin on prepare_offset_, which deadlocked
    // whenever the preparer's lead was capped below pipe_depth_.
    ROMULUS_ASSERT(prepare_offset_.load(std::memory_order_acquire) > slot,
                   "Slot {} not prepared (prepare_offset={})", slot,
                   prepare_offset_.load(std::memory_order_acquire));

    if (d == 0)
      ++pipe_seq_;
    State *p = &proposed_state_[slot];
    Ballot pb = p->GetPromiseBallot();
    if (p->GetBallot() == 0)
      p->SetProposal(pb, v);
    pipe_vals_[d] = v;
    pipe_expected_[d] = State(pb, 0, Value(0)); // what Prepare() installed

    // ---- post (every call) ----
    for (uint32_t i = 0; i < n; ++i) {
      uint32_t k = d * n + i;
      RemoteContext *c = promise_contexts_[i];
      c->log_raddr.addr_info.offset = slot * kSlotSize;
      romulus::AddrInfo laddr = pipe_laddr_;
      laddr.offset = k * kSlotSize;
      uint64_t wr_id = (pipe_seq_ << 48) | (static_cast<uint64_t>(d) << 40) |
                       (static_cast<uint64_t>(host_id_) << 32) | i;
      romulus::WorkRequest::BuildCAS(laddr, c->log_raddr, pipe_expected_[d].raw,
                                     p->raw, wr_id, &c->wr);
      ROMULUS_ASSERT(c->conn->Post(&c->wr, 1),
                     "<Promise_Pipe> Failed when posting requests.");
    }
    if (++pipe_outstanding_ < pipe_depth_)
      return true; // mid-pipeline: post only

    // ---- drain (every D-th call) ----
    const uint32_t depth = pipe_outstanding_;
    pipe_outstanding_ = 0;
    uint32_t remaining = depth * n;
    const uint64_t seq_tag = pipe_seq_ & 0xFFFF;
    struct ibv_wc wcs[16];
    while (remaining > 0) {
      for (uint32_t i = 0; i < n; ++i) {
        int cnt = ibv_poll_cq(promise_contexts_[i]->conn->GetCQ(), 16, wcs);
        ROMULUS_ASSERT(cnt >= 0, "<Promise_Pipe> ibv_poll_cq failed");
        for (int c = 0; c < cnt; ++c) {
          ROMULUS_ASSERT(wcs[c].status == IBV_WC_SUCCESS,
                         "<Promise_Pipe> CAS to {} failed: {}", i,
                         ibv_wc_status_str(wcs[c].status));
          if ((wcs[c].wr_id >> 48) == seq_tag)
            --remaining;
        }
      }
    }

    // ---- commit in order ----
    for (uint32_t j = 0; j < depth; ++j) {
      uint32_t good = 0;
      for (uint32_t i = 0; i < n; ++i) {
        uint32_t k = j * n + i;
        State observed =
          *reinterpret_cast<State *>(pipe_laddr_.addr + k * kSlotSize);
        if (observed.raw == pipe_expected_[j].raw)
          ++good;
      }
      if (good >= quorum_) {
        log_[promise_offset_] = proposed_state_[promise_offset_];
        ++promise_offset_;
      } else {
        ROMULUS_DEBUG("<Promise_Pipe> slot {} got {}/{}, serial fallback",
                      promise_offset_, good, quorum_);
        ROMULUS_COUNTER_INC("pipe_fallback");
        if (!Promise_Single(pipe_vals_[j])) // operates on promise_offset_
          return false;
      }
    }
    return true;
  }

  void DumpLogs() {
    ROMULUS_INFO("Dumping proposed region...");
    auto loopback_context = prepare_contexts_[host_id_];
    loopback_context->scratch_laddr.offset = 0;
    for (uint64_t i = 0; i <= prepare_offset_; ++i) {
      loopback_context->proposal_raddr.addr_info.offset = i * kSlotSize;
      romulus::WorkRequest::BuildRead(loopback_context->scratch_laddr,
                                      loopback_context->proposal_raddr, wr_id_,
                                      &loopback_context->wr);
      ROMULUS_ASSERT(loopback_context->conn->Post(&loopback_context->wr, 1),
                     "Dump: Failed to post requests when updating local "
                     "slot.");
      while (!PollCompletionsOnce(loopback_context->conn, wr_id_))
        ;
      State result =
        *reinterpret_cast<State *>(loopback_context->scratch_laddr.addr);
      ROMULUS_INFO("Proposed_i={}", result.ToString());
    }

    ROMULUS_INFO("Dumping log region...");
    for (uint64_t i = 0; i <= prepare_offset_; ++i) {
      loopback_context->log_raddr.addr_info.offset = i * kSlotSize;
      romulus::WorkRequest::BuildRead(loopback_context->scratch_laddr,
                                      loopback_context->log_raddr, wr_id_,
                                      &loopback_context->wr);
      ROMULUS_ASSERT(loopback_context->conn->Post(&loopback_context->wr, 1),
                     "Dump: Failed to post requests when updating local "
                     "slot.");
      while (!PollCompletionsOnce(loopback_context->conn, wr_id_))
        ;
      State result =
        *reinterpret_cast<State *>(loopback_context->scratch_laddr.addr);
      ROMULUS_INFO("Log_i={}", result.ToString());
    }
  }

  uint32_t PrepareOffset() {
    // ROMULUS_INFO("PrepareOffset={}", prepare_offset_.load());
    return prepare_offset_.load();
  }

private:
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
  void ProposeInternal([[maybe_unused]] Value &v) {
    ROMULUS_DEBUG("<ProposeInternal> Starting.");
  }

  /// @brief Issues RMDA-reads to all replicas, busy-waits until all reads
  /// complete. Scan's scratch buffers for quorum agreement -- if committed,
  /// ammend local log with RDMA CAS
  /// @return bool
  bool TryCatchUp() {
    ROMULUS_DEBUG("<TryCatchUp> Catching up slot: {}", promise_offset_);

    // Post READs to all remote peers.
    RemoteContext *c;
    std::vector<bool> ok;
    uint32_t ok_count = 0;
    ok.resize(system_size_);
    for (uint32_t i = 0; i < system_size_; ++i) {
      ok[i] = false;
      c = promise_contexts_[i];
      c->log_raddr.addr_info.offset = promise_offset_ * kSlotSize;
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
        if (ok[i])
          continue;
        c = promise_contexts_[i];
        ok[i] = PollCompletionsOnce(c->conn, wr_id_);
        if (ok[i])
          ++ok_count;
      }
    }

    // Compare returned values to determine if this slot has been committed
    // already.
    RemoteContext *c_i, *c_j;
    uint32_t num_agreed = 0;
    Value accepted_val;
    for (uint32_t i = 0; i < quorum_; ++i) {
      c_i = promise_contexts_[i];
      if (!ok[i] || c_i->scratch_state->GetBallot() == kNullBallot)
        continue;
      num_agreed = 1;
      accepted_val = c_i->scratch_state->GetValue();
      for (uint32_t j = i + 1; j < system_size_ && num_agreed < quorum_; ++j) {
        c_j = promise_contexts_[j];
        if (!ok[j] || c_j->scratch_state->GetBallot() == kNullBallot)
          continue;
        if (c_j->scratch_state->GetValue() == accepted_val) {
          ++num_agreed;
        }
      }

      // This slot is committed.
      if (num_agreed >= quorum_) {
        // If the local log does not reflect the accepted value then CAS it
        // in.
        if (log_[promise_offset_].GetValue() != accepted_val) {
          auto loopback_context = promise_contexts_[host_id_];
          loopback_context->log_raddr.addr_info.offset =
            promise_offset_ * kSlotSize;
          romulus::WorkRequest::BuildCAS(
            loopback_context->scratch_laddr, loopback_context->log_raddr,
            log_[promise_offset_].raw, c_i->scratch_state->raw, wr_id_,
            &loopback_context->wr);
          // StageLogRequest(loopback_context);
          ROMULUS_ASSERT(
            loopback_context->conn->Post(&loopback_context->wr, 1),
            "<TryCatchUp> Failed to post requests when updating local "
            "slot.");

          // Only expect a single outstanding completion.
          while (!PollCompletionsOnce(loopback_context->conn, wr_id_))
            ;
        }
        ROMULUS_DEBUG("<TryCatchUp> Caught up: log_offset={}, state={}",
                      promise_offset_, log_[promise_offset_].ToString());
        ++promise_offset_;
        ++wr_id_;
        return true;
      }
    }
    ++wr_id_;
    ROMULUS_DEBUG("<TryCatchUp> Failed. log_offset={}", promise_offset_);
    return false;
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

  bool isLeaderStable() { return stable_leader_; }

  bool isLeader() { return is_leader_; }

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
  romulus::APArray<State, kSlotSize, CACHE_PREFETCH_SIZE> *raw_;
  romulus::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> scratch_;
  romulus::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> proposed_state_;
  romulus::APArraySlice<State, kSlotSize, CACHE_PREFETCH_SIZE> log_;
  State leader_;

  // Buffer fields for commit
  [[maybe_unused]] uint8_t *buf_;
  [[maybe_unused]] uint32_t buf_offset_ = 0;
  [[maybe_unused]] uint64_t buf_chunk_size_;

  uint64_t wr_id_ = 0;
  // Written by the prepare thread, read by the promise thread.
  std::atomic<uint32_t> prepare_offset_{0};
  uint32_t promise_offset_ = 0;

  // Promise_Pipe() window state (promise thread only).
  uint32_t pipe_outstanding_ = 0;
  uint64_t pipe_seq_ = 0;
  romulus::AddrInfo pipe_laddr_;
  std::vector<State> pipe_expected_; // [d * n + i]
  // std::vector<uint64_t> pipe_wr_ids_; // [d * n + i]
  std::vector<Value> pipe_vals_; // [d]

  // Whether this node thinks its the leader.
  bool is_leader_;
  // Whether there exists a stable leader
  bool stable_leader_;

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
  std::unordered_map<uint64_t, std::vector<romulus::ReliableConnection *>>
    remote_conns_;

  std::vector<RemoteContext *> prepare_contexts_;
  std::vector<RemoteContext *> promise_contexts_;

  Ballot local_ballot_ = 0;

  // RDMA related members.
  romulus::Device device_;
  std::unique_ptr<romulus::ConnectionManager> conn_manager_;
  std::unique_ptr<romulus::ConnectionRegistry> registry_;
  romulus::MemBlock memblock_;
  uint64_t buf_size_;
  uint64_t num_qps_;
  uint64_t num_shared_cq_;
  uint64_t pipe_depth_;
};