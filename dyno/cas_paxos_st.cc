#include "cas_paxos_st.h"

#include <absl/time/time.h>

#include <memory>
#include <numeric>

#include "common/aparray.h"
#include "common/common.h"
#include "common/rome.h"

//+ Generalized values

namespace paxos_st {

ROME_STOPWATCH_DECLARE();

CasPaxos::CasPaxos(uint32_t capacity, std::string hostname, uint8_t host_id,
                   std::vector<std::string> peers)
    : system_size_(peers.size() + 1),
      capacity_(capacity),
      wr_id_(0),
      log_offset_(0),
      is_leader_(false),
      hostname_(hostname),
      host_id_(host_id),
      quorum_(dyno::GetQuorum(peers.size() + 1)),
      peers_(peers) {}

CasPaxos::~CasPaxos() {
  for (auto* c : contexts_) {
    delete c;
  }
  delete raw_;
  SyncNodes();
}

// DONE
void CasPaxos::Init(std::string_view dev_name, int dev_port,
                    std::unique_ptr<dyno::ConnectionRegistry> registry,
                    bool stable_leader) {
  // Set up remotely accessible memory.
  DYNO_TRACE("Initializing CAS-based Paxos");
  DYNO_TRACE("Quorum size: {}", quorum_);
  DYNO_CHECK(DYNO_ABORT, device_.Open(dev_name, dev_port),
             "Failed to open device.");
  device_.AllocatePd(kPdId);

  stable_leader_ = stable_leader;

  DYNO_TRACE("Registering remotely accessible memory");
  uint64_t scratch_len = system_size_;
  uint64_t proposal_len = capacity_;
  uint64_t log_len = capacity_;
  uint64_t buf_len = kNumBufSlots;  //! Hacky. Makes write buf accessible
                                    //! through the same connection.
  raw_ = new dyno::APArray<State, kSlotSize, CACHE_PREFETCH_SIZE>(
      scratch_len + proposal_len + log_len + buf_len);
  std::memset(raw_->Get(), 0, raw_->GetTotalBytes());

  memblock_ = dyno::MemBlock(
      kBlockId, device_.GetPd(kPdId), reinterpret_cast<uint8_t*>(raw_->Get()),
      (scratch_len + proposal_len + log_len + buf_len) * kSlotSize);
  memblock_.RegisterMemRegion(kScratchRegionId, 0, scratch_len * kSlotSize);
  memblock_.RegisterMemRegion(kBallotRegionId, scratch_len * kSlotSize,
                              proposal_len * kSlotSize);
  memblock_.RegisterMemRegion(kLogRegionId,
                              (scratch_len + proposal_len) * kSlotSize,
                              log_len * kSlotSize);

  // Set up local view of log memory
  scratch_ = dyno::APArraySlice(raw_, 0, scratch_len);
  proposed_state_ =
      dyno::APArraySlice(raw_, scratch_len, scratch_len + proposal_len);
  log_ = dyno::APArraySlice(raw_, scratch_len + proposal_len,
                            scratch_len + proposal_len + log_len);

  // Register and setup buffer memory
  auto base_offset = (scratch_len + proposal_len + log_len) * kSlotSize;
  auto chunk_len = stable_leader_ ? buf_len : (buf_len / system_size_);
  buf_chunk_size_ = chunk_len * kSlotSize;
  for (uint32_t i = 0; i < system_size_; ++i) {
    //! Hacky. With a stable leader, we make the entire buffer available to run
    //! longer tests. Ideally, we would have a more elegant solution. ¯\_(ツ)_/¯
    uint64_t node_offset = stable_leader_ ? 0 : (chunk_len * i * kSlotSize);
    memblock_.RegisterMemRegion(kBufRegionPrefix + std::to_string(i),
                                base_offset + node_offset, buf_chunk_size_);
    if (i == host_id_) {
      buf_ = &raw_->Get()[base_offset + node_offset];
    }
  }

  // Initialize proposed state (remote peers read this). +1 because 0 is a
  // special value in the state.
  DYNO_CHECK(DYNO_ABORT, kSlotSize == sizeof(State),
             "kSlotSize != sizeof(State) not supported.");
  for (uint32_t i = 0; i < capacity_; ++i) {
    proposed_state_[i] = State(host_id_ + 1, kNullBallot, kNullBallot);
    log_[i] = State(0, kNullBallot, kNullBallot);
  }

  DYNO_CHECK_DEBUG(DYNO_ABORT,
                   reinterpret_cast<uint64_t>(scratch_.Begin()) ==
                       memblock_.GetAddrInfo(kScratchRegionId).addr,
                   "Address mismatch (scratch)");
  DYNO_CHECK_DEBUG(DYNO_ABORT,
                   reinterpret_cast<uint64_t>(proposed_state_.Begin()) ==
                       memblock_.GetAddrInfo(kBallotRegionId).addr,
                   "Address mismatch (proposals)");
  DYNO_CHECK_DEBUG(DYNO_ABORT,
                   reinterpret_cast<uint64_t>(log_.Begin()) ==
                       memblock_.GetAddrInfo(kLogRegionId).addr,
                   "Address mismatch (log)");
  DYNO_CHECK_DEBUG(
      DYNO_ABORT,
      reinterpret_cast<uint64_t>(buf_) ==
          memblock_.GetAddrInfo(kBufRegionPrefix + std::to_string(host_id_))
              .addr,
      "Address mismatch (buf)");

  // Register memory and connect to other nodes
  registry_ = std::move(registry);
  conn_manager_ =
      std::make_unique<dyno::ConnectionManager>(hostname_, registry_.get());

  bool ok = conn_manager_->Register(peers_, memblock_, device_) &&
            conn_manager_->WaitForNodesWithTimeout(system_size_, kTimeout) &&
            conn_manager_->Connect(peers_, memblock_) &&
            conn_manager_->WaitForNodesWithTimeout(system_size_, kTimeout);
  DYNO_CHECK(DYNO_ABORT, ok, "Failed to register and connect log memory");

  // Initialize worker contexts for each participant (including this node).
  // Assign the first thread to work through loopback.
  contexts_.reserve(system_size_);
  for (int i = 0; i < system_size_; ++i) {
    contexts_.push_back(new RemoteContext());
    RemoteContext* context = contexts_[i];
    context->proposed_state = &proposed_state_[0];

    // Setup connection info.
    auto peer =
        i == host_id_ ? dyno::kLoopback : peers_[i < host_id_ ? i : i - 1];

    // Log.
    context->conn = conn_manager_->GetConnection(peer, kBlockId);

    context->scratch_laddr = memblock_.GetAddrInfo(kScratchRegionId);
    context->scratch_laddr.addr += kSlotSize * i;  // Offset into scratch mem
    context->scratch_laddr.length = sizeof(State);
    context->scratch_state =
        reinterpret_cast<State*>(context->scratch_laddr.addr);

    DYNO_CHECK(DYNO_ABORT,
               conn_manager_->GetRemoteAddr(peer, kBlockId, kLogRegionId,
                                            &context->log_raddr),
               "Failed to get remote address");
    context->log_raddr.addr_info.length = sizeof(State);

    DYNO_CHECK(DYNO_ABORT,
               conn_manager_->GetRemoteAddr(peer, kBlockId, kBallotRegionId,
                                            &context->proposal_raddr),
               "Failed to get remote address");
    context->proposal_raddr.addr_info.length = sizeof(State);

    // Buffer.
    DYNO_CHECK(DYNO_ABORT,
               conn_manager_->GetRemoteAddr(
                   peer, kBlockId, kBufRegionPrefix + std::to_string(host_id_),
                   &context->buf_raddr),
               "Failed to get remote address");
    context->buf_laddr =
        memblock_.GetAddrInfo(kBufRegionPrefix + std::to_string(host_id_));
  }
}
// DONE
void CasPaxos::Reset() {
  log_offset_ = 0;
  buf_offset_ = 0;
}

namespace {

void StageLogRequest(RemoteContext* c) {
  if (c->post_buf_wr) {
    c->buf_wr.append(&c->log_wr);
  }
}

bool PostRequests(RemoteContext* c) {
  bool ok = c->post_buf_wr ? c->conn->Post(&c->buf_wr, 1)
                           : c->conn->Post(&c->log_wr, 1);
  c->post_buf_wr = false;
  return ok;
}

bool PollCompletionsOnce(RemoteContext* c, uint64_t wr_id) {
  return c->conn->TryProcessOutstanding() > 0 &&
         c->conn->CheckCompletionsForId(wr_id);
}

}  // namespace

// DONE (CHECK)
bool CasPaxos::TryCatchUp() {
  DYNO_TRACE("<TryCatchUp> Catching up slot: {}", log_offset_);

  // Post READs to all remote peers.
  RemoteContext* c;
  std::vector<bool> ok;
  uint32_t ok_count = 0;
  ok.resize(system_size_);
  for (uint32_t i = 0; i < system_size_; ++i) {
    ok[i] = false;
    c = contexts_[i];
    c->log_raddr.addr_info.offset = log_offset_ * kSlotSize;
    dyno::WorkRequest::BuildRead(c->scratch_laddr, c->log_raddr, wr_id_,
                                 &c->log_wr);
    StageLogRequest(c);
    DYNO_CHECK(DYNO_ABORT, PostRequests(c),
               "<TryCatchUp> Failed to post requests.");
  }

  // Wait for a response from all nodes.
  //+ Timeout if this takes too long.
  while (ok_count < system_size_) {
    for (uint32_t i = 0; i < system_size_; ++i) {
      if (ok[i]) continue;
      c = contexts_[i];
      ok[i] = PollCompletionsOnce(c, wr_id_);
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
        loopback_context->log_raddr.addr_info.offset = log_offset_ * kSlotSize;
        dyno::WorkRequest::BuildCAS(
            loopback_context->scratch_laddr, loopback_context->log_raddr,
            log_[log_offset_].raw, c_i->scratch_state->raw, wr_id_,
            &loopback_context->log_wr);
        StageLogRequest(loopback_context);
        DYNO_CHECK(
            DYNO_ABORT, PostRequests(loopback_context),
            "<TryCatchUp> Failed to post requests when updating local slot.");

        // Only expect a single outstanding completion.
        while (!PollCompletionsOnce(loopback_context, wr_id_))
          ;
      }
      DYNO_DEBUG("<TryCatchUp> Caught up: log_offset={}, state={}", log_offset_,
                 log_[log_offset_].ToString());
      ++log_offset_;
      ++wr_id_;
      return true;
    }
  }
  ++wr_id_;
  DYNO_DEBUG("<TryCatchUp> Failed. log_offset={}", log_offset_);
  return false;
} // END TryCatchUp

void CasPaxos::CatchUp() {
  if (is_leader_) return;
  DYNO_DEBUG("<CatchUp> Catching up");
  while (TryCatchUp()) {
    ROME_COUNTER_INC("skipped");
  }
}

namespace {

// DONE
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

}  // namespace

// DONE
void CasPaxos::UpdateBallot() {
  // Skip if we are the leader.
  if (is_leader_) return;
  DYNO_DEBUG("Updating ballot");

  // Post READs to all remote peers.
  RemoteContext* c;
  std::vector<bool> ok;
  uint32_t ok_count = 0;
  ok.resize(system_size_);
  for (uint32_t i = 0; i < system_size_; ++i) {
    ok[i] = false;
    c = contexts_[i];
    c->proposal_raddr.addr_info.offset = log_offset_ * kSlotSize;
    dyno::WorkRequest::BuildRead(c->scratch_laddr, c->proposal_raddr, wr_id_,
                                 &c->log_wr);
    StageLogRequest(c);
    DYNO_CHECK(DYNO_ABORT, PostRequests(c),
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
  DYNO_DEBUG("<UpdateBallot> Updated ballot: ballot={}",
             curr_proposal->GetMaxBallot());
  ++wr_id_;
}

// DONE
bool CasPaxos::Prepare() {
  // Skip preparation if we are the leader.
  if (is_leader_) return true;

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

// Post CAS ops.
#ifndef DYNO_NO_SENDALL
  // Issue CAS to all nodes.
  uint32_t send_to = system_size_;
#else
  // Issue CAS only to quorum assuming they are available.
  uint32_t send_to = quorum_;
#endif

  // Retry until a quroum succeeds.
  while (done_count < quorum_) {
    uint32_t ok_count = 0;

    // Post CAS ops.
    for (uint32_t i = 0; i < send_to; ++i) {
      if (done[i]) {
        ++ok_count;
      } else {
        ok[i] = false;
        c = contexts_[i];
        c->log_raddr.addr_info.offset = log_offset_ * kSlotSize;
        dyno::WorkRequest::BuildCAS(c->scratch_laddr, c->log_raddr,
                                    expected[i].raw, swap[i].raw, wr_id_,
                                    &c->log_wr);
        StageLogRequest(c);
        DYNO_CHECK(DYNO_ABORT, PostRequests(c),
                   "<Prepare> Failed when posting requests.");
      }
    }

    // Poll for completions
    while (ok_count < send_to) {
      for (uint32_t i = 0; i < send_to; ++i) {
        if (done[i] || ok[i]) continue;
        c = contexts_[i];
        ok[i] = PollCompletionsOnce(c, wr_id_);
        if (ok[i]) ++ok_count;
      }
    }

    // Check return value of CAS.
    auto* curr_proposal = &proposed_state_[log_offset_];
    for (uint32_t i = 0; i < send_to; ++i) {
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
        DYNO_TRACE("<Promise> Updating: swap={}, expected={}",
                   swap[i].ToString(), expected[i].ToString());
      } else {
        // CAS failed and higher ballot. Abort.
        DYNO_TRACE("Failed: state={}", c->scratch_state->ToString());
        return false;
      }
    }
    ++wr_id_;
  }

  // Update current proposal to reflect the highest balloted proposal.
  for (uint32_t i = 0; i < send_to; ++i) {
    if (swap[i].GetBallot() > curr_proposal->GetBallot()) {
      curr_proposal->SetProposal(swap[i].GetBallot(), swap[i].GetValue());
    }
  }
  DYNO_DEBUG("<Prepare> Prepared slot: log_offset={}, state={}", log_offset_,
             curr_proposal->ToString());
  return true;
}

// DONE
bool CasPaxos::Promise(Value v) {
  // DYNO_CHECK_DEBUG(DYNO_ABORT, is_leader_,
  //                  "Entering Phase 2 but not the leader.");

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

  DYNO_DEBUG("<Promise> slot={}, state={}", log_offset_,
             proposed_state_[log_offset_].ToString());

// Post CAS ops.
#ifndef DYNO_NO_SENDALL
  // Issue CAS to all nodes.
  uint32_t send_to = system_size_;
#else
  // Issue CAS only to quorum assuming they are available.
  uint32_t send_to = quorum_;
#endif

  // Retry until a quroum succeeds and the local log is written to. Making
  // sure that we write to the local log allows a follower to be certain that
  // if the slot is filled that the value is committed.
  while (done_count < quorum_) {
    // Post CAS ops.
    uint32_t ok_count = 0;
    for (uint32_t i = 0; i < send_to; ++i) {
      if (done[i]) {
        // Already succeeded.
        ++ok_count;
      } else {
        // Post a request
        ok[i] = false;
        c = contexts_[i];
        c->log_raddr.addr_info.offset = log_offset_ * kSlotSize;
        dyno::WorkRequest::BuildCAS(c->scratch_laddr, c->log_raddr,
                                    expected[i].raw, curr_proposal->raw, wr_id_,
                                    &c->log_wr);
        DYNO_CHECK(DYNO_ABORT, PostRequests(c),
                   "<Promise> Failed when posting requests.");
      }
    }

    // Poll for completions
    while (ok_count < send_to) {
      for (uint32_t i = 0; i < send_to; ++i) {
        if (done[i] || ok[i]) continue;
        c = contexts_[i];
        ok[i] = PollCompletionsOnce(c, wr_id_);
        if (ok[i]) ++ok_count;
      }
    }

    for (uint32_t i = 0; i < send_to; ++i) {
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
        DYNO_TRACE("<Promise> Updating expected: expected={}",
                   expected[i].ToString());
      } else {
        // CAS failed on first try (as leader) or a higher ballot is found.
        DYNO_TRACE("<Promise> Failed: state={}", c->scratch_state->ToString());
        return false;
      }
    }
    ++wr_id_;
  }
  return true;
}

namespace {

inline absl::Duration DoBackoff(absl::Duration backoff) {
  DYNO_DEBUG("Backing off for {} ms", absl::ToDoubleMilliseconds(backoff));
  // Backoff if an abort was triggered this round.
  ROME_STOPWATCH_START();
  while (ROME_STOPWATCH_SPLIT(ROME_NANOSECONDS) <
         absl::ToDoubleNanoseconds(backoff))
    ;
  return backoff * 2;
}

}  // namespace

//+ Prepare work request then post with first request.
uint32_t CasPaxos::PerpareWrite(uint32_t len, [[maybe_unused]] uint8_t* buf) {
  DYNO_CHECK(DYNO_ABORT, buf_offset_ < buf_chunk_size_,
             "Buffer offset out of bounds! actual={}, max={}", buf_offset_,
             buf_chunk_size_);
  // Write the buffer into the local copy of this node's buffer.
  *reinterpret_cast<uint32_t*>(&buf_[buf_offset_]) = len;
  std::memcpy(&buf_[buf_offset_ + sizeof(uint32_t)], buf, len);
  auto total_len = len + sizeof(uint32_t);

  // Propagate the local copy to all other peers.
  RemoteContext* c;
  for (uint32_t i = 0; i < system_size_; ++i) {
    if (i == host_id_) continue;  // Already written.
    c = contexts_[i];
    DYNO_ASSERT_DEBUG(c->buf_laddr.addr == reinterpret_cast<uint64_t>(buf_),
                      "Bad address! expected={}, actual={:#x}", fmt::ptr(buf_),
                      c->buf_laddr.addr);
    DYNO_ASSERT_DEBUG(c->buf_laddr.offset == buf_offset_,
                      "Bad offset! expected={}, actual={}", buf_offset_,
                      c->buf_laddr.offset);

    // Stage the buf request.
    c->buf_laddr.length = total_len;
    c->buf_raddr.addr_info.length = total_len;
    c->post_buf_wr = true;  // Needs to be posted.
    dyno::WorkRequest::BuildWrite(c->buf_laddr, c->buf_raddr, wr_id_,
                                  &c->buf_wr);
    c->buf_wr.unsignaled();  // Does not need a completion since we always to
                             // another op after.
    c->buf_raddr.addr_info.offset += total_len;
    c->buf_laddr.offset += total_len;
  }
  ++wr_id_;
  return total_len;
}

// Write the buffer to nodes before proposing the offset.
void CasPaxos::Propose([[maybe_unused]] uint32_t len,
                       [[maybe_unused]] uint8_t* buf) {
#ifndef DYNO_NO_WRITEBUF
  uint32_t delta = PerpareWrite(len, buf);
#endif

  Value v;
  v.SetId(host_id_);
  v.SetOffset(buf_offset_);
  ProposeInternal(v);

#ifndef DYNO_NO_WRITEBUF
  buf_offset_ += delta;
#else
  buf_offset_ += 1;
#endif
}

// Repeated attempt to propose the given value until it is successfully
// committed. Initially, try to update the log by repeatedly calling
// TryCatchUp until it returns false, which indicates that the log offset is
// at an entry that was not committed yet. Then, update the ballot to be the
// next highest unique ballot number for that slot. Next, prepare peers by
// attempting to CAS in the newly chosen ballot. Finally, commit the value by
// writing to a quorum of nodes. During this process it is possible that a
// previously proposed value is adopted, in which case the cycle will repeat
// in an attempt to commit the provided value. If the prepare phase is
// successful, then the node considers itself the leader. If it remains the
// leader then future ballot updates and prepare phases will be skipped.
void CasPaxos::ProposeInternal(Value v) {
  DYNO_TRACE("<InlineProposal> Starting.");
  auto backoff = absl::Nanoseconds(std::rand() % kMaxStartingBackoff);
  bool ok, done = false;
  while (!done) {
    ROME_COUNTER_INC("attempts");
    // Catch up on previously committed values.
    CatchUp();

    // Run Paxos on the next free slot.
    UpdateBallot();
    ok = Prepare();
    if (ok) {
      ok = Promise(v);
      if (ok) {
        auto committed = log_[log_offset_].GetValue();
        if (committed == v) {
          // Given value was committed. Done.
          DYNO_DEBUG("Proposed slot committed: value=({}, {}), log_offset={}",
                     v.id(), v.offset(), log_offset_);
          is_leader_ = true;
          done = true;
        } else {
          // Commit an existing value.
          DYNO_DEBUG("Slot committed: value=({}, {}), log_offset={}",
                     committed.id(), committed.offset(), log_offset_);
          is_leader_ = false;
        }
        ++log_offset_;
      } else {
        ROME_COUNTER_INC("p2_aborts");
      }
    } else {
      ROME_COUNTER_INC("p1_aborts");
    }

    // If aborted, backoff.
    if (!ok) {
      is_leader_ = false;
      backoff = DoBackoff(backoff);
    }
  }
  ROME_COUNTER_INC("proposed");
}

void CasPaxos::DumpLog() {
  for (uint32_t i = 0; i < log_offset_; ++i) {
    DYNO_DEBUG("log[{}]: {}", i, log_[i].ToString());
  }
}

void CasPaxos::CleanUp() {
  SyncNodes();
  ROME_COUNTER_ACC("p1_aborts");
  ROME_COUNTER_ACC("p2_aborts");
  ROME_COUNTER_ACC("attempts");
  ROME_COUNTER_ACC("skipped");
  ROME_COUNTER_ACC("proposed");
  DYNO_INFO("!> p1_aborts={}", ROME_COUNTER_GET("p1_aborts"));
  DYNO_INFO("!> p2_aborts={}", ROME_COUNTER_GET("p2_aborts"));
  DYNO_INFO("!> attempts={}", ROME_COUNTER_GET("attempts"));
  DYNO_INFO("!> skipped={}", ROME_COUNTER_GET("skipped"));
  DYNO_INFO("!> proposed={}", ROME_COUNTER_GET("proposed"));
  CatchUp();
  DumpLog();
}

void CasPaxos::SyncNodes() {
  DYNO_DEBUG("Syncing nodes");
  DYNO_CHECK(DYNO_ABORT,
             conn_manager_->WaitForNodesWithTimeout(system_size_, kTimeout),
             "SyncNodes failed");
  DYNO_DEBUG("Nodes synced");
}

}  // namespace paxos_st