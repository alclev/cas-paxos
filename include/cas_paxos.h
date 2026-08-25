#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <vector>

#include "romulus/common.h"
#include "romulus/rc.h"
#include "state.h"
#include "util.h"
#include "contexts.h"

namespace cas_paxos {

constexpr uint64_t kMaxStartingBackoff = 100;  // us
constexpr uint32_t kSlotSize = sizeof(State);

}  // namespace cas_paxos

class CasPaxos {
 public:
  explicit CasPaxos(cons_ctx_t ctx, uint64_t id, uint64_t system_size,
                    uint64_t quorum);

  bool Prepare(uint32_t shard_id);
  bool Promise(uint32_t shard_id, Value v);

 private:
  Ballot BumpBallot(Ballot b);
  Ballot MakeBallot(uint32_t round);


  // RDMA resources
  cons_ctx_t cons_ctx_;

  uint64_t id_;
  uint64_t system_size_;
  uint64_t quorum_;

  Ballot local_ballot_;
  std::vector<State> proposed_state_;
  std::vector<State> expected_;
  State* installed_promise_ = nullptr;
};