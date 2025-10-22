#include "paxos.h"


State::State(uint16_t max, uint16_t accepted, Value value) {
  raw = (static_cast<uint64_t>(max) << (kTotalBits - kMaxBallotBits)) |
        (static_cast<uint64_t>(accepted) << kValueBits) |
        static_cast<uint64_t>(value.raw());
}