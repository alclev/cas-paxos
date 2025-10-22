#pragma once

#include <sstream>
#include <thread>
#include <vector>

#include "memblock/memblock.h"

using Ballot = uint16_t;

constexpr uint8_t kIdBits = 8;
constexpr uint8_t kOffsetBits = 24;
constexpr uint32_t kIdMask = ((1 << kIdBits) - 1) << kOffsetBits;
constexpr uint32_t kOffsetMask = (1 << kOffsetBits) - 1;

// A wrapper around a 32-bit unsigned integer that encodes an id and an offset.
class Value {
 public:
  Value() : raw_(0) {}
  Value(const Value& v) : raw_(v.raw_) {}
  Value(uint32_t raw) : raw_(raw) {}
  void SetId(uint8_t id) {
    raw_ = (raw_ & ~(kIdMask)) | (static_cast<uint32_t>(id) << kOffsetBits);
  }

  void SetOffset(uint32_t offset) {
    offset = offset & kOffsetMask;
    raw_ = (raw_ & ~(kOffsetMask)) | offset;
  }

  uint8_t id() const {
    return static_cast<uint8_t>((raw_ & kIdMask) >> kOffsetBits);
  }
  uint32_t offset() const { return raw_ & kOffsetMask; }
  uint32_t raw() const { return raw_; }
  void operator=(const Value& rhs) { this->raw_ = rhs.raw_; }
  bool operator==(const Value& rhs) { return this->raw_ == rhs.raw_; }
  bool operator!=(const Value& rhs) { return this->raw_ != rhs.raw_; }

 private:
  uint32_t raw_;
};
static_assert(sizeof(Value) == sizeof(uint32_t));

enum Step { kIdle, kReady, kCatchUp, kNextBallot, kPrepare, kPromise };

/**
 * @brief A virtual class defining the capabilities required by a Paxos
 * implementation.
 */
class Paxos {
 public:
  virtual void Reset() = 0;
  virtual void CatchUp() = 0;
  virtual void Propose(uint32_t len, uint8_t* buf) = 0;
  virtual void SyncNodes() = 0;
  virtual void CleanUp() = 0;
  virtual ~Paxos() {}
};

static constexpr uint8_t kMaxBallotBits = sizeof(Ballot) * 8;
static constexpr uint8_t kBallotBits = kMaxBallotBits;
static constexpr uint8_t kValueBits = sizeof(Value) * 8;
static constexpr uint8_t kTotalBits = kMaxBallotBits + kBallotBits + kValueBits;

static constexpr uint64_t kMaxBallotMask = ((1ULL << kMaxBallotBits) - 1)
                                           << (kBallotBits + kValueBits);
static constexpr uint64_t kBallotMask = ((1ULL << kBallotBits) - 1)
                                        << kValueBits;
static constexpr uint64_t kValueMask = ((1ULL << kValueBits) - 1);

// Sanity check.
static_assert(sizeof(Value) + (sizeof(Ballot) * 2) == (kTotalBits / 8));

/**
 * @brief Encapsulates a message type, ballot and value required for the
 * CAS-based approach to Paxos
 */
struct State {
  uint64_t raw;
  /**
   * @brief Construct a new State object
   *
   * Sets the highest 31 bits to \p ballot then the next 32 bits to
   * \p value. We accomplish this by casting the ballot to an unsigned 64-bit
   * integer and shifting it left 32 bits. Then, we cast the value to an
   * unsigned 64-bit integer then OR it with the shifted ballot. Finally, we set
   * the last bit by clearing it then OR-ing the type casted as an unsigned
   * 64-bit integer.
   *
   * @param ballot
   * @param value
   * @param type
   */
  State() : raw(0) {}
  State(uint16_t max, uint16_t accepted, Value value) {
    raw = (static_cast<uint64_t>(max) << (kTotalBits - kMaxBallotBits)) |
          (static_cast<uint64_t>(accepted) << kValueBits) |
          static_cast<uint64_t>(value.raw());
  }

  // Getters
  inline Ballot GetMaxBallot() const {
    return static_cast<Ballot>((raw & kMaxBallotMask) >>
                               (kTotalBits - kMaxBallotBits));
  }
  inline Ballot GetBallot() const {
    return static_cast<Ballot>((raw & kBallotMask) >> kValueBits);
  }
  inline Value GetValue() const { return Value(raw & kValueMask); }

  // Setters
  inline void SetMaxBallot(Ballot max) {
    raw = (raw & (~kMaxBallotMask)) |
          (static_cast<uint64_t>(max) << (kTotalBits - kMaxBallotBits));
  }
  inline void SetBallot(Ballot ballot) {
    raw =
        (raw & (~kBallotMask)) | (static_cast<uint64_t>(ballot) << kValueBits);
  }
  inline void SetValue(Value value) {
    raw = (raw & (~kValueMask)) | static_cast<uint64_t>(value.raw());
  }
  inline void SetProposal(Ballot ballot, Value value) {
    raw = (raw & kMaxBallotMask) |
          ((static_cast<uint64_t>(ballot) << kValueBits) |
           static_cast<uint64_t>(value.raw()));
  }

  std::string ToString() const {
    std::stringstream ss;
    ss << "<max=" << GetMaxBallot() << ", <ballot=" << GetBallot()
       << ", value=(" << static_cast<uint32_t>(GetValue().id()) << ", "
       << GetValue().offset() << ")"
       << ">> (raw=" << raw << ")";
    return ss.str();
  }
};
