#pragma once

#include <sstream>

// constexpr uint64_t kNullLease = 0;

class RemoteLeaseEntry {
  private:
    uint64_t raw_;
  // epoch (32 bits) | owner_id (16 bits) | FUO (16 bits) |
  // [ epoch (32 bits) | owner_id (16 bits) | shard_id (16 bits) ]
  public:
    RemoteLeaseEntry() : raw_(0) {}
    RemoteLeaseEntry(uint64_t epoch, uint64_t owner_id, uint64_t shard_id) {
      raw_ = (epoch << 32) | (owner_id << 16) | shard_id;
    }
    void SetEpoch(uint64_t id, uint64_t fuo) {
      uint64_t epoch = (id << 48) | (fuo << 32);
      raw_ = raw_ | (epoch <<  32); 
    }
    void SetOwnerId(uint64_t owner_id) { raw_ = raw_ | (owner_id << 16); }
    void SetShardId(uint64_t shard_id) { raw_ = raw_ | shard_id; }

    uint64_t GetEpoch() const { return raw_ >> 32; }
    uint64_t GetOwnerId() const { return (raw_ >> 16) & 0xFFFF; }
    uint64_t GetShardId() const { return raw_ & 0xFFFF; }

    std::string ToString() const {
      std::ostringstream oss;
      oss << "Epoch: " << GetEpoch() << ", OwnerId: " << GetOwnerId() << ", ShardId: " << GetShardId();
      return oss.str();
    }
  };