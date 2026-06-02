#pragma once

#include <cstdint>
#include <format>
#include <fstream>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "romulus/logging.h"

constexpr uint32_t VALRANGE = 100'000;

template <typename T> struct KVPair {
  T key;
  T value;
  KVPair(T k, T v) : key(k), value(v) {}
  std::string ToString() const {
    return std::format("KVPair{{key: {}, value: {}}}", key, value);
  }
};

class WorkloadGenerator {
 private:
  std::shared_ptr<romulus::ArgMap> args_;
  std::vector<std::pair<uint32_t, uint8_t*>> ops_;
  int key_range_;
  int value_range_;
  int num_ops_;
  int system_size_;

 public:
  WorkloadGenerator(std::shared_ptr<romulus::ArgMap> args, int key_range, int num_ops, int system_size)
      : args_(args), key_range_(key_range), value_range_(VALRANGE), num_ops_(num_ops), system_size_(system_size) {}
 
  void generate() {
    std::random_device rand_device;
    auto engine = std::mt19937(rand_device());
    // end values are reserved for ctrl messages
    auto rng = std::uniform_int_distribution<uint16_t>(
        0, std::numeric_limits<uint16_t>::max());
    // const std::string alphanum =
    //     "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::vector<std::pair<uint32_t, uint8_t*>> proposals;
    proposals.reserve(num_ops_);

    // First proposal is reserved for the lease
    auto* lease_buf = new uint8_t[sizeof(uint32_t)];
    auto id = args_->uget(romulus::NODE_ID);
    // auto 
    *reinterpret_cast<uint32_t*>(lease_buf) = id * (key_range_ / system_size_);
    proposals.emplace_back(sizeof(uint32_t), lease_buf);

    for (uint16_t i = 0; i < num_ops_; ++i) {
      uint16_t key = rng(engine) % key_range_;
      [[maybe_unused]] uint16_t value_base = rng(engine) % value_range_;
      // id | iter
      uint16_t value = (static_cast<uint16_t>(id) << 12) | i;
      KVPair<uint16_t>* kv = new KVPair<uint16_t>(key, value);
      auto kv_size = sizeof(*kv);
      auto* new_buf = new uint8_t[kv_size];
      *reinterpret_cast<KVPair<uint16_t>*>(new_buf) = *kv;
      ROMULUS_ASSERT(sizeof(uint32_t) == kv_size,
                     "Size of KV pair is not what we expected!");
      proposals.emplace_back(kv_size, new_buf);
    }
    ops_ = proposals;
  }

  std::vector<std::pair<uint32_t, uint8_t*>>& get_ops() { return ops_; }

  void print(uint64_t start_idx, uint64_t end_idx) {
    ROMULUS_ASSERT(start_idx < end_idx && end_idx <= ops_.size(),
                   "Invalid index range for printing operations.");
    ROMULUS_INFO("########### Workload ###########");
    for (uint64_t i = start_idx; i < end_idx && i < ops_.size(); ++i) {
      ROMULUS_INFO(
          "Operation {}: {}", i,
          reinterpret_cast<KVPair<uint16_t>*>(ops_[i].second)->ToString());
      ROMULUS_INFO("-----------------------------------");
    }
  }
};