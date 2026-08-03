#pragma once

#include <cstdint>
#include <format>
#include <fstream>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "cfg.h"
#include "romulus/cfg.h"
#include "romulus/cli.h"
#include "romulus/logging.h"

template <typename T>
struct txn_t {
  std::vector<T> keys;
  std::vector<T> values;
};

struct WorkloadConfig {
  uint64_t num_ops;
  uint64_t txn_size;
  uint64_t key_range;
  uint64_t value_range;
};

namespace WorkloadGenerator {

template <typename T>
static inline std::vector<txn_t<T>> generate(
    WorkloadConfig& config, uint64_t low, uint64_t high) {
  // Initialize random number generator and distribution
  std::random_device rand_device;
  auto engine = std::mt19937(rand_device());
  auto rng = std::uniform_int_distribution<T>(low, high);
  
  std::vector<txn_t<T>> proposals;
  proposals.reserve(config.num_ops);

  for (int i = 0; i < (int)config.num_ops; ++i) {
    txn_t<T> t;
    for (uint64_t j = 0; j < config.txn_size; ++j) {
      t.keys.push_back(rng(engine) % config.key_range);
      t.values.push_back(rng(engine) % config.value_range);
    }
    proposals.push_back(t);
  }

  return proposals;
}

}  // namespace WorkloadGenerator
