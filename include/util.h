#pragma once

#include <immintrin.h>

#include <atomic>
#include <chrono>
#include <thread>

constexpr auto kTimeout = std::chrono::nanoseconds(500'000'000);

inline std::atomic<bool> dump_requested_ = false;
inline std::atomic<bool> failure_detector_running_ = true;

template <typename Rep, typename Period>
void busy_wait(std::chrono::duration<Rep, Period> d,
               std::atomic<bool>* stop_flag = nullptr) {
  auto start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start < d) {
    if (stop_flag && stop_flag->load(std::memory_order_relaxed)) {
      break;
    }
  }
}

inline void pin_thread_to_core(int core_id) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

inline int target_id(uint16_t key, int shard_size, int system_size) {
  return std::min((int)(key / shard_size), system_size - 1);
}

#define INGEST_ARGS(args)                                                     \
  /* Configure remotes vector */                                              \
  int id = args->uget(romulus::NODE_ID);                                      \
  const std::string remote_str = args->sget(romulus::REMOTES);                \
  std::stringstream ss(remote_str);                                           \
  std::string remote;                                                         \
  std::vector<std::string> machines;                                          \
  while (std::getline(ss, remote, ',')) {                                     \
    machines.push_back(remote);                                               \
  }                                                                           \
  std::string hostname = machines.at(id);                                     \
  uint64_t system_size = machines.size();                                     \
  std::vector<std::string> remotes = machines;                                \
  remotes.erase(remotes.begin() + id);                                        \
  /* Command line arguments */                                                \
  std::string registry_ip = args->sget(romulus::REGISTRY_IP);                 \
  std::string output_file = args->sget(romulus::OUTPUT_FILE);                 \
  /* Clear any stale output file */                                           \
  if (std::filesystem::exists(output_file))                                   \
    std::filesystem::remove(output_file);                                     \
  auto testtime = std::chrono::seconds(args->uget(TESTTIME));                 \
  auto dev_name = args->sget(romulus::DEV_NAME);                              \
  auto dev_port = args->uget(romulus::DEV_PORT);                              \
  ROMULUS_INFO("Node {} of {} is {}", id + 1, system_size, hostname);         \
  std::unordered_map<uint64_t, std::string> mach_map;                         \
  for (int n = 0; n < (int)machines.size(); ++n) {                            \
    mach_map.emplace(n, machines.at(n));                                      \
  }                                                                           \
  auto transport = args->sget(romulus::TRANSPORT_TYPE);                       \
  [[maybe_unused]] uint8_t transport_flag;                                    \
  if (transport == "IB") {                                                    \
    transport_flag = IBV_LINK_LAYER_INFINIBAND;                               \
  } else if (transport == "RoCE") {                                           \
    transport_flag = IBV_LINK_LAYER_ETHERNET;                                 \
  }                                                                           \
  [[maybe_unused]] auto loop = args->uget(LOOP);                              \
  [[maybe_unused]] auto capacity = args->uget(CAPACITY);                      \
  [[maybe_unused]] auto sleep = std::chrono::milliseconds(args->uget(SLEEP)); \
  [[maybe_unused]] auto num_qps = args->uget(NUM_QP);                         \
  [[maybe_unused]] auto key_range = args->uget(KEY_RANGE);                    \
  [[maybe_unused]] auto num_shared_cq = args->uget(NUM_SHARED_CQ);            \
  [[maybe_unused]] auto num_shards = args->uget(NUM_SHARDS);                  \
  [[maybe_unused]] auto txn_size = args->uget(TXN_SIZE);

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

// Function to take elements of a and b and interleave them into a single
// vector, starting with a. The dispersion factor is the probability that an
// element will be from a.
template <class T>
std::vector<T> Disperse(std::vector<T>& a, std::vector<T>& b,
                        const double dispersion_factor) {
  std::vector<T> out;
  out.reserve(a.size() + b.size());
  while(!a.empty() || !b.empty()) {
    if (a.empty()) {
      out.push_back(b.back());
      b.pop_back();
    } else if (b.empty()) {
      out.push_back(a.back());
      a.pop_back();
    } else {
      double r = static_cast<double>(rand()) / RAND_MAX;
      if (r < dispersion_factor) {
        out.push_back(a.back());
        a.pop_back();
      } else {
        out.push_back(b.back());
        b.pop_back();
      }
    }
  }

  return out;
}

}  // namespace
