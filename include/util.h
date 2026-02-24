#pragma once

#include <immintrin.h>

constexpr auto kTimeout = std::chrono::nanoseconds(500'000'000);

std::atomic<bool> dump_requested{false};

template <typename Rep, typename Period>
void busy_wait(std::chrono::duration<Rep, Period> d) {
  auto start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start < d)
    ;
}

#define INGEST_ARGS(args)                                                      \
  /* Configure remotes vector */                                               \
  int id = args->uget(romulus::NODE_ID);                                       \
  const std::string remote_str = args->sget(romulus::REMOTES);                 \
  std::stringstream ss(remote_str);                                            \
  std::string remote;                                                          \
  std::vector<std::string> machines;                                           \
  while (std::getline(ss, remote, ',')) {                                      \
    machines.push_back(remote);                                                \
  }                                                                            \
  std::string hostname = machines.at(id);                                      \
  uint64_t system_size = machines.size();                                      \
  std::vector<std::string> remotes = machines;                                 \
  remotes.erase(remotes.begin() + id);                                         \
  /* Command line arguments */                                                 \
  std::string registry_ip = args->sget(romulus::REGISTRY_IP);                  \
  std::string output_file = args->sget(romulus::OUTPUT_FILE);                  \
  /* Clear any stale output file */                                            \
  if (std::filesystem::exists(output_file))                                    \
    std::filesystem::remove(output_file);                                      \
  auto testtime = std::chrono::seconds(args->uget(romulus::TESTTIME));         \
  auto dev_name = args->sget(romulus::DEV_NAME);                               \
  auto dev_port = args->uget(romulus::DEV_PORT);                               \
  ROMULUS_INFO("Node {} of {} is {}", id + 1, system_size, hostname);          \
  std::unordered_map<uint64_t, std::string> mach_map;                          \
  for (int n = 0; n < (int)machines.size(); ++n) {                             \
    mach_map.emplace(n, machines.at(n));                                       \
  }                                                                            \
  auto transport = args->sget(romulus::TRANSPORT_TYPE);                        \
  [[maybe_unused]] uint8_t transport_flag;                                     \
  if (transport == "IB") {                                                     \
    transport_flag = IBV_LINK_LAYER_INFINIBAND;                                \
  } else if (transport == "RoCE") {                                            \
    transport_flag = IBV_LINK_LAYER_ETHERNET;                                  \
  }                                                                            \
  auto loop = args->uget(romulus::LOOP);                                       \
  auto capacity = args->uget(romulus::CAPACITY);                               \
  auto buf_size = args->uget(romulus::BUF_SIZE);                               \
  auto sleep = std::chrono::milliseconds(args->uget(romulus::SLEEP));          \
  auto leader_fixed = args->bget(romulus::STABLE_LEADER);                      \
  auto policy = args->sget(romulus::POLICY);                                   \
  auto duration = std::chrono::milliseconds(args->uget(romulus::DURATION));    \
  /* More config */                                                            \
  std::ofstream outfile(output_file, std::ios::app);                           \
  if (!outfile)                                                                \
    ROMULUS_FATAL("Could not open output file.");                              \
  outfile << "lat_avg_us,lat_50p_us,lat_99p_us,lat_99_9p_us,throughput_ops_"   \
             "s\n";                                                            \
  if (policy == "rotating") {                                                  \
    if (duration >= testtime) {                                                \
      ROMULUS_FATAL(                                                           \
          "Configured to rotate leaders but duration is not short enough "     \
          "duration={}ms, testtime={}ms",                                      \
          duration.count(), testtime.count());                                 \
    }                                                                          \
  }                                                                            \
  [[maybe_unused]] auto multipax_opt = args->bget(romulus::MULTIPAX_OPT);      \
  [[maybe_unused]] auto verbose = args->bget(romulus::VERBOSE);

#define FILL_PROPOSALS()                                                       \
  /* Populate some proposals. */                                               \
  /* Random number generator. */                                               \
  std::random_device rand_device;                                              \
  auto engine = std::mt19937(rand_device());                                   \
  /* end values are reserved for ctrl messages */                              \
  auto rng = std::uniform_int_distribution<uint32_t>(                          \
      0, std::numeric_limits<uint32_t>::max() - 10);                           \
  std::vector<std::pair<uint32_t, uint8_t *>> proposals;                       \
  proposals.reserve(kNumProposals);                                            \
  const std::string alphanum =                                                 \
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";        \
  for (uint32_t i = 0; i < kNumProposals; ++i) {                               \
    proposals.emplace_back(buf_size, new uint8_t[buf_size]);                   \
    for (uint32_t j = 0; j < buf_size; ++j) {                                  \
      proposals[i].second[j] = alphanum[rng(engine) % alphanum.size()];        \
    }                                                                          \
  }


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
