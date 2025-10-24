#include <romulus/common.h>
#include <romulus/connection_manager.h>
#include <romulus/registry.h>
#include <romulus/romulus.h>

#include <filesystem>
#include <functional>
#include <random>
#include <string>

#include "cas_paxos_impl.h"
#include "cfg.h"
#include "state.h"

#define PAXOS_NS paxos_st
constexpr uint32_t kNumProposals = 8092;

template <typename Rep, typename Period>
void busy_wait(std::chrono::duration<Rep, Period> d) {
  auto start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start < d);
}

int main(int argc, char* argv[]) {
  ROMULUS_STOPWATCH_DECLARE();
  romulus::INIT();
  auto args = std::make_shared<romulus::ArgMap>();
  args->import(romulus::ARGS);
  args->parse(argc, argv);
  // Configure remotes vector
  const std::string remote_str = args->sget(romulus::REMOTES);
  std::stringstream ss(remote_str);
  std::string remote;
  std::string hostname = args->sget(romulus::HOSTNAME);
  std::vector<std::string> remotes;
  while (std::getline(ss, remote, ',')) {
    if (remote == hostname) continue;
    remotes.push_back(remote);
  }
  // Command line arguments
  int host_id = args->uget(romulus::NODE_ID);
  std::string registry_ip = args->sget(romulus::REGISTRY_IP);
  std::string outfile = args->sget(romulus::OUTPUT_FILE);
  // Clear any stale output file
  if (std::filesystem::exists(outfile)) std::filesystem::remove(outfile);
  auto testtime =
      std::chrono::seconds(args->uget(romulus::TESTTIME));  // in seconds
  auto dev_name = args->sget(romulus::DEV_NAME);
  auto dev_port = args->uget(romulus::DEV_PORT);
  auto transport = args->sget(romulus::TRANSPORT_TYPE);
  uint8_t transport_flag;
  if (transport == "IB") {
    transport_flag = IBV_LINK_LAYER_INFINIBAND;
  } else if (transport == "RoCE") {
    transport_flag = IBV_LINK_LAYER_ETHERNET;
  }
  auto loop = args->uget(romulus::LOOP);
  auto capacity = args->uget(romulus::CAPACITY);
  auto buf_size = args->uget(romulus::BUF_SIZE);
  auto sleep =
      std::chrono::milliseconds(args->uget(romulus::SLEEP));  // in milliseconds
  auto leader_fixed = args->bget(romulus::LEADER_FIXED);
  auto policy = args->sget(romulus::POLICY);
  auto duration = std::chrono::milliseconds(
      args->uget(romulus::DURATION));  // in milliseconds
  // More config
  int system_size = remotes.size() + 1;

  // Random number generator.
  std::random_device rand_device;
  auto engine = std::mt19937(rand_device());
  auto rng = std::uniform_int_distribution<uint32_t>(
      0, std::numeric_limits<uint32_t>::max());

  // Print configuration.
  ROMULUS_INFO("Experimental Configuration:");
  ROMULUS_INFO("!> [CONF] hostname={}", hostname);
  ROMULUS_INFO("!> [CONF] host id={}", host_id);
  ROMULUS_INFO("!> [CONF] registry ip={}", registry_ip);
  ROMULUS_INFO("!> [CONF] output file={}", outfile);
  ROMULUS_INFO("!> [CONF] testtime={}_s", testtime.count());
  ROMULUS_INFO("!> [CONF] device name={}", dev_name);
  ROMULUS_INFO("!> [CONF] device port={}", dev_port);
  ROMULUS_INFO("!> [CONF] transport type={}", transport);
  ROMULUS_INFO("!> [CONF] loop={}", loop);
  ROMULUS_INFO("!> [CONF] capacity={}", capacity);
  ROMULUS_INFO("!> [CONF] buf_size={}", buf_size);
  ROMULUS_INFO("!> [CONF] sleep={}_ms", sleep.count());
  ROMULUS_INFO("!> [CONF] leader_fixed={}", leader_fixed);
  ROMULUS_INFO("!> [CONF] policy={}", policy);
  ROMULUS_INFO("!> [CONF] duration={}_ms", duration.count());
  ROMULUS_INFO("!> [CONF] system_size={}", system_size);

  if (policy == "rotating") {
    if (duration >= testtime) {
      ROMULUS_FATAL(
          "Configured to rotate leaders but duration is not short enough "
          "duration={}ms, testtime={}ms",
          duration.count(), testtime.count());
    }
  }
  ROMULUS_INFO("Remotes size {}", remotes.size());
  INIT_CONSENSUS(transport_flag, buf_size);
  // Populate some proposals.
  std::vector<std::pair<uint32_t, uint8_t*>> proposals;
  proposals.reserve(kNumProposals);
  const std::string alphanum =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  for (uint32_t i = 0; i < kNumProposals; ++i) {
    proposals.emplace_back(buf_size, new uint8_t[buf_size]);
    for (uint32_t j = 0; j < buf_size; ++j) {
      proposals[i].second[j] = alphanum[rng(engine) % alphanum.size()];
    }
  }

  std::function<void(void)> init = INIT_LATENCY;
  std::function<void(void)> exec = EXEC_LATENCY;
  std::function<void(void)> done = DONE_LATENCY;
  std::function<void(void)> calc = CALC_LATENCY;
  std::function<void(void)> reset = RESET;

  init();
  ROMULUS_INFO("Starting latency test");
  ROMULUS_STOPWATCH_BEGIN();
  while (ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS) <
         static_cast<uint64_t>(testtime.count())) {
    for (uint32_t i = 0; i < loop; ++i) {
      if (leader_fixed && host_id == 0) {
        exec();
      } else if (!leader_fixed && policy == "all") {
        // All hosts try to propose.
        exec();
      } else if (!leader_fixed && policy == "rotating") {
        // Each host leads a portion of the overall execution.
        auto curr_us = ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS);
        if ((static_cast<uint32_t>(curr_us) /
             static_cast<uint32_t>(duration.count())) %
                system_size ==
            static_cast<uint32_t>(host_id)) {
          exec();
        } else {
          done();
        }
      }
      busy_wait(sleep);
    }
  }
  done();
  calc();

  init = INIT_THROUGHPUT;
  exec = EXEC_THROUGHPUT;
  done = DONE_THROUGHPUT;
  calc = CALC_THROUGHPUT;

  [[maybe_unused]] int x = 0;
  bool first = true;
  init();
  ROMULUS_INFO("Starting throughput test");
  ROMULUS_STOPWATCH_BEGIN();
  ROMULUS_STOPWATCH_START();
  while (ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS) < testtime.count()) {
    for (uint32_t i = 0; i < loop; ++i) {
      if (leader_fixed && host_id == 0) {
        exec();
      } else if (!leader_fixed && policy == "all") {
        // All hosts try to propose.
        exec();
      } else if (!leader_fixed && policy == "rotating") {
        // Each host leads a portion of the overall execution.
        auto curr_us = ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS);
        if ((static_cast<uint32_t>(curr_us) /
             static_cast<uint32_t>(duration.count())) %
                system_size ==
            static_cast<uint32_t>(host_id)) {
          if (first) x++;
          first = false;
          exec();
        } else {
          first = true;
          done();
        }
      }
      busy_wait(sleep);
    }
  }
  done();
  calc();
  ROMULUS_DEBUG("times_leader={}", x);

  for (auto& p : proposals) {
    delete[] p.second;
  }
  return 0;
}