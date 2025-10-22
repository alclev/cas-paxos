
#include "paxos.h"

#include <absl/flags/flag.h>
#include <absl/flags/parse.h>

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <vector>

#ifdef USEMU
#include "mu_impl.h"
#else
#include "cas_paxos_impl.h"
#endif

#include "common/common.h"
#include "common/rome.h"
#include "connection_manager/connection_manager.h"
#include "registry/registry.h"

#define PAXOS_NS paxos_st

ABSL_FLAG(std::string, exp_name, "paxos",
          "Name of experiment. Helpful to differentiate when parsing output "
          "for .csv file.");
ABSL_FLAG(int, host_id, -1, "Identifier of this node.");
ABSL_FLAG(std::string, hostname, "", "Name of this node.");
ABSL_FLAG(std::vector<std::string>, remotes, std::vector<std::string>{""},
          "List of names of the other nodes in the system.");
ABSL_FLAG(std::string, registry_ip, "10.10.1.1",
          "IP address of the connection registry.");
ABSL_FLAG(std::string, dev_name, "",
          "Name of NIC to use when setting up RDMA resources.");
ABSL_FLAG(int, dev_port, -1, "Port to use when setting up RDMA resources.");
ABSL_FLAG(int, reset_barrier, -1,
          "Reset the barrier to the given value; if -1 then do not set. Should "
          "only be called by one node.");
ABSL_FLAG(absl::Duration, testtime, absl::Seconds(10),
          "Amount of time spent writing to remote buffers,");
ABSL_FLAG(uint32_t, loop, 1000,
          "Number of iterations to perform between checking the runtime.");
ABSL_FLAG(uint32_t, capacity, 10000000, "Size of log.");
ABSL_FLAG(uint32_t, buf_size, 64, "Size of buffer to write to remotes.");
ABSL_FLAG(absl::Duration, sleep, absl::Milliseconds(100),
          "Length of time to sleep between proposals.");
ABSL_FLAG(bool, leader_fixed, true,
          "Run the experiment with only a single node proposing commands.");
ABSL_FLAG(
    std::string, policy, "rotating",
    "Policy to use when the leader is not fixed. One of: {rotating, all}");
ABSL_FLAG(absl::Duration, duration, absl::Milliseconds(100),
          "Length of time a node is the leader when running with the rotating "
          "policy.");

constexpr uint32_t kNumProposals = 8092;

int main(int argc, char* argv[]) {
  DYNO_INIT_LOG();
  ROME_STOPWATCH_DECLARE();
  absl::ParseCommandLine(argc, argv);

  // Command line arguments
  std::string exp_name = absl::GetFlag(FLAGS_exp_name);
  std::string hostname = absl::GetFlag(FLAGS_hostname);
  int host_id = absl::GetFlag(FLAGS_host_id);
  std::vector<std::string> remotes = absl::GetFlag(FLAGS_remotes);
  std::string registry_ip = absl::GetFlag(FLAGS_registry_ip);
  [[maybe_unused]] int reset_barrier = absl::GetFlag(FLAGS_reset_barrier);
  auto testtime = absl::GetFlag(FLAGS_testtime);
  auto dev_name = absl::GetFlag(FLAGS_dev_name);
  auto dev_port = absl::GetFlag(FLAGS_dev_port);
  [[maybe_unused]] auto loop = absl::GetFlag(FLAGS_loop);
  auto capacity = absl::GetFlag(FLAGS_capacity);
  auto buf_size = absl::GetFlag(FLAGS_buf_size);
  auto sleep = absl::GetFlag(FLAGS_sleep);
  auto leader_fixed = absl::GetFlag(FLAGS_leader_fixed);
  auto policy = absl::GetFlag(FLAGS_policy);
  auto duration = absl::GetFlag(FLAGS_duration);

  // More connfig
  int system_size = remotes.size() + 1;
  auto testtime_us = absl::ToDoubleMicroseconds(testtime);
  auto duration_us = absl::ToDoubleMicroseconds(duration);

  // Random number generator.
  std::random_device rand_device;
  auto engine = std::mt19937(rand_device());
  auto rng = std::uniform_int_distribution<uint32_t>(
      0, std::numeric_limits<uint32_t>::max());

  // Print configuration.
  DYNO_INFO("!> [CONF] exp_name={}", exp_name);
  DYNO_INFO("!> [CONF] sys_size={}", system_size);
  DYNO_INFO("!> [CONF] host={}", hostname);
  DYNO_INFO("!> [CONF] dev_name={}", dev_name);
  DYNO_INFO("!> [CONF] dev_port={}", dev_port);
  DYNO_INFO("!> [CONF] capacity={}", capacity);
  DYNO_INFO("!> [CONF] buf_size={}", buf_size);
  DYNO_INFO("!> [CONF] leader_fixed={}", leader_fixed);
  DYNO_INFO("!> [CONF] policy={}", policy);
  if (policy == "rotating") {
    if (duration >= testtime) {
      DYNO_WARN(
          "Configured to rotate leaders but duration is not short enough. "
          "duration={:4.2f}ms, testtime={:4.2f}ms",
          absl::ToDoubleMilliseconds(duration),
          absl::ToDoubleMilliseconds(testtime));
    }
    DYNO_INFO("!> [CONF] duration={:4.2f}ms",
              absl::ToDoubleMilliseconds(duration));
  }
  DYNO_INFO("!> [CONF] testtime={:4.2f}ms",
            absl::ToDoubleMilliseconds(testtime));

  INIT_CONSENSUS();

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
  DYNO_INFO("Starting latency test");
  ROME_STOPWATCH_BEGIN();
  while (ROME_STOPWATCH_RUNTIME(ROME_MICROSECONDS) < testtime_us) {
    for (uint32_t i = 0; i < loop; ++i) {
      if (leader_fixed && host_id == 0) {
        exec();
      } else if (!leader_fixed && policy == "all") {
        // All hosts try to propose.
        exec();
      } else if (!leader_fixed && policy == "rotating") {
        // Each host leads a portion of the overall execution.
        auto curr_us = ROME_STOPWATCH_RUNTIME(ROME_MICROSECONDS);
        if ((static_cast<uint32_t>(curr_us) /
             static_cast<uint32_t>(duration_us)) %
                system_size ==
            static_cast<uint32_t>(host_id)) {
          exec();
        } else {
          done();
        }
      }

      if (sleep > absl::ZeroDuration()) {
        auto start = absl::Now();
        while (absl::Now() - start < sleep)
          ;
      }
    }
  }
  done();
  calc();

  init = INIT_THROUGHPUT;
  exec = EXEC_THROUGHPUT;
  done = DONE_THROUGHPUT;
  calc = CALC_THROUGHPUT;

  int x = 0;
  bool first = true;
  init();
  DYNO_INFO("Starting throughput test");
  ROME_STOPWATCH_BEGIN();
  ROME_STOPWATCH_START();
  while (ROME_STOPWATCH_RUNTIME(ROME_MICROSECONDS) < testtime_us) {
    for (uint32_t i = 0; i < loop; ++i) {
      if (leader_fixed && host_id == 0) {
        exec();
      } else if (!leader_fixed && policy == "all") {
        // All hosts try to propose.
        exec();
      } else if (!leader_fixed && policy == "rotating") {
        // Each host leads a portion of the overall execution.
        auto curr_us = ROME_STOPWATCH_RUNTIME(ROME_MICROSECONDS);
        if ((static_cast<uint32_t>(curr_us) /
             static_cast<uint32_t>(duration_us)) %
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

      if (sleep > absl::ZeroDuration()) {
        auto start = absl::Now();
        while (absl::Now() - start < sleep)
          ;
      }
    }
  }
  done();
  calc();
  DYNO_WARN("times_leader={}", x);

  for (auto& p : proposals) {
    delete[] p.second;
  }
  return 0;
}