

#include <romulus/romulus.h>
#include <romulus/common.h>
#include <romulus/connection_manager.h>
#include <romulus/registry.h>

#include "cas_paxos_impl.h"
#include "state.h"

#define PAXOS_NS paxos_st
constexpr uint32_t kNumProposals = 8092;

int main(int argc, char* argv[]) {
  romulus::INIT();
  auto args = std::make_shared<romulus::ArgMap>();
  args->import(romulus::ARGS);
  args->parse(argc, argv);

  // Command line arguments
  int id = args->uget(romulus::NODE_ID);
  std::string outfile = args->sget(romulus::OUTPUT_FILE);
  std::string registry_ip = args->sget(romulus::REGISTRY_IP);

  const std::string remote_str = args->sget(romulus::REMOTES);
  std::stringstream ss(remote_str);
  std::string remote;
  std::string hostname = args->sget(romulus::HOSTNAME);
  std::vector<std::string> remotes;
  while (std::getline(ss, remote, ',')) {
    if (remote == hostname) continue;
    remotes.push_back(remote);
  }

  if (std::filesystem::exists(outfile)) std::filesystem::remove(outfile);

  // TODO --------------------------------------------
  ROME_STOPWATCH_DECLARE();
  absl::ParseCommandLine(argc, argv);

  // Command line arguments
  std::string exp_name = absl::GetFlag(FLAGS_exp_name);
  std::string hostname = absl::GetFlag(FLAGS_hostname);
  int host_id = absl::GetFlag(FLAGS_host_id);
  std::vector<std::string> remotes = absl::GetFlag(FLAGS_remotes);
  std::string registry_ip = absl::GetFlag(FLAGS_registry_ip);
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
  return 0
}