#include "romulus/cfg.h"
#include "romulus/common.h"
#include "romulus/connection_manager.h"
#include "romulus/device.h"
#include "romulus/memblock.h"
#include "romulus/qp_pol.h"
#include "romulus/stats.h"
#include "romulus/util.h"

#include <filesystem>
#include <functional>
#include <random>
#include <string>

#include "cfg.h"
#include "state.h"
#include "util.h"

#ifdef USE_MU
#include "mu/mu_impl.h"
#else
#include "cas_paxos_impl.h"
#endif

#define PAXOS_NS paxos_st
constexpr uint32_t kNumProposals = 8092;

int main(int argc, char* argv[]) {
  ROMULUS_STOPWATCH_DECLARE();
  romulus::INIT();
  auto args = std::make_shared<romulus::ArgMap>();
  args->import(romulus::ARGS);
  args->import(romulus::EXTRA_ARGS);
  args->parse(argc, argv);
  INGEST_ARGS(args);

  // Print configuration.
  ROMULUS_INFO("Experimental Configuration:");
  ROMULUS_INFO("!> [CONF] hostname={}", hostname);
  ROMULUS_INFO("!> [CONF] host id={}", id);
  ROMULUS_INFO("!> [CONF] registry ip={}", registry_ip);
  ROMULUS_INFO("!> [CONF] output file={}", output_file);
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
  ROMULUS_INFO("!> [CONF] output file={}", output_file);
  
  INIT_CONSENSUS(transport_flag, buf_size, mach_map);
  FILL_PROPOSALS();

  std::function<void(void)> init = INIT_LATENCY;
  std::function<void(void)> exec = EXEC_LATENCY;
  std::function<void(void)> done = DONE_LATENCY;
  std::function<void(std::fstream&)> calc = CALC_LATENCY;
  std::function<void(void)> reset = RESET;

  init();

  ROMULUS_INFO("Starting latency test");
  ROMULUS_INFO("MultiPaxos Optimization: {}", multipax_opt ? "ON" : "OFF");
  auto testtime_us =
      std::chrono::duration_cast<std::chrono::microseconds>(testtime);
  ROMULUS_STOPWATCH_BEGIN();
  while (ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS) <
         static_cast<uint64_t>(testtime_us.count())) {
    for (uint32_t i = 0; i < loop; ++i) {
      exec();
      busy_wait(sleep);
    }
  }
  done();
  calc(outfile);

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
  while (ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MICROSECONDS) <
         static_cast<uint64_t>(testtime_us.count())) {
    for (uint32_t i = 0; i < loop; ++i) {
      if (leader_fixed && id == 0) {
        exec();
      } else if (!leader_fixed && policy == "all") {
        // All hosts try to propose.
        exec();
      } else if (!leader_fixed && policy == "rotating") {
        // Each host leads a portion of the overall execution.
        auto curr_ms = ROMULUS_STOPWATCH_RUNTIME(ROMULUS_MILLISECONDS);
        if ((static_cast<uint32_t>(curr_ms) / duration.count()) % system_size ==
            id) {
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
  calc(outfile);
  ROMULUS_INFO("Experiment is finished. Cleaning up...");
  outfile.close();
  for (auto& p : proposals) {
    delete[] p.second;
  }
  exit(0);
  return 0;
}