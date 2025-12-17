#pragma once

#include <romulus/cli.h>

namespace romulus {
constexpr const char* HOSTNAME = "--hostname";
constexpr const char* TESTTIME = "--testtime";
constexpr const char* LOOP = "--loop";
constexpr const char* CAPACITY = "--capacity";
constexpr const char* BUF_SIZE = "--buf-size";
constexpr const char* SLEEP = "--sleep";
constexpr const char* STABLE_LEADER = "--stable-leader";
constexpr const char* DURATION = "--duration";
constexpr const char* MULTIPAX_OPT = "--multipax-opt";
constexpr const char* VERBOSE = "--verbose";


// Cloudlab notes:
// r320
// - device: mlx4_0
// - port 1: IB
// - port 2: RoCE

// xl170
// - device: mlx5_0 (10 Gbps)
//  - port 1: RoCE
// - device: mlx5_3 (25 Gbps)
//  - port 1: RoCE

inline auto EXTRA_ARGS = {
    STR_ARG(HOSTNAME, "Hostname of this node."),
    U64_ARG_OPT(TESTTIME, "Experiment duration in seconds", 5),
    U64_ARG_OPT(LOOP, "Number of iterations between runtime checks.", 1000),
    U64_ARG_OPT(CAPACITY, "Capacity of the replicated log.", (1ULL << 15)),
    U64_ARG_OPT(BUF_SIZE, "Buffer size for remote writes.", 64),
    U64_ARG_OPT(SLEEP, "Sleep interval between proposals in ms", 10),
    BOOL_ARG_OPT(STABLE_LEADER,
                 "If true, only a single node proposes commands."),
    U64_ARG_OPT(DURATION, "Duration of leadership in rotating policy in ms.",
                100),
    BOOL_ARG_OPT(MULTIPAX_OPT, "Enable the multipaxos optimization"),
    BOOL_ARG_OPT(VERBOSE, "Enable verbose mode")};
};  // namespace romulus