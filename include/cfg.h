#pragma once

#include <romulus/cli.h>

namespace romulus {
constexpr const char* NODE_ID = "--node-id";
constexpr const char* HOSTNAME = "--hostname";
constexpr const char* REMOTES = "--remotes";
constexpr const char* REGISTRY_IP = "--registry-ip";
constexpr const char* TRANSPORT_TYPE = "--transport-type";
constexpr const char* DEV_NAME = "--dev-name";
constexpr const char* DEV_PORT = "--dev-port";
constexpr const char* TESTTIME = "--testtime";
constexpr const char* LOOP = "--loop";
constexpr const char* CAPACITY = "--capacity";
constexpr const char* BUF_SIZE = "--buf-size";
constexpr const char* SLEEP = "--sleep";
constexpr const char* LEADER_FIXED = "--leader-fixed";
constexpr const char* POLICY = "--policy";
constexpr const char* DURATION = "--duration";
constexpr const char* OUTPUT_FILE = "--output-file";
constexpr const char* HELP = "--help";

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

inline auto ARGS = {
    U64_ARG(NODE_ID, "Numerical identifier for this node."),
    STR_ARG(HOSTNAME, "Hostname of this node."),
    STR_ARG(REMOTES, "Comma-separated list of other nodes."),
    STR_ARG_OPT(REGISTRY_IP, "IP address of the connection registry.",
                "10.10.1.1"),
    ENUM_ARG_OPT(TRANSPORT_TYPE, "RDMA transport type.", "RoCE", {"IB", "RoCE"}),
    STR_ARG_OPT(DEV_NAME, "Name of the RDMA device to use.", "mlx5_3"),
    U64_ARG_OPT(DEV_PORT, "Port number of the RDMA device.", 1),
    U64_ARG_OPT(TESTTIME, "Experiment duration in seconds", 5),
    U64_ARG_OPT(LOOP, "Number of iterations between runtime checks.", 1000),
    U64_ARG_OPT(CAPACITY, "Capacity of the replicated log.", (1ULL << 15)),
    U64_ARG_OPT(BUF_SIZE, "Buffer size for remote writes.", 64),
    U64_ARG_OPT(SLEEP, "Sleep interval between proposals in ms", 100),
    BOOL_ARG_OPT(LEADER_FIXED,
                 "If true, only a single node proposes commands."),
    ENUM_ARG_OPT(POLICY, "Leader rotation policy when not fixed.", "all",
                 {"rotating", "all"}),
    U64_ARG_OPT(DURATION, "Duration of leadership in rotating policy in ms.", 100),
    STR_ARG_OPT(OUTPUT_FILE, "File to output results to.", "stats.csv"),
    BOOL_ARG_OPT(HELP, "Print usage information.")};
}  // namespace romulus
