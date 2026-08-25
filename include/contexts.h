#pragma once

#include <cstdint>
#include <vector>

#include "romulus/common.h"
#include "romulus/rc.h"
#include "state.h"

struct cons_ctx_t {
  std::vector<romulus::ReliableConnection*> conns_;
  std::vector<romulus::RemoteAddr> proposed_;
  romulus::LocalAddr laddr_;
  std::vector<std::vector<romulus::RemoteAddr>> log_mat_;
  cons_ctx_t() = default;
  cons_ctx_t(std::vector<romulus::ReliableConnection*> conns,
             std::vector<romulus::RemoteAddr> proposed,
             romulus::LocalAddr scratch,
             std::vector<std::vector<romulus::RemoteAddr>> log_mat)
      : conns_(std::move(conns)),
        proposed_(std::move(proposed)),
        laddr_(scratch),
        log_mat_(std::move(log_mat)) {}
};

struct replication_ctx_t {
  std::vector<std::vector<romulus::ReliableConnection*>> conns_mat_;
  std::vector<std::vector<romulus::RemoteAddr>> raddrs_mat_;
  romulus::LocalAddr laddr_;
  replication_ctx_t() = default;
  replication_ctx_t(
      std::vector<std::vector<romulus::ReliableConnection*>> conns_mat,
      std::vector<std::vector<romulus::RemoteAddr>> raddrs_mat,
      romulus::LocalAddr laddr)
      : conns_mat_(std::move(conns_mat)),
        raddrs_mat_(std::move(raddrs_mat)),
        laddr_(laddr) {}
};

struct perm_ctx_t {
  std::vector<romulus::ReliableConnection*> conns_;
  romulus::LocalAddr laddr_;
  std::vector<romulus::RemoteAddr> req_raddrs_;
  std::vector<romulus::RemoteAddr> grant_raddrs_;
  perm_ctx_t() = default;
  perm_ctx_t(std::vector<romulus::ReliableConnection*> conns,
             std::vector<romulus::RemoteAddr> req_raddrs,
             std::vector<romulus::RemoteAddr> grant_raddrs,
             romulus::LocalAddr laddr)
      : conns_(std::move(conns)),
        laddr_(laddr),
        req_raddrs_(std::move(req_raddrs)),
        grant_raddrs_(std::move(grant_raddrs)) {}
};

struct fd_ctx_t {
  std::vector<romulus::ReliableConnection*> conns_;
  std::vector<romulus::RemoteAddr> raddrs_;
  romulus::LocalAddr laddr_;

  fd_ctx_t() = default;
  fd_ctx_t(std::vector<romulus::ReliableConnection*> conns,
           std::vector<romulus::RemoteAddr> raddrs,
           romulus::LocalAddr laddr)
      : conns_(std::move(conns)),
        raddrs_(std::move(raddrs)),
        laddr_(laddr) {}
};