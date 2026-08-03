# Core Algorithm

## Version 1

1. Node n receives a request with kv-pair kv belonging to shard s
2. Node n begins `Propose(kv)` for shard s
   1. Perform quorum READ on the lease table at index s
   2. Reduce over the quorum to obtain the state with the highest ballot
      number --> state_highest
   3. state_highest.extract_id() --> current_lease_holder
   4. If current_lease_holder == n --> `Fast-Commit(kv)`. Done.
   5. Else, `Lease-Acquisition` for shard s:
      1. Node n executes `Prepare(s)` + `Promise(s)` on a quorum of nodes
      2. If quorum of successful `Promise(s)`, n has acquired the lease
         for shard s
         1. `Fast-Commit(kv)` on a quorum of nodes. Done.
      3. Else, n failed to acquire the lease; retry `Propose(kv)` after backoff

## Version 2

1. Node n receives a request with kv-pair kv belonging to shard s
2. Node n begins `Propose(kv)` for shard s
   1. Read local lease cache at index s --> current_lease_holder
   2. If current_lease_holder == n --> `Fast-Commit(kv)`. Done.
   3. Else, `Lease-Acquisition` for shard s:
      1. Node n executes `Prepare(s)` + `Promise(s)`
      2. If quorum of successful `Promise(s)`, n has claimed the lease for
         shard s --> permission switch:
         1. Revoke prior owner's write access to n's own log[s]
         2. Send (N-1) `PermSwitch` requests
            - Receiving node C's background thread: validate the request
              against C's local lease cell, then revoke prior owner and
              grant n on C's (peer, shard s) QPs, then ack
         3. **Wait for a quorum of acks**
         4. Update local lease cache: lease[s] = {owner: n, epoch: e}
         5. `Fast-Commit(kv)`. Done.
      3. Else, n failed to claim the lease; retry `Propose(kv)` after backoff