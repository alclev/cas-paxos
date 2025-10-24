# CAS-Paxos : A cas-based Paxos consensus variant

This library builds upon abortable cas-based consensus over Remote Direct Memory Access (RDMA). 
The RDMA control plane is defined in the following library: [Romulus](https://github.com/sss-lehigh/remus/tree/romulus).

## Usage

Refer to script `tools/cl.sh`, which can and should be used to manage experiments and development. 
The first thing that **must** be done before running any code is to update the parameters in `config/cloudlab.conf`. This tells the script which machines to deploy the binary executables to. Then, the enviroment must be prepared using `cl.sh install-deps`. From here, the remaining usage is application-specific. 
#### Dependencies
- `librdmacm-dev`
-  `ibverbs-utils `
-  `libnuma-dev`
-  `libgtest-dev`
-  `libibverbs-dev`
-  `libmemcached-dev`
-  `memcached `
-  `ibevent-dev`
-  `libhugetlbfs-dev`
-  `numactl`
-  `libgflags-dev` 
-  `libssl-dev`

Example #1
---
```bash
# Edit config/cloudlab.conf
cd tools/
bash cl.sh install-deps
bash cl.sh build-run release "experiments/simple"
```
Example #2
---
```bash
# Edit config/cloudlab.conf
cd tools/
bash cl.sh install-deps
bash cl.sh build-release
bash cl.sh run "experiments/simple"
```

Example #3 : Debugging
---
```bash
# Edit config/cloudlab.conf
cd tools/
bash cl.sh install-deps
bash cl.sh build-run release "experiments/simple"
bash cl.sh run-debug "experiments/simple" "catch throw"
```

## Key Features

## System Architecture / Design

### Algorithm

#### Normal Execution (Leader Fixed)
- Background thread listening for failures
  - In the event of a failure detection → [Failure case](#failure-case)
- Leader remains fixed throghout execution and delivers all proposals
  - `Propose`
  - `Prepare`
  - `Promise`
  - `Commit`
#### Normal Execution (All)
- Background thread listening for failures
  - In the event of a failure detection → [Failure case](#failure-case)
- All hosts propose a value, and leadership is **earned** through successful commit
- Safety is ensured by the properties of the cas-based PREPARE and ACCEPT phases
All hosts execute:
  - `Propose` 
  - `Prepare` (CAS quorum)
  - `Promise` (CAS quorum)
  - `Commit` (whoever committed this will become leader for the next round)
    → [Leader Change](#leader-change)
  - From here, we employ Multi-Paxos optimization by assuming stable leader for subsequent rounds
  - Repeat
#### Normal Execution (Rotating)
- Background thread listening for failures
  - In the event of a failure detection → [Failure case](#failure-case)
- Each host leads a **portion** of the overall execution
- Leadership is earned by the means as [Normal Rotating Execution](#normal-execution-rotating)
A subset of the hosts execute:
  - `Propose` 
  - `Prepare` (CAS quorum)
  - `Promise` (CAS quorum)
  - `Commit` (whoever committed this will become leader for the next round)
    - [Leader Change](#leader-change)
  - From here, we employ Multi-Paxos optimization by assuming stable leader for subsequent rounds
  - Repeat
#### Failure Case
1. [Leader Relection](#leader-change)
2. `CatchUp`
   - Sync committed slots starting at log offset
#### Leader change
1. New leader is chosen by simple rule
2. New leader updates its thread-local state to reflect leadership
3. New leader `CAS`s <leader> slot with it's STATE metadata
4. **Everyone** else perform a `READ` on <leader> slot
    - If empty → No leader has been elected yet, proceed. 
    - Else, update the following relavent host-local metadata:
       1. `<Leader's ballot number>`
       2. `<Last accepted (ballot, value)>`
5. Continue normal execution

## Citations

## Contributing

This project is part of ongoing academic research at Lehigh University.  
Collaboration is encouraged, and for any external contributions please open a PR.

## License

Copyright (c) 2025 Alex Clevenger

This code is made available under a [MIT License](./LICENSE) intended for **academic research**.  


