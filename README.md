# Cas-Paxos : A CAS-based Paxos abortable-consensus variant

This library builds upon classical cas-based consensus over Remote Direct Memory Access (RDMA). 
The RDMA control plane is developed and managed by the following library: [Remus](https://github.com/sss-lehigh/remus)

## Usage

Refer to script `tools/cl.sh`, which can and should be used to manage experiments and development. 
The first thing that **must** be done before running any code is to update the parameters in `config/cloudlab.conf`. This tells the script which machines to deploy the binary executables to. Then, the enviroment must be prepared using `cl.sh install-deps`. From here, the remaining usage is experiment-specific. 
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

## Citations

## Contributing