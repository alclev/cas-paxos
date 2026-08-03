BIN_PATH="experiments/caspaxos"

# parse args:
# usage: $1 == experiment name (caspaxos-plain, caspaxos-multipax, mu, mu_squared, all)

if [ "$#" -ne 1 ]; then
	echo "Usage: <caspaxos-plain | caspaxos-multipax | mu | mu_squared | all>"
	exit 1
fi
EXPERIMENT=""
if [ "$1" == "caspaxos-plain" ]; then
	EXPERIMENT="caspaxos-plain"
elif [ "$1" == "caspaxos-multipax" ]; then
	EXPERIMENT="caspaxos-multipax"
elif [ "$1" == "mu" ]; then
	EXPERIMENT="mu"
elif [ "$1" == "mu_squared" ]; then
	EXPERIMENT="mu_squared"
elif [ "$1" == "all" ]; then
	EXPERIMENT="all"
else
	echo "Invalid experiment name: $1"
	echo "Usage: $0 <caspaxos-plain | caspaxos-multipax | mu | mu_squared | all>"
	exit 1
fi

# 1. Run for default CasPaxos without Multi-Paxos Optimization
if [ "$EXPERIMENT" == "caspaxos-plain" ] || [ "$EXPERIMENT" == "all" ]; then
	outfile="results/sys_size/caspaxos.csv"
	echo "system_size,total_work_us,total_ops,election_lat_ns,lat_avg_ns,lat_50p_ns,lat_99p_ns,lat_99_9p_ns" >"$outfile"
	# compile for default CasPaxos
	bash tools/build.sh release default

	ORIG_MACHINES=("${MACHINES[@]}")
	for i in $(seq 3 ${#ORIG_MACHINES[@]}); do
		MACHINES=("${ORIG_MACHINES[@]:0:$i}")
		load_cfg
		echo "Resetting..."
		reset-all
		reset_memcached
		SYS_SIZE=${#MACHINES[@]}
		echo "Launching experiment with ${SYS_SIZE} nodes..."
		EXTRA_ARGS="--num-qp $((SYS_SIZE + 2))"
		cl_run "$BIN_PATH"
		grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
	done
fi

# 1. Run for default CasPaxos with Multi-Paxos Optimization
if [ "$EXPERIMENT" == "caspaxos-multipax" ] || [ "$EXPERIMENT" == "all" ]; then
	outfile="results/sys_size/caspaxos_multipax.csv"
	echo "system_size,total_work_us,total_ops,election_lat_ns,lat_avg_ns,lat_50p_ns,lat_99p_ns,lat_99_9p_ns" >"$outfile"
	# compile for default CasPaxos
	bash tools/build.sh release default

	ORIG_MACHINES=("${MACHINES[@]}")
	for i in $(seq 3 ${#ORIG_MACHINES[@]}); do
		MACHINES=("${ORIG_MACHINES[@]:0:$i}")
		load_cfg
		echo "Resetting..."
		reset-all
		reset_memcached
		SYS_SIZE=${#MACHINES[@]}
		echo "Launching experiment with ${SYS_SIZE} nodes..."
		EXTRA_ARGS="--num-qp $((SYS_SIZE + 2)) --multipax-opt"
		cl_run "$BIN_PATH"
		grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
	done
fi

# 2. Run for Mu Squared
if [ "$EXPERIMENT" == "mu_squared" ] || [ "$EXPERIMENT" == "all" ]; then
	outfile="results/sys_size/mu_squared.csv"
	outfile_2="results/sys_size/mu_squared_thrus.csv"
	outfile_3="results/sys_size/mu_squared_latencies.csv"
	rm -f "$outfile" "$outfile_2" "$outfile_3"
	# node_id, system_size, total_commits, total_worktime_us, lat_avg, lat_50p, lat_99p, lat_99_9p
	echo "node_id,system_size,total_commits,total_worktime_us,lat_avg,lat_50p,lat_99p,lat_99_9p" >"$outfile"
	# compile for Mu Squared
	bash tools/build.sh release lease
	ORIG_MACHINES=("${MACHINES[@]}")
	for i in $(seq 3 ${#ORIG_MACHINES[@]}); do
		MACHINES=("${ORIG_MACHINES[@]:0:$i}")
		load_cfg
		echo "Resetting..."
		reset-all
		reset_memcached
		SYS_SIZE=${#MACHINES[@]}
		echo "Launching experiment with ${SYS_SIZE} nodes..."
		EXTRA_ARGS="--num-qp $((SYS_SIZE + 2))"
		cl_run "$BIN_PATH"
		# Aggregate logs and extract all lines with [PARSE]
		grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
		grep -hoP '\[THROUGHPUTS\] \K.*' logs/* >>"$outfile_2"
		grep -hoP '\[LATENCIES\] \K.*' logs/* >>"$outfile_3"
	done
fi
# 3. Run for Mu
if [ "$EXPERIMENT" == "mu" ] || [ "$EXPERIMENT" == "all" ]; then
	outfile="results/sys_size/mu.csv"
	echo "system_size,total_work_us,total_ops,election_lat_ns,lat_avg_ns,lat_50p_ns,lat_99p_ns,lat_99_9p_ns" >"$outfile"
	# compile for Mu
	bash tools/build.sh release mu
	source tools/mu.sh
	ORIG_MACHINES=("${MACHINES[@]}")
	echo "Resetting..."
	reset-all
	reset_memcached
	for i in $(seq 3 ${#ORIG_MACHINES[@]}); do
		MACHINES=("${ORIG_MACHINES[@]:0:$i}")
		load_cfg
		SYS_SIZE=${#MACHINES[@]}
		echo "Launching experiment with ${SYS_SIZE} nodes..."
		run_mu "$BIN_PATH"
		# Aggregate logs and extract all lines with [PARSE]
		grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"

	done
fi
