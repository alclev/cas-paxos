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
elif [ "$1" == "velos" ]; then
	EXPERIMENT="velos"
elif [ "$1" == "all" ]; then
	EXPERIMENT="all"
else
	echo "Invalid experiment name: $1"
	echo "Usage: $0 <caspaxos-plain | caspaxos-multipax | mu | mu_squared | all>"
	exit 1
fi


function extract_raw() {
	outfile="$(dirname "$1")/$(basename "$1" .csv)"
	lat_file="${outfile}_lats.raw"
	thru_file="${outfile}_thrus.raw"
	rm -f "$lat_file" "$thru_file"
	grep -hoP '\[THROUGHPUTS\] \K.*' logs/* >>"${thru_file}"
	grep -hoP '\[LATENCIES\] \K.*' logs/* >>"${lat_file}"
}

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
	echo "system_size,total_work_us,total_ops,election_lat_ns,lat_avg_ns,lat_50p_ns,lat_99p_ns,lat_99_9p_ns" >"$outfile"
	# compile for Mu Squared
	bash tools/build.sh release lease

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

	done
fi
# 3. Run for Mu
if [ "$EXPERIMENT" == "mu" ] || [ "$EXPERIMENT" == "all" ]; then

	# compile for Mu
	bash tools/build.sh release mu
	source tools/mu.sh

	outfile="results/v1/mu_sysize.csv"
	rm -f "$outfile"
	echo 'system_size,pipe_depth,total_work_us,total_ops,lat_avg_ns,lat_50p_ns,lat_99p_ns,lat_99_9p_ns' >"$outfile"
	echo "Resetting..."
	send_libs
	reset_mu
	# Varying system size
	echo "Varying system size..."
	ORIG_MACHINES=("${MACHINES[@]}")
	for i in $(seq 3 ${#ORIG_MACHINES[@]}); do
		MACHINES=("${ORIG_MACHINES[@]:0:$i}")
		load_cfg
		SYS_SIZE=${#MACHINES[@]}
		echo "Launching experiment with ${SYS_SIZE} nodes..."
		run_mu "$BIN_PATH"
		# Aggregate logs and extract all lines with [PARSE]
		grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
	done

	extract_raw $(realpath "$outfile")

	outfile="results/v1/mu_pipe.csv"
	rm -f "$outfile"
	echo 'system_size,pipe_depth,total_work_us,total_ops,lat_avg_ns,lat_50p_ns,lat_99p_ns,lat_99_9p_ns' >"$outfile"
	
	echo "Resetting..."
	send_libs
	reset_mu
	# varying the pipeline depth
	echo "Varying the pipeline depth..."
	for p in $(seq 0 11); do
		echo "Launching experiment with pipeline depth ${p}..."
		load_cfg
		ARGS="$ARGS --outstanding-reqs $p"

		run_mu "$BIN_PATH"
		# Aggregate logs and extract all lines with [PARSE]
		grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
	done

	extract_raw $(realpath "$outfile")
fi

if [ "$EXPERIMENT" == "velos" ] || [ "$EXPERIMENT" == "all" ]; then
	# compile for Velos
	bash tools/build.sh release velos

	# echo "Varying system size..."
	# outfile="results/v1/velos_sysize.csv"
	# rm -f "$outfile"
	# echo 'id,system_size,total_commits,total_work_us,lat_avg_us,lat_50p_us,lat_99p_us,lat_99_9p_us' >"$outfile"
	# ORIG_MACHINES=("${MACHINES[@]}")

	# for i in $(seq 3 ${#ORIG_MACHINES[@]}); do
	# 	MACHINES=("${ORIG_MACHINES[@]:0:$i}")
	# 	load_cfg
	# 	echo "Resetting..."
	# 	reset-all
	# 	reset_memcached
	# 	SYS_SIZE=${#MACHINES[@]}
	# 	echo "Launching experiment with ${SYS_SIZE} nodes..."
	# 	EXTRA_ARGS="--outstanding-reqs 1"
	# 	cl_run "$BIN_PATH"
	# 	# Aggregate logs and extract all lines with [PARSE]
	# 	grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
	# done

	# extract_raw $(realpath "$outfile")

	outfile="results/v1/velos_pipe.csv"
	rm -f "$outfile"
	echo 'pipe_depth,id,system_size,total_commits,total_work_us,lat_avg_us,lat_50p_us,lat_99p_us,lat_99_9p_us' >"$outfile"
	
	# varying the pipeline depth
	echo "Varying the pipeline depth..."
	for p in $(seq 1 12); do
		load_cfg
		echo "Resetting..."
		reset-all
		reset_memcached
		echo "Launching experiment with pipeline depth ${p}..."
		EXTRA_ARGS="--outstanding-reqs $p"
		cl_run "$BIN_PATH"
		# Aggregate logs and extract all lines with [PARSE]
		grep -hoP '\[PARSE\] \K.*' logs/* | sed "s/^/${p},/" >>"$outfile"
	done

	extract_raw $(realpath "$outfile")
fi