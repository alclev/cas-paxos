BIN_PATH="experiments/caspaxos"

# parse args
# only valid: $1: sys_size, pipe, shards, threads, outliers
if [ $# -ne 2 ]; then
	echo "Usage: $0 <sys_size|pipe|shards|threads|outliers|all> <musq|vesq>"
	echo "$#" "$1" "$2"
	exit 1
fi

ARG1=$1
ARG2=$2

function extract_raw() {
	outfile="$(dirname "$1")/$(basename "$1" .csv)"
	lat_file="${outfile}_lats.raw"
	thru_file="${outfile}_thrus.raw"
	rm -f "$lat_file" "$thru_file"
	grep -hoP '\[THROUGHPUTS\] \K.*' logs/* >>"${thru_file}"
	grep -hoP '\[LATENCIES\] \K.*' logs/* >>"${lat_file}"
}

if [[ "$ARG2" == "musq" ]]; then
	bash tools/build.sh release musq
	OUTPATH="results/v1/musq"
elif [[ "$ARG2" == "vesq" ]]; then
	bash tools/build.sh release vesq
	OUTPATH="results/v1/vesq"
else
	echo "Invalid argument: $ARG2"
	exit 1
fi

mkdir -p "$OUTPATH"

if [[ "$ARG1" == "all" || "$ARG1" == "sys_size" ]]; then
	echo "Varying system size..."

	outfile="${OUTPATH}/sys_size.csv"
	echo "id,system_size,total_commits,work_time_us,lat_avg_us,lat_50p_us,lat_99p_us,lat_99_9p_us" >"$outfile"

	ORIG_MACHINES=("${MACHINES[@]}")
	for i in $(seq 3 ${#ORIG_MACHINES[@]}); do
		MACHINES=("${ORIG_MACHINES[@]:0:$i}")
		load_cfg
		echo "Resetting..."
		reset-all
		reset_memcached
		SYS_SIZE=${#MACHINES[@]}
		echo "Launching experiment with ${SYS_SIZE} nodes..."
		EXTRA_ARGS="--num-shards $((SYS_SIZE * 2)) --num-handlers 1 --pipeline-depth 1 --no-outliers"
		cl_run "$BIN_PATH"
		grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"

	done

	extract_raw $(realpath "$outfile")
fi

if [[ "$ARG1" == "all" || "$ARG1" == "pipe" ]]; then
	echo "Varying pipeline depth..."

	outfile="${OUTPATH}/pipe.csv"
	echo "pipe_depth,id,system_size,total_commits,work_time_us,lat_avg_us,lat_50p_us,lat_99p_us,lat_99_9p_us" >"$outfile"

	PIPE_DEPTHS=($(seq 1 12))
	for d in "${PIPE_DEPTHS[@]}"; do
		load_cfg
		echo "Resetting..."
		reset-all
		reset_memcached
		EXTRA_ARGS="--num-shards 20 --num-handlers 1 --pipeline-depth ${d} --no-outliers"
		cl_run "$BIN_PATH"
		grep -hoP '\[PARSE\] \K.*' logs/* | sed "s/^/${d},/" >>"$outfile"
	done

	extract_raw $(realpath "$outfile")
fi

if [[ "$ARG1" == "all" || "$ARG1" == "shards" ]]; then
	echo "Varying number of shards..."

	outfile="${OUTPATH}/shards.csv"
	echo "num_shards,id,system_size,total_commits,work_time_us,lat_avg_us,lat_50p_us,lat_99p_us,lat_99_9p_us" >"$outfile"

	NUM_SHARDS=(10 15 20 25 30 35 40 45 50 55 60 65 70 75)
	for s in "${NUM_SHARDS[@]}"; do
		load_cfg
		echo "Resetting..."
		reset-all
		reset_memcached
		EXTRA_ARGS="--num-shards ${s} --num-handlers 1 --pipeline-depth 1 --no-outliers"
		cl_run "$BIN_PATH"
		grep -hoP '\[PARSE\] \K.*' logs/* | sed "s/^/${s},/" >>"$outfile"
	done

	extract_raw $(realpath "$outfile")
fi

if [[ "$ARG1" == "all" || "$ARG1" == "threads" ]]; then
	echo "Varying number of permissions handler threads..."

	outfile="${OUTPATH}/threads.csv"
	echo "handler_threads,id,system_size,total_commits,work_time_us,lat_avg_us,lat_50p_us,lat_99p_us,lat_99_9p_us" >"$outfile"

	NUM_THREADS=(1 2 3 4 5 6)
	for t in "${NUM_THREADS[@]}"; do
		load_cfg
		echo "Resetting..."
		reset-all
		reset_memcached
		EXTRA_ARGS="--num-shards 20 --num-handlers ${t} --pipeline-depth 1 --no-outliers"
		cl_run "$BIN_PATH"
		grep -hoP '\[PARSE\] \K.*' logs/* | sed "s/^/${t},/" >>"$outfile"
	done

	extract_raw $(realpath "$outfile")
fi

# if [[ "$ARG1" == "all" || "$ARG1" == "outliers" ]]; then
# 	echo "Running with outliers..."

# 	NUM_ITERS=10
# 	for ((i = 0; i < NUM_ITERS; i++)); do
# 		load_cfg
# 		echo "Resetting..."
# 		reset-all
# 		reset_memcached
# 		EXTRA_ARGS="--num-shards 20 --num-handlers 1 --pipeline-depth 1"
# 		cl_run "$BIN_PATH"
# 		grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
# 	done

# 	extract_raw $(realpath "$outfile")
# fi
