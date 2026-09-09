BIN_PATH="experiments/caspaxos"
OUTFILE="results/mu_v1.csv"

echo "id,system_size,total_commits,work_time_us,lat_avg_us,lat_50p_us,lat_99p_us,lat_99_9p_us" >"$outfile"
bash tools/build.sh release lease

echo "Varying system size..."

ORIG_MACHINES=("${MACHINES[@]}")
for i in $(seq 3 ${#ORIG_MACHINES[@]}); do
	MACHINES=("${ORIG_MACHINES[@]:0:$i}")
	load_cfg
	echo "Resetting..."
	reset-all
	reset_memcached
	SYS_SIZE=${#MACHINES[@]}
	echo "Launching experiment with ${SYS_SIZE} nodes..."
	EXTRA_ARGS="--num-shards 20 --num-handlers 1 --pipeline-depth 1 --no-outliers"
	cl_run "$BIN_PATH"
	grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
done

echo "Varying pipeline depth..."

PIPE_DEPTHS=($(seq 1 12))
for d in "${PIPE_DEPTHS[@]}"; do
	load_cfg
	echo "Resetting..."
	reset-all
	reset_memcached
	EXTRA_ARGS="--num-shards 20 --num-handlers 1 --pipeline-depth ${d} --no-outliers"
	cl_run "$BIN_PATH"
	grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
done

echo "Varying number of shards..."

NUM_SHARDS=(10 20 30 40 50 60 70 80 90 100)
for s in "${NUM_SHARDS[@]}"; do
	load_cfg
	echo "Resetting..."
	reset-all
	reset_memcached
	EXTRA_ARGS="--num-shards ${s} --num-handlers 1 --pipeline-depth 1 --no-outliers"
	cl_run "$BIN_PATH"
	grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
done

echo "Varying number of permissions handler threads..."

NUM_THREADS=(1 2 3 4 5 6)
for t in "${NUM_THREADS[@]}"; do
	load_cfg
	echo "Resetting..."
	reset-all
	reset_memcached
	EXTRA_ARGS="--num-shards 20 --num-handlers ${t} --pipeline-depth 1 --no-outliers"
	cl_run "$BIN_PATH"
	grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
done


echo "Running with outliers..."

NUM_ITERS=10
for ((i = 0; i < NUM_ITERS; i++)); do
	load_cfg
	echo "Resetting..."
	reset-all
	reset_memcached
	EXTRA_ARGS="--num-shards 20 --num-handlers 1 --pipeline-depth 1"
	cl_run "$BIN_PATH"
	grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
done

