BIN_PATH="experiments/caspaxos"
OUTFILE="results/mu_v1.csv"

echo "id,system_size,total_commits,work_time_us,lat_avg_us,lat_50p_us,lat_99p_us,lat_99_9p_us" >"$outfile"
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
	grep -hoP '\[PARSE\] \K.*' logs/* >>"$outfile"
done
