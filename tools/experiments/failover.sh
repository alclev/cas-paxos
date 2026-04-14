wait_for_pattern() {
	local pattern="$1"
	local file="$2"

	# Wait until the file exists
	while [[ ! -f "$file" ]]; do
		sleep 0.1
	done

	# Poll the file until pattern appears
	while ! grep -q "$pattern" "$file" 2>/dev/null; do
		sleep 0.1
	done
}

echo "failover_time_us" >results/failover.csv
ITERATION=0
NUM_ITERATIONS=500

while [[ $ITERATION -lt $NUM_ITERATIONS ]]; do
	echo "Starting iteration ${ITERATION}..."
	ITERATION=$((ITERATION + 1))
	# check if file exists
	EXE_NAME=$(basename "$1")
	if [[ ! -f "build/$1" ]]; then
		echo "Executable not found: $1"
		exit 1
	fi
	for m in ${MACHINES[*]}; do
		scp "build/$1" "${USER}@${m}.${DOMAIN}:${EXE_NAME}" &
	done
	wait
	rm -rf logs
	mkdir logs

	# Mechanism to inject failure on last node
	(
		wait_for_pattern "Detected leader change: 9" logs/log_9.txt
		sleep 5

		# Kill the last node
		LAST_HOST="${MACHINES[$((NUM_MACHINES - 1))]}"
		echo "Killing ${EXE_NAME} on ${LAST_HOST}..."
		ssh "${USER}@${LAST_HOST}.${DOMAIN}" "sudo pkill -9 -f '${EXE_NAME}.*'" || true
		# Kill everyone else because they will hang
		reset-all
		reset_memcached
	) &

	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen "$tmp_screen"
	NUM_MACHINES=${#MACHINES[@]}
	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		# CMD="sudo perf record --call-graph dwarf -o perf_node${i}.data ./${EXE_NAME} --hostname ${host} --node-id ${i} --output-file stats_${i}.csv ${ARGS} --multipax-opt && sudo perf report --stdio -g -i perf_node${i}.data > perf_report_${i}.txt"
		# CMD="sudo perf stat -e cycles,task-clock,cache-misses,LLC-load-misses -I 5000 -o perf_stat_${i}.txt ./${EXE_NAME} --hostname ${host} --node-id ${i} --output-file stats_${i}.csv ${ARGS} --multipax-opt"
		CMD="./${EXE_NAME} --hostname ${host} --node-id ${i} ${ARGS} --multipax-opt ${EXTRA_ARGS}"
		echo "$CMD"
		cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh -t ${USER}@${host}.${DOMAIN} ${CMD}
logfile logs/log_${i}.txt
log on
EOF
	done
	screen -c "$tmp_screen"
	rm "$tmp_screen"

	wait_for_pattern "[FAILOVER TIME]" logs/log_0.txt
	RESULT=$(grep -oP '\[FAILOVER TIME\] \K\d+' logs/log_0.txt || true)

	if [[ -n "$RESULT" && "$RESULT" != "0" ]]; then
		echo "$RESULT" >>results/failover.csv
		echo "Failover time: $RESULT us"
	else
		echo "Warning: No failover time found"
	fi

done
