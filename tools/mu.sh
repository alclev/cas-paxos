EXE_PATH="experiments/caspaxos"

function make_screen {
	echo 'startup_message off' >>$1
	echo 'defscrollback 10000' >>$1
	echo 'autodetach on' >>$1
	echo 'escape ^jj' >>$1
	echo 'defflow off' >>$1
	echo 'hardstatus alwayslastline "%w"' >>$1
}

function reset-memcached() {
	# Reset the memcached server
	ssh ${USER}@${MACHINES[0]}.${DOMAIN} "sudo pkill memcached"
	sleep 1
	# Launch the memcached server
	MEMCACHED_ARGS="-vv -p 9999"
	ssh ${USER}@${MACHINES[0]}.${DOMAIN} "nohup env LD_LIBRARY_PATH=/users/${USER} ./memcached ${MEMCACHED_ARGS} > memcached.log 2>&1 &"
}

function do_all {
	for i in "${!MACHINES[@]}"; do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "$1" &
	done
	wait
}

function reset-all() {
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes
	for i in $(seq 0 ${last_valid_index}); do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "sudo killall -9 -u $USER" &
	done
	wait
	echo "Nodes have been reset."
}

function send_libs() {
	for m in ${MACHINES[*]}; do
		scp "lib/libcrashconsensus.so" "${USER}@${m}.${DOMAIN}:~/" &
		scp "lib/memcached" "${USER}@${m}.${DOMAIN}:~/" &
		scp "lib/libevent-2.1.so.6" "${USER}@${m}.${DOMAIN}:~/" &
	done
	wait
}

function wait_for_pattern() {
	local pattern="$1"
	local file="$2"

	# Wait until the file exists
	while [[ ! -f "$file" ]]; do
		sleep 0.1
	done

	# Poll the file until pattern appears
	while ! grep -qF "$pattern" "$file" 2>/dev/null; do
		sleep 0.1
	done
}

function run_mu {
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

	(
		wait_for_pattern "[PARSE]" logs/log_0.txt
		echo "Pattern found. Killing processes..."
		sleep 1
		# Kill everyone else because they will hang
		echo "Resetting..."
		reset-all
		reset-memcached
	) &

	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen "$tmp_screen"

	IDS=$(seq 1 $((${#MACHINES[@]})) | paste -sd, -)
	STARTING_PORT="6379"
	DORY_REGISTRY_IP="10.10.1.1:9999"
	NUM_MACHINES=${#MACHINES[@]}
	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		ENV_ARGS="EXPER_PORT=${STARTING_PORT} SID=$((i + 1)) IDS=${IDS} DORY_REGISTRY_IP=${DORY_REGISTRY_IP} LD_LIBRARY_PATH=~/"
		CMD="${ENV_ARGS} ./${EXE_NAME} --hostname ${host} --node-id ${i} --output-file mu_stats_${NUM_MACHINES}.csv ${ARGS}"
		echo "$CMD"
		cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh -t ${USER}@${host}.${DOMAIN} ${CMD}
logfile logs/log_${i}.txt
log on
EOF
	done

	screen -c "$tmp_screen"
	rm "$tmp_screen"
}

function failover_test {
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

	(
		wait_for_pattern "[FAILOVER]" logs/log_1.txt
		echo "Pattern found. Killing processes..."
		sleep 1

		# Kill everyone else because they will hang
		echo "Resetting..."
		reset-all
		reset-memcached
	) &


	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen "$tmp_screen"

	IDS=$(seq 1 $((${#MACHINES[@]})) | paste -sd, -)
	STARTING_PORT="6379"
	DORY_REGISTRY_IP="10.10.1.1:9999"
	NUM_MACHINES=${#MACHINES[@]}
	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		ENV_ARGS="EXPER_PORT=${STARTING_PORT} SID=$((i + 1)) IDS=${IDS} DORY_REGISTRY_IP=${DORY_REGISTRY_IP} LD_LIBRARY_PATH=~/"
		CMD="${ENV_ARGS} ./${EXE_NAME} --hostname ${host} --node-id ${i} --output-file mu_stats_${NUM_MACHINES}.csv ${ARGS}"
		echo "$CMD"
		cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh -t ${USER}@${host}.${DOMAIN} ${CMD}
logfile logs/log_${i}.txt
log on
EOF
	done

	screen -c "$tmp_screen"
	rm "$tmp_screen"
}


# cmd="$1"
# count=$#

# cd $(git rev-parse --show-toplevel)
# source config/cloudlab.conf

# REMOTES=""
# for machine in "${MACHINES[@]}"; do
# 	if [[ -z "$REMOTES" ]]; then
# 		REMOTES="$machine"
# 	else
# 		REMOTES="$REMOTES,$machine"
# 	fi
# done
# ARGS="--remotes ${REMOTES}"

# if [[ "$cmd" == "build" && "$count" -eq 1 ]]; then
# 	cd ~/mu
# 	sudo docker run --privileged --rm -v $(pwd):/mu --name mu -it mu:latest
# 	bash transfer.sh
# 	cd ~/cas-paxos
# elif [[ "$cmd" == "run" && "$count" -eq 1 ]]; then
# 	run_mu "$EXE_PATH"
# elif [[ "$cmd" == "reset" && "$count" -eq 1 ]]; then
# 	reset-all
# elif [[ "$cmd" == "reset-memcached" && "$count" -eq 1 ]]; then
# 	reset-memcached
# elif [[ "$cmd" == "send-libs" && "$count" -eq 1 ]]; then
# 	send_libs
# elif [[ "$cmd" == "do-all" && "$count" -eq 2 ]]; then
# 	do_all "$2"
# elif [[ "$cmd" == "run-debug" && "$count" -eq 1 ]]; then
# 	run_mu_debug "$EXE_PATH"
# elif [[ "$cmd" == "failover-test" && "$count" -eq 1 ]]; then
# 	rm -f logs/*
# 	OUTFILE="results/mu_failover.csv"
# 	echo "failover_time_us" > "$OUTFILE"
# 	NUM_ITERATIONS=1000
# 	for i in $(seq 1 $NUM_ITERATIONS); do
# 		echo "Resetting for iteration $i..."
# 		reset-all
# 		reset-memcached
# 		failover_test "$EXE_PATH"
# 		grep -oP '\[FAILOVER\]: \K[0-9]+' logs/log_1.txt >> "$OUTFILE" || true
# 	done
# else
# 	echo "Usage: $0 [build|run|reset|reset-memcached|run-debug|failover-test]"
# 	exit 1
# fi
