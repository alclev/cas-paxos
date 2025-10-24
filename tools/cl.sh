#!/bin/bash

# cl.sh
#
# A script for uploading and running romulus applications on CloudLab

set -e # Halt the script on any error

# Print usage information
usage() {
	cat <<EOF

cl.sh — tool for building & running romulus apps on CloudLab

Usage: cl.sh <command> [args]

Commands:
  install-deps
      Install dependencies onto the CloudLab machines.
      (no additional args)

  build-run <mode> <path>
      Build via build-cl.sh in the given <mode> and then run on CloudLab.
      <mode> must be one of:
        debug
        release
  run <path>
      Run the previously built executable on CloudLab.
      (no build step, no additional args)

  run-debug <path>
      Run the previously built executable on CloudLab with gdb.
      (no build step, no additional args)

  build-debug
      Equivalent to: source tools/build-cl.sh debug

  build-release
      Equivalent to: source tools/build-cl.sh release

  connect
      SSH/connect to the CloudLab machines for interactive debugging.
      (no additional args)

  reset <machine>
      Kill all remote processes on the given CloudLab machine.
      <machine> must be a valid hostname in your cluster config.

NOTE: Make sure your *.conf files are up to date before you run any command.

EOF
}

function load_cfg {
	# Create REMOTES string from MACHINES array
	REMOTES=""
	for machine in "${MACHINES[@]}"; do
		if [[ -z "$REMOTES" ]]; then
			REMOTES="$machine"
		else
			REMOTES="$REMOTES,$machine"
		fi
	done
	ARGS="--remotes ${REMOTES}"
}

# SSH into MACHINES once, to fix known_hosts
function cl_first_connect {
	echo "Performing one-time connection to CloudLab MACHINES, to get known_hosts right"
	for machine in ${MACHINES[@]}; do
		ssh -o StrictHostKeyChecking=no $USER@$machine.$DOMAIN echo "Connected"
	done
}

# Append the default configuration of a screenrc to the given file
function make_screen {
	echo 'startup_message off' >>$1
	echo 'defscrollback 10000' >>$1
	echo 'autodetach on' >>$1
	echo 'escape ^jj' >>$1
	echo 'defflow off' >>$1
	echo 'hardstatus alwayslastline "%w"' >>$1
}

# Check the status of IBV on the target MACHINES
function check_ibv {
	echo "Checking ibv status:"
	for machine in ${MACHINES[@]}; do
		echo "$machine:"
		ssh $USER@$machine.$DOMAIN "ibv_devinfo -v"
	done
}

#  Configure the set of CloudLab MACHINES
function cl_install_deps() {
	config_command=prepare_to_run.sh          # The script to put on remote nodes
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes

	# Names of packages that we need to install on CloudLab
	package_deps="librdmacm-dev ibverbs-utils libnuma-dev gdb libgtest-dev libibverbs-dev libmemcached-dev memcached libevent-dev libhugetlbfs-dev numactl libgflags-dev libssl-dev"
	# First-time SSH
	cl_first_connect

	# Build a script to run on all the MACHINES
	tmp_script_file="$(mktemp)" || exit 1
	echo 'echo `hostname`' >${tmp_script_file}
	# Turn off interactive prompts
	echo "sudo sed -i 's|http://us.archive.ubuntu.com/ubuntu/|http://mirror.math.princeton.edu/pub/ubuntu/|g' /etc/apt/sources.list.d/ubuntu.sources" >>${tmp_script_file}
	echo 'sudo apt update' >>"${tmp_script_file}"
	echo "sudo apt upgrade -y" >>${tmp_script_file}
	echo "sudo apt install -y ${package_deps}" >>${tmp_script_file}
	echo "echo 'kernel.perf_event_paranoid=-1' | sudo tee -a /etc/sysctl.conf" >>${tmp_script_file}
	echo "sudo sysctl -p" >>${tmp_script_file}

	# Send the script to all MACHINES via parallel SCP
	echo "Sending configuration script to ${MACHINES[*]}"
	for m in ${MACHINES[*]}; do
		scp ${tmp_script_file} ${USER}@${m}.${DOMAIN}:${config_command} &
	done
	wait
	rm ${tmp_script_file}

	# Use screen to run the script in parallel
	tmp_screen="$(mktemp)" || exit 1
	make_screen $tmp_screen
	for i in $(seq 0 ${last_valid_index}); do
		echo "screen -t node${i} ssh ${USER}@${MACHINES[$i]}.${DOMAIN} bash ${config_command}" >>${tmp_screen}
	done
	screen -c ${tmp_screen}
	rm ${tmp_screen}

	# Check the status of the IBV library on the target MACHINES
	check_ibv
}

# SEND and RUN a binary on the CloudLab MACHINES
# $1 : Relative path of exe
function cl_run() {
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
	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen "$tmp_screen"

	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		CMD="./${EXE_NAME} --hostname ${host} --node-id ${i} --leader-fixed ${ARGS}"
		echo "$CMD"
		cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} ${CMD}; bash
logfile logs/log_${i}.txt
log on
EOF
	done

	screen -c "$tmp_screen"
	rm "$tmp_screen"
}

# $1 : Relative path of exe
function cl_debug() {
	EXE_NAME=$(basename "$1")
	if [[ ! -f "build/$1" ]]; then
		echo "Executable not found: $1"
		exit 1
	fi
	# Send the executable to all MACHINES
	for m in ${MACHINES[*]}; do
		scp "build/$1" "${USER}@${m}.${DOMAIN}:${EXE_NAME}" &
	done
	wait
	rm -rf gdb-logs
	mkdir gdb-logs

	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen $tmp_screen

	gdb_cmd="$2"
	echo "Running gdb with command: $gdb_cmd"

	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		CMD="--hostname ${host} --node-id ${i} --leader-fixed ${ARGS}"
		if [[ $i -eq 0 && -n "$gdb_cmd" ]]; then
			cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} gdb -ex \"${gdb_cmd}\" -ex \"r\" --args ./${EXE_NAME} ${CMD}; bash
logfile gdb-logs/gdb_${i}.log
log on
EOF
		else
			cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} gdb -ex \"r\" --args ./${EXE_NAME} ${CMD}; bash
logfile gdb-logs/gdb_${i}.log
log on
EOF
		fi
	done
	screen -c $tmp_screen
	rm $tmp_screen
}

function reset_memcached() {
	APPLICATION="memcached"
	# Reset the application
	# NOTE: race condition with deletion and creation of memcached server
	ssh ${USER}@${MACHINES[0]}.${DOMAIN} "sudo pkill ${APPLICATION}; sleep 1; nohup ${APPLICATION} -vv -d -l 10.10.1.1 -p 9999 > memcached.log 2>&1 &"
}

# Connect to CloudLab nodes (e.g., for debugging)
function cl_connect() {
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes

	# Set up a screen script for connecting
	tmp_screen="$(mktemp)" || exit 1
	make_screen $tmp_screen
	for i in $(seq 0 ${last_valid_index}); do
		echo "screen -t node${i} ssh ${USER}@${MACHINES[$i]}.${DOMAIN}" >>${tmp_screen}
	done
	screen -c $tmp_screen
	rm $tmp_screen
}

function do_all {
	for i in "${!MACHINES[@]}"; do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "$1" &
	done
	wait
}

function reset() {
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes
	for i in $(seq 0 ${last_valid_index}); do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "sudo killall -9 -u $USER" &
	done
	wait
	echo "Nodes have been reset."
}

# $1 : Name of the results file to retrieve
function retrieve_results {
	# random 4-char string
	rand_str=$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 4 | head -n 1)
	result_file="results_${rand_str}.csv"
	scp ${USER}@${MACHINES[0]}.${DOMAIN}:"~/$1" results/$result_file &
	wait
	echo "Results retrieved to results/$result_file"
}

function test_remus {
	ARGS="--seg-size 29 --segs-per-mn 3 --first-cn-id 0 --last-cn-id 9 --first-mn-id 0 --last-mn-id 9 --qp-lanes 1 --qp-sched-pol ONE_TO_ONE --mn-port 33330 --cn-threads 1 --cn-ops-per-thread 1 --cn-wrs-per-seq 1 --cn-thread-bufsz 20 --alloc-pol LOCAL-MOD"
	REMUS_BIN="benchmark/remus-basic-ops"
	# check if file exists
	EXE_NAME=$(basename "$REMUS_BIN")
	if [[ ! -f "build/$REMUS_BIN" ]]; then
		echo "Executable not found: $REMUS_BIN"
		exit 1
	fi
	for m in ${MACHINES[*]}; do
		scp "build/$REMUS_BIN" "${USER}@${m}.${DOMAIN}:${EXE_NAME}" &
	done
	wait
	rm -rf logs
	mkdir logs
	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen "$tmp_screen"

	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} ./${EXE_NAME} --node-id ${i} ${ARGS}; bash
logfile logs/log_${i}.txt
log on
EOF
	done

	screen -c "$tmp_screen"
	rm "$tmp_screen"
}

function test_perftest {
	cmds=(
		"ib_read_bw"
		"ib_write_bw"
		"ib_atomic_bw"
		"ib_read_lat"
		"ib_write_lat"
		"ib_atomic_lat"
	)
	sizes=(8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768)
	# -R : enables the connection manager
	# -F : disables CPU mismatch warning
	COMMON_ARGS="--iters 10000 -F"
	SERVER_ID="node0"
	NUM_CLIENTS=$((${#MACHINES[@]} - 1))
	SERVER="${USER}@${MACHINES[0]}.${DOMAIN}"
	CLIENT="${USER}@${MACHINES[1]}.${DOMAIN}"
	for cmd in "${cmds[@]}"; do
		for size in "${sizes[@]}"; do
			if [[ ${cmd} != *"atomic"* ]]; then
				ARGS="${COMMON_ARGS} -s ${size}"
			else
				ARGS="${COMMON_ARGS}"
			fi
			echo "Launching server: ${cmd} ${ARGS}"
			ssh ${SERVER} "sudo pkill ${cmd}; nohup ${cmd} ${ARGS} >/users/${USER}/server_${cmd}.log 2>&1 &"
			# Launch client
			echo "Launching client: ${cmd} ${ARGS}"
			ssh ${CLIENT} "${cmd} ${ARGS} --out_json --out_json_file=${cmd}_${size}.json ${SERVER_ID}"
		done
	done
	echo "Done. Pulling json files..."
	scp ${CLIENT}:"~/*json" results/
	echo "Done."
}

# Get the important stuff out of the command-line args
cmd=$1   # The requested command
count=$# # The number of command-line args
# Navigate the the project root directory
cd $(git rev-parse --show-toplevel)
# Load the config right away
for file in config/*.conf; do
	if [ -f $file ]; then
		source $file
	fi
done
# Load the config in the ARGS variable
load_cfg

if [[ "$cmd" == "install-deps" && "$count" -eq 1 ]]; then
	cl_install_deps
elif [[ "$cmd" == "build-run" && "$count" -eq 3 ]]; then
	if [[ "$2" != "debug" && "$2" != "release" ]]; then
		usage
		exit 1
	fi
	source tools/build.sh "$2"
	cl_run "$3"
elif [[ "$cmd" == "run" && "$count" -eq 2 ]]; then
	cl_run "$2"
elif [[ "$cmd" == "run-experiment" && "$count" -eq 2 ]]; then
	cl_run "$2"
	retrieve_results
elif [[ "$cmd" == "run-debug" && ("$count" -eq 2 || "$count" -eq 3) ]]; then
	cl_debug "$2" "${*:3}"
elif [[ "$cmd" == "reset-memcached" && "$count" -eq 1 ]]; then
	reset_memcached
elif [[ "$cmd" == "build-debug" && "$count" -eq 1 ]]; then
	source tools/build.sh debug
elif [[ "$cmd" == "build-release" && "$count" -eq 1 ]]; then
	source tools/build.sh release
elif [[ "$cmd" == "connect" && "$count" -eq 1 ]]; then
	cl_connect
elif [[ "$cmd" == "reset" && "$count" -eq 1 ]]; then
	reset
elif [[ "$cmd" == "do-all" && "$count" -eq 2 ]]; then
	do_all "$2"
else
	usage
fi
