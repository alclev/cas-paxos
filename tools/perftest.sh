

. ../config/cloudlab.conf
# Parse script args
if [[ "$1" == "LAT" ]]; then
	cmds=("${lat_cmds[@]}")
elif [[ "$1" == "BW" ]]; then
	cmds=("${bw_cmds[@]}")
else
	echo "Invalid argument: $1. Expected 'LAT' or 'BW'."
	exit 1
fi
lat_cmds=(
	"ib_read_lat"
	"ib_write_lat"
	"ib_atomic_lat"
)
bw_cmds=(
	"ib_read_bw"
	"ib_write_bw"
	"ib_atomic_bw"
)
sizes=(8)
# -R : enables the connection manager
# -F : disables CPU mismatch warning
# -s : define message size in bytes
SERVER_ID="node0"
NUM_CLIENTS=$((${#MACHINES[@]} - 1))
SERVER="${USER}@${MACHINES[0]}.${DOMAIN}"
CLIENT="${USER}@${MACHINES[1]}.${DOMAIN}"
cmds=()
OUTFILE=""
if [[ "$1" == "BW" ]]; then
	OUTFILE="results/perftest_bw.csv"
	echo 'cmd,bytes_conflict,num_iters,bw_peak_MB_s,bw_avg_MB_s,msg_rate' >"$OUTFILE"
	cmds=("${bw_cmds[@]}")
else
	OUTFILE="results/perftest_lat.csv"
	echo 'cmd,size,num_iters,t_min,t_max,t_typical,t_avg,t_stdev,p99,p99_9' >"$OUTFILE"
	cmds=("${lat_cmds[@]}")
fi

# Start with the lat cmds
for cmd in "${cmds[@]}"; do
	for size in "${sizes[@]}"; do
		# -F: ignore CPU freq warnings
		COMMON_ARGS="-F --iters 10000"
		if [[ ${cmd} != *"atomic"* ]]; then
			ARGS="${COMMON_ARGS} -s ${size}"
		else
			ARGS="${COMMON_ARGS} -s 8"
		fi
		echo "Launching server: ${cmd} ${ARGS}"
		ssh ${SERVER} "sudo pkill ${cmd}; nohup ${cmd} ${ARGS} >/users/${USER}/server_${cmd}.log 2>&1 &"
		# Launch client
		echo "Launching client: ${cmd} ${ARGS}"

		ssh ${CLIENT} "${cmd} ${ARGS} ${SERVER_ID} >/users/${USER}/perf_output.log 2>&1"

		scp ${CLIENT}:"~/perf_output.log" "results/${cmd}_${size}.log"

		# Parse out metrics into a csv
		tail -n 2 "results/${cmd}_${size}.log" | head -n 1 | awk -v cmd="${cmd}" 'BEGIN{OFS=","} {$1=$1; print cmd,$0}' >>"$OUTFILE"

		rm "results/${cmd}_${size}.log"
	done
done
# Go through and delete all the logs
ssh ${SERVER} "rm ~/server_*.log"
echo "Done."
