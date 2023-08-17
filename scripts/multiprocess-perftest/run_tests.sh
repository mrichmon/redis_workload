#!/bin/bash

SCRIPT_ROOT_DIR=$(realpath $(dirname ${0}))

REPO_ROOT_DIR=$(realpath ${SCRIPT_ROOT_DIR}/../..)

BIN_DIR="${REPO_ROOT_DIR}/build_dir_release"

OUTPUT_DIR=$(pwd)

echo "run_test for ${OUTPUT_DIR}"

OUTPUT_PREFIX=threads

#### Per process data file
#DATA="${REPO_ROOT_DIR}/data/redis_query_log.csv"
DATA="${REPO_ROOT_DIR}/data/redis_query_sample-200.csv"

CMD="${BIN_DIR}/bin/run_redis_workload"

#### Per process thread count
#THREAD_COUNTS="1 2 4 8 16 32 64 128 256 512 1024 2048"
#THREAD_COUNTS="1 2 4 8 16 32 64 128 256 512"
THREAD_COUNTS="1"

#### Workload operating mode: divide vs replicate
#MODE="-r"
MODE=""

date
for t in $(echo ${THREAD_COUNTS}); do
    echo $t
    ${CMD} ${MODE} -t ${t} -f ${DATA} | tee ./${OUTPUT_PREFIX}-${t}.txt
    sleep 1
done
cd ${PWD}
date
