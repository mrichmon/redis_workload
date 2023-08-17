#!/bin/bash

SCRIPT_ROOT_DIR=$(realpath $(dirname ${0}))

#PROCESS_TOTALS="1 2 4 8 16 32 64 128 256 512"
#PROCESS_TOTALS="1 2 4 8 16 32 64 128"
PROCESS_TOTALS="1 2"

PWD=$(pwd)

for PROCESS_COUNT in ${PROCESS_TOTALS} ; do
    echo "test run: ${PROCESS_COUNT} processes"
    mkdir count${PROCESS_COUNT}
    cd count${PROCESS_COUNT}
    parallel -j ${PROCESS_COUNT} ${SCRIPT_ROOT_DIR}/parallel_test_runner.sh {} ::: $(seq 1 ${PROCESS_COUNT})
    cd ..
done
