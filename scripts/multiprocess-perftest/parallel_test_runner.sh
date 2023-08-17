#!/bin/bash

SCRIPT_ROOT_DIR=$(realpath $(dirname ${0}))

ID=${1}

echo "runner $ID"

PWD=$(pwd)

WORKDIR="data-${ID}"

mkdir -p ${WORKDIR}

cd ${WORKDIR}
${SCRIPT_ROOT_DIR}/run_tests.sh
cd ${PWD}
