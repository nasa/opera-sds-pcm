#!/bin/bash

set -ex

echo "args: $*"

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export JOB_HOME=/home/ops/verdi/ops/ecmwf-api-client
export PYTHONPATH=$BASE_PATH:$JOB_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH=/opt/conda/lib:$LD_LIBRARY_PATH

#source $HOME/verdi/bin/activate
ls -halt

. $JOB_HOME/venv/bin/activate
env
echo 'EC2 INSTANCE TYPE:' $(wget -q -O - http://169.254.169.254/latest/meta-data/instance-type) || true
pwd
python $JOB_HOME/entrypoint_subsetter.py $* > run_subsetter.log 2>&1
