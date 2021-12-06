#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env

export OPERA_HOME=$(dirname "${BASE_PATH}")
export COMMONS_HOME=$OPERA_HOME/commons
export MIXED_MODES_HOME=/home/ops/verdi/ops/nisar-mixed-modes
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$MIXED_MODES_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PGE=$(basename "${BASE_PATH}")
export PYTHONDONTWRITEBYTECODE=1

# source environment
source ~/.bash_profile

echo "##########################################" 1>&2
echo -n "Running eval_track_frame.py: " 1>&2
date 1>&2
$BASE_PATH/eval_track_frame.py $* > eval_track_frame.log 2>&1
STATUS=$?
echo -n "Finished running eval_track_frame.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run eval_track_frame.py." 1>&2
  cat eval_track_frame.log 1>&2
  echo "{}"
  exit $STATUS
fi
