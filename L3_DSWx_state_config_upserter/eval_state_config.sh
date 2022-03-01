#!/bin/bash
################################################################
# Wrapper script for the L3 DSWx state config upserter
################################################################
env | sort 1>&2

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env

export OPERA_HOME=$(dirname "${BASE_PATH}")
export COMMONS_HOME=$OPERA_HOME/commons
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PGE=$(basename "${BASE_PATH}")
export PYTHONDONTWRITEBYTECODE=1

# source environment
source ~/.bash_profile

env | sort 1>&2
echo "##########################################" 1>&2
echo -n "Running eval_state_config.py: " 1>&2
date 1>&2

$BASE_PATH/eval_state_config.py $* > eval_state_config.log 2>&1
STATUS=$?

echo -n "Finished running eval_state_config.py: " 1>&2
date 1>&2

if [ $STATUS -ne 0 ]; then
  echo "Failed to run eval_state_config.py." 1>&2
  cat eval_state_config.log 1>&2
  echo "{}"
  exit $STATUS
fi
