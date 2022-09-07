#!/bin/bash
################################################################
# Wrapper script for the ISL purger
################################################################
env | sort 1>&2

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env

export OPERA_HOME=$(dirname "${BASE_PATH}")
export COMMONS_HOME=$OPERA_HOME/commons
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1

# source environment
source $HOME/verdi/bin/activate

env | sort 1>&2
echo "##########################################" 1>&2
echo -n "Running purge_isl.py: " 1>&2
date 1>&2

$BASE_PATH/purge_isl.py $* > purge_isl.log 2>&1
STATUS=$?

echo -n "Finished running purge_isl.py: " 1>&2
date 1>&2

if [ $STATUS -ne 0 ]; then
  echo "Failed to run purge_isl.py." 1>&2
  cat purge_isl.log 1>&2
  echo "{}"
  exit $STATUS
fi
