#!/bin/bash
#################
# This is a wrapper script for the job that is responsible for creating
# an Accountability Report.
#################

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH=/opt/conda/lib:$LD_LIBRARY_PATH

# source environment
source $HOME/verdi/bin/activate

echo "##########################################" 1>&2
echo -n "Running job to create an Accountability Report"
date 1>&2
echo -n "Running python code: create_accountability_report.py"
python $BASE_PATH/create_accountability_report.py > create_accountability_report.log 2>&1
STATUS=$?
echo -n "Finished running create_accountability_report.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run create_accountability_report.py" 1>&2
  cat create_accountability_report.log 1>&2
  echo "{}"
  exit $STATUS
fi
