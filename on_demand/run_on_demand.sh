#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export VERDI_ROOT=$(dirname $(dirname "${OPERA_HOME}"))
export CHIMERA_HOME=$VERDI_ROOT/ops/chimera
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$CHIMERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1

# source environment
source $VERDI_ROOT/bin/activate

echo "##########################################" 1>&2
echo -n "Running run_on_demand.py: " 1>&2
date 1>&2
python $BASE_PATH/run_on_demand.py _context.json > run_on_demand.log 2>&1
STATUS=$?
echo -n "Finished running run_on_demand.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run run_on_demand.py." 1>&2
  cat run_on_demand.log 1>&2
  echo "{}"
  exit $STATUS
fi