#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1

# source environment
source $HOME/verdi/bin/activate

echo "##########################################" 1>&2
echo -n "Running NET PGE"
date 1>&2
echo -n "Running python code: run_net_pge.py"
python $BASE_PATH/run_net_pge.py $* > run_net.log
STATUS=$?
echo -n "Finished running run_net_pge.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run run_net_pge.py" 1>&2
  cat run_net.log 1>&2
  echo "{}"
  exit $STATUS
fi
