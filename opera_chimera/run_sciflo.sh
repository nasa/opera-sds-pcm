#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export CHIMERA_HOME=$HOME/verdi/ops/opera-pcm/opera_chimera
export PYTHONPATH=$BASE_PATH:$CHIMERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PGE=$(basename "${BASE_PATH}")
export PYTHONDONTWRITEBYTECODE=1

# source environment
source $HOME/verdi/bin/activate

MODULE_PATH="$1"
WF_DIR="$2"
WF_NAME="$3"

export PYTHONPATH=${MODULE_PATH}:$PYTHONPATH

echo "##########################################" 1>&2
echo -n "Running $PGE run_sciflo.py with params $WF_DIR/$WF_NAME.sf.xml and _context.json: " 1>&2
date 1>&2
python $CHIMERA_HOME/run_sciflo.py $WF_DIR/$WF_NAME.sf.xml _context.json sfl_output > run_sciflo_$WF_NAME.log 2>&1
STATUS=$?
echo -n "Finished running $PGE run_sciflo.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run $PGE run_sciflo.py" 1>&2
  cat run_sciflo_$WF_NAME.log 1>&2
  echo "{}"
  exit $STATUS
fi
