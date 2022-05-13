#!/bin/bash

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

MINUTES=$1

if [ ! -z "$2" ]; then
  BOUNDING_BOX="-b ${2}"
fi

if [ ! -z "$3" ]; then
  PROVIDER="-p ${3}"
fi


# source PGE env
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH=/opt/conda/lib:$LD_LIBRARY_PATH

# source environment
source $HOME/verdi/bin/activate

echo "##########################################"
echo "Running job to query LPDAAC HLSS30 data"
date

# Forward processing use case; query previous 60 minutes
echo "python $OPERA_HOME/data_subscriber/daac_data_subscriber.py query -m $MINUTES -c HLSS30 ${BOUNDING_BOX} ${PROVIDER}"
python $OPERA_HOME/data_subscriber/daac_data_subscriber.py query -m $MINUTES -c HLSS30 ${BOUNDING_BOX} ${PROVIDER}

if [ $? -eq 0 ]; then
  echo "Finished running job"
  date
  exit 0
else
  echo "Failed to run daac_data_subscriber.py"
  date
  exit 1
fi

