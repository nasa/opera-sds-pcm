#!/bin/bash
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$OPERA_HOME:$PYTHONPATH
source $HOME/verdi/bin/activate
python $OPERA_HOME/data_subscriber/gcov/gcov_catalog_ingest.py > run_catalog_ingest.log 2>&1
