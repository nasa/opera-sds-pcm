#!/bin/bash
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$OPERA_HOME:$PYTHONPATH
source $HOME/verdi/bin/activate
python -u $OPERA_HOME/ops_reporting/ops_reporting.py > run_ops_report.log 2>&1
