#!/bin/bash
export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$OPERA_HOME:$PYTHONPATH
source $HOME/verdi/bin/activate
python $OPERA_HOME/data_subscriber/cslc/disp_s1_cycle_evaluator.py > run_cycle_evaluator.log 2>&1
