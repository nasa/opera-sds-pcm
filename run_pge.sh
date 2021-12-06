#!/usr/bin/env bash
# Runs the OPERA PGEs in the docker providing necessary conversions to and from HySDS

# Set PYTHONPATH
export PYTHONPATH=/home/ops/verdi/ops/opera-pcm
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH=/opt/conda/lib:$LD_LIBRARY_PATH

CONTEXT_FILE="_context.json"
WRAPPER="wrapper.opera_pge_wrapper"
WORKDIR=$PWD

echo "Setting workdir as $WORKDIR"

if [ $# -eq 1 ]; then
    CONTEXT_FILE=$1
fi

if [ ! -f $CONTEXT_FILE ]; then
    echo "Context file not found!"
    exit 1
fi

python -m $WRAPPER $CONTEXT_FILE $WORKDIR
