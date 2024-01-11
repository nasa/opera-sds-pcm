#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)


# Set PYTHONPATH
export PYTHONPATH=/home/ops/verdi/ops/CNM_product_delivery:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PGE=$(basename "${BASE_PATH}")
export PYTHONDONTWRITEBYTECODE=1

CONTEXT_FILE="_context.json"
WRAPPER="product_delivery.utils.send_notify_msg"
WORKDIR=$PWD

echo "Setting workdir as $WORKDIR"

# source environment
source $HOME/verdi/bin/activate

#$HOME/verdi/ops/CNM_product_delivery/product_delivery/utils/send_notify_msg.py $CONTEXT_FILE --reuse_md5
$HOME/verdi/ops/CNM_product_delivery/product_delivery/utils/send_notify_msg.py $CONTEXT_FILE 
