BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)


# Set PYTHONPATH
export PYTHONPATH=/home/ops/verdi/ops/CNM_product_delivery:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PGE=$(basename "${BASE_PATH}")
export PYTHONDONTWRITEBYTECODE=1

CONTEXT_FILE="_context.json"
WORKDIR=$PWD

echo "Setting workdir as $WORKDIR"

# source environment
source $HOME/verdi/bin/activate

if [ $# -eq 1 ]; then
    CONTEXT_FILE=$1
fi

if [ ! -f $CONTEXT_FILE ]; then
    echo "Context file not found!"
    exit 1
fi

$HOME/verdi/ops/CNM_product_delivery/product_delivery/utils/update_es.py

if [ $? -eq 0 ]; then
  echo "Extracting S3 URLs into GRQ"
  $HOME/verdi/ops/opera-pcm/tools/set_daac_urls.py
fi

