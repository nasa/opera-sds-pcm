#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

source $HOME/sciflo/bin/activate

ARTIFACTORY_REPO=$1
GRQ_ES_PUB_IP_VERDI=$2
AWS_REGION=$3

FILE=$HOME/aws-es-proxy

cd $HOME
if [ -f "$FILE" ]; then
    echo "$FILE exists."
else
    echo "$FILE does NOT exist."
    curl -O "https://cae-artifactory.jpl.nasa.gov/artifactory/$ARTIFACTORY_REPO/gov/nasa/jpl/iems/sds/pcm/tools/aws-es-proxy"
fi

chmod +x $HOME/aws-es-proxy
sudo systemctl stop elasticsearch.service
exec $HOME/aws-es-proxy -endpoint https://$GRQ_ES_PUB_IP_VERDI -listen 0.0.0.0:9200 -region $AWS_REGION -insecure -verbose
