#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# detect HySDS virtualenv
if [ -d "${HOME}/mozart" ]; then
  HYSDS_DIR=${HOME}/mozart
elif [ -d "${HOME}/metrics" ]; then
  HYSDS_DIR=${HOME}/metrics
elif [ -d "${HOME}/sciflo" ]; then
  HYSDS_DIR=${HOME}/sciflo
elif [ -d "${HOME}/verdi" ]; then
  HYSDS_DIR=${HOME}/verdi
else
  echo "Couldn't detect installation of HySDS." >&2
  exit 1
fi

# source virtualenv
source $HYSDS_DIR/bin/activate

# if verdi, make jobs dir and set verdi params
if [[ "${HYSDS_DIR}" == *verdi ]]; then
  mkdir -p /data/work/jobs
  verdi_params="-v /data/work/jobs:/sdswatch/jobs"
else
  verdi_params=""
fi

# Start up SDSWatch client
IPADDRESS_ETH0=$(/usr/sbin/ifconfig $(/usr/sbin/route | awk '/default/{print $NF}') | grep 'inet ' | sed 's/addr://' | awk '{print $2}')
FQDN=$IPADDRESS_ETH0
export LOGSTASH_IMAGE="s3://{{ CODE_BUCKET }}/logstash-oss-7.16.3.tar.gz"
export LOGSTASH_IMAGE_BASENAME="$(basename $LOGSTASH_IMAGE 2>/dev/null)"
if [ -z "$(docker images -q logstash-oss:7.16.3)" ]; then
  rm -rf /tmp/logstash-oss-7.16.3.tar.gz
  aws s3 cp ${LOGSTASH_IMAGE} /tmp/${LOGSTASH_IMAGE_BASENAME}
  docker load -i /tmp/${LOGSTASH_IMAGE_BASENAME}
else
  echo "Logstash already exists in Docker. Will not download image"
fi
exec docker run --rm -e HOST=${FQDN} \
  -e XPACK_MONITORING_ENABLED=false \
  $verdi_params \
  -v $HYSDS_DIR/log:/sdswatch/log \
  -v sdswatch_data:/usr/share/logstash/data \
  -v $HYSDS_DIR/etc/sdswatch_client.conf:/usr/share/logstash/config/conf/logstash.conf \
  --name=sdswatch-client logstash-oss:7.16.3 \
  logstash -f /usr/share/logstash/config/conf/logstash.conf --config.reload.automatic
