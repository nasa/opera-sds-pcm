#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# This is the base directory where the verdi codebase resides on the worker at the host level
HOST_VERDI_HOME={{ '$HOME' }}
DATA_DIR=/data

source $HOST_VERDI_HOME/verdi/bin/activate

# copy hysds configs
rm -rf $HOST_VERDI_HOME/verdi/etc
cp -rp $BASE_PATH/etc $HOST_VERDI_HOME/verdi/etc
cp -rp $HOST_VERDI_HOME/verdi/ops/hysds/celeryconfig.py $HOST_VERDI_HOME/verdi/etc/
ln -s $HOST_VERDI_HOME/verdi/ops/hysds/scripts/harikiri_sqs.py $HOST_VERDI_HOME/verdi/bin/harikiri.py
ln -s $HOST_VERDI_HOME/verdi/ops/hysds/scripts/spot_termination_detector.py $HOST_VERDI_HOME/verdi/bin/spot_termination_detector.py

# This allows us to use a custom start-verdi.sh
ln -s $HOST_VERDI_HOME/verdi/ops/hysds-dockerfiles/verdi/start-verdi.sh $HOST_VERDI_HOME/verdi/bin/start-verdi.sh

# detect GPUs/NVIDIA driver configuration
# GPUS=0
# if [ -e "/usr/bin/nvidia-smi" ]; then
#  NVIDIA_SMI_OUTPUT=$(/usr/bin/nvidia-smi -L)
#  if [ "$?" -eq 0 ]; then
#    GPUS=$(echo "$NVIDIA_SMI_OUTPUT" | /usr/bin/wc -l)
#  fi
# fi

# write supervisord from template
ESCAPED_HOST_VERDI_HOME=$(printf '%s\n' "$HOST_VERDI_HOME" | sed -e 's/[]\/$*.^[]/\\&/g');
IPADDRESS_ETH0=$(/usr/sbin/ifconfig $(/usr/sbin/route | awk '/default/{print $NF}') | grep 'inet ' | sed 's/addr://' | awk '{print $2}')
FQDN=$IPADDRESS_ETH0
sed "s/__IPADDRESS_ETH0__/$IPADDRESS_ETH0/g" $HOST_VERDI_HOME/verdi/etc/supervisord.conf.tmpl | \
  #sed "s/__HYSDS_GPU_AVAILABLE__/$GPUS/g" | \
  sed "s/__HOST_VERDI_HOME__/$ESCAPED_HOST_VERDI_HOME/g" | \
  sed "s/__HOST_USER__/$USER/g" | \
  sed "s/__FQDN__/$FQDN/g" > $HOST_VERDI_HOME/verdi/etc/supervisord.conf

# move creds
rm -rf $HOST_VERDI_HOME/.aws
cp -r $BASE_PATH/creds/.aws $HOST_VERDI_HOME/
rm -rf $HOST_VERDI_HOME/.boto; cp -f $BASE_PATH/creds/.boto $HOST_VERDI_HOME/
rm -rf $HOST_VERDI_HOME/.s3cfg; cp -f $BASE_PATH/creds/.s3cfg $HOST_VERDI_HOME/
rm -rf $HOST_VERDI_HOME/.netrc; cp -f $BASE_PATH/creds/.netrc $HOST_VERDI_HOME/; chmod 600 $HOST_VERDI_HOME/.netrc

# extract beefed autoindex
mkdir -p ${DATA_DIR}/work
tar xvfj $BASE_PATH/beefed-autoindex-open_in_new_win.tbz2

# make jobs and tasks dir
mkdir -p ${DATA_DIR}/work/jobs
mkdir -p ${DATA_DIR}/work/tasks

# prime verdi docker image
export VERDI_PRIMER_IMAGE="{{ VERDI_PRIMER_IMAGE }}"
export VERDI_PRIMER_IMAGE_BASENAME="$(basename $VERDI_PRIMER_IMAGE 2>/dev/null)"

# Start up Docker Registry if CONTAINER_REGISTRY is defined
export CONTAINER_REGISTRY="{{ CONTAINER_REGISTRY }}"
export CONTAINER_REGISTRY_BUCKET="{{ CONTAINER_REGISTRY_BUCKET }}"
export DOCKER_REGISTRY_IMAGE="s3://{{ CODE_BUCKET }}/docker-registry-2.tar.gz"
export DOCKER_REGISTRY_IMAGE_BASENAME="$(basename $DOCKER_REGISTRY_IMAGE 2>/dev/null)"
if [ ! -z "$CONTAINER_REGISTRY" -a ! -z "$CONTAINER_REGISTRY_BUCKET" ]; then
  if [ -z "$(docker images -q registry:2)" ]; then
    rm -rf /tmp/docker-registry-2.tar.gz
    aws s3 cp ${DOCKER_REGISTRY_IMAGE} /tmp/${DOCKER_REGISTRY_IMAGE_BASENAME}
    docker load -i /tmp/${DOCKER_REGISTRY_IMAGE_BASENAME}
  else
    echo "Registry already exists in Docker. Will not download image"
  fi
  docker run -p 5050:5000 -e REGISTRY_STORAGE=s3 \
    -e REGISTRY_STORAGE_S3_BUCKET={{ CONTAINER_REGISTRY_BUCKET }} \
    -e REGISTRY_STORAGE_S3_REGION={{ AWS_REGION }} --name=registry -d registry:2
fi

# Start up SDSWatch client
export LOGSTASH_IMAGE="s3://{{ CODE_BUCKET }}/logstash-oss-7.16.3.tar.gz"
export LOGSTASH_IMAGE_BASENAME="$(basename $LOGSTASH_IMAGE 2>/dev/null)"
if [ -z "$(docker images -q logstash-oss:7.16.3)" ]; then
  rm -rf /tmp/logstash-oss-7.16.3.tar.gz
  aws s3 cp ${LOGSTASH_IMAGE} /tmp/${LOGSTASH_IMAGE_BASENAME}
  docker load -i /tmp/${LOGSTASH_IMAGE_BASENAME}
else
  echo "Logstash already exists in Docker. Will not download image"
fi
docker run -e HOST=${FQDN} -v /data/work/jobs:/sdswatch/jobs \
  -v $HOST_VERDI_HOME/verdi/log:/sdswatch/log \
  -v sdswatch_data:/usr/share/logstash/data \
  -v $HOST_VERDI_HOME/verdi/etc/sdswatch_client.conf:/usr/share/logstash/config/conf/logstash.conf \
  --name=sdswatch-client -d logstash-oss:7.16.3 \
  logstash -f /usr/share/logstash/config/conf/logstash.conf --config.reload.automatic

# Load verdi docker image
if [ -z "$(docker images -q hysds/verdi:{{ VERDI_TAG }})" ]; then
  rm -rf /tmp/${VERDI_PRIMER_IMAGE_BASENAME}
  aws s3 cp ${VERDI_PRIMER_IMAGE} /tmp/${VERDI_PRIMER_IMAGE_BASENAME}
  docker load -i /tmp/${VERDI_PRIMER_IMAGE_BASENAME}
  docker tag hysds/verdi:{{ VERDI_TAG }} hysds/verdi:latest
else
  echo "Verdi already exists in Docker. Will not download image"
fi
