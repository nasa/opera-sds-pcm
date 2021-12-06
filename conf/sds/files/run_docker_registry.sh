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

# Start up Docker Registry if CONTAINER_REGISTRY is defined
export CONTAINER_REGISTRY="{{ CONTAINER_REGISTRY }}"
export CONTAINER_REGISTRY_BUCKET="{{ CONTAINER_REGISTRY_BUCKET }}"
export DOCKER_REGISTRY_IMAGE="s3://{{ CODE_BUCKET }}/docker-registry-2.tar.gz"
export DOCKER_REGISTRY_IMAGE_BASENAME="$(basename $DOCKER_REGISTRY_IMAGE 2>/dev/null)"
if [ ! -z "$CONTAINER_REGISTRY" -a ! -z "$CONTAINER_REGISTRY_BUCKET" ]
then
  rm -rf /tmp/docker-registry-2.tar.gz
  aws s3 cp ${DOCKER_REGISTRY_IMAGE} /tmp/${DOCKER_REGISTRY_IMAGE_BASENAME}
  docker load -i /tmp/${DOCKER_REGISTRY_IMAGE_BASENAME}
  docker rm -f registry
  exec docker run -p 5050:5000 -e REGISTRY_STORAGE=s3 \
    -e REGISTRY_STORAGE_S3_BUCKET={{ CONTAINER_REGISTRY_BUCKET }} \
    -e REGISTRY_STORAGE_S3_REGION={{ AWS_REGION }} --name=registry registry:2
fi
