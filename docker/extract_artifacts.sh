#!/bin/bash
set -ex

# get docker image
IMG=$1

# extract test, code analysis & coverage artifacts
CONTAINER_ID=$(docker run --rm -d ${IMG} sleep 60)
docker cp ${CONTAINER_ID}:/tmp/pytest_unit.xml .
docker cp ${CONTAINER_ID}:/tmp/flake8.log .
docker cp ${CONTAINER_ID}:/tmp/coverage.html .
docker rm -f ${CONTAINER_ID}

exit 0
