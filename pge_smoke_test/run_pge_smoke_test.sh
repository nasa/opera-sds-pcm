#!/bin/bash

WORKING_DIR=$(pwd)
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

export OPERA_HOME=/home/ops/verdi/ops/opera-pcm
export PYTHONPATH=$BASE_PATH:$OPERA_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONDONTWRITEBYTECODE=1
export LD_LIBRARY_PATH=/opt/conda/lib:$LD_LIBRARY_PATH

source $HOME/verdi/bin/activate

PGE_NAME=$1
S3_BUCKET=$2
PGE_REPO_URL="https://github.com/nasa/opera-sds-pge.git"
PGE_HOME=/home/ops/verdi/ops/opera-sds-pge

echo "##########################################" 2>&1
echo -n "Starting PGE integration smoke test for ${PGE_NAME}: " 2>&1
date 2>&1

echo "Working dir is ${WORKING_DIR}" 2>&1

VERSION_TAG=$(docker images | grep opera_pge/${PGE_NAME} -m 1 | xargs sh -c 'echo $1') 2>&1
STATUS=$?

if [ $STATUS -ne 0 ]; then
  echo "Could not determine version tag for ${PGE_NAME}" 2>&1
  exit $STATUS
fi

echo "Version tag for ${PGE_NAME} is ${VERSION_TAG}" 2>&1

echo "Cloning opera-sds-pge repository"
git clone ${PGE_REPO_URL} ${PGE_HOME} 2>&1
STATUS=$?

if [ $STATUS -ne 0 ]; then
  echo "Failed to clone opera-sds-pge repository" 2>&1
  exit $STATUS
fi

echo "Running integration smoke test for container ${PGE_NAME}-${VERSION_TAG}" 2>&1
${PGE_HOME}/.ci/scripts/${PGE_NAME}/test_int_${PGE_NAME}.sh --tag ${VERSION_TAG} --temp-root ${WORKING_DIR} --no-metrics --no-cleanup 2>&1
STATUS=$?

if [ $STATUS -eq 2 ]; then
  echo "One or more product comparison failures occurred after running ${PGE_NAME}-${VERSION_TAG}" 2>&1
elif [ $STATUS -ne 0 ]; then
  echo "Failed to execute integration test for container ${PGE_NAME}-${VERSION_TAG}" 2>&1
  exit $STATUS
fi

TIMESTAMP=$(date +"%Y-%m-%dT%T")
S3_DESTINATON="s3://${S3_BUCKET}/smoke_test/${PGE_NAME}/${VERSION_TAG}/${TIMESTAMP}/"

echo "Uploading smoke test results to ${S3_DESTINATON}"

aws s3 cp "${WORKING_DIR}/_stderr.txt" "${S3_DESTINATON}" && \
    aws s3 cp "${WORKING_DIR}/_stdout.txt" "${S3_DESTINATON}" && \
    aws s3 cp --recursive --include="*" "${PGE_HOME}/test_results/${PGE_NAME}/" "${S3_DESTINATON}"

STATUS=$?

if [ $STATUS -ne 0 ]; then
  echo "Failed to upload smoke test results to ${S3_DESTINATON}" 2>&1
  exit $STATUS
fi
