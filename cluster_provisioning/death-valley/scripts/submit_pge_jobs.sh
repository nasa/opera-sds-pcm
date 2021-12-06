#!/bin/bash
set -ex

# check args
if [ "$#" -eq 6 ]; then
  NUM_JOBS=$1
  PGE_BRANCH=$2
  QUEUE_NAME=$3
  MIN_SLEEP=$4
  MAX_SLEEP=$5
  GITHUB_OAUTH_TOKEN=$6
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

source ~/.bash_profile
source ~/mozart/bin/activate

rm -rf baseline-pge || true # Don't exit script if command fails
git clone --single-branch -b ${PGE_BRANCH} https://${GITHUB_OAUTH_TOKEN}@github.jpl.nasa.gov/IEMS-SDS/baseline-pge.git
cd baseline-pge

./utilities/submit_dumby_landsat.py --min_sleep ${MIN_SLEEP} --max_sleep ${MAX_SLEEP} -jv ${PGE_BRANCH} ${QUEUE_NAME} ${NUM_JOBS}
