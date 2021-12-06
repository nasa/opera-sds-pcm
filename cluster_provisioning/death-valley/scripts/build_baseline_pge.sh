#!/bin/bash
set -ex

# check args
if [ "$#" -eq 1 ]; then
  PGE_BRANCH=$1
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

source ~/mozart/bin/activate

sds -d ci add_job --token https://github.jpl.nasa.gov/IEMS-SDS/baseline-pge.git s3 -b ${PGE_BRANCH} -k
sds -d ci build_job https://github.jpl.nasa.gov/IEMS-SDS/baseline-pge.git -b ${PGE_BRANCH}
sds -d ci remove_job https://github.jpl.nasa.gov/IEMS-SDS/baseline-pge.git -b ${PGE_BRANCH}
