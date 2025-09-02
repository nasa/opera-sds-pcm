#!/usr/bin/env bash

#######################################################################
# INSTALLATION SCRIPT FOR cmr_audit.py
#
# This script will setup the execution environment for cmr_audit.py
#######################################################################

set -e

cmdname=$(basename $0)

######################################################################
# Function definitions
######################################################################

echoerr() { if [[ $QUIET -ne 1 ]]; then echo "$@" 1>&2; fi }

# Output script usage information.
usage()
{
    cat << USAGE >&2
Usage:
  $cmdname [options]
Examples:
  $cmdname --branch=2.0.0-rc.10.0
  $cmdname --branch=develop
  $cmdname --branch=issue_576
Options:
      --branch The branch to retrieve cmr_audit tools from.
USAGE
}

######################################################################
# Argument parsing
######################################################################

# defaults for optional args
branch_or_tag=develop

# parse args
if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

for i in "$@"; do
  case $i in
    -h|--help)
      usage
      shift
      exit 0
      ;;
    --branch=*)
      branch_or_tag="${i#*=}"
      shift
      ;;
    *)
      # unknown option
      echoerr "Unsupported argument $i. Exiting."
      usage
      exit 1
      ;;
  esac
done


######################################################################
# Argument validation
######################################################################

# NOTE: purposely left empty


######################################################################
# Main script body
######################################################################


# DEV: emergency handle
# git sparse-checkout disable

# deactivate any existing python virtual environments (typically "metrics")
set +e
deactivate
set -e

# create virtual environment and install dependencies
cd /export/home/hysdsops/metrics/ops/opera-pcm
python --version
python -m venv venv_cmr_audit

source ./venv_cmr_audit/bin/activate
python -m pip install --upgrade pip
conda install -y -c conda-forge gdal
pip install --progress-bar off -e '.[cmr_audit]'
pip install geopandas

deactivate
