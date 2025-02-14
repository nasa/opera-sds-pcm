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

mkdir -p /export/home/hysdsops/cmr_audit
cd /export/home/hysdsops/cmr_audit

git --version
git clone --quiet -b "${branch_or_tag}" --filter=blob:none --no-checkout https://github.com/nasa/opera-sds-pcm.git

cd /export/home/hysdsops/cmr_audit/opera-sds-pcm
git sparse-checkout init --cone
git sparse-checkout set tools
git sparse-checkout set geo
git sparse-checkout set commons
git checkout --quiet

# DEV: emergency handle
# git sparse-checkout disable

# deactivate any existing python virtual environments (typically "metrics")
set +e
deactivate
set -e

# create virtual environment and install dependencies
cd /export/home/hysdsops/cmr_audit/opera-sds-pcm
python --version
python -m venv venv_cmr_audit

source ./venv_cmr_audit/bin/activate
python -m pip install --upgrade pip
pip install --progress-bar off -e '.[cmr_audit]'
deactivate
