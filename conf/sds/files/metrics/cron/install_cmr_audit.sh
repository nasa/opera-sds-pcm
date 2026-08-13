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

# Install native GDAL into the conda environment (not into the venv below), with
# the login shell's "metrics" venv neutralised.
#
# ~/.bash_profile activates that venv, so VIRTUAL_ENV and PATH arrive here
# already pointing at it. The v6.4.2 conda env's bin/conda is
# "#!/usr/bin/env python", so a bare `conda` resolves to the venv's interpreter
# and fails -- with NoBaseEnvironmentError, or ModuleNotFoundError once a second
# venv is layered on. v6.1.2's conda used an absolute interpreter, which is why
# this only started breaking on v6.4.2.
#
# Calling `deactivate` cannot fix this: it is a shell function belonging to the
# parent login shell and does not cross into this script's process.
env -u VIRTUAL_ENV PATH="$HOME/conda/bin:$PATH" "$HOME/conda/bin/conda" \
  install -y -c conda-forge gdal

# create virtual environment and install dependencies
cd /export/home/hysdsops/metrics/ops/opera-pcm
python --version
python -m venv venv_cmr_audit

source ./venv_cmr_audit/bin/activate
python -m pip install --upgrade pip
pip install --progress-bar off -e '.[cmr_audit]'
pip install geopandas

deactivate
