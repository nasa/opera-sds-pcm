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

# deactivate any existing python virtual environment (typically "metrics").
# Only call it when it is the venv's own shell function: with no venv active,
# bash instead finds conda's bin/deactivate on PATH and tries to execute it,
# which fails with "Permission denied" (that file is not executable in the
# v6.4.2 conda env).
if [ "$(type -t deactivate 2>/dev/null)" = "function" ]; then
  deactivate
fi

# Native GDAL belongs to the conda environment, not to the venv, so install it
# BEFORE the venv is activated. conda's launcher is "#!/usr/bin/env python" in
# the v6.4.2 conda env (v6.1.2 used an absolute interpreter), so running it with
# a venv active resolves python to the venv, which has no conda module, and it
# dies with "ModuleNotFoundError: No module named 'conda'".
conda install -y -c conda-forge gdal

# create virtual environment and install dependencies
cd /export/home/hysdsops/metrics/ops/opera-pcm
python --version
python -m venv venv_cmr_audit

source ./venv_cmr_audit/bin/activate
python -m pip install --upgrade pip
pip install --progress-bar off -e '.[cmr_audit]'
pip install geopandas

deactivate
