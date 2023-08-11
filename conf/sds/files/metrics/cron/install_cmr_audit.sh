#!/usr/bin/env bash

#######################################################################
# INSTALLATION SCRIPT FOR cmr_audit.py
#
# This script will setup the execution environment for cmr_audit.py
#######################################################################

set -ex

env

mkdir -p /export/home/hysdsops/cmr_audit
cd /export/home/hysdsops/cmr_audit

git --version
set +e
git clone -b develop --filter=blob:none --no-checkout https://github.com/nasa/opera-sds-pcm.git
set -e

cd /export/home/hysdsops/cmr_audit/opera-sds-pcm
git sparse-checkout init --cone
git sparse-checkout set tools
git checkout

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
pip install -e '.[cmr_audit]'
deactivate
