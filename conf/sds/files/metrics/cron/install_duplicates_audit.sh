#!/usr/bin/env bash

set +e
deactivate
set -e

cd /export/home/hysdsops/
git clone --quiet --single-branch -b 'main' https://github.com/nasa/opera-sds-ops.git
cd opera-sds-ops/duplicates/

python --version
python -m venv venv

source ./venv/bin/activate
pip install -r requirements.txt

deactivate
