#!/usr/bin/env bash

#######################################################################
# EXECUTION SCRIPT FOR cmr_audit.py
#
# This script will execute for cmr_audit.py
#######################################################################

set -ex

# deactivate any existing python virtual environments (typically "metrics")
set +e
deactivate
set -e

now=$(date)
start_dt=$(date --iso-8601=s -d "$now - 2 week")
end_dt=$(date --iso-8601=s -d "$now - 1 weeks")

cd /export/home/hysdsops/cmr_audit/opera-sds-pcm

source venv_cmr_audit/bin/activate
python /export/home/hysdsops/cmr_audit/opera-sds-pcm/tools/ops/cmr_audit/cmr_audit_hls.py \
  --start-datetime=$start_dt \
  --end-datetime=$end_dt
deactivate
