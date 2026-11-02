#!/usr/bin/env bash

set +e
deactivate
set -ex

cd /export/home/hysdsops/opera-sds-ops/duplicates/
source ./venv/bin/activate

source /export/home/hysdsops/metrics/conf/sds/files/metrics/cron/duplicates.env

python duplicate_and_accountability_cron.py DSWX_HLS CSLC_S1 RTC_S1 DSWX_S1 DISP_S1 TROPO \
       -d 31 \
       --report-dir cron_reports \
       --s3-report-path "s3://${S3_BUCKET}/duplicate_reports/" \
       --plot-dir cron_plots \
       --s3-plot-path "s3://${S3_BUCKET}/duplicate_plots/" \
#        --opensearch "${ES_URL}" \
