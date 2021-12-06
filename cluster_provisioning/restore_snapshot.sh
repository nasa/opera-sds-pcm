#!/bin/bash
source $HOME/.bash_profile


MOZART_ES_URL=$1
GRQ_ES_URL=$2
METRICS_ES_URL=$3
SNAPSHOT_BUCKET=$4
BUCKET_PATH=$5
ROLE_ARN=$6
CRID=$7
CLUSTER_TYPE=$8

RESTORE_REPOSITORY=restore-repository
TIME_SLEEP=30

set -ex


# creating the repository to restore snapshots
~/mozart/bin/snapshot_es_data.py --es-url ${GRQ_ES_URL} create-repository --repository ${RESTORE_REPOSITORY} --bucket ${SNAPSHOT_BUCKET} --bucket-path ${BUCKET_PATH}/grq --role-arn ${ROLE_ARN}
~/mozart/bin/snapshot_es_data.py --es-url ${MOZART_ES_URL} create-repository --repository ${RESTORE_REPOSITORY} --bucket ${SNAPSHOT_BUCKET} --bucket-path ${BUCKET_PATH}/mozart --role-arn ${ROLE_ARN}
~/mozart/bin/snapshot_es_data.py --es-url ${METRICS_ES_URL} create-repository --repository ${RESTORE_REPOSITORY} --bucket ${SNAPSHOT_BUCKET} --bucket-path ${BUCKET_PATH}/metrics --role-arn ${ROLE_ARN}
echo "[$(date -u +"%Y-%m-%d %H:%M:%S")] sleeping ${TIME_SLEEP} seconds to populate the historical snapshot data"
sleep ${TIME_SLEEP}


# getting the most recent snapshot to restore
NEWEST_GRQ_SNAPSHOT=`~/mozart/bin/snapshot_es_data.py --es-url ${GRQ_ES_URL} newest-snapshot --repository ${RESTORE_REPOSITORY}`
NEWEST_MOZART_SNAPSHOT=`~/mozart/bin/snapshot_es_data.py --es-url ${MOZART_ES_URL} newest-snapshot --repository ${RESTORE_REPOSITORY}`
NEWEST_METRICS_SNAPSHOT=`~/mozart/bin/snapshot_es_data.py --es-url ${METRICS_ES_URL} newest-snapshot --repository ${RESTORE_REPOSITORY}`
echo "[$(date -u +"%Y-%m-%d %H:%M:%S")] newest grq snapshot: ${NEWEST_GRQ_SNAPSHOT}";
echo "[$(date -u +"%Y-%m-%d %H:%M:%S")] newest mozart snapshot: ${NEWEST_MOZART_SNAPSHOT}";
echo "[$(date -u +"%Y-%m-%d %H:%M:%S")] newest metrics snapshot: ${NEWEST_METRICS_SNAPSHOT}";


# close the indices for restoration
~/mozart/bin/snapshot_es_data.py --es-url ${GRQ_ES_URL} close-indices --index-pattern grq_*,*_catalog
~/mozart/bin/snapshot_es_data.py --es-url ${MOZART_ES_URL} close-indices --index-pattern *_status-*,user_rules-*,job_specs,hysds_ios-*,containers
~/mozart/bin/snapshot_es_data.py --es-url ${METRICS_ES_URL} close-indices --index-pattern logstash-*,sdswatch-*


# restore the snapshots in grq, mozart and metrics
~/mozart/bin/snapshot_es_data.py --es-url ${GRQ_ES_URL} restore --repository ${RESTORE_REPOSITORY} --snapshot ${NEWEST_GRQ_SNAPSHOT}
~/mozart/bin/snapshot_es_data.py --es-url ${MOZART_ES_URL} restore --repository ${RESTORE_REPOSITORY} --snapshot ${NEWEST_MOZART_SNAPSHOT}
~/mozart/bin/snapshot_es_data.py --es-url ${METRICS_ES_URL} restore --repository ${RESTORE_REPOSITORY} --snapshot ${NEWEST_METRICS_SNAPSHOT}

echo "[$(date -u +"%Y-%m-%d %H:%M:%S")] sleeping ${TIME_SLEEP} seconds before starting the dataset check"
sleep ${TIME_SLEEP}

# dataset check
if [[ "${CLUSTER_TYPE}" = "forward" ]]; then
  ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_datasets_file.py --max_time=2700 --crid=${CRID} datasets_e2e.json 1,2,3,4,L0A_FWD,5,L0B_FWD,NETWORK_PAIR_FWD /tmp/datasets.txt
  ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_datasets_file.py --max_time=600 --crid=${CRID} datasets_e2e.json OBS_REPORT /tmp/report_datasets.txt
else
  ~/mozart/ops/nisar-pcm/conf/sds/files/test/check_datasets_file.py --max_time=2700 --crid=${CRID} datasets_e2e.json 1,2,3,4,L0A_REPROC,5,L0B_REPROC,NETWORK_PAIR_REPROC /tmp/datasets.txt
fi


# open the indices
~/mozart/bin/snapshot_es_data.py --es-url ${GRQ_ES_URL} open-indices --index-pattern grq_*,*_catalog
~/mozart/bin/snapshot_es_data.py --es-url ${MOZART_ES_URL} open-indices --index-pattern *_status-*,user_rules-*,job_specs,hysds_ios-*,containers
~/mozart/bin/snapshot_es_data.py --es-url ${METRICS_ES_URL} open-indices --index-pattern logstash-*,sdswatch-*
