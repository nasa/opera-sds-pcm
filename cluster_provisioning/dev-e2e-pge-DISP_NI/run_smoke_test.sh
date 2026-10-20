#!/bin/bash
##############################################################################
# DISP-NI historical processing smoke test.
#
# Walks frame 26410 two k-sets (k=6) through the whole historical chain on real
# NISAR GSLCs, as the DISP-S1 smoke test does:
#
#   run_disp_ni_historical_processing.py -> gslc_query_hist -> gslc_download
#     -> SCIFLO_L3_DISP_NI -> L3_DISP_NI products + compressed GSLC
#
#   1. The first k-set is queried (one CMR date the frame database excludes is
#      dropped) and its download job submits a SCIFLO_L3_DISP_NI job with no
#      compressed GSLCs. The second k-set waits on the first's compressed GSLC.
#   2. That job publishes K-1 = 5 L3_DISP_NI products and 1 compressed GSLC.
#   3. The compressed GSLC unblocks the second k-set, which runs with m=2 and
#      finishes the batch proc, adding K = 6 products and a second compressed
#      GSLC (ICS 2.6.2).
#
# No GUNWs are used yet (OPERA-2674), so each SAS run estimates the ionosphere
# from the GSLCs.
##############################################################################
source $HOME/.bash_profile

TEST_DIR="${HOME}/mozart/ops/opera-pcm/cluster_provisioning/dev-e2e-pge-DISP_NI"
FRAME_ID=26410
K=6
M=6
BATCH_PROC_LABEL="DISP-NI Smoke"

# check args
if [ "$#" -eq 1 ]; then
  config_file=${1}
else
  echo "Invalid number or arguments ($#) $*" 1>&2
  exit 1
fi

source ${config_file}

# fail on any errors
set -ex

SETTINGS=~/mozart/ops/opera-pcm/conf/settings.yaml
cp ${SETTINGS} ${SETTINGS}.bak

# The DISP-NI static ancillaries are read from the S3 paths in settings.yaml. Fail now, with a clear
# message, rather than hours later in the PGE job if the ADT delivery files were not uploaded there.
for key in FRAME_TO_BOUNDS_JSON REFERENCE_DATE_DATABASE_JSON ALGORITHM_PARAMETERS_OVERRIDES_JSON; do
  path=$(grep -E "^\s+${key}:" ${SETTINGS} | awk '{print $2}' | tr -d '"')
  aws s3 ls "${path}" > /dev/null || { echo "ERROR: DISP_NI.${key} ${path} does not exist" 1>&2; exit 1; }
done

# ADT has not delivered the DISP-NI frame database, so deploy the smoke test's
# own and point settings.yaml at it. Real PGEs, as in the DISP-S1 test.
DATASET_BUCKET=$(grep "^DATASET_BUCKET:" ${SETTINGS} | awk "{print \$2}" | tr -d "'")
FRAME_DB_S3PATH="s3://${DATASET_BUCKET}/smoke_test/disp_ni/disp_ni_smoke_frame_db.json"
aws s3 cp ${TEST_DIR}/disp_ni_smoke_frame_db.json ${FRAME_DB_S3PATH}

sed -i "s/PGE_SIMULATION_MODE: !!bool true/PGE_SIMULATION_MODE: !!bool false/g" ${SETTINGS}
sed -i "s|^DISP_NI_CBDB_S3PATH:.*|DISP_NI_CBDB_S3PATH: \"${FRAME_DB_S3PATH}\"|" ${SETTINGS}
grep -E "^PGE_SIMULATION_MODE:|^DISP_NI_CBDB_S3PATH:" ${SETTINGS}

cd ~/.sds/files
fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
sds ship

cd ${TEST_DIR}

python ~/mozart/ops/opera-pcm/tools/pcm_batch.py create --file disp_ni_test_batch_proc.json

nohup python ~/mozart/ops/opera-pcm/tools/run_disp_ni_historical_processing.py --sleep-secs 60 &
HIST_PID=$!
trap "kill ${HIST_PID} 2>/dev/null || true" EXIT

# The checks exit non-zero on failure and this script runs under `set -e`;
# `|| true` keeps the run going so every result file is left for check_pcm.py.
check_walk() {
  local k_sets=$1 expect=$2 out=$3
  (cd ~/mozart/ops/opera-pcm && PYTHONPATH=$PWD python conf/sds/files/test/check_disp_ni_historical.py \
    --label "${BATCH_PROC_LABEL}" --frame-id ${FRAME_ID} --k ${K} --m ${M} \
    --k-sets ${k_sets} --expect ${expect} --timeout-mins 60 --out ${out}) || true
}

# ============================================================
# Phase 1: the first k-set is queried and the second waits
# ============================================================
check_walk 1 blocked /tmp/disp_ni_hist_blocked.txt

# ============================================================
# Phase 2: the first k-set's PGE job publishes its products and compressed GSLC
# ============================================================
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} ${TEST_DIR}/datasets_e2e.json k_set_1 --max_time 43200 /tmp/datasets_disp_ni_k_set_1.txt || true

# ============================================================
# Phase 3: the compressed GSLC unblocks the second k-set
# ============================================================
check_walk 2 finished /tmp/disp_ni_hist_finished.txt

~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} ${TEST_DIR}/datasets_e2e.json hist --max_time 43200 /tmp/datasets_disp_ni_hist.txt || true

kill ${HIST_PID} 2>/dev/null || true
trap - EXIT

# Restore the shipped settings so the venue is left as deployed
mv ${SETTINGS}.bak ${SETTINGS}
cd ~/.sds/files
fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
sds ship

cat /tmp/disp_ni_hist_blocked.txt /tmp/datasets_disp_ni_k_set_1.txt /tmp/disp_ni_hist_finished.txt \
  /tmp/datasets_disp_ni_hist.txt || true
