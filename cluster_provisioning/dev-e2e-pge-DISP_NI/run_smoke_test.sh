#!/bin/bash
##############################################################################
# DISP-NI historical processing smoke test.
#
# Walks frames 26410 (Hawaii) and 13217 (Alabama/Mississippi, whose GSLCs mix
# DHDH and SHSH polarizations) side by side, each through K_SETS k-sets of the
# whole historical chain on real NISAR GSLCs and GUNWs, as the DISP-S1 smoke
# test does:
#
#   run_disp_ni_historical_processing.py -> gslc_query_hist -> gslc_download
#     -> SCIFLO_L3_DISP_NI -> L3_DISP_NI products + compressed GSLC
#
# For each frame and k-set i:
#   1. k-set i is queried and its download job submits a SCIFLO_L3_DISP_NI job
#      with the compressed GSLCs of up to M-1 k-sets before it; k-set i+1 waits
#      on k-set i's compressed GSLC.
#   2. That job publishes its L3_DISP_NI products (K-1 for the first k-set, K for
#      every later one) and one compressed GSLC (ICS 2.6.2).
# After every frame's last k-set the batch proc finishes.
#
# K, M and K_SETS come from the disp_ni_smoke_k / disp_ni_smoke_m /
# disp_ni_smoke_k_sets settings of the config file (default 5, 3 and 5). The
# frame database, the two frames of ADT's consistent GSLC database, must hold
# K_SETS*K dates for each.
##############################################################################
source $HOME/.bash_profile

TEST_DIR="${HOME}/mozart/ops/opera-pcm/cluster_provisioning/dev-e2e-pge-DISP_NI"
FRAME_IDS="26410 13217"
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

K=${disp_ni_smoke_k:-5}
M=${disp_ni_smoke_m:-3}
K_SETS=${disp_ni_smoke_k_sets:-5}

# The batch proc and the expected dataset counts follow from K, M and K_SETS. Written to /tmp so the
# checked-in files stay as they are; fails now if the frame database is too short for K_SETS*K dates.
BATCH_PROC=/tmp/disp_ni_smoke_batch_proc.json
DATASETS=/tmp/disp_ni_smoke_datasets_e2e.json
python ${TEST_DIR}/make_smoke_inputs.py --k ${K} --m ${M} --k-sets ${K_SETS} --frame-ids ${FRAME_IDS} \
  --frame-db ${TEST_DIR}/disp_ni_smoke_frame_db.json --batch-proc ${TEST_DIR}/disp_ni_test_batch_proc.json \
  --out-batch-proc ${BATCH_PROC} --out-datasets ${DATASETS}

# How long to wait for one k-set's PGE job to publish: 2 h plus 2 h per GSLC, 12 h at K=5. ADT's
# benchmarks without GUNWs are about 9.5 h for 7 GSLCs and 20 h for 14.
PGE_WAIT_SECS=$(( (2 + 2 * K) * 3600 ))

SETTINGS=~/mozart/ops/opera-pcm/conf/settings.yaml
cp ${SETTINGS} ${SETTINGS}.bak

# Deploy the smoke test's frame database, the two frames cut from ADT's, and point settings.yaml at it.
# Real PGEs, as in the DISP-S1 test.
DATASET_BUCKET=$(grep "^DATASET_BUCKET:" ${SETTINGS} | awk "{print \$2}" | tr -d "'")
SMOKE_S3_PREFIX="s3://${DATASET_BUCKET}/smoke_test/disp_ni"
FRAME_DB_S3PATH="${SMOKE_S3_PREFIX}/disp_ni_smoke_frame_db.json"
aws s3 cp ${TEST_DIR}/disp_ni_smoke_frame_db.json ${FRAME_DB_S3PATH}

sed -i "s/PGE_SIMULATION_MODE: !!bool true/PGE_SIMULATION_MODE: !!bool false/g" ${SETTINGS}
sed -i "s|^DISP_NI_CBDB_S3PATH:.*|DISP_NI_CBDB_S3PATH: \"${FRAME_DB_S3PATH}\"|" ${SETTINGS}

# The ADT static ancillaries (frame-to-bounds, reference dates, algorithm parameters overrides) are
# not in s3://opera-ancillaries yet, so deploy the smoke test's copies, cut down to the test's frames, and
# point the DISP_NI settings at them. The edits stay inside the DISP_NI block because DISP_S1 has
# keys of the same name.
for anc in FRAME_TO_BOUNDS_JSON:disp_ni_smoke_frame_to_bounds.json \
           REFERENCE_DATE_DATABASE_JSON:disp_ni_smoke_reference_dates.json \
           ALGORITHM_PARAMETERS_OVERRIDES_JSON:disp_ni_smoke_algorithm_parameters_overrides.json; do
  key=${anc%%:*}
  file=${anc#*:}
  aws s3 cp ${TEST_DIR}/${file} ${SMOKE_S3_PREFIX}/${file}
  sed -i "/^DISP_NI:/,/^[A-Za-z]/s|^\([[:space:]]*\)${key}:.*|\1${key}: \"${SMOKE_S3_PREFIX}/${file}\"|" ${SETTINGS}
done

grep -E "^PGE_SIMULATION_MODE:|^DISP_NI_CBDB_S3PATH:" ${SETTINGS}
sed -n "/^DISP_NI:/,/^[A-Za-z]/p" ${SETTINGS} | grep -E "_JSON:|HISTORICAL:"

# The algorithm parameters files are read from s3://opera-ancillaries. Fail now, with a clear message,
# rather than hours later in the PGE job if one is missing.
for path in $(sed -n "/^DISP_NI:/,/^[A-Za-z]/p" ${SETTINGS} | grep -E "^[[:space:]]+(IONOSPHERE_)?HISTORICAL:" | awk '{print $2}' | tr -d '"'); do
  aws s3 ls "${path}" > /dev/null || { echo "ERROR: DISP_NI algorithm parameters ${path} does not exist" 1>&2; exit 1; }
done

cd ~/.sds/files
fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
sds ship

cd ${TEST_DIR}

python ~/mozart/ops/opera-pcm/tools/pcm_batch.py create --file ${BATCH_PROC}

nohup python ~/mozart/ops/opera-pcm/tools/run_disp_ni_historical_processing.py --sleep-secs 60 &
HIST_PID=$!
trap "kill ${HIST_PID} 2>/dev/null || true" EXIT

# The checks exit non-zero on failure and this script runs under `set -e`;
# `|| true` keeps the run going so every result file is left for check_pcm.py.
check_walk() {
  local frame_id=$1 k_sets=$2 expect=$3 out=$4
  (cd ~/mozart/ops/opera-pcm && PYTHONPATH=$PWD python conf/sds/files/test/check_disp_ni_historical.py \
    --label "${BATCH_PROC_LABEL}" --frame-id ${frame_id} --k ${K} --m ${M} \
    --k-sets ${k_sets} --expect ${expect} --timeout-mins 60 --out ${out}) || true
}

# One frame's walk, k-set by k-set. The frames walk independently, so each is checked in its own
# background loop: a frame must be checked while it is on the k-set the check expects.
check_frame() {
  local frame_id=$1
  for i in $(seq 1 ${K_SETS}); do
    # k-set ${i} is queried; the next one waits on its compressed GSLC (after the last, the frame is done)
    if [ ${i} -lt ${K_SETS} ]; then expect=blocked; else expect=frame_done; fi
    check_walk ${frame_id} ${i} ${expect} /tmp/disp_ni_hist_f${frame_id}_k_set_${i}.txt

    # k-set ${i}'s PGE job publishes its products and compressed GSLC
    ~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} ${DATASETS} \
      f${frame_id}_k_set_${i} --max_time ${PGE_WAIT_SECS} /tmp/datasets_disp_ni_f${frame_id}_k_set_${i}.txt || true
  done
}

CHECK_PIDS=()
for frame_id in ${FRAME_IDS}; do
  check_frame ${frame_id} > /tmp/disp_ni_smoke_checks_f${frame_id}.log 2>&1 &
  CHECK_PIDS+=($!)
done
wait "${CHECK_PIDS[@]}" || true

# Once every frame is done, the batch proc has finished and disabled itself
for frame_id in ${FRAME_IDS}; do
  check_walk ${frame_id} ${K_SETS} finished /tmp/disp_ni_hist_f${frame_id}_finished.txt
done

kill ${HIST_PID} 2>/dev/null || true
trap - EXIT

# Restore the shipped settings so the venue is left as deployed
mv ${SETTINGS}.bak ${SETTINGS}
cd ~/.sds/files
fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
sds ship

cat /tmp/disp_ni_hist_f*.txt /tmp/datasets_disp_ni_f*.txt || true
