#!/bin/bash
##############################################################################
# DISP-S1 all-phases smoke test.
#
# This is the deep test for phased (mode-switching) historical processing. It
# is not part of the standard deploy: run it periodically, and whenever the
# DISP-S1 pipeline changes -- the phase model, the compressed CSLC lineage
# math, the k-cycle evaluator guards, or the historical processing tool.
#
# It walks frame 17235, whose annotations in the shipping consistent burst
# database carry every phase kind and two multi-year gaps:
#
#     historical_01[15]  2016-07-07 .. 2017-09-06   one whole k-set
#     forward_01[1]      2017-09-18
#     ---- 2.8 year gap ----
#     historical_02[15]  2020-06-22 .. 2021-08-04   one whole k-set, fresh lineage
#     forward_02[2]      2021-08-28 .. 2021-09-09
#     ---- 3.8 year gap ----
#     no_run[9]          2025-06-20 .. 2025-09-24   skipped whole
#
# What that exercises which the standard smoke test cannot:
#   - a compressed CSLC lineage reset at a new historical phase, and a boundary
#     produced at a position that is not a multiple of k on the absolute grid
#   - --m=1 on the post-gap k-set
#   - forward KSCs referencing only the current lineage's boundary, ignoring
#     the pre-gap ones still present in GRQ
#   - a no_run block skipped entirely, with the batch proc still reaching 100%
#
# Frame 17235 carries 27 bursts, so real-PGE execution is slow by design. Budget
# most of a day. data_end_date deliberately extends past the trailing no_run
# block so the skip path runs rather than the walk simply ending early.
##############################################################################
source $HOME/.bash_profile

TEST_DIR="${HOME}/mozart/ops/opera-pcm/cluster_provisioning/dev-e2e-pge-DISP_S1-all_phases"

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

cd ~/.sds/files

cp ${SETTINGS} ${SETTINGS}.bak

# Real PGEs, and the processing-mode master switch on for the whole run. Unlike
# the standard smoke test this venue has no un-phased stage to protect, so the
# switch stays on throughout and is never restored.
sed -i "s/PGE_SIMULATION_MODE: !!bool true/PGE_SIMULATION_MODE: !!bool false/g" ${SETTINGS}
sed -i "s/^DISP_S1_PROCESSING_MODE_ENABLED:.*/DISP_S1_PROCESSING_MODE_ENABLED: true/" ${SETTINGS}
grep -E "^PGE_SIMULATION_MODE:|^DISP_S1_PROCESSING_MODE_ENABLED:" ${SETTINGS}

fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
sds ship

# ingest Sacramento AOI to test ingest
~/mozart/ops/hysds/scripts/ingest_dataset.py AOI_sacramento_valley ~/mozart/etc/datasets.json

# The SCIFLO_L3_DISP_S1 trigger rule gates on metadata.region_id against a whitelist
# that defaults to ["0"]. Frame 17235 is region 4, so with the default in place its
# forward KSCs never match the rule and the forward dates publish nothing -- the
# state configs are complete and final, and the products simply never trigger. This
# test is not exercising region gating, so turn it off for the run, as the standard
# smoke test does at the end of its own.
python ~/mozart/ops/opera-pcm/tools/disp_s1_set_whitelist.py --disable-whitelist

cd ${TEST_DIR}

# pcm_batch rejects a phased batch proc whose frames are not annotated, whose
# annotations would be quarantined, or whose k differs from the batch size the
# labels were generated for. A failure here means the deployed burst database
# is not the annotated variant, or the master switch did not propagate.
python ~/mozart/ops/opera-pcm/tools/pcm_batch.py create --file disp_s1_all_phases_batch_proc.json

nohup python ~/mozart/ops/opera-pcm/tools/run_disp_s1_historical_processing.py &
HIST_PID=$!

# --max_time is a per-entry budget. The first run of this test took ~21.5h end to
# end -- two k-sets plus three forward dates on a 27-burst frame, each SCIFLO ~3h --
# and a 20h budget expired about 90 minutes before the last two products published,
# failing the count on a run whose every structural assertion had passed. 30h leaves
# room for a slower PGE or a scale-up delay without turning that into a red result.
#
# check_datasets_file.py exits non-zero when a count is not met and this script
# runs under `set -e`; `|| true` keeps the run going so the result file is left
# for check_pcm.py to assert on and the daemon is always cleaned up below.
~/mozart/ops/opera-pcm/conf/sds/files/test/check_datasets_file.py --crid=${crid} ${TEST_DIR}/datasets_e2e.json all_phases --max_time 108000 /tmp/datasets_all_phases.txt || true

kill $HIST_PID 2>/dev/null || true

##############################################################################
# Phase-structure assertions.
#
# Dataset counts alone do not prove the walk took the phased path -- an
# un-phased walk over the same frame would produce a different count, but a
# regression that silently reverted to the absolute grid while still producing
# 31 products would slip through. These checks read the batch proc state and
# the compressed CSLC lineage directly.
##############################################################################
cd ~/mozart/ops/opera-pcm
python ~/mozart/ops/opera-pcm/conf/sds/files/test/check_disp_s1_phases.py \
  --frame-id 17235 --k 15 --out /tmp/phases_all_phases.txt || true
cd ${TEST_DIR}

cat /tmp/datasets_all_phases.txt
cat /tmp/phases_all_phases.txt
