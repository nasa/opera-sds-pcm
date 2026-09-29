#!/bin/bash
# Smoke test orchestrator.
# Handles shared setup (simulation mode, CNM workers) then dispatches
# per-PGE tests in parallel with dependency gating.
#
# Each PGE lives in its own dev-e2e-pge-{PGE}/ directory and is
# independently runnable. This script is the only place that knows
# the dependency graph and parallelism strategy.

source $HOME/.bash_profile

# check args
if [ "$#" -eq 1 ]; then
  config_file=${1}
else
  echo "Invalid number of arguments ($#) $*" 1>&2
  exit 1
fi

source ${config_file}

# fail on any errors
set -ex

PGE_BASE="${HOME}/mozart/ops/opera-pcm/cluster_provisioning"

cd ~/.sds/files

# ============================================================
# Shared setup
# ============================================================

# backup settings.yaml
cp ~/mozart/ops/opera-pcm/conf/settings.yaml ~/mozart/ops/opera-pcm/conf/settings.yaml.bak

# disable simulation mode
sed -i "s/PGE_SIMULATION_MODE: !!bool true/PGE_SIMULATION_MODE: !!bool false/g" ~/mozart/ops/opera-pcm/conf/settings.yaml

# propagate settings change
fab -f ~/.sds/cluster.py -R mozart,grq,factotum update_opera_packages
sds ship

# Scale shared CNM notification workers
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py \
  ${project}-${venue}-${counter}-opera-job_worker-send_cnm_notify_podaac --desired-capacity 1
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py \
  ${project}-${venue}-${counter}-opera-job_worker-send_cnm_notify_asf --desired-capacity 1
~/mozart/ops/opera-pcm/conf/sds/files/test/update_asg.py \
  ${project}-${venue}-${counter}-opera-job_worker-rcv_cnm_notify --desired-capacity 1

# ============================================================
# PGE dispatch (parallel with dependency gating)
# ============================================================
# Dependency graph:
#   Tier 0 (independent): DSWx-HLS, DSWx-NI, TROPO, DISP-NI
#   Tier 1 (SLC-based):   RTC-S1, CSLC-S1
#   Tier 2 (gates on T1): DSWx-S1 (←RTC), DIST-S1 (←RTC), DISP-S1 (←CSLC)
#   Tier 3 (gates on T2): CAL-DISP (←DISP-S1)

# Tier 0: Independent chains
${PGE_BASE}/dev-e2e-pge-DSWx_HLS/run_smoke_test.sh "${config_file}" &
PID_DSWX_HLS=$!

# --- Future PGEs (uncomment as implemented) ---
# Tier 1: SLC-based PGEs (parallel with each other and Tier 0)
# ${PGE_BASE}/dev-e2e-pge-RTC_S1/run_smoke_test.sh "${config_file}" & PID_RTC=$!
# ${PGE_BASE}/dev-e2e-pge-CSLC_S1/run_smoke_test.sh "${config_file}" & PID_CSLC=$!

# Tier 2: Dependent PGEs (gate on prerequisites — run only if prerequisite passed)
# (wait $PID_RTC  && ${PGE_BASE}/dev-e2e-pge-DSWx_S1/run_smoke_test.sh "${config_file}") & PID_DSWX_S1=$!
# (wait $PID_RTC  && ${PGE_BASE}/dev-e2e-pge-DIST_S1/run_smoke_test.sh "${config_file}") & PID_DIST=$!
# (wait $PID_CSLC && ${PGE_BASE}/dev-e2e-pge-DISP_S1_smoke/run_smoke_test.sh "${config_file}") & PID_DISP=$!

# Tier 3
# (wait $PID_DISP && ${PGE_BASE}/dev-e2e-pge-CAL_DISP/run_smoke_test.sh "${config_file}") & PID_CAL=$!

# ============================================================
# Collect results
# ============================================================
# Wait for each PGE and track failures. The || FAILED=1 idiom
# does not trigger set -e because it is part of an OR list.
FAILED=0

wait $PID_DSWX_HLS || FAILED=1

# --- Future PGEs (uncomment as implemented) ---
# wait $PID_RTC       || FAILED=1
# wait $PID_CSLC      || FAILED=1
# wait $PID_DSWX_S1   || FAILED=1
# wait $PID_DIST      || FAILED=1
# wait $PID_DISP      || FAILED=1
# wait $PID_CAL       || FAILED=1

if [ $FAILED -ne 0 ]; then
  echo "ERROR: One or more PGE smoke tests failed"
fi

exit $FAILED
