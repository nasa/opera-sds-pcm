#!/usr/bin/env bash

#######################################################################
# EXECUTION SCRIPT FOR cmr_audit.py
#
# This script will execute cmr_audit.py
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
  $cmdname --hls
  $cmdname --slc
Options:
      --hls Instructs this script to run cmr_audit_hls.py
      --slc Instructs this script to run cmr_audit_slc.py
USAGE
}

######################################################################
# Argument parsing
######################################################################

# defaults for optional args
# NOTE: purposely left empty

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
    --hls)
      cmr_audit_filename=cmr_audit_hls
      shift
      ;;
    --slc)
      cmr_audit_filename=cmr_audit_slc
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

if [[ ! -v cmr_audit_filename ]]; then
  usage
  exit 1
fi


######################################################################
# Main script body
######################################################################

# deactivate any existing python virtual environments (typically "metrics")
set +e
deactivate
set -e

now=$(date --iso-8601=d)
start_dt=$(date --iso-8601=s -d "$now - 2 weeks")
end_dt=$(date --iso-8601=s -d "$now - 1 week")

cd /export/home/hysdsops/cmr_audit/opera-sds-pcm

source venv_cmr_audit/bin/activate
python /export/home/hysdsops/cmr_audit/opera-sds-pcm/tools/ops/cmr_audit/${cmr_audit_filename}.py \
  --start-datetime=$start_dt \
  --end-datetime=$end_dt
deactivate
