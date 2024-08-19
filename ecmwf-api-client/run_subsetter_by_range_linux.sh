#!/usr/bin/env bash

#######################################################################
# This script will create run the subsetter utility. Must be ran from a
# Verdi worker.
#######################################################################
set -e
# DEV: uncomment when debugging
#set -x

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
  $cmdname --start-date=2024-01-01 --end-date=2024-01-10 --bucket=my_source_bucket_name --target-bucket=my_target_bucket_name ...
Options:
      --start-date    REQUIRED. The start date in YYYY-MM-DD format.
      --end-date      REQUIRED. The end date in YYYY-MM-DD format. Not inclusive.
      --bucket        REQUIRED. Source bucket name.
      --target-bucket REQUIRED. Target bucket name.
USAGE
}

######################################################################
# Argument parsing
######################################################################

# parse args
if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

for i in "$@"; do
  case $i in
    -h|--help)
      usage
      exit 0
      shift
      ;;
    *)
      # PASS
      ;;
  esac
done

for i in "$@"; do
  case $i in
    --start-date=*)
      IN_START_DATE="${i#*=}"
      shift
      ;;
    --end-date=*)
      IN_END_DATE="${i#*=}"
      shift
      ;;
    --bucket=*)
      bucket="${i#*=}"
      shift
      ;;
    --target-bucket=*)
      target_bucket="${i#*=}"
      shift
      ;;
    -h|--help)
      # SKIP
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

# check if required params unset or empty
if [[ -z $IN_START_DATE ]]; then
  echo Missing required arg \"--start-date\". See usage.
  exit 1
fi
if [[ -z $IN_END_DATE ]]; then
  echo Missing required arg \"--end-date\". See usage.
  exit 1
fi
if [[ -z $bucket ]]; then
  echo Missing required arg \"--bucket\". See usage.
  exit 1
fi
if [[ -z $target_bucket ]]; then
  echo Missing required arg \"--target-bucket\". See usage.
  exit 1
fi

# print all variables that have been set
(set -o posix ; set)


######################################################################
# Main script body
######################################################################

# check AWS session
aws s3 ls 1>/dev/null
echo Validated AWS CLI session

# PARSE RANGE

# parse dates
IN_START_DATE=$(echo ${IN_START_DATE} | tr '-' ' ' | xargs printf -- '%s')
IN_END_DATE=$(echo "${IN_END_DATE}" | tr '-' ' ' | xargs printf -- '%s')

IN_DATE=${IN_START_DATE}
while [ "${IN_DATE}" != ${IN_END_DATE} ]; do
  if [ 'None' == "$(aws s3api list-objects-v2 --bucket opera-ecmwf --prefix raw/"${IN_DATE}"/ --query 'Contents[*].Key' --output text --no-paginate)" ]; then
    echo "No files found in s3 for ${IN_DATE}"
    exit 1
  fi
  IN_LIST=$(aws s3api list-objects-v2 --bucket opera-ecmwf --prefix raw/"${IN_DATE}"/ --query 'Contents[*].Key' --output text --no-paginate | xargs echo)
  echo run_subsetter.sh --bucket=${bucket} --target-bucket=${target_bucket} --s3-keys "${IN_LIST}"

  # linux date increment
  # IN_DATE=$(date -j -v +1d -f "%Y%m%d" ${IN_DATE} +%Y%m%d)

  # macOS date increment
  IN_DATE=$(date -j -v +1d -f "%Y%m%d" ${IN_DATE} +%Y%m%d)
done
