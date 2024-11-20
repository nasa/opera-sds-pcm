#!/usr/bin/env bash

#######################################################################
# This script will submit subsetter jobs. Recommended to be run from
# Mozart.
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
  $cmdname --start-date=2024-01-01 --end-date=2024-01-10 ...
  $cmdname --start-date=2024-01-01 --end-date=2024-01-10 --target-bucket=my_target_bucket ...
  $cmdname --start-date=2024-01-01 --end-date=2024-01-10 --bucket=my_source_bucket --target-bucket=my_target_bucket --mozart-ip=1.2.3.4 ...
Options:
      --start-date    REQUIRED. The start date in YYYY-MM-DD format.
      --end-date      REQUIRED. The end date in YYYY-MM-DD format. Not inclusive.
      --bucket        Source bucket name. Defaults to "opera-ecmwf".
      --target-bucket Target bucket name. Defaults to "opera-ancillaries".
      --mozart-ip     REQUIRED. Local IP address for Mozart. Optional if ran on Mozart.
      --release       Release version of the job Docker image. Defaults to "develop".
      --job-type      Job type. Defaults to "job-subsetter".
      --job-queue     Job queue. Defaults to opera-job_worker-ecmwf-subsetter.
      --job-name      Custom job name. Defaults to "subsetter".
      --job-tag       Custom job tag. Defaults to "subsetter".
USAGE
}

######################################################################
# Argument parsing
######################################################################

echo args: "$@"

# parse args
if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

# handle help
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

# initialize required param defaults
set +e
curl -s http://169.254.169.254/latest/meta-data/local-ipv4 -m 1
STATUS=$?
set -e
if [ $STATUS -ne 0 ]; then
    MOZART_IP=''
else
    MOZART_IP="$(curl -s http://169.254.169.254/latest/meta-data/local-ipv4 -m 1)"
fi

# default values
RELEASE='develop'
JOB_TYPE='job-subsetter'
JOB_QUEUE='opera-job_worker-ecmwf-subsetter'

JOB_NAME='subsetter'
JOB_TAGS='subsetter'

# override base script param defaults
for i in "$@"; do
  case $i in
    --release=*)
      RELEASE="${i#*=}"
      ;;
    --job-type=*)
      JOB_TYPE="${i#*=}"
      ;;
    --job-queue=*)
      JOB_QUEUE="${i#*=}"
      ;;

    --job-name=*)
      JOB_NAME="${i#*=}"
      ;;
    --job-tags=*)
      JOB_TAGS="${i#*=}"
      ;;

    --mozart-ip=*)
      MOZART_IP="${i#*=}"
      ;;
    *)
      # PASS
      ;;
  esac
done

# check if base script params unset or empty
if [[ -z "${RELEASE}" ]]; then
  echo Missing required arg \"--release\". See usage.
  exit 1
fi
if [[ -z "${JOB_TYPE}" ]]; then
  echo Missing required arg \"--job-type\". See usage.
  exit 1
fi
if [[ -z "${JOB_QUEUE}" ]]; then
  echo Missing required arg \"--job-queue\". See usage.
  exit 1
fi

if [[ -z "${JOB_NAME}" ]]; then
  echo Missing required arg \"--job-name\". See usage.
  exit 1
fi
if [[ -z "${JOB_TAGS}" ]]; then
  echo Missing required arg \"--job-tags\". See usage.
  exit 1
fi
if [[ -z "${MOZART_IP}" ]]; then
  echo Missing required arg \"--mozart-ip\". See usage.
  exit 1
fi

PARAM_SRC_BUCKET='opera-ecmwf'
PARAM_TARGET_BUCKET='opera-ancillaries'

# override param defaults
for i in "$@"; do
  case $i in
    --bucket=*)
      PARAM_SRC_BUCKET="${i#*=}"
      ;;
    --target-bucket=*)
      PARAM_TARGET_BUCKET="${i#*=}"
      ;;
    *)
      # PASS
      ;;
  esac
done

# check if preset params unset or empty
if [[ -z $PARAM_SRC_BUCKET ]]; then
  echo Missing required arg \"--bucket\". See usage.
  exit 1
fi
if [[ -z $PARAM_TARGET_BUCKET ]]; then
  echo Missing required arg \"--target-bucket\". See usage.
  exit 1
fi

# parse job args
# final for loop consumes all args
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
      # SKIP
      shift
      ;;
    --target-bucket=*)
      # SKIP
      shift
      ;;
    --release=*)
      # SKIP
      shift
      ;;
    --job-type=*)
      # SKIP
      shift
      ;;
    --job-queue=*)
      # SKIP
      shift
      ;;
    --job-name=*)
      # SKIP
      shift
      ;;
    --job-tags=*)
      # SKIP
      shift
      ;;
    --mozart-ip=*)
      # SKIP
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
echo "$@"

# check if script params unset or empty
if [[ -z $IN_START_DATE ]]; then
  echo Missing required arg \"--start-date\". See usage.
  exit 1
fi
if [[ -z $IN_END_DATE ]]; then
  echo Missing required arg \"--end-date\". See usage.
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


echo "Enter the username you use to access the HySDS UI (Afterwards, you will be prompted for your password upon job submission):"
read username
if [[ -z $username ]]; then
  echo Missing username. Exiting.
  exit 1
fi

echo "Enter password:"
read -r -s password
if [[ -z $password ]]; then
  echo Missing password. Exiting.
  exit 1
fi

# PARSE RANGE

# parse dates
IN_START_DATE=$(echo ${IN_START_DATE} | tr '-' ' ' | xargs printf -- '%s')
IN_END_DATE=$(echo "${IN_END_DATE}" | tr '-' ' ' | xargs printf -- '%s')

IN_DATE=${IN_START_DATE}
while [ "${IN_DATE}" != ${IN_END_DATE} ]; do
  PARAM_S3_KEYS=$(aws s3api list-objects-v2 --bucket opera-ecmwf --prefix raw/"${IN_DATE}"/ --query 'Contents[*].Key' --output text --no-paginate | xargs echo)
  if [ 'None' == "${PARAM_S3_KEYS}" ]; then
    echo "No files found in s3 for ${IN_DATE}"
    exit 1
  fi

  echo curl --location 'https://'"${MOZART_IP}"'/mozart/api/v0.1/job/submit?enable_dedup=false' \
  --insecure \
  -u "${username}:${password}" \
  --form 'queue="'"${JOB_QUEUE}"'"' \
  --form 'priority="0"' \
  --form 'tags="[\"'"${JOB_TAGS}"'\"]"' \
  --form 'type="'"${JOB_TYPE}"':'"${RELEASE}"'"' \
  --form 'params="{\"bucket\":\"--bucket='"${PARAM_SRC_BUCKET}"'\",\"target_bucket\":\"--target-bucket='"${PARAM_TARGET_BUCKET}"'\",\"s3_keys\":\"--s3-keys '"${PARAM_S3_KEYS}"'\"}"' \
  --form 'name="'"${JOB_NAME}"'"'

    IN_DATE=$(date -d "${IN_DATE} + 1 day"  +%Y%m%d)
done
