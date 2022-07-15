#!/usr/bin/env bash
#######################################################################
# This script is a wrapper for running opera smoke tests.
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
  $cmdname --mozart-ip=123.123.123.123 ...
Options:
      --mozart-ip                                The private IP address of the PCM's mozart instance.
      --grq-host                                 The hostname and port of GRQ.
      --cnm-r-topic-arn                          The CNM-R SNS topic ARN.
      --isl-bucket                               The ISL S3 bucket name.
      --rs-bucket                                The RS S3 bucket name.
      --L30-input-dir                            The expected path to the directory containing THE sample L30 data after download.
      --S30-input-dir                            The expected path to the directory containing THE sample S30 data after download.
      --L30-data-subscriber-query-lambda         The name of the AWS Lambda function that submits L30 query jobs.
      --S30-data-subscriber-query-lambda         The name of the AWS Lambda function that submits S30 query jobs.
      --artifactory-fn-api-key                   The Artifactory FN API Key. Used to download the sample data.
      --sample-data-artifactory-dir              The repository path to the "hls_l2.tar.gz" sample data's parent directory.
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
      ;;
    --mozart-ip=*)
      mozart_ip="${i#*=}"
      shift
      ;;
    --grq-host=*)
      grq_host="${i#*=}"
      shift
      ;;
    --cnm-r-topic-arn=*)
      cnm_r_topic_arn="${i#*=}"
      shift
      ;;
    --isl-bucket=*)
      isl_bucket="${i#*=}"
      shift
      ;;
    --rs-bucket=*)
      rs_bucket="${i#*=}"
      shift
      ;;
    --L30-input-dir=*)
      L30_input_dir="${i#*=}"
      shift
      ;;
    --S30-input-dir=*)
      S30_input_dir="${i#*=}"
      shift
      ;;
    --L30-data-subscriber-query-lambda=*)
      L30_data_subscriber_query_lambda="${i#*=}"
      shift
      ;;
    --S30-data-subscriber-query-lambda=*)
      S30_data_subscriber_query_lambda="${i#*=}"
      shift
      ;;
    --artifactory-fn-api-key=*)
      artifactory_fn_api_key="${i#*=}"
      shift
      ;;
    --sample-data-artifactory-dir=*)
      sample_data_artifactory_dir="${i#*=}"
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
# Main script body
######################################################################

export ES_HOST=${mozart_ip}
export ES_BASE_URL="https://${mozart_ip}/grq_es/"
export GRQ_HOST=${grq_host}
export GRQ_BASE_URL="https://${mozart_ip}/grq/api/v0.1"
export CNMR_TOPIC=${cnm_r_topic_arn}
export ISL_BUCKET=${isl_bucket}
export RS_BUCKET=${rs_bucket}
export L30_INPUT_DIR=${L30_input_dir}
export S30_INPUT_DIR=${S30_input_dir}
export L30_DATA_SUBSCRIBER_QUERY_LAMBDA=${L30_data_subscriber_query_lambda}
export S30_DATA_SUBSCRIBER_QUERY_LAMBDA=${S30_data_subscriber_query_lambda}

set -e
echo Running smoke tests

echo Downloading test data
if [[ ! -f hls_l2.tar.gz ]]; then
  curl -H "X-JFrog-Art-Api:${artifactory_fn_api_key}" -O ${sample_data_artifactory_dir}/hls_l2.tar.gz
else
  echo test data previously downloaded. Skipping re-download
fi
  rm -rf hls_l2
  mkdir -p hls_l2
  tar xfz hls_l2.tar.gz -C hls_l2

echo Executing integration tests. This can take at least 20 minutes...
python -m venv venv
source venv/bin/activate
pip install '.[integration]'

set +e
pytest --maxfail=1 integration/test_integration.py::test_l30
pytest --maxfail=1 integration/test_integration.py::test_s30
pytest --maxfail=1 integration/test_integration.py::test_subscriber_l30
pytest --maxfail=1 integration/test_integration.py::test_subscriber_s30
set -e