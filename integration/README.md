# INTEGRATION TESTS

Execute with `pytest integration/`

To run the tests in parallel (recommended), execute `pytest -n auto integration/`. 

>Note that live logs will not be available when executing in parallel.

See pytest-xdist documentation. (https://pytest-xdist.readthedocs.io/en/latest/)

## .ENV

The tests require a `.env` configuration file. Modify the sample below as needed.

```bash
# .env

# Elasticsearch connection
ES_HOST = https://123.123.123.123/grq_es/ # Elasticsearch URL
ES_USER = foo # Elasticsearch username
ES_PASSWORD = bar # Elasticsearch password

# CNM-R queue
CNMR_QUEUE = https://sqs.us-west-2.amazonaws.com/123456789012/opera-foo-1-daac-cnm-response

# S3 storage
ISL_BUCKET = opera-foo-isl-fwd-bar
RS_BUCKET = opera-foo-rs-fwd-bar

# Test input
L30_INPUT_DIR = ~/Downloads/test_datasets/l30_greenland/input_files_hls_v2.0
S30_INPUT_DIR = ~/Downloads/test_datasets/s30_louisiana/input_files_hls_v2.0

```