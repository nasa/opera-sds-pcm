# INTEGRATION TESTS

Install test dependencies with `pip install '.[integration]'`

Execute with `pytest integration/`. See prerequisites below.

To run the tests in parallel (recommended), execute `pytest -n auto integration/`. 

>Note that live logs will not be available when executing in parallel.

See pytest-xdist documentation. (https://pytest-xdist.readthedocs.io/en/latest/)

## .ENV

The tests require a `.env` configuration file. Modify the sample below as needed.

```bash
# .env

# Elasticsearch connection
ES_HOST = 123.123.123.123
ES_BASE_URL = https://${ES_HOST}/grq_es/ # Elasticsearch URL
ES_USER = foo # Elasticsearch username
ES_PASSWORD = bar # Elasticsearch password

# CNM-R
CNMR_TOPIC = arn:aws:sns:us-west-2:123456789012:opera-foo-1-daac-cnm-response
CNMR_QUEUE = https://sqs.us-west-2.amazonaws.com/123456789012/opera-foo-1-daac-cnm-response

# S3 storage
RS_BUCKET = opera-foo-rs-fwd-bar

# Data subscriber feature
DATA_SUBSCRIBER_QUERY_LAMBDA = opera-foo-data-subscriber-query-timer
L30_DATA_SUBSCRIBER_QUERY_LAMBDA = opera-foo-hlsl30-query-timer
S30_DATA_SUBSCRIBER_QUERY_LAMBDA = opera-foo-hlss30-query-timer
SLC_DATA_SUBSCRIBER_QUERY_LAMBDA = opera-foo-slcs1a-query-timer

CLEAR_DATA = true
```

### CLEAR_DATA

This env entry cleans the test system.
* It drops the relevant Elasticsearch indexes from GRQ and Mozart.
* It removes all products from rolling storage.

# Prerequisites

The system under test must meet the following conditions:

* The query timer EventBridge rules MUST be disabled. This is to prevent irrelevant jobs from running during testing.
* The query trigger lambdas MUST have SMOKE_RUN enabled. This is to prevent extra data from being downloaded from the input DAAC, LP.DAAC.
* OPERA SDS MUST have PGE_SIMULATION_MODE enabled. The timeouts the tests employ, though modifiable, are based on the mock PGE runtime for a particular sample data set.
