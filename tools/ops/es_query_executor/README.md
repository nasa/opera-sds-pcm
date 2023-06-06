<!-- Header block for project -->
<hr>

<div align="center">

<h1 align="center">Elasticsearch Query Executor</h1>

</div>

<pre align="center">A script for executing version-controlled Elasticsearch queries.</pre>

<!-- Header block for project -->

This is a simple script meant to execute an Elasticsearch query specified in an external config file, log the query transaction, and print how many docs were affected by the query. It is flexible enough to work with any Elasticsearch instance, though the sample queries provided are intended for use with the [HySDS](https://github.com/hysds/) Elasticsearch environment. 

[![SLIM](https://img.shields.io/badge/Best%20Practices%20from-SLIM-blue)](https://nasa-ammos.github.io/slim/)

## Features

* Contains static set of pre-configured Elasticsearch queries for the HySDS system
* Executes static queries against a specified Elasticsearch instance, either in "count" mode (find docs) or "delete" mode (delete docs)
* Logs the transaction to a log file (daily logs up to max-14 days rolling)
* Prints the number of documents affected by the query
  
This script includes a number of pre-configured Elasticsearch queries described below. Note the folder structure: `queries/[index_name]/*.json`. **This folder structure is important: the `index_name` is extracted from the folder in the path to the query JSON file.** This design prevents mistakes of running a delete query against the wrong index.
- [queries/job_status-current/jobs_nominal_old.json](queries/job_status-current/jobs_nominal_old.json) - (Mozart) if status in {completed, revoked, deduped} AND creation timestamp > 14 days old 
- [queries/job_status-current/jobs_failed_old.json](queries/job_status-current/jobs_failed_old.json) - (Mozart) if status = failed AND creation timestamp > 30 days old
- [queries/task_status-current/resources_old.json](queries/task_status-current/resources_old.json) - (Mozart) if resource in {task, event, worker} AND creation timestamp > 7 days old 
- [queries/grq_v1.0_l3_dswx_hls/old.json](queries/grq_v1.0_l3_dswx_hls/old.json) - (GRQ) if dataset in {L2_HLS_L30, L2_HLS_S30} AND creation timestamp > 14 days old 
- [queries/grq_v2.0_l2_hls_l30/old.json](queries/grq_v2.0_l2_hls_l30/old.json) - (GRQ) if dataset L2_HLS_L30 AND creation timestamp > 14 days old 
- [queries/grq_v2.0_l2_hls_s30/old.json](queries/grq_v2.0_l2_hls_s30/old.json) - (GRQ) if dataset L2_HLS_S30 AND creation timestamp > 14 days old 
- [queries/grq_v1.1.10_triaged_job/old.json](queries/grq_v1.1.10_triaged_job/old.json) - (GRQ) triaged jobs dataset AND creation timestamp > 30 days old

## Contents

- [Features](#features)
- [Contents](#contents)
- [Quick Start](#quick-start)
  - [Requirements](#requirements)
  - [Setup Instructions](#setup-instructions)
  - [Run Instructions](#run-instructions)
  - [Usage Examples](#usage-examples)
    - [Crontab](#crontab)
- [Frequently Asked Questions (FAQ)](#frequently-asked-questions-faq)
- [License](#license)
- [Support](#support)

## Quick Start

This guide provides a quick way to get started with our project.

### Requirements

* Python 3.9+
* Elasticsearch Python SDK 
* Elasticsearch 7+
  
### Setup Instructions

1. Ensure you have Python 2.7+ installed on your system
2. Install the Elasticsearch Python SDK. See: https://elasticsearch-py.readthedocs.io/en/7.x/ 
3. Ensure your machine has network access to a given Elasticsearch instance you want to execute queries against, without authentication

### Run Instructions

```
python es_query_executor.py --host [HOST] --index [ES_INDEX] --query_file [PATH_TO_QUERY_FILE] --action [count|delete]
```

### Usage Examples

* Search for old Mozart jobs considered nominal, print the number of docs found and log the results to a log file
  ```
  python es_query_executor.py --host http://localhost:9200 --query_file queries/job_status-current/jobs_nominal_old.json --log_file es_query_executor.log --action count
  ```
* Delete / clean-up old Mozart jobs considered nominal, print the number of docs deleted and log the results of the transaction to a log file
  ```
  python es_query_executor.py --host http://localhost:9200 --query_file queries/job_status-current/jobs_nominal_old.json --log_file es_query_executor.log --action delete
  ```  

#### Crontab

The below crontab examples are intended to be utilized to automatically run the sample query files under the `queries` folder at a sample *every night at midnight*. 

To ensure not errors when running the crontab, you may need to customize:
- The `python` binary to point to your custom Python
- The full path to the `es_query_executor.py` script
- The `--host` parameter to reflect your intended ES host
- The `--log_file` parameter to point to a log file to write to (folder needs to exist)

```
0 0 * * * python es_query_executor.py --host http://localhost:9200 --query_file queries/grq_v1.0_l3_dswx_hls/old.json --log_file es_query_executor.log --action delete
0 0 * * * python es_query_executor.py --host http://localhost:9200 --query_file queries/grq_v1.1.10_triaged_job/old.json --log_file es_query_executor.log --action delete
0 0 * * * python es_query_executor.py --host http://localhost:9200 --query_file queries/grq_v2.0_l2_hls_l30/old.json --log_file es_query_executor.log --action delete
0 0 * * * python es_query_executor.py --host http://localhost:9200 --query_file queries/grq_v2.0_l2_hls_s30/old.json --log_file es_query_executor.log --action delete
0 0 * * * python es_query_executor.py --host http://localhost:9200 --query_file queries/job_status-current/jobs_failed_old.json --log_file es_query_executor.log --action delete
0 0 * * * python es_query_executor.py --host http://localhost:9200 --query_file queries/job_status-current/jobs_nominal_old.json --log_file es_query_executor.log --action delete
0 0 * * * python es_query_executor.py --host http://localhost:9200 --query_file queries/task_status-current/resources_old.json --log_file es_query_executor.log --action delete
```

## Frequently Asked Questions (FAQ)

Q: How do I change the logging level?
A: Open the script `es_query_executor.py` and find the line marked `logging_level = logging.INFO` and change it to one of the values specified in https://docs.python.org/3/library/logging.html#logging-levels

Q: How do I know which index a query file is going to be run against?
A: The archive query files have a folder structure that embeds the index name within their paths, i.e. `queries/[index_name]/*.json`.

Q: How do I add a new query file?
A: Create a new Elasticsearch-compliant JSON query body, place it in a file and put that file within this repository's folder for queries - paying close attention to the folder structure as it is relevant for your query. Example: `queries/[index_name]/my_new_query.json`.

## License

See our: [LICENSE](LICENSE)

## Support

Key points of contact are: [@riverma](https://github.com/riverma) and [@niarenaw](https://github.com/niarenaw)
