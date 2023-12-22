<!-- Header block for project -->
<hr>

<div align="center">

<h1 align="center">DSWx-S1 Validator</h1>
<!-- ☝️ Replace with your repo name ☝️ -->

</div>

<pre align="center">A tool for validating DSWx-S1 processing.</pre>
<!-- ☝️ Replace with a single sentence describing the purpose of your repo / proj ☝️ -->

<!-- Header block for project -->

[![SLIM](https://img.shields.io/badge/Best%20Practices%20from-SLIM-blue)](https://nasa-ammos.github.io/slim/)
<!-- ☝️ Add badges via: https://shields.io e.g. ![](https://img.shields.io/github/your_chosen_action/your_org/your_repo) ☝️ -->

Example:
```
$ python dswx-s1-validator.py --start "2023-12-05T01:00:00Z" --end "2023-12-05T03:59:59Z" --db MGRS_tile_collection_v0.2.sqlite
 --threshold 50
Retrieving 2316 granules from CMR.

Fetching granules: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2316/2316 [00:34<00:00, 67.10it/s]

Calculating coverage: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 31500/31500 [00:48<00:00, 643.51it/s]

+-------------+---------------------+
| MGRS Set ID | Coverage Percentage |
+-------------+---------------------+
|  MS_166_30  |        70.0         |
|  MS_166_31  |        100.0        |
|  MS_166_32  |        100.0        |
|  MS_166_33  |        100.0        |
|  MS_166_34  |        100.0        |
...
+-------------+---------------------+
```

This repository contains a Python script for querying the CMR (Common Metadata Repository) to help validate DSWx-S1 job triggering. The script takes in specific temporal ranges to identify RTC bursts from CMR (or a granule file list) and generates a list of MGRS Tile Set IDs that represent the types of DSWx-S1 jobs that should have been run.  It includes functionality for handling large datasets, implementing efficient data fetching with exponential backoff and jittering, and standard out tabular results.

## Features

* Parallelized CMR querying with customizable temporal range to obtain a list of RTC bursts.
* Optional file input that lists RTC granule IDs to extract burst information from
* Exponential backoff and jittering for CMR data fetching.
* Lookup of RTC burst IDs from a provided MGRS Tile Collection Database (SQLITE)
* Visual results display using Pandas and Tabulate.

## Contents

* [Quick Start](#quick-start)
* [Changelog](#changelog)
* [FAQ](#frequently-asked-questions-faq)
* [Contributing Guide](#contributing)
* [License](#license)
* [Support](#support)

## Quick Start

This guide provides a quick way to get started with the script. 

### Requirements

* Python 3.6 or higher
* Pandas and Tabulate libraries
* Access to CMR API
* Availability of an MGRS Tile Collection Database file and sqlite3 library

### Setup Instructions

1. Clone the repository to your local machine.
2. Install the required Python libraries: `pip install pandas tabulate tqdm sqlite3 requests`.
3. Ensure you have internet access to the CMR API
   
### Run Instructions

1. Run the script using Python: `python dswx-s1-validator.py --start <start_date> --end <end_date> --db <database_path>`.
2. Optionally, use the `--file` argument to specify a file with granule IDs.
3. Optionally, use the `--threshold` argument to a threshold percentage to filter MGRS Tile Set coverages by.
 
### Usage Examples

* Retrieve a list of MGRS Tile Set IDs for the RTC burst processing a given time range on CMR, and filter the results to show only MGRS Tile Sets that had coverage of greater than or equal to 50%.

```
$ python dswx-s1-validator.py --start "2023-12-05T01:00:00Z" --end "2023-12-05T03:59:59Z" --db MGRS_tile_collection_v0.2.sqlite
 --threshold 50
Retrieving 2316 granules from CMR.
Fetching granules: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2316/2316 [00:34<00:00, 67.10it/s]
Calculating coverage: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 31500/31500 [00:48<00:00, 643.51it/s]
+-------------+---------------------+
| MGRS Set ID | Coverage Percentage |
+-------------+---------------------+
|  MS_166_30  |        70.0         |
|  MS_166_31  |        100.0        |
|  MS_166_32  |        100.0        |
|  MS_166_33  |        100.0        |
|  MS_166_34  |        100.0        |
...
+-------------+---------------------+
```

* Process and display data related to granule IDs.

  ```
  $ cat granules.txt
  OPERA_L2_RTC-S1_T080-170376-IW2_20231211T043316Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170376-IW3_20231211T043317Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170377-IW1_20231211T043317Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170376-IW3_20231211T043317Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170377-IW1_20231211T043317Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170377-IW2_20231211T043318Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170377-IW2_20231211T043318Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170377-IW3_20231211T043319Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170377-IW3_20231211T043319Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170378-IW1_20231211T043320Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170378-IW1_20231211T043320Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170378-IW2_20231211T043321Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170378-IW2_20231211T043321Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170378-IW3_20231211T043322Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170378-IW3_20231211T043322Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170379-IW1_20231211T043323Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170379-IW1_20231211T043323Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170379-IW2_20231211T043324Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170379-IW2_20231211T043324Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170379-IW3_20231211T043325Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170379-IW3_20231211T043325Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170380-IW1_20231211T043326Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170380-IW1_20231211T043326Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170380-IW2_20231211T043327Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170380-IW2_20231211T043327Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170380-IW3_20231211T043328Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170381-IW1_20231211T043328Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170380-IW3_20231211T043328Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170381-IW1_20231211T043328Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170381-IW2_20231211T043329Z_20231211T215846Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170381-IW2_20231211T043329Z_20231211T224813Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170381-IW3_20231211T043330Z_20231211T215905Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170381-IW3_20231211T043330Z_20231211T224845Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170382-IW1_20231211T043331Z_20231211T215905Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170382-IW1_20231211T043331Z_20231211T224845Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170382-IW2_20231211T043332Z_20231211T215905Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170382-IW2_20231211T043332Z_20231211T224845Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170382-IW3_20231211T043333Z_20231211T215905Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170382-IW3_20231211T043333Z_20231211T224845Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170383-IW1_20231211T043334Z_20231211T215905Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170383-IW1_20231211T043334Z_20231211T224845Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170383-IW2_20231211T043335Z_20231211T215905Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170383-IW2_20231211T043335Z_20231211T224845Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170383-IW3_20231211T043336Z_20231211T215905Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170383-IW3_20231211T043336Z_20231211T224845Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170384-IW1_20231211T043337Z_20231211T215905Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170384-IW1_20231211T043337Z_20231211T224845Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170384-IW2_20231211T043338Z_20231211T215905Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170384-IW2_20231211T043338Z_20231211T224845Z_S1A_30_v1.0
  OPERA_L2_RTC-S1_T080-170384-IW3_20231211T043339Z_20231211T215905Z_S1A_30_v1.0
  ```

  ```
  $ python3 dswx-s1-validator.py --file granules.txt --db MGRS_tile_collection_v0.2.sqlite --threshold 50

  Calculating coverage: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 31500/31500 [00:01<00:00, 24706.38it/s]

  +-------------+---------------------+
  | MGRS Set ID | Coverage Percentage |
  +-------------+---------------------+
  |  MS_80_59   |        63.41        |
  +-------------+---------------------+
  ```

## Frequently Asked Questions (FAQ)

No questions yet. Propose a question to be added here by reaching out to our contributors! See support section below.

## Support

Key points of contact are: [@riverma](https://github.com/riverma)
