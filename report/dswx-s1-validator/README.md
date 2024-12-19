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

* Python 3.6 or higher with special library dependencies (see `pip` command below)
* Access to CMR API (i.e. network access to the internet)
* Availability of an MGRS Tile Collection Database file
  - NOTE: the script will not work without this database file. It is passed in via the argument `--db`. Consult [@riverma](https://github.com/riverma) for access to this file.

### Setup Instructions

1. Clone the repository to your local machine.
2. Install the required Python libraries: `pip install pandas tabulate tqdm sqlite3 requests python-cmr`.
3. Ensure you have internet access to the CMR API
   
### Run Instructions

1. Run the script using Python: `python dswx_s1_validator.py --start <start_date> --end <end_date> --db <database_path>`.
2. Optionally, use the `--file` argument to specify a file with granule IDs.
3. Optionally, use the `--threshold` argument to a threshold percentage to filter MGRS Tile Set coverages by or use the `--matching_burst_count` to specify the minimum number of bursts to expect a match for for filtering. If both are provided, `--threshold` is used and `--matching_burst_count` is ignored. 
4. Optionally, use the `--timestamp` argument to specify the type of timestamp to query CMR with. Example values: `TEMPORAL|PRODUCTION|REVISION|CREATED`. Default value is `TEMPORAL`. See [CMR documentation](https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html) for details. 
5. Optionally, use the `--endpoint_rtc` argument to specify the CMR endpoint venue for RTC granules. Accepted values are `OPS|UAT`, with `OPS` set as the default value.
6. Optionally, use the `--endpoint_dswx_s1` argument to specify the CMR endpoint venue for DSWx-S1 granules. Accepted values are `OPS|UAT`, with `OPS` set as the default value.
7. Optionally, use the `--verbose` argument to get detailed information like a list of matching bursts and granule IDs
8. Optionally, use the `--validate` argument to check if expected DSWx-S1 products (tiles) exist for relevant RTC input bursts 
 
### Usage Examples

* Retrieve a list of MGRS Tile Set IDs for the RTC burst processing a given time range on CMR, and filter the results to show only MGRS Tile Sets that had coverage of greater than or equal to 50%.

  ```
  $ python dswx_s1_validator.py --start "2023-12-05T01:00:00" --end "2023-12-05T03:59:59" --db MGRS_tile_collection_v0.3.sqlite --threshold 50
  Total granules: 2316
  Querying CMR for time range 2023-12-05T01:00:00 to 2023-12-05T03:59:59.

  Fetching granules: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 2316/2316 [00:01<00:00, 1266.29it/s]

  Granule fetching complete.

  Calculating coverage: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████| 14687/14687 [00:08<00:00, 1834.83it/s]

  MGRS Set IDs covered: 34
  MGRS Set ID      Coverage Percentage    Total RTC Burst IDs Count    Covered RTC Burst ID Count
  MS_166_30                      70                              40                            28
  MS_166_31                     100                              40                            40
  MS_166_32                     100                              40                            40
  MS_166_33                     100                              40                            40
  MS_166_34                     100                              40                            40
  MS_166_35                     100                              40                            40
  MS_166_36                     100                              41                            41
  MS_166_37                     100                              41                            41
  MS_166_38                     100                              42                            42
  MS_166_39                     100                              42                            42
  MS_166_69                      90.24                           41                            37
  MS_166_70                     100                              40                            40
  MS_166_71                     100                              40                            40
  MS_166_72                     100                              39                            39
  MS_166_73                     100                              40                            40
  MS_166_74                     100                              40                            40
  MS_166_75                     100                              40                            40
  MS_166_76                     100                              41                            41
  MS_166_77                     100                              41                            41
  MS_166_78                     100                              41                            41
  MS_166_79                     100                              40                            40
  MS_166_80                     100                              40                            40
  MS_166_81                      50                              40                            20
  MS_166_88                     100                              40                            40
  MS_166_89                      90                              40                            36
  MS_167_68                     100                              41                            41
  MS_167_69                     100                              40                            40
  MS_167_70                     100                              40                            40
  MS_167_71                     100                              39                            39
  MS_167_72                     100                              40                            40
  MS_167_73                     100                              40                            40
  MS_167_74                     100                              39                            39
  MS_167_75                     100                              41                            41
  MS_167_77                     100                              41                            41
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
  $ python3 dswx_s1_validator.py --file granules.txt --db MGRS_tile_collection_v0.3.sqlite --threshold 50
  Calculating coverage: 100%|█████████████████████████████████| 12585/12585 [00:00<00:00, 41014.33it/s]

  MGRS Set IDs covered: 1
  MGRS Set ID      Coverage Percentage    Total RTC Burst IDs Count    Covered RTC Burst ID Count
  MS_80_59                       63.41                           41                            26
  ```

* Validate whether DSWx-S1 processing has kept up with input RTC processing (success condition)

  ```
  $ python dswx_s1_validator.py --endpoint_dswx_s1 UAT --start "2024-05-12T08:00:00" --end "2024-05-12T08:59:00" --db MGRS_tile_collection_v0.3.sqlite --threshold 99 --validate
  Total granules: 114
  Querying CMR for time range 2024-05-12T08:00:00 to 2024-05-12T08:59:00.

  Fetching granules: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 114/114 [00:00<00:00, 146.10it/s]

  Granule fetching complete.

  Calculating coverage: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████| 14687/14687 [00:00<00:00, 15351.02it/s]


  Expected DSWx-S1 product sensing time range: 2024-05-12 08:30:14 to 2024-05-12 08:31:23

  ✅ Validation successful: All DSWx-S1 products available at CMR for corresponding matched input RTC bursts within sensing time range.

  MGRS Set ID      Coverage Percentage    Total RTC Burst IDs Count    Covered RTC Burst ID Count    Unprocessed RTC Native IDs Count
  MS_38_176                        100                           40                            40                                   0
  MS_38_177                        100                           41                            41                                   0
  ```

* Validate whether DSWx-S1 processing has kept up with input RTC processing (failure condition)

  ```
  $ python dswx_s1_validator.py --endpoint_dswx_s1 UAT --start "2024-05-12T04:10:00" --end "2024-05-12T05:10:00" --db MGRS_tile_collection_v0.3.sqlite --threshold 99 --validate

  Total granules: 894
  Querying CMR for time range 2024-05-12T04:10:00 to 2024-05-12T05:10:00.

  Fetching granules: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 894/894 [00:01<00:00, 460.74it/s]

  Granule fetching complete.

  Calculating coverage: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████| 14687/14687 [00:06<00:00, 2301.05it/s]


  Expected DSWx-S1 product sensing time range: 2024-05-12 04:10:50 to 2024-05-12 04:28:28

  ❌ Validation failed: Mismatch in DSWx-S1 products available at CMR for corresponding matched input RTC bursts within sensing time range.

  Incomplete MGRS Set IDs: 1
  MGRS Set ID      Coverage Percentage    Total RTC Burst IDs Count    Covered RTC Burst ID Count    Unprocessed RTC Native IDs Count
  MS_36_64                         100                           41                            41                                  22
  ``` 

  Note that the above validation failure can also be validated using cURL, to check individually if the expected tiles have been processed and delivered to the DAACs. In the below example, we verify whether the missing tile `T36UWG` exists at CMR in a very wide time window range, and we see it does not return a document match.  

  ```
  curl "https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json?short_name=OPERA_L3_DSWX-S1_PROVISIONAL_V0&temporal[]=2024-05-10T02:10:00.000Z,2024-05-14T09:55:13.000Z&page_size=1000&provider=POCLOUD" | jq '.items[] | select(.umm.AdditionalAttributes[] | .Name == "MGRS_TILE_ID" and .Values[] == "T36UWG")'
  ```

## Frequently Asked Questions (FAQ)

- **Q: One or more of the Python dependencies installed via `pip install` are not available on my machine / environment**
  
  A: Consider [building the dependency from source](https://devguide.python.org/getting-started/setup-building/) on your machine. For example, a reported missing dependency on an Apple M2 MacBook with Python 3.9.6 is `sqllite3` and requires a build from source following [this guide](https://til.simonwillison.net/sqlite/build-specific-sqlite-pysqlite-macos).

- **Q: How do I get more detailed output like matching granule or burst IDs when running the script?**
  
  A: Run in *verbose mode* by adding the `--verbose` flag on the command-line.

## Support

Key points of contact are: [@riverma](https://github.com/riverma)
