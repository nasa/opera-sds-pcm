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
2. Install the required Python libraries: `pip install pandas tabulate tqdm sqlite3 requests python_cmr`.
3. Ensure you have internet access to the CMR API
   
### Run Instructions

1. Run the script using Python: `python dswx_s1_validator.py --start <start_date> --end <end_date> --db <database_path>`.
2. Optionally, use the `--file` argument to specify a file with granule IDs.
3. Optionally, use the `--threshold` argument to a threshold percentage to filter MGRS Tile Set coverages by or use the `--matching_burst_count` to specify the minimum number of bursts to expect a match for for filtering. If both are provided, `--threshold` is used and `--matching_burst_count` is ignored. 
4. Optionally, use the `--timestamp` argument to specify the type of timestamp to query CMR with. Example values: `TEMPORAL|PRODUCTION|REVISION|CREATED`. Default value is `TEMPORAL`. See [CMR documentation](https://cmr.earthdata.nasa.gov/search/site/docs/search/api.html) for details. 
5. Optionally, use the `--endpoint` argument to specify the CMR endpoint venue. Accepted values are `OPS|UAT`, with `OPS` set as the default value.
6. Optionally, use the `--verbose` argument to get detailed information like a list of matching bursts and granule IDs
7. Optionally, use the `--validate` argument to check if expected DSWx-S1 products (tiles) exist for relevant RTC input bursts 
 
### Usage Examples

* Retrieve a list of MGRS Tile Set IDs for the RTC burst processing a given time range on CMR, and filter the results to show only MGRS Tile Sets that had coverage of greater than or equal to 50%.

```
$ python dswx_s1_validator.py --start "2023-12-05T01:00:00" --end "2023-12-05T03:59:59" --db MGRS_tile_collection_v0.2.sqlite --threshold 50
Querying CMR for time range 2023-12-05T01:00:00 to 2023-12-05T03:59:59.
Querying CMR for 2316 granules.

Fetching granules: 100%|███████████████████████████████████████| 2316/2316 [00:01<00:00, 1660.76it/s]

Granule fetching complete.

Calculating coverage: 100%|██████████████████████████████████| 12585/12585 [00:09<00:00, 1359.21it/s]

MGRS Set IDs covered: 34
MGRS Set ID      Coverage Percentage    Matching Burst Count    Total Burst Count
MS_166_30                      70                         28                   40
MS_166_31                     100                         40                   40
MS_166_32                     100                         40                   40
MS_166_33                     100                         40                   40
MS_166_34                     100                         40                   40
MS_166_35                     100                         40                   40
MS_166_36                     100                         41                   41
MS_166_37                     100                         41                   41
MS_166_38                     100                         42                   42
MS_166_39                     100                         42                   42
MS_166_69                      90.24                      37                   41
MS_166_70                     100                         40                   40
MS_166_71                     100                         40                   40
MS_166_72                     100                         39                   39
MS_166_73                     100                         40                   40
MS_166_74                     100                         40                   40
MS_166_75                     100                         40                   40
MS_166_76                     100                         41                   41
MS_166_77                     100                         41                   41
MS_166_78                     100                         41                   41
MS_166_79                     100                         40                   40
MS_166_80                     100                         40                   40
MS_166_81                      50                         20                   40
MS_166_88                     100                         40                   40
MS_166_89                      90                         36                   40
MS_167_68                     100                         41                   41
MS_167_69                     100                         40                   40
MS_167_70                     100                         40                   40
MS_167_71                     100                         39                   39
MS_167_72                     100                         40                   40
MS_167_73                     100                         40                   40
MS_167_74                     100                         39                   39
MS_167_75                     100                         41                   41
MS_167_77                     100                         41                   41
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
  $ python3 dswx_s1_validator.py --file granules.txt --db MGRS_tile_collection_v0.2.sqlite --threshold 50
  Calculating coverage: 100%|█████████████████████████████████| 12585/12585 [00:00<00:00, 41014.33it/s]

  MGRS Set IDs covered: 1
  MGRS Set ID      Coverage Percentage    Matching Burst Count    Total Burst Count
  MS_80_59                       63.41                      26                   41
  ```

* Validate whether DSWx-S1 processing has kept up with input RTC processing (success condition)

  ```
  $ python dswx_s1_validator.py --start "2024-05-12T08:00:00" --end "2024-05-12T08:59:00" --db MGRS_tile_collection_v0.3.sqlite --threshold 99 --validate
  Total granules: 114
  Querying CMR for time range 2024-05-12T08:00:00 to 2024-05-12T08:59:00.

  Fetching granules: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 114/114 [00:01<00:00, 69.54it/s]

  Granule fetching complete.

  Calculating coverage: 100%|███████████████████████████████████████████████████████████████████████████████████████████████████████████| 14687/14687 [00:00<00:00, 15863.74it/s]

  MGRS Set IDs covered: 2
  MGRS Set ID      Coverage Percentage    Matching Burst Count    Total Burst Count  MGRS Tiles
  MS_38_176                        100                      40                   40  55LEL, 55LFL, 55LGL, 55LHL, 55MDM, 55MEM, 55MFM, 55MGM
  MS_38_177                        100                      41                   41  55MDN, 55MDP, 55MEN, 55MEP, 55MFN, 55MFP, 55MGN, 55MGP

  Expected DSWx-S1 product sensing time range: 2024-05-12 08:30:14 to 2024-05-12 08:31:23

  ✅ Validation successful: All DSWx-S1 tiles available at CMR for corresponding matched input RTC bursts within sensing time range.
  ```

* Validate whether DSWx-S1 processing has kept up with input RTC processing (failure condition)

  ```
  $ python dswx_s1_validator.py --start "2024-05-12T04:10:00" --end "2024-05-12T05:10:00" --db MGRS_tile_collection_v0.3.sqlite --threshold 99 --validate

  Total granules: 894
  Querying CMR for time range 2024-05-12T04:10:00 to 2024-05-12T05:10:00.

  Fetching granules: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 894/894 [00:01<00:00, 463.16it/s]

  Granule fetching complete.

  Calculating coverage: 100%|████████████████████████████████████████████████████████████████████████████████████████████████████████████| 14687/14687 [00:06<00:00, 2315.51it/s]

  MGRS Set IDs covered: 24
  MGRS Set ID      Coverage Percentage    Matching Burst Count    Total Burst Count  MGRS Tiles
  MS_36_64                         100                      41                   41  35UPA, 35UPB, 36UUF, 36UUG, 36UVF, 36UVG, 36UWF, 36UWG
  MS_36_65                         100                      41                   41  35UPU, 35UPV, 35UQU, 35UQV, 36UUD, 36UUE, 36UVD, 36UVE, 36UWE
  MS_36_66                         100                      40                   40  35UNS, 35UPS, 35UPT, 35UQS, 35UQT, 36UUB, 36UUC, 36UVB, 36UVC
  MS_36_67                         100                      40                   40  35UNQ, 35UNR, 35UPQ, 35UPR, 35UQQ, 35UQR, 36UUA, 36UUV, 36UVA, 36UVV
  MS_36_68                         100                      39                   39  35TNN, 35TPN, 35TQN, 35UNP, 35UPP, 35UQP, 36TUT, 36UUU
  MS_36_69                         100                      40                   40  35TNL, 35TNM, 35TPL, 35TPM, 35TQL, 35TQM, 36TUR, 36TUS
  MS_36_70                         100                      40                   40  35TMJ, 35TMK, 35TNJ, 35TNK, 35TPJ, 35TPK, 35TQJ, 35TQK
  MS_36_71                         100                      40                   40  35TMG, 35TMH, 35TNG, 35TNH, 35TPG, 35TPH, 35TQG, 35TQH, 36TTM
  MS_36_72                         100                      41                   41  35TME, 35TMF, 35TNE, 35TNF, 35TPE, 35TPF, 35TQF, 36TTL
  MS_36_73                         100                      41                   41  35SLC, 35SLD, 35SMC, 35SMD, 35SNC, 35SND, 35SPC, 35SPD
  MS_36_74                         100                      40                   40  35SLA, 35SLB, 35SMA, 35SMB, 35SNA, 35SNB, 35SPA, 35SPB
  MS_36_75                         100                      40                   40  35SKU, 35SLU, 35SLV, 35SMU, 35SMV, 35SNU, 35SNV, 35SPV
  MS_36_77                         100                      40                   40  34RGV, 35RKQ, 35RLQ, 35RMQ, 35RNQ, 35SKR, 35SLR, 35SMR, 35SNR
  MS_36_88                         100                      40                   40  34PDT, 34PDU, 34PET, 34PEU, 34PFT, 34PFU, 34PGT, 34PGU
  MS_36_89                         100                      40                   40  34PCR, 34PCS, 34PDR, 34PDS, 34PER, 34PES, 34PFR, 34PFS
  MS_36_90                         100                      40                   40  34NCP, 34NDP, 34NEP, 34NFP, 34PCQ, 34PDQ, 34PEQ, 34PFQ
  MS_36_91                         100                      40                   40  34NBM, 34NCM, 34NCN, 34NDM, 34NDN, 34NEM, 34NEN, 34NFN
  MS_36_92                         100                      40                   40  34NBK, 34NBL, 34NCK, 34NCL, 34NDK, 34NDL, 34NEK, 34NEL
  MS_36_93                         100                      41                   41  33NZC, 33NZD, 34NBH, 34NBJ, 34NCH, 34NCJ, 34NDH, 34NDJ, 34NEH, 34NEJ
  MS_36_94                         100                      41                   41  33NZA, 33NZB, 34NBF, 34NBG, 34NCF, 34NCG, 34NDF, 34NDG
  MS_36_95                         100                      40                   40  33MYU, 33MZU, 33MZV, 34MBD, 34MBE, 34MCD, 34MCE, 34MDD, 34MDE
  MS_36_96                         100                      41                   41  33MYS, 33MYT, 33MZS, 33MZT, 34MBB, 34MBC, 34MCB, 34MCC
  MS_36_97                         100                      40                   40  33MYQ, 33MYR, 33MZQ, 33MZR, 34MBA, 34MBV, 34MCA, 34MCV
  MS_36_98                         100                      41                   41  33MXN, 33MXP, 33MYN, 33MYP, 33MZN, 33MZP, 34MBT, 34MBU, 34MCU

  Expected DSWx-S1 product sensing time range: 2024-05-12 04:10:50 to 2024-05-12 04:28:28

  ❌ Validation failed: Mismatch in DSWx-S1 tiles available at CMR for corresponding matched input RTC bursts within sensing time range.

  Expected(202): {'T35UPB', 'T34NDM', 'T35RMQ', 'T33MZS', 'T35SMC', 'T35UQP', 'T35SMR', 'T34NEJ', 'T35TMK', 'T35TQK', 'T35TPH', 'T34MBD', 'T35SNV', 'T34PCS', 'T34MBU', 'T36TTL', 'T35TPJ', 'T36UWF', 'T33NZC', 'T35SLC', 'T36UVV', 'T35UNQ', 'T34MDD', 'T34NDF', 'T34NBM', 'T34NCK', 'T34NDK', 'T36TUS', 'T35SKR', 'T34MCC', 'T36UUE', 'T34PER', 'T35SMD', 'T34NEN', 'T36UUA', 'T33MZQ', 'T34NEH', 'T34PFT', 'T35TNE', 'T35UNR', 'T35SPC', 'T34PEU', 'T34PGU', 'T34PDS', 'T34NEK', 'T35TQH', 'T35UQT', 'T33MZU', 'T35SLD', 'T33NZB', 'T35SMA', 'T34NBH', 'T34NFN', 'T35TQG', 'T35UPU', 'T35SNU', 'T33MYU', 'T33MYQ', 'T35SLB', 'T35TQN', 'T34NCH', 'T33MZR', 'T35UNP', 'T36UVE', 'T34MBC', 'T34PFU', 'T33MZN', 'T35TNG', 'T35SPD', 'T34NCF', 'T35SMU', 'T35SLA', 'T34NEM', 'T35TMG', 'T34MCA', 'T35TPF', 'T34NBL', 'T34PCQ', 'T34NDP', 'T35RLQ', 'T33MZT', 'T35UQV', 'T35SLR', 'T34MBE', 'T34MDE', 'T35SNA', 'T35TMJ', 'T34NCP', 'T34NDN', 'T34NCG', 'T35UQQ', 'T35TNJ', 'T34NBJ', 'T35SNC', 'T34PDQ', 'T35UPR', 'T35SNB', 'T34MBB', 'T34PFR', 'T34NEL', 'T35TNN', 'T34RGV', 'T34NBG', 'T33MXP', 'T36TTM', 'T35RKQ', 'T36UVG', 'T36UVD', 'T35TQM', 'T35SND', 'T34PDU', 'T36UVB', 'T35UQU', 'T36UUV', 'T35TNH', 'T36UUF', 'T34MCB', 'T33MYP', 'T36UUG', 'T35TPN', 'T33MZP', 'T35UPP', 'T34MBT', 'T35UQS', 'T34PES', 'T35SKU', 'T34NCM', 'T36UUD', 'T33MYT', 'T36UUU', 'T34PEQ', 'T35TQL', 'T35TPG', 'T34PDT', 'T34MCD', 'T35SMB', 'T36UWE', 'T35TNL', 'T36UVA', 'T34NDH', 'T36UVF', 'T33MYN', 'T36TUR', 'T35SPA', 'T35UPQ', 'T35TQF', 'T34NDL', 'T33MYS', 'T35UPS', 'T35SMV', 'T34PDR', 'T34NCJ', 'T35SLU', 'T34PET', 'T35RNQ', 'T34PCR', 'T34PFS', 'T33NZD', 'T35SPV', 'T34MCU', 'T35TPE', 'T35UPA', 'T35TMH', 'T34PGT', 'T34NFP', 'T34MCV', 'T35TNF', 'T34MBV', 'T36UVC', 'T34MCE', 'T35UPV', 'T35TME', 'T35TPM', 'T36UWG', 'T34PFQ', 'T34NCN', 'T34NDJ', 'T33NZA', 'T33MZV', 'T34MBA', 'T35SLV', 'T36TUT', 'T35SNR', 'T36UUB', 'T34NBF', 'T35UPT', 'T33MXN', 'T34NBK', 'T34NEP', 'T35TQJ', 'T35TNM', 'T35UQR', 'T35TNK', 'T35SPB', 'T34NCL', 'T35TMF', 'T36UUC', 'T34NDG', 'T33MYR', 'T35UNS', 'T35TPK', 'T35TPL'}

  Received(200): {'T35UPB', 'T34NDM', 'T35RMQ', 'T33MZS', 'T35SMC', 'T35UQP', 'T35SMR', 'T34NEJ', 'T35TMK', 'T35TQK', 'T35TPH', 'T34MBD', 'T35SNV', 'T34PCS', 'T34MBU', 'T36TTL', 'T35TPJ', 'T36UWF', 'T33NZC', 'T35SLC', 'T36UVV', 'T35UNQ', 'T34MDD', 'T34NDF', 'T34NBM', 'T34NCK', 'T34NDK', 'T36TUS', 'T35SKR', 'T34MCC', 'T36UUE', 'T34PER', 'T35SMD', 'T34NEN', 'T36UUA', 'T33MZQ', 'T34NEH', 'T34PFT', 'T35TNE', 'T35UNR', 'T35SPC', 'T34PGU', 'T34PEU', 'T34PDS', 'T34NEK', 'T35TQH', 'T35UQT', 'T33MZU', 'T35SLD', 'T33NZB', 'T35SMA', 'T34NBH', 'T34NFN', 'T35TQG', 'T35UPU', 'T35SNU', 'T33MYU', 'T33MYQ', 'T35SLB', 'T35TQN', 'T34NCH', 'T33MZR', 'T35UNP', 'T36UVE', 'T34MBC', 'T34PFU', 'T33MZN', 'T35TNG', 'T35SPD', 'T34NCF', 'T35SMU', 'T35SLA', 'T34NEM', 'T35TMG', 'T34MCA', 'T35TPF', 'T34NBL', 'T34PCQ', 'T34NDP', 'T35RLQ', 'T33MZT', 'T35UQV', 'T35SLR', 'T34MBE', 'T34MDE', 'T35SNA', 'T35TMJ', 'T34NCP', 'T34NDN', 'T34NCG', 'T35UQQ', 'T35TNJ', 'T34NBJ', 'T35SNC', 'T34PDQ', 'T35UPR', 'T35SNB', 'T34MBB', 'T34PFR', 'T34NEL', 'T35TNN', 'T34RGV', 'T34NBG', 'T33MXP', 'T36TTM', 'T35RKQ', 'T35SND', 'T36UVD', 'T35TQM', 'T34PDU', 'T36UVB', 'T35UQU', 'T36UUV', 'T35TNH', 'T36UUF', 'T34MCB', 'T33MYP', 'T36UUG', 'T35TPN', 'T33MZP', 'T35UPP', 'T34MBT', 'T35UQS', 'T34PES', 'T35SKU', 'T34NCM', 'T36UUD', 'T33MYT', 'T36UUU', 'T34PEQ', 'T35TQL', 'T35TPG', 'T34PDT', 'T34MCD', 'T35SMB', 'T36UWE', 'T35TNL', 'T36UVA', 'T34NDH', 'T36UVF', 'T33MYN', 'T36TUR', 'T35SPA', 'T35UPQ', 'T35TQF', 'T34NDL', 'T33MYS', 'T35UPS', 'T35SMV', 'T34PDR', 'T34NCJ', 'T35SLU', 'T34PET', 'T35RNQ', 'T34PFS', 'T34PCR', 'T33NZD', 'T35SPV', 'T34MCU', 'T35TPE', 'T35UPA', 'T35TMH', 'T34PGT', 'T34NFP', 'T34MCV', 'T35TNF', 'T34MBV', 'T36UVC', 'T34MCE', 'T35UPV', 'T35TME', 'T35TPM', 'T33NZA', 'T34PFQ', 'T34NCN', 'T34NDJ', 'T33MZV', 'T34MBA', 'T35SLV', 'T36TUT', 'T35SNR', 'T36UUB', 'T34NBF', 'T35UPT', 'T33MXN', 'T34NBK', 'T34NEP', 'T35TQJ', 'T35TNM', 'T35UQR', 'T35TNK', 'T35SPB', 'T34NCL', 'T35TMF', 'T36UUC', 'T34NDG', 'T33MYR', 'T35UNS', 'T35TPK', 'T35TPL'}

  Missing tiles(2): {'T36UWG', 'T36UVG'}
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
