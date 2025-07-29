<!-- Header block for project -->
<hr>

<div align="center">

<h1 align="center">OPERA PCM Tools</h1>
<!-- ☝️ Replace with your repo name ☝️ -->

</div>

<pre align="center">Useful tools for OPERA PCM testing and operations</pre>
<!-- ☝️ Replace with a single sentence describing the purpose of your repo / proj ☝️ -->

<!-- Header block for project -->

[![SLIM](https://img.shields.io/badge/Best%20Practices%20from-SLIM-blue)](https://nasa-ammos.github.io/slim/)
<!-- ☝️ Add badges via: https://shields.io e.g. ![](https://img.shields.io/github/your_chosen_action/your_org/your_repo) ☝️ -->

This repository contains several Pyton scripts that are useful in various testing and operation scenarios.

## Contents

* [Quick Start](#quick-start)
* [Changelog](#changelog)
* [FAQ](#frequently-asked-questions-faq)
* [Contributing Guide](#contributing)
* [License](#license)
* [Support](#support)
* [disp_s1_burst_db_tool.py](#disp_s1_burst_db_tool)

## Quick Start
This guide provides a quick way to get started with the script. 

## disp_s1_burst_db_tool.py

OPERA PCM must be installed for this tool to work.

Tool to query and analyze the DISP S1 historical burst database. The burst database file must be in the same directory as this script

The database file can be found here
https://opera-ancillaries.s3.us-west-2.amazonaws.com/opera-disp-s1-consistent-burst-ids-with-datetimes.json

    python tools/disp_s1_burst_db_tool.py --help     
    usage: disp_s1_burst_db_tool.py [-h] {list,summary,native_id,frame,burst,time_range,unique_id,simulate} ...
    
    positional arguments:
    
      {list,summary,native_id,frame,burst,time_range,unique_id,simulate}
    
        list                List all frame numbers
        summary             List all frame numbers, number of bursts, and sensing datetimes
        native_id           Print information based on native_id
        frame               Print information based on frame
        burst               Print information based on burst id.
        time_range          Print frame that are available within a time range
        unique_id           Print information based on unique_id... unique_id is combination of burst patern and acquisition time (not yet implemented)
        simulate            Simulate a historical processing run (not yet implemented)
    
    optional arguments:
      -h, --help            show this help message and exit

#### Examples: 

    python tools/disp_s1_burst_db_tool.py frame 24733                                                                                    
    Frame number:  24733
    Burst ids (27): 
    {'T093-197865-IW2', ... 'T093-197861-IW3'}
    Sensing datetimes (196): 
    ['2016-05-10T01:35:47', ... '2023-08-14T01:36:34', '2023-08-26T01:36:35', '2023-09-07T01:36:35']
    Day indices:  [0, 12, 36, 60, 84, ...2616, 2628, 2640, 2652, 2664, 2676]
    
    python tools/disp_s1_burst_db_tool.py native_id OPERA_L2_CSLC-S1_T093-197858-IW3_20231118T013640Z_20231119T073552Z_S1A_VV_v1.0 --k=20
    Burst id:  T093-197858-IW3
    Acquisition datetime:  2023-11-18 01:36:40
    Acquisition cycles:  {24733: 2748}
    Frame ids:  [24733]
    K-cycle: 2 out of 20

    python tools/disp_s1_burst_db_tool.py time_range 2016-05-01T00:00:00 2016-07-01T00:00:00
    ...
    Frame number:  46540
	Sensing datetime:  2016-05-15T15:15:45
	Burst ids (27):
	 {'T174-372316-IW2', ...'T174-372314-IW1'}
    Frame number:  46540
	Sensing datetime:  2016-06-08T15:15:46
	Burst ids (27):
	 {'T174-372316-IW2', ... 'T174-372314-IW1'}

    python tools/disp_s1_burst_db_tool.py time_range 2016-05-01T00:00:00 2016-07-01T00:00:00 | grep Sensing | wc -l
    1669

    # Validate frame 832 in the database file against real-time CMR query data
    python tools/disp_s1_burst_db_tool.py validate 832 --print-each-result=true

    # Validate all frames in the database file using a cmr survey csv file. This then creates a pickle file of that csv file in the working directory.
    python tools/disp_s1_burst_db_tool.py validate all --cmr-survey-csv cmr_survey.csv.raw.2016-07-01_to_2024-09-04.csv

    # Validate using existing CMR survey csv pickle file. This is the fastest way to validate many bursts at once.
    # all or ALL means validate all frames
    python tools/disp_s1_burst_db_tool.py validate all --all-granules-pickle cmr_survey.csv.raw.2016-07-01_to_2024-09-04.csv.pickle 

## run_disp_s1_historical_processing.py

OPERA PCM must be installed for this tool to work.

This is a stand-alone CLI application that runs the DISP S1 historical processing.
It uses the batch_procs stored in GRQ ES ```batch_proc``` index which are managed by ```pcm_batch.py``` 
which is the same as how historical processing works for R2 CSLC products.
This application, however, replaces the batch processing lambda used for R2 historical processing.

Every sleep cycle, it will query the batch_proc index for processing. If the batch_proc is ready to be processed,
 it will submit the corresponding download job.

See OPERA wiki for detailed CONOPS and use-cases of DISP-S1 historical processing.

    ./run_disp_s1_historical_processing.py --help
    usage: run_disp_s1_historical_processing.py [-h] [--verbose VERBOSE] [--sleep-secs SLEEP_SECS] [--dry-run DRY_RUN]
    
    optional arguments:
      -h, --help            show this help message and exit
      --verbose VERBOSE     If true, print out verbose information, mainly cmr queries and k-cycle calculation.
      --sleep-secs SLEEP_SECS
                            Sleep between running for a cycle in seconds
      --dry-run DRY_RUN     If true, do not submit jobs

#### Examples:
    # Dry run will print out what it will do but does not submit a job. Useful for double checking the batch_proc
    # May be good to use a quicker sleep cycle to see the results faster
    ./run_disp_s1_historical_processing.py --sleep-secs 10 --dry-run

    # Run the historical processing with a 5 minute sleep cycle. Every sleep cycle will print out to the terminal
    # and that may become verbose. If you're actually running, you'd have to wait at least 1 hr between 
    # job submissions anyway so no point in running too often.
    ./run_disp_s1_historical_processing.py --sleep-secs 300


## download_from_daac.py

OPERA PCM must be installed for this tool to work.

This is a stand-alone CLI application that copies over DISP-S1 products from the ASF DAAC to an existing OPERA S3 bucket.
The use-case is to backfill expired products in the INT S3 bucket but there may be other use-cases. 
While this code isn't yet generalized to copy from any S3 resource to another or to a local dir or local dir to S3, etc, 
those features can be easily be implemented. The code is written in a bit more generalized way than necessary for this one known use-case.

It's highly recommended to run dry-run before actually performing the copies. There is no undo.

Copying one ~300MB .nc DISP-S1 file takes about 3 seconds. This tool currently copies only the .nc file - .xml, .png, etc are not copied. If those are needed, they can be added easily.

    ./download_from_daac.py --help
    usage: download_from_daac.py [-h] [--verbose] [--dry-run] [--daac-endpoint {UAT,OPS}] --s3-destination S3_DESTINATION --frame-list-file FRAME_LIST_FILE --product-version PRODUCT_VERSION
    
    optional arguments:
      -h, --help            show this help message and exit
      --verbose             If set, print out verbose information.
      --dry-run             If set, do not actually copy any files.
      --daac-endpoint {UAT,OPS}
                            CMR endpoint venue
      --s3-destination S3_DESTINATION
                            S3 bucket name and path to write files to. e.g s3://opera-int-rs-pop1/products/DISP_S1/
      --frame-list-file FRAME_LIST_FILE
                            Text file containing DISP-S1 frame numbers to copy over. They can be separated by commas and/or newlines. e.g '8882, 33039', etc
      --product-version PRODUCT_VERSION
                            Product version to search for. e.g. 0.8, 0.9, etc

#### Examples:
    # Dry run will print out what it will do but does not copy any files. Useful for double checking the frame list
    ./download_from_daac.py --daac-endpoint=UAT --s3-destination=s3://opera-int-rs-pop1/products/DISP_S1/ --frame-list-file=frame_list.txt --product-version=0.9 --dry-run > dry.log

    # Actually copy the files from the ASF UAT DAAC to the OPERA S3 bucket
    ./download_from_daac.py --daac-endpoint=UAT --s3-destination s3://opera-int-rs-pop1/products/DISP_S1/ --frame-list-file=frame_list.txt --product-version=0.9

    # Copy from OPS instead of UAT, product version 1.0 instead of 0.9
    ./download_from_daac.py --daac-endpoint=OPS --s3-destination s3://opera-int-rs-pop1/products/DISP_S1/ --frame-list-file=frame_list.txt --product-version=1.0

    # Example of a frame_list.txt file (you can mix comma-separated or new lines)
    8882, 33039, 33040
    33041, 33042
    33043

## dist_s1_burst_db_tool.py
OPERA PCM must be installed for this tool to work.

Tool to query and analyze the DIST-S1 burst database parquet file. The parquet file must be specified or can be automatically retrieved from the OPERA S3 bucket if running from a deployed cluster.

The database file can be found here: s3://opera-ancillaries/dist_s1/mgrs_burst_lookup_table-2025-02-19.parquet

    python tools/dist_s1_burst_db_tool.py --help
    usage: dist_s1_burst_db_tool.py [-h] [--verbose VERBOSE] [--db-file DB_FILE] [--no-geometry] {list,summary,native_id,tile_id,burst_id} ...
    
    positional arguments:
      {list,summary,native_id,tile_id,burst_id}
        list                List all tile numbers
        summary             List all tile numbers, number of products and their bursts
        native_id           Print information based on native_id
        tile_id             Print information based on tile
        burst_id            Print information based on burst id.
        trigger_granules    Run the list of granules through the triggering logic. Listed by increasing latest acquisition time.
    
    optional arguments:
      -h, --help            show this help message and exit
      --verbose VERBOSE     If true, print out verbose information.
      --db-file DB_FILE     Specify the DIST-S1 burst database parquet file on the local file system instead of using the standard one in S3 ancillary
      --no-geometry         Do not print burst geometry information. This speeds up this tool significantly.

#### Examples:

native_id example

    python tools/dist_s1_burst_db_tool.py --db-file=mgrs_burst_lookup_table-2025-02-19.parquet native_id OPERA_L2_RTC-S1_T064-135520-IW1_20250614T015042Z_20250622T152306Z_S1A_30_v1.0
    
    Burst id:  T064-135520-IW1
    Acquisition datetime:  20250614T015042Z
    Acquisition index:  348
    Product IDs:  {'11SMU_0', '11SLU_0', '11SLT_0', '11SMT_0'}
    --product-id-time:  11SMU_0,20250614T015042Z
    --product-id-time:  11SLU_0,20250614T015042Z
    --product-id-time:  11SLT_0,20250614T015042Z
    --product-id-time:  11SMT_0,20250614T015042Z
    Burst geometry minx, miny, maxx, maxy:  (-118.896936, 34.007766, -117.929224, 34.350227)

Trigger list of granules, non-complete tiles, and view all granules used for each triggered product

    python tools/dist_s1_burst_db_tool.py --db-file=mgrs_burst_lookup_table-2025-02-19.parquet --no-geometry trigger_granules cmr_survey_rtc_june1_to_june30_2025.csv --tile-to-trigger=35XMK

    product_id='35XMK_19_347' 2025-06-08 06:15:28 product.used_bursts=10 product.possible_bursts=14
    RTC granules: ['OPERA_L2_RTC-S1_T154-329223-IW1_20250608T061528Z', 'OPERA_L2_RTC-S1_T154-329222-IW1_20250608T061525Z', 'OPERA_L2_RTC-S1_T154-329221-IW2_20250608T061523Z', 'OPERA_L2_RTC-S1_T154-329221-IW1_20250608T061522Z', 'OPERA_L2_RTC-S1_T154-329220-IW2_20250608T061520Z', 'OPERA_L2_RTC-S1_T154-329220-IW1_20250608T061519Z', 'OPERA_L2_RTC-S1_T154-329219-IW2_20250608T061518Z', 'OPERA_L2_RTC-S1_T154-329219-IW1_20250608T061517Z', 'OPERA_L2_RTC-S1_T154-329218-IW2_20250608T061515Z', 'OPERA_L2_RTC-S1_T154-329218-IW1_20250608T061514Z']

    product_id='35XMK_22_347' 2025-06-09 15:05:30 product.used_bursts=15 product.possible_bursts=17
    RTC granules: ['OPERA_L2_RTC-S1_T174-372076-IW3_20250609T150530Z', 'OPERA_L2_RTC-S1_T174-372076-IW2_20250609T150529Z', 'OPERA_L2_RTC-S1_T174-372075-IW2_20250609T150526Z', 'OPERA_L2_RTC-S1_T174-372074-IW2_20250609T150523Z', 'OPERA_L2_RTC-S1_T174-372075-IW3_20250609T150527Z', 'OPERA_L2_RTC-S1_T174-372074-IW3_20250609T150524Z', 'OPERA_L2_RTC-S1_T174-372073-IW3_20250609T150522Z', 'OPERA_L2_RTC-S1_T174-372073-IW2_20250609T150521Z', 'OPERA_L2_RTC-S1_T174-372072-IW3_20250609T150519Z', 'OPERA_L2_RTC-S1_T174-372072-IW2_20250609T150518Z', 'OPERA_L2_RTC-S1_T174-372071-IW3_20250609T150516Z', 'OPERA_L2_RTC-S1_T174-372071-IW2_20250609T150515Z', 'OPERA_L2_RTC-S1_T174-372070-IW3_20250609T150513Z', 'OPERA_L2_RTC-S1_T174-372070-IW2_20250609T150512Z', 'OPERA_L2_RTC-S1_T174-372069-IW3_20250609T150511Z']

    product_id='35XMK_19_348' 2025-06-20 06:15:27 product.used_bursts=10 product.possible_bursts=14
    RTC granules: ['OPERA_L2_RTC-S1_T154-329223-IW1_20250620T061527Z', 'OPERA_L2_RTC-S1_T154-329222-IW1_20250620T061524Z', 'OPERA_L2_RTC-S1_T154-329221-IW2_20250620T061522Z', 'OPERA_L2_RTC-S1_T154-329221-IW1_20250620T061521Z', 'OPERA_L2_RTC-S1_T154-329220-IW2_20250620T061520Z', 'OPERA_L2_RTC-S1_T154-329220-IW1_20250620T061519Z', 'OPERA_L2_RTC-S1_T154-329219-IW2_20250620T061517Z', 'OPERA_L2_RTC-S1_T154-329219-IW1_20250620T061516Z', 'OPERA_L2_RTC-S1_T154-329218-IW2_20250620T061514Z', 'OPERA_L2_RTC-S1_T154-329218-IW1_20250620T061513Z']

    product_id='35XMK_22_348' 2025-06-21 15:05:29 product.used_bursts=15 product.possible_bursts=17
    RTC granules: ['OPERA_L2_RTC-S1_T174-372076-IW3_20250621T150529Z', 'OPERA_L2_RTC-S1_T174-372076-IW2_20250621T150528Z', 'OPERA_L2_RTC-S1_T174-372075-IW2_20250621T150525Z', 'OPERA_L2_RTC-S1_T174-372074-IW2_20250621T150523Z', 'OPERA_L2_RTC-S1_T174-372075-IW3_20250621T150526Z', 'OPERA_L2_RTC-S1_T174-372074-IW3_20250621T150524Z', 'OPERA_L2_RTC-S1_T174-372073-IW3_20250621T150521Z', 'OPERA_L2_RTC-S1_T174-372073-IW2_20250621T150520Z', 'OPERA_L2_RTC-S1_T174-372072-IW3_20250621T150518Z', 'OPERA_L2_RTC-S1_T174-372072-IW2_20250621T150517Z', 'OPERA_L2_RTC-S1_T174-372071-IW3_20250621T150515Z', 'OPERA_L2_RTC-S1_T174-372071-IW2_20250621T150514Z', 'OPERA_L2_RTC-S1_T174-372070-IW3_20250621T150513Z', 'OPERA_L2_RTC-S1_T174-372070-IW2_20250621T150512Z', 'OPERA_L2_RTC-S1_T174-372069-IW3_20250621T150510Z']

## populate_cmr_rtc_cache.py

OPERA PCM must be installed for this tool to work.

Tool to populate the GRQ ElasticSearch `cmr_rtc_cache` index with RTC granules from a CMR survey CSV file. This cache is used by the DIST-S1 triggering system to quickly query for available RTC granules without having to make real-time CMR queries.

The script parses RTC granule IDs from the CSV file, breaks down the granule IDs into metadata (burst_id, acquisition timestamp, acquisition cycle, etc), and stores them into `cmr_rtc_cache` If that store doesn't exist, it will be automatically created. DIST-S1 burst database pickle file is not required but will speed up the process if provided.

    python tools/populate_cmr_rtc_cache.py --help
    usage: populate_cmr_rtc_cache.py [-h] [--verbose] [--db-file DB_FILE] csv_file
    
    positional arguments:
      csv_file              Path to the CMR survey CSV file (e.g., cmr_survey.csv.raw.csv)
    
    optional arguments:
      -h, --help            show this help message and exit
      --verbose, -v         Enable verbose logging
      --db-file DB_FILE     Path to the DIST-S1 burst database pickle file

#### Examples:

    # Populate cache using default DIST-S1 database location
    python tools/populate_cmr_rtc_cache.py cmr_survey.csv.raw.2024-01-01_to_2024-2-28.csv

    # Populate cache with verbose logging
    python tools/populate_cmr_rtc_cache.py --verbose cmr_survey.csv.raw.2024-01-01_to_2024-2-28.csv

    # Populate cache using a specific DIST-S1 database file
    python tools/populate_cmr_rtc_cache.py --db-file mgrs_burst_lookup_table-2025-02-19.parquet cmr_survey.csv.raw.2024-01-01_to_2024-2-28.csv