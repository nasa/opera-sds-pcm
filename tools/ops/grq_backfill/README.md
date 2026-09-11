# GRQ Backfilling from CMR

This is a simple tool that syncs granules present in CMR to GRQ. It ensures no existing GRQ document is overwritten (it 
dedupes the IDs from CMR against the doc IDs from GRQ), but it does NOT do a full dedupe of granules (duplicates in CMR
will be copied to GRQ as well). It does its best to replicate the structure and data of the GRQ documents, but, due to 
limited availability in CMR and the likely huge performance penalty of scraping ISO XMLs, some omissions and/or 
modifications will likely be present in some fields; developer review may be needed to ensure these docs are properly 
usable. An additional top-level field, `is_backfilled: true` is inserted to all created docs to allow for easy 
identification of these docs.

Support for all collections in active production is available, but only for production CMR.

## Using the tool

### Environment

This tool is intended to be run within one of the OPERA PCM cluster machines (ie, mozart). Some modification may be
required to run elsewhere.

### Usage

```
usage: grq_backfill.py [-h] [-s START_DATE] [-e END_DATE] [-b MIN_LON MIN_LAT MAX_LON MAX_LAT] [--use-revision] {DSWx_HLS,DSWx_S1,CSLC_S1,RTC_S1,CSLC_S1_STATIC,RTC_S1_STATIC,DISP_S1,DISP_S1_STATIC,DIST_S1,TROPO}

positional arguments:
  {DSWx_HLS,DSWx_S1,CSLC_S1,RTC_S1,CSLC_S1_STATIC,RTC_S1_STATIC,DISP_S1,DISP_S1_STATIC,DIST_S1,TROPO}
                        Collection to backfill

options:
  -h, --help            show this help message and exit
  -s START_DATE, --start-date START_DATE
                        The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z
  -e END_DATE, --end-date END_DATE
                        The ISO date time before which data should be retrieved. For Example, --end-date 2021-01-14T00:00:00Z
  -b MIN_LON MIN_LAT MAX_LON MAX_LAT, --bbox MIN_LON MIN_LAT MAX_LON MAX_LAT
                        Bounding box. 4 float coordinates: min_lon, min_lat, max_lon, max_lat. -90 <= lat <= 90; -180 <= lon <= 180.
  --use-revision        Toggle for using revision date range rather than temporal range in the query.
```

### Considerations

This tool will likely take a long time to run; tmux or screen or a similar application should be used to allow running
asynchronously.

The tool also makes a best-effort attempt to insert docs into GRQ. Failures are counted and logged, as is a write status
JSON document titled `backfill_results_<collection_name>.json`, these should be reviewed after the tool finishes running.
You can use `jq -n 'reduce inputs as $item ({}; .[input_filename] += $item)' *.json` for a neat, consolidated view of these
files. In the event of insertion failures, try again. If failures persist, contact a PCM developer.

For DIST-S1, since the collection is not public yet, you should ensure `~/.netrc` contains an Earthdata login 
(`urs.earthdata.nasa.gov`) for an EDL account that has been granted read permission for the DIST collection.

If an exception is raised during the CMR scanning or GRQ doc generation, please contact a PCM developer.
