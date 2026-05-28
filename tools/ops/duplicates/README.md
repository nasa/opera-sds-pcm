# Duplicate Product Check Tool

This tool queries CMR and analyzes product IDs to determine duplicate products.

The following products are supported:

| Product        | Supported Venues |
|----------------|------------------|
| DSWX_HLS       | PROD             |
| CSLC_S1        | PROD             |
| RTC_S1         | PROD, UAT        |
| DSWX_S1        | PROD             |
| DISP_S1        | PROD             |
| TROPO          | PROD             |
| DIST_ALERT_HLS | PROD             |
| DIST_ALERT_S1  | PROD, UAT        |

## Setting up the environment

This tool only requires a simply Python virtual environment, it does not require `opera-pcm` to be installed. To set up, simply run:

```shell
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running the tool

The only required parameter the product to inspect, refer to the table above for valid values of this parameter.

Use `--venue UAT` to query for products in CMR UAT.

Granules can be filtered by acquisition time or revision time using `--start-date` and `--end-date` with timestamps with the 
format `YYYY-MM-DDTHH:MM:SSZ`. Use the `--use-revision` flag to filter by revision time.

The duplicates report is dumped as JSON to a file. By default, this file is named `duplicate_report.json` but can be set with the `--output` option.

## Output JSON Report

The output JSON report contains 2-3 top-level fields. The first is `summary`, containing a high-level summary of the duplicate report:

```json
{
    "product": "DSWX_HLS",
    "venue": "PROD",
    "ccid": "C2617126679-POCLOUD",
    "n_granules": 42813,
    "n_duplicates": 532,
    "percent_duplicates": 1.242613224955037,
    "min_duplicates_per_granule": 1,
    "max_duplicates_per_granule": 2,
    "avg_duplicates_per_granule": 1.0018832391713748,
    "report_run_time": "0:01:26.012284"
}
```

The other field(s) are the detected unique products with duplicates, faceted either by month (`months`), day (`dates`), or both (this is controlled by the `--facet` CLI option, 
but this should probably be left to the default of just faceting by month)

The structure of these fields maps the facet timestamp `YYYY-MM[-DD]` to the number of granules in that time range, the number of duplicates
in that time range, the percentage of duplicates in that time range and the mapping of tuples of fields that identify a unique product 
(ie, tile ID, acquisition time, sensor) to the duplicate information for that unique product: the latest product ID and the list of 
duplicate product IDs. The separation between latest and duplicates is determined by production time.

```json
{
  "months": {
    "2026-05": {
      "n_granules": 42813,
      "n_duplicates": 532,
      "percent_duplicates": 1.242613224955037,
      "duplicates": {
        "('T55HDB', '20260514T000757Z', 'L8')": {
          "latest_product": "OPERA_L3_DSWx-HLS_T55HDB_20260514T000757Z_20260518T034055Z_L8_30_v1.1",
          "duplicate_products": [
            "OPERA_L3_DSWx-HLS_T55HDB_20260514T000757Z_20260516T032222Z_L8_30_v1.1"
          ]
        },
        "('T56LLQ', '20260514T001721Z', 'S2C')": {
          "latest_product": "OPERA_L3_DSWx-HLS_T56LLQ_20260514T001721Z_20260516T174449Z_S2C_30_v1.1",
          "duplicate_products": [
            "OPERA_L3_DSWx-HLS_T56LLQ_20260514T001721Z_20260516T122703Z_S2C_30_v1.1"
          ]
        },
        "('T56LKQ', '20260514T001721Z', 'S2C')": {
          "latest_product": "OPERA_L3_DSWx-HLS_T56LKQ_20260514T001721Z_20260516T174433Z_S2C_30_v1.1",
          "duplicate_products": [
            "OPERA_L3_DSWx-HLS_T56LKQ_20260514T001721Z_20260516T122704Z_S2C_30_v1.1"
          ]
        }
      }
    }
  }  
}
```

You can use `jq` to reduce the report into a flat list of duplicate granule IDs:

```shell
jq -r '.months[].duplicates[].duplicate_products[]' < REPORT_NAME.json > FLAT_FILE_NAME.txt
```

or

```shell
jq -r '.dates[].duplicates[].duplicate_products[]' < REPORT_NAME.json > FLAT_FILE_NAME.txt
```


