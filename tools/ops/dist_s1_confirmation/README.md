# DIST-S1 Confirmation Chain Check Tool

This tool inspects DIST-S1 products delivered to the DAACs via either the production or UAT CMR venues. The tool checks
for any errors in the DIST-S1 product confirmation chains. Three types of errors are checked for:

1. Products produced out of order (`production_products_misordered` in output report): These are products where the ordering
   of production differs from product acquisition. Products produced out of order have inherently invalid confirmation chains
2. Chaining discontinuities (`chaining_discontinuities`): If there is a disconnect in the confirmation chain
3. Chaining bad ordering (`chaining_bad_orders`): If there are any products for which confirmation was run against a
   DIST-S1 product that did not come immediately before

If there are any findings, the tool will produce a JSON report which can contain the following fields:

1. A list of MGRS tiles for which the confirmation chain is incorrect
2. The `production_products_misordered` list, containing the IDs of the products out of order, when they were produced, 
   and when the previous product (in acquisition time) was produced.
3. The `chaining_discontinuities` list containing the IDs of the incorrect products, and the expected ID of the previous product
4. The `chaining_bad_orders` list containing the IDs of the incorrect products, the expected ID of the previous product, 
   and the ID of the product that was actually used.
5. A list of tiles with warnings. Warnings are ONLY produced when a) the first product in a confirmation chain has no 
   previous product for confirmation AND b) the tool was run on a temporal subset of DIST-S1 products that begins after
   the earliest acquisition date in the DIST-S1 record. The warnings indicate that the chain MAY have a discontinuity
   at the first surveyed granule.
6. A list of products that could not be inspected. These are due to data fields missing from CMR. **This should only happen
   with the UAT venue!**

## Setting up the environment

This tool uses a separate conda environment due to its GDAL dependency. To create and activate the environment, simply run:

```shell
conda env create -f environment.yaml
conda activate dist-s1-confirmation-tool
```

## Running the tool

Prior to running the tool, you must ensure you have valid EDL credentials for production and/or UAT CMR configured in your
`.netrc` file:

```netrc
machine urs.earthdata.nasa.gov
    login <username>
    password <password>

machine uat.urs.earthdata.nasa.gov
    login <username>
    password <password>
```

This tool can also utilize direct S3 access. OPERA developers can utilize any EC2 instance in an OPERA VPC, whereas other
users should configure credentials via ASF DAAC's [S3 credentials endpoint](https://cumulus.asf.earthdatacloud.nasa.gov/s3credentials).
If direct S3 access is desired, you must be running in the us-west-2 region.

By default, the tool will inspect all DIST-S1 products available from the target CMR venue. A number of optional filters
are provided to narrow down the scope of the search:

- Filter to one or more MGRS tiles
- Filter by temporal range of both acquisition and production times
- Filter by products produced by one or more PGE versions

The tool's command line usage is as follows:

```
usage: dist_s1_confirmation.py [-h] [-s START_DATE] [-e END_DATE] [-t TILES [TILES ...]] [--production-start-date PRODUCTION_START_DATE] 
                               [--production-end-date PRODUCTION_END_DATE] [--pge-versions PGE_VERSIONS [PGE_VERSIONS ...]] 
                               [--venue {PROD,UAT}] [--ignore-first-null]

options:
  -h, --help            show this help message and exit
  --venue {PROD,UAT}    Venue to check: PROD or UAT. Default: PROD
  --ignore-first-null   For a given confirmation chain, if the first product did not use a previous product as an input 
                        and the survey's start time is after the DIST-S1 start time, by default, the chain's tile will 
                        be flagged with a warning, as we'll have no way to determine if there should be a product before 
                        it in the chain. If this option is set, these warnings are inhibited.

Product selection options:
  Options to narrow down products checked by filtering DIST results from CMR

  -s, --start-date START_DATE
                        The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z
  -e, --end-date END_DATE
                        The ISO date time before which data should be retrieved. For Example, --end-date 2021-01-14T00:00:00Z
  -t, --tiles TILES [TILES ...]
                        One or more MGRS tiles to restrict survey to
  --production-start-date PRODUCTION_START_DATE
                        The ISO date time for filtering by production time. For Example, --production-start-date 2021-01-14T00:00:00Z
  --production-end-date PRODUCTION_END_DATE
                        The ISO date time for filtering by production time. For Example, --production-end-date 2021-01-14T00:00:00Z
  --pge-versions PGE_VERSIONS [PGE_VERSIONS ...]
                        PGE version number(s) to filter products to

```

### Small note on UAT

For UAT, there are 2 collections for DIST-S1: `OPERA_L3_DIST-ALERT-S1_PROVISIONAL_V0` (`C1275699124-ASF`) and 
`OPERA_L3_DIST-ALERT-S1_V1` (`C1275699127-ASF`). Only one of these can be used at a time, and it is currently hardcoded into
the tools script (currently the latter collection is used). If you'd like to inspect the other collection, you'll need to 
edit the `dist_s1_confirmation.py` file to change the commented-out `UAT` entry in the `CCIDS` dictionary.

## Example command and report

```shell
python dist_s1_confirmation.py --venue UAT
```

The above command produces the following report (truncated for brevity):

```json
{
  "bad_tiles": [
    "T41SKS",
    "T60UXC",
    "T19GDN",
    "T40SGB",
    "T40RFV",
    "T40SFB"
  ],
  "production_products_misordered": [
    {
      "misordered_product_id": "OPERA_L3_DIST-ALERT-S1_T40SFB_20260331T020514Z_20260401T023149Z_S1A_30_v0.1",
      "production_time": "20260401T023149Z",
      "prior_product_production_time": "20260401T024058Z"
    }
  ],
  "chaining_discontinuities": [
    {
      "discontinuous_product_id": "OPERA_L3_DIST-ALERT-S1_T40RFV_20260331T020539Z_20260401T024020Z_S1A_30_v0.1",
      "expected_prev_product_id": "OPERA_L3_DIST-ALERT-S1_T40RFV_20260330T140207Z_20260401T024014Z_S1A_30_v0.1"
    },
    {
      "discontinuous_product_id": "OPERA_L3_DIST-ALERT-S1_T40SFB_20260331T020514Z_20260401T023149Z_S1A_30_v0.1",
      "expected_prev_product_id": "OPERA_L3_DIST-ALERT-S1_T40SFB_20260330T140221Z_20260401T024058Z_S1A_30_v0.1"
    },
    {
      "discontinuous_product_id": "OPERA_L3_DIST-ALERT-S1_T19GDN_20260330T233401Z_20260401T023239Z_S1A_30_v0.1",
      "expected_prev_product_id": "OPERA_L3_DIST-ALERT-S1_T19GDN_20260330T233344Z_20260401T001615Z_S1A_30_v0.1"
    }
  ],
  "chaining_bad_orders": [
    {
      "misordered_product_id": "OPERA_L3_DIST-ALERT-S1_T60UXC_20250118T181923Z_20260331T120908Z_S1A_30_v0.1",
      "expected_prev_product_id": "OPERA_L3_DIST-ALERT-S1_T60UXC_20250111T182735Z_20260331T122354Z_S1A_30_v0.1",
      "incorrect_previous_product_id": "OPERA_L3_DIST-ALERT-S1_T60UXC_20250106T181924Z_20260331T115423Z_S1A_30_v0.1"
    },
    {
      "misordered_product_id": "OPERA_L3_DIST-ALERT-S1_T41SKS_20260331T020514Z_20260401T025846Z_S1A_30_v0.1",
      "expected_prev_product_id": "OPERA_L3_DIST-ALERT-S1_T41SKS_20260330T140217Z_20260401T030746Z_S1A_30_v0.1",
      "incorrect_previous_product_id": "OPERA_L3_DIST-ALERT-S1_T41SKS_20260101T015659Z_20260331T141233Z_S1A_30_v0.1"
    },
    {
      "misordered_product_id": "OPERA_L3_DIST-ALERT-S1_T40SGB_20260331T020514Z_20260401T022728Z_S1A_30_v0.1",
      "expected_prev_product_id": "OPERA_L3_DIST-ALERT-S1_T40SGB_20260330T140219Z_20260401T024812Z_S1A_30_v0.1",
      "incorrect_previous_product_id": "OPERA_L3_DIST-ALERT-S1_T40SGB_20260101T015703Z_20260331T144605Z_S1A_30_v0.1"
    }
  ],
  "dropped_products": [
    "OPERA_L3_DIST-ALERT-S1_T10SGD_20250102T015857Z_20250418T194347Z_S1_30_v0.1"
  ]
}
```
