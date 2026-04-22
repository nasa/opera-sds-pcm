# CMR Audit

The CMR audit tools are used to compare input products and output product quantities.

## Getting Started

See the README one level above for instructions on how to set up the environment and run the tools.

## Running

### DSWx-S1 CMR Audit

```bash
python cmr_audit_dswx_s1.py  --start-datetime 2025-05-19T23:30:00Z --end-datetime 2025-05-20T00:00:00Z
```

For any missing products, an output file is generated and its name looks like the following
```missing_granules_RTC-DSWx_20250519-233000ZZ_20250520-000000ZZ_20250616-175805Z.txt``` 

A list of RTC-S1 granules will be included in the report (e.g., missing*.txt) as shown below. 

```bash
% more missing_granules_RTC-DSWx_20250519-233000ZZ_20250520-000000ZZ_20250616-175805Z.txt 
OPERA_L2_RTC-S1_T048-101760-IW3_20250519T235845Z_20250520T050410Z_S1A_30_v1.0
OPERA_L2_RTC-S1_T048-101188-IW2_20250519T233227Z_20250520T055956Z_S1A_30_v1.0
OPERA_L2_RTC-S1_T048-101156-IW2_20250519T233058Z_20250520T110037Z_S1A_30_v1.0
OPERA_L2_RTC-S1_T048-101150-IW3_20250519T233043Z_20250520T110054Z_S1A_30_v1.0
OPERA_L2_RTC-S1_T048-101160-IW1_20250519T233108Z_20250520T110037Z_S1A_30_v1.0
OPERA_L2_RTC-S1_T048-101178-IW2_20250519T233159Z_20250520T065935Z_S1A_30_v1.0
OPERA_L2_RTC-S1_T048-101139-IW2_20250519T233011Z_20250520T070714Z_S1A_30_v1.0
OPERA_L2_RTC-S1_T048-101165-IW3_20250519T233124Z_20250520T070105Z_S1A_30_v1.0
```

The file above lists all RTC-S1 granules but does not indicate which MGRS tile collection sets are used for each granule.

To find a granule for a given tile collection set, use the following command:
```bash
python ~/mozart/ops/opera-pcm/tools/ops/data_subscriber/data_subscriber_client.py --rtc-native-ids-file missing_granules_RTC-DSWx_20250519-233000ZZ_20250520-000000ZZ_20250616-175805Z.txt  --output native_id_per_tilecollectionsets_RTC-DSWx_20250519-233000ZZ_20250520-000000ZZ_20250616-175805Z.txt
```
The output file from above command can be used to submit DSWx-S1 jobs to recover missing DSWx-S1 granules.

Each line represents a single `daac_data_subscriber.py` command to create the missing product. 

The first line item will translate to the following command
```bash
python3 ~/mozart/ops/opera-pcm/data_subscriber/daac_data_subscriber.py query -c OPERA_L2_RTC-S1_V1   --job-queue=opera-job_worker-rtc_data_download  --chunk-size 1 --native-id=OPERA_L2_RTC-S1_T048-101760-IW3_20250519T235845Z_20250520T050410Z_S1A_30_v1.0
```



### DISP-S1 CMR Audit

DISP-S1 CMR Audit generally works in the same way as other CMR Audit tools with DISP-S1-specific parameters. 
DISP-S1 execution works very differently depending on its processing mode. Correspondingly, there are two distinct ways 
to run the DISP-S1 CMR audit tool.

<b>NOTE</b> that DISP-S1 CMR Audit must be run from a deployed Mozart machine. This is because it requires the GRQ ES to retrieve 
provenance information. Provenance information is not stored in the CMR, unlike other products, so it must be retrieved from GRQ.

#### Historical Mode
Historical mode is run over a large range of dates for specific frames. Therefore, you want to specify those parameters 
when using cmr audit. The following is an example of how to run the DISP-S1 CMR audit tool in historical mode for a specific set of frames
```bash
python cmr_audit_disp_s1.py  --start-datetime 2016-07-01T00:00:00 --end-datetime 2025-01-01T00:00:00 --processing-mode=historical --frames-only=16936,46294,18904,18905,38504,38503,11115,11116,11117,26689
```
The output shows number of products that should have been found and of those how many are missing. 
```bash
INFO:  ... - Fully published (granules) (DISP-S1): len(disp_s1_products)=9,039
INFO:  ... - Missing (granules) (DISP-S1): len(disp_s1_products_miss)=225
```

For any missing products, an output file is generated and its name looks like the following
```missing_granules_CSLC-DISP-S1_20160701-000000Z_20250101-000000Z_20250515-213315Z``` The file contains information needed to run OPERA PCM data_subscriber tool create the missing products.
```bash
Frame ID, Start Date, End Date, K-Cycle
16936, 2017-09-06T01:18:11Z, 2018-02-21T02:18:09Z, 1
16936, 2018-03-05T01:18:09Z, 2019-01-23T02:18:16Z, 2
16936, 2019-03-12T01:18:15Z, 2019-09-08T02:18:24Z, 3
46294, 2017-11-30T13:15:42Z, 2018-05-17T14:15:42Z, 1
46294, 2018-05-29T13:15:43Z, 2018-11-13T14:15:49Z, 2
...
```

Each line represents a single `daac_data_subscriber.py` command to create the missing product. `K-Cycle` is 
not needed; it's there for context. The first line item will translate to the following command.
```bash
daac_data_subscriber.py query -c OPERA_L2_CSLC-S1_V1 -s 2017-09-06T01:18:11Z -e 2018-02-21T02:18:09Z --frame-id=16936 --processing-mode=historical ... (complete all other parameters as needed)
```

#### Forward Mode
Historical mode is run over a small range of dates, often an hour at a time, for all frames. The following is an example of how to run the DISP-S1 CMR audit tool in forward mode
```bash
python cmr_audit_disp_s1.py  --start-datetime 2025-01-01T01:00:00 --end-datetime 2025-01-01T03:00:00 --processing-mode=forward
```
The output shows the number of products that should have been found and of those how many are missing. 
```bash
INFO:  ... - Fully published (granules) (DISP-S1): len(disp_s1_products)=0
INFO:  ... - Missing (granules) (DISP-S1): len(disp_s1_products_miss)=4
```

For any missing products, an output file is generated and its name looks like the following
```missing_granules_CSLC-DISP-S1_20250101-000000Z_20250101-030000Z_20250516-193912Z```

The file contains information needed to run OPERA PCM data_subscriber tool create the missing products.
For `forward` processing mode the K-Cycle is not computed because it's largely irrelevant. 
```bash
Frame ID, Start Date, End Date, K-Cycle
32501, 2025-01-01T00:43:39Z, 2025-01-01T01:43:39Z
32502, 2025-01-01T00:43:47Z, 2025-01-01T01:43:47Z
32503, 2025-01-01T00:44:23Z, 2025-01-01T01:44:23Z
32504, 2025-01-01T00:44:47Z, 2025-01-01T01:44:47Z
```

Each line represents a single `daac_data_subscriber.py` command to create the missing product. To create missing products 
from a forward run while specifying the frame number, you must run in `reprocessing` mode.

The first line item will translate to the following command
```bash
daac_data_subscriber.py query -c OPERA_L2_CSLC-S1_V1 -s 2025-01-01T00:43:39Z -e 2025-01-01T01:43:39Z --frame-id=32501 --processing-mode=reprocessing ... (complete all other parameters as needed)
```

### DIST-S1 CMR Audit
```bash
python tools/ops/cmr_audit/cmr_audit_dist_s1.py --start-datetime 2025-06-19T00:00:00Z --end-datetime 2025-06-20T00:00:00Z
```

For any missing products, an output file is generated with the following naming convention:

`DIST_S1_potential_missing_products_{start_time}_{end_time}_{creation_time}.txt` 

ex:
 `DIST_S1_potential_missing_products_20260225T213000Z_20260225T220000Z_20260407T203700Z.txt`

The default contents are the listing of MGRS tile ids and associated acquisition group, sorted by tile id and within that, acquisition group.
They have been further subset to select one acq group time per tile/acquisition to remove redundancies:

```bash
% more DIST_S1_potential_missing_products_20260225T213000Z_20260225T220000Z_20260407T203700Z.txt
19VFD_2,20260225T215726Z
19VFE_2,20260225T215726Z
19VFF_2,20260225T215740Z
19VFG_3,20260225T215753Z
19VFJ_2,20260225T215825Z
20VLJ_2,20260225T215726Z
20VLK_2,20260225T215726Z
...
```

This list can be used in conjunction with the DIST-S1 input tool (`tools/dist_s1_input_tool.py`) to determine which 
of the audit identified missing products are truly missing by checking input validity for each tile + acquisition time pair. It will identify any truly missing DIST-S1 products that could be triggered. The two tools can be chanined together via the `--run-input-validation` flag:
```bash
python tools/ops/cmr_audit/cmr_audit_dist_s1.py --start-datetime 2025-06-20T00:00:00Z --end-datetime 2025-06-20T02:00:00Z --run-input-validation
```

This will result in a `DIST_S1_validated_missing_products_{start_time}_{end_time}_{creation_time}.txt` file containing a subset of the `DIST_S1_validated_missing_products...` list. 

#### DIST-S1 Output Options

There are optional flags for different formatting of output from the `cmr_audit_dist_s1` tool (note these do not apply to the `dist_s1_input_tool`). 

Use the optional flag `--rtc-output` to instead list the input RTC granules unused in a DIST-S1 product by the RTC native id:

```bash
OPERA_L2_RTC-S1_T047-099147-IW1_20260225T215726Z_20260226T013141Z_S1C_30_v1.0
OPERA_L2_RTC-S1_T047-099147-IW2_20260225T215727Z_20260226T013141Z_S1C_30_v1.0
OPERA_L2_RTC-S1_T047-099147-IW3_20260225T215728Z_20260226T013141Z_S1C_30_v1.0
OPERA_L2_RTC-S1_T047-099148-IW1_20260225T215729Z_20260226T013141Z_S1C_30_v1.0
OPERA_L2_RTC-S1_T047-099148-IW2_20260225T215730Z_20260226T013141Z_S1C_30_v1.0
OPERA_L2_RTC-S1_T047-099148-IW3_20260225T215731Z_20260226T013141Z_S1C_30_v1.0
OPERA_L2_RTC-S1_T047-099149-IW1_20260225T215731Z_20260226T013141Z_S1C_30_v1.0
OPERA_L2_RTC-S1_T047-099149-IW2_20260225T215732Z_20260226T013141Z_S1C_30_v1.0
...
```

Use the optional flag `--full-output` to output additional metadata. It can be used in conjunction with the `--rtc-output` flag.
Because some fields contain `,` as well as lists of values, `|` is used as a delimiter between columns and `;` as a delimiter between distinct values in a given column.

Example output with just `--full-output`:

```bash
mgrs_tile_id_acq_group|rtc_granules|product_id_time
19VFD_2|OPERA_L2_RTC-S1_T047-099147-IW1_20260225T215726Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099148-IW1_20260225T215729Z_20260226T013141Z_S1C_30_v1.0|19VFD_2,20260225T215726Z;19VFD_2,20260225T215729Z
19VFE_2|OPERA_L2_RTC-S1_T047-099147-IW1_20260225T215726Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099148-IW1_20260225T215729Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099149-IW1_20260225T215731Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099150-IW1_20260225T215734Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099151-IW1_20260225T215737Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099152-IW1_20260225T215740Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099153-IW1_20260225T215742Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099154-IW1_20260225T215745Z_20260226T013141Z_S1C_30_v1.0|19VFE_2,20260225T215726Z;19VFE_2,20260225T215729Z;19VFE_2,20260225T215731Z;19VFE_2,20260225T215734Z;19VFE_2,20260225T215737Z;19VFE_2,20260225T215740Z;19VFE_2,20260225T215742Z;19VFE_2,20260225T215745Z
19VFF_2|OPERA_L2_RTC-S1_T047-099152-IW1_20260225T215740Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099153-IW1_20260225T215742Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099154-IW1_20260225T215745Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099155-IW1_20260225T215748Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099156-IW1_20260225T215751Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099157-IW1_20260225T215753Z_20260226T013141Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099157-IW2_20260225T215754Z_20260226T013324Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099158-IW1_20260225T215756Z_20260226T013324Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099158-IW2_20260225T215757Z_20260226T013324Z_S1C_30_v1.0;OPERA_L2_RTC-S1_T047-099159-IW1_20260225T215759Z_20260226T013324Z_S1C_30_v1.0|19VFF_2,20260225T215740Z;19VFF_2,20260225T215742Z;19VFF_2,20260225T215745Z;19VFF_2,20260225T215748Z;19VFF_2,20260225T215751Z;19VFF_2,20260225T215753Z;19VFF_2,20260225T215754Z;19VFF_2,20260225T215756Z;19VFF_2,20260225T215757Z;19VFF_2,20260225T215759Z
...
```

Example output with both `--full-output` and `--rtc-output`:

```bash
native_id|revision_id|revision_date|burst_id|bid_acq
OPERA_L2_RTC-S1_T047-099147-IW1_20260225T215726Z_20260226T013141Z_S1C_30_v1.0|1|2026-02-26T02:20:11.291Z|T047-099147-IW1|T047-099147-IW1_20260225T215726Z
OPERA_L2_RTC-S1_T047-099147-IW2_20260225T215727Z_20260226T013141Z_S1C_30_v1.0|1|2026-02-26T02:20:15.027Z|T047-099147-IW2|T047-099147-IW2_20260225T215727Z
OPERA_L2_RTC-S1_T047-099147-IW3_20260225T215728Z_20260226T013141Z_S1C_30_v1.0|1|2026-02-26T02:20:10.205Z|T047-099147-IW3|T047-099147-IW3_20260225T215728Z
OPERA_L2_RTC-S1_T047-099148-IW1_20260225T215729Z_20260226T013141Z_S1C_30_v1.0|1|2026-02-26T02:20:10.696Z|T047-099148-IW1|T047-099148-IW1_20260225T215729Z
OPERA_L2_RTC-S1_T047-099148-IW2_20260225T215730Z_20260226T013141Z_S1C_30_v1.0|1|2026-02-26T02:20:15.627Z|T047-099148-IW2|T047-099148-IW2_20260225T215730Z
OPERA_L2_RTC-S1_T047-099148-IW3_20260225T215731Z_20260226T013141Z_S1C_30_v1.0|1|2026-02-26T02:20:11.387Z|T047-099148-IW3|T047-099148-IW3_20260225T215731Z
```