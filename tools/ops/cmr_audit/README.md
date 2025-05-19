# CMR Audit

The CMR audit tools are used to compare input products and output product quantities.

## Getting Started

See the README one level above for instructions on how to set up the environment and run the tools.

## Running

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
