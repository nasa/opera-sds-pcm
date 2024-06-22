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

Examples: 

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