#!/usr/bin/env python3
import sys
''' Merge two or more CSV files that look like the following. Remove any duplicate granules. 
    Pass in as many input files as you want to merge, and the output file as the last argument.

# Granule ID, Revision Time, Temporal Time, Revision-Temporal Delta Hours, revision-id 
OPERA_L2_CSLC-S1_T048-101196-IW3_20240909T233256Z_20240910T193554Z_S1A_VV_v1.1, 2024-09-10T20:32:26.882Z, 2024-09-09T23:32:56Z,      20.99, 1
OPERA_L2_CSLC-S1_T048-101196-IW2_20240909T233255Z_20240910T193554Z_S1A_VV_v1.1, 2024-09-10T20:32:30.136Z, 2024-09-09T23:32:55Z,      20.99, 1
OPERA_L2_CSLC-S1_T048-101196-IW1_20240909T233254Z_20240910T193554Z_S1A_VV_v1.1, 2024-09-10T20:32:27.815Z, 2024-09-09T23:32:54Z,      20.99, 1
OPERA_L2_CSLC-S1_T048-101195-IW3_20240909T233253Z_20240910T193554Z_S1A_VV_v1.1, 2024-09-10T20:32:29.054Z, 2024-09-09T23:32:53Z,      20.99, 1
OPERA_L2_CSLC-S1_T048-101195-IW2_20240909T233252Z_20240910T193554Z_S1A_VV_v1.1, 2024-09-10T20:32:26.457Z, 2024-09-09T23:32:52Z,      20.99, 1
OPERA_L2_CSLC-S1_T048-101195-IW1_20240909T233251Z_20240910T193554Z_S1A_VV_v1.1, 2024-09-10T20:32:30.283Z, 2024-09-09T23:32:51Z,      20.99, 1
OPERA_L2_CSLC-S1_T048-101194-IW3_20240909T233250Z_20240910T193554Z_S1A_VV_v1.1, 2024-09-10T20:32:30.158Z, 2024-09-09T23:32:50Z,      20.99, 1
OPERA_L2_CSLC-S1_T048-101194-IW2_20240909T233249Z_20240910T193554Z_S1A_VV_v1.1, 2024-09-10T20:32:29.208Z, 2024-09-09T23:32:49Z,      20.99, 1
'''

def merge_csv_files(csv_files: list[str], output_file: str):
    '''Merge two or more CSV files that look like the following. Remove any duplicate granules.'''
    granules = {}
    all_line_count = 0
    for csv_file in csv_files:
        with open(csv_file, 'r') as f:
            local_line_count = 0
            for line in f:
                if line.startswith('#'):
                    continue
                granule_id = line.split(',')[0]
                #if granule_id in granules:
                #    print(f'Granule {granule_id} already exists, skipping')
                granules[granule_id] = line
                all_line_count += 1
                local_line_count += 1
            print(f'Merged {local_line_count} granules from {csv_file}')

    with open(output_file, 'w') as f:
        f.write('Granule ID, Revision Time, Temporal Time, Revision-Temporal Delta Hours, revision-id\n')
        for granule in granules.values():
            f.write(granule)
    print(f'Merged {len(granules)} granules from {all_line_count} lines in {len(csv_files)} files to {output_file}')

merge_csv_files(sys.argv[1:-1], sys.argv[-1])