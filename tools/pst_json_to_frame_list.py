#!/usr/bin/env python3

''' Given an input json file that looks like the following and a user-specified priority, print out all frame_id as a comma-separated list.

{
"type": "FeatureCollection",
"crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
"features": [
{ "type": "Feature", "properties": { "frame_id": 10860, "region_name": "México", "priority": 2.0, "orbit_pass": "DESCENDING" }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -101.496842, 18.994589 ], [ -101.199859, 20.529861 ], [ -98.804805, 20.216855 ], [ -99.122969, 18.688424 ], [ -101.496842, 18.994589 ] ] ] } },
{ "type": "Feature", "properties": { "frame_id": 42810, "region_name": "Hawaii", "priority": 2.0, "orbit_pass": "DESCENDING" }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -158.987848, 19.558146 ], [ -158.690933, 21.09337 ], [ -156.287409, 20.781579 ], [ -156.606325, 19.25325 ], [ -158.987848, 19.558146 ] ] ] } },
{ "type": "Feature", "properties": { "frame_id": 42809, "region_name": "Hawaii", "priority": 2.0, "orbit_pass": "DESCENDING" }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -158.730609, 20.888699 ], [ -158.434054000001368, 22.423741999999041 ], [ -156.007904, 22.114757 ], [ -156.328342, 20.586709 ], [ -158.730609, 20.888699 ] ] ] } },
{ "type": "Feature", "properties": { "frame_id": 35023, "region_name": "Hawaii", "priority": 2.0, "orbit_pass": "DESCENDING" }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -162.906051000001526, 20.574379999998918 ], [ -162.609481000001551, 22.109482999999003 ], [ -160.189036, 21.799911 ], [ -160.509008000001387, 20.271772999998909 ], [ -162.906051000001526, 20.574379999998918 ] ] ] } }
]}}
'''

import json
import argparse

def main():
    parser = argparse.ArgumentParser(description='Given an input json file that looks like the following and a user-specified priority, print out all frame_id as a comma-separated list.')
    parser.add_argument('input_json', type=str, help='Input json file')
    parser.add_argument('priority', type=float, help='Priority')
    args = parser.parse_args()

    with open(args.input_json) as f:
        data = json.load(f)

    frame_ids = [f['properties']['frame_id'] for f in data['features'] if f['properties']['priority'] == args.priority]
    print(','.join(map(str, frame_ids)))

if __name__ == '__main__':
    main()
