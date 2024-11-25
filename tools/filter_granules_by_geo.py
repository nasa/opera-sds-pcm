#!/usr/bin/env python3
import logging
from typing import TypedDict
import json
import argparse
from data_subscriber.geojson_utils import localize_geojsons
from geo.geo_util import does_bbox_intersect_region

''' Tool to query the DISP S1 burst database 
    The burst database file must be in the same directory as this script'''

'''
Parses input file that looks like this:
{
"type": "FeatureCollection",
"name": "priority_1_SAFE_files_missing_CSLCs_20160701_20241115",
"crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
"features": [
{ "type": "Feature", "properties": { "fileID": "S1A_IW_SLC__1SDV_20241113T141656_20241113T141723_056537_06EE96_0AFB-SLC" }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -122.344925, 34.885368 ], [ -121.958122, 36.508919 ], [ -124.763496, 36.907421 ], [ -125.091042, 35.285538 ], [ -122.344925, 34.885368 ] ] ] } },
{ "type": "Feature", "properties": { "fileID": "S1A_IW_SLC__1SDV_20241112T133826_20241112T133853_056522_06EDFF_B7B9-SLC" }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -114.185638, 25.311655 ], [ -113.832977, 26.938837 ], [ -116.37664, 27.355173 ], [ -116.692116, 25.730932 ], [ -114.185638, 25.311655 ] ] ] } }
]}
'''

class Coordinate(TypedDict):
    lat: float
    lon: float

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)

localize_geojsons(["north_america_opera"])

parser = argparse.ArgumentParser()
parser.add_argument("geojson", help="Geojson file to filter")

args = parser.parse_args()

# Open the geojson file for parsing
with open(args.geojson, "r") as f:
    geojson = json.load(f)

    for feature in geojson["features"]:
        coords = feature["geometry"]["coordinates"][0]
        bbox = []
        for coord in coords:
            c = Coordinate(lat=coord[1], lon=coord[0])
            bbox.append(c)
        if does_bbox_intersect_region(bbox, "north_america_opera"):
            print(f"{feature['properties']['fileID']} intersects with North America (OPERA)")
        #else:
        #    logger.info(f"Feature {feature['properties']['fileID']} does not intersect with North America (OPERA)")