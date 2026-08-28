from __future__ import print_function

from pathlib import Path

import xmltodict
from more_itertools import one
import re
from shapely import from_wkt, to_geojson
from itertools import batched
import json


def read_iso_xml_as_dict(iso_xml_path: Path):
    with iso_xml_path.open() as fp:
        iso_xml = xmltodict.parse(fp.read())
    return iso_xml


def get_extents(iso_xml: dict) -> dict:
    return (
        iso_xml
        .get("gmi:MI_Metadata")
        .get("gmd:identificationInfo")
        .get("gmd:MD_DataIdentification")
        .get("gmd:extent")
    )


def get_additional_attributes(iso_xml):
    additional_attributes = (
        iso_xml
        .get("gmi:MI_Metadata")
        .get("gmd:contentInfo")
        .get("gmd:MD_CoverageDescription")
        .get("gmd:dimension")
        .get("gmd:MD_Band")
        .get("gmd:otherProperty")
        .get("gco:Record")
        .get("eos:AdditionalAttributes")
    )["eos:AdditionalAttribute"]
    return additional_attributes


def get_additional_attributes_as_dict(additional_attributes: list):
    additional_attributes = {
        attr_["eos:reference"]["eos:EOS_AdditionalAttributeDescription"]["eos:name"]["gco:CharacterString"]:
            attr_["eos:value"]
        for attr_ in additional_attributes
    }
    return additional_attributes


def get_additional_attribute_from_additional_attributes(additional_attributes: dict, name) -> str:
    return additional_attributes[name]["gco:CharacterString"]


def get_rtc_sensing_start_time_from_additional_attributes(additional_attributes: dict) -> str:
    return get_additional_attribute_from_additional_attributes(additional_attributes, name="RTCSensingStartTime")


def get_rtc_sensing_end_time_from_additional_attributes(additional_attributes: dict) -> str:
    return get_additional_attribute_from_additional_attributes(additional_attributes, name="RTCSensingEndTime")


def get_rtc_input_list_from_additional_attributes(additional_attributes: dict) -> str:
    return get_additional_attribute_from_additional_attributes(additional_attributes, name="RTCInputList")


def get_tile_id_extent(extents):
    return one([extent for extent in extents if extent["gmd:EX_Extent"]["@id"] == "TilingIdentificationSystem"])


def get_bounding_polygon_as_geojson(extents):
    geo_element = (
        extents
        .get("gmd:EX_Extent", {})
        .get("gmd:geographicElement", {})
    )

    polygon = None

    if isinstance(geo_element, dict):
        polygon = (
            geo_element
            .get("gmd:EX_BoundingPolygon", {})
            .get("gmd:polygon", {})
            .get("gml:Polygon", {})
            .get("gml:exterior", {})
            .get("gml:LinearRing", {})
            .get("gml:posList")
        )
    elif isinstance(geo_element, list):
        for ge in geo_element:
            polygon = (
                ge
                .get("gmd:EX_BoundingPolygon", {})
                .get("gmd:polygon", {})
                .get("gml:Polygon", {})
                .get("gml:exterior", {})
                .get("gml:LinearRing", {})
                .get("gml:posList")
            )

            if polygon is not None:
                break
    else:
        raise TypeError(f"Unexpected type {type(geo_element)}")

    if polygon is None:
        raise ValueError("No bounding polygon found")

    def _fix_commaless_coords(coords):
        coords = coords.split(' ')
        coords = [' '.join(b) for b in batched(coords, 2)]
        return ', '.join(coords)

    try:
        polygon = from_wkt(polygon)
    except Exception as e:
        # CLSC polygon in ISO is not valid WKT, do a regex search for Poly/MultiPoly,
        #  repair to valid WKT and try parsing again

        if re.fullmatch(
            r'\(-?\d+(\.\d+)? -?\d+(\.\d+)?( -?\d+(\.\d+)? -?\d+(\.\d+)?)*\)', polygon
        ):
            polygon = from_wkt(f'POLYGON({_fix_commaless_coords(polygon)})')
        elif re.fullmatch(
            r'\(\(-?\d+(\.\d+)? -?\d+(\.\d+)?( -?\d+(\.\d+)? -?\d+(\.\d+)?)*\)\) '
            r'(\(\(-?\d+(\.\d+)? -?\d+(\.\d+)?( -?\d+(\.\d+)? -?\d+(\.\d+)?)*\)\))*'
        ):
            polygon = from_wkt(f'MULTIPOLYGON(({_fix_commaless_coords(polygon)})')
        else:
            raise ValueError(f'Unrecognized polygon format: {polygon}')

    return json.loads(to_geojson(polygon))


def get_tile_id(tile_id_extent):
    tile_id = (
        tile_id_extent
        .get("gmd:EX_Extent")
        .get("gmd:geographicElement")
        .get("gmd:EX_GeographicDescription")
        .get("gmd:geographicIdentifier")
        .get("gmd:MD_Identifier")
        .get("gmd:code")
    )["gco:CharacterString"]
    return tile_id
