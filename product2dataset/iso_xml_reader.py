from __future__ import print_function

from pathlib import Path

import xmltodict
from more_itertools import one


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
