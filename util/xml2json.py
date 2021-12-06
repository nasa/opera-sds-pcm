"""
Module that converts XML to JSON

@author: mcayanan
"""

import xml.etree.cElementTree as ET
import urllib.parse
import sys

from util.type_util import set_type


def elem_to_internal(elem, lower_case=True, strip=1):
    """Convert an Element into an internal dictionary (not JSON!)."""

    d = {}
    # loop over subelements to merge them
    for subelem in elem:
        v = elem_to_internal(subelem, lower_case=lower_case, strip=strip)
        tag = subelem.tag
        if lower_case:
            tag = tag.lower()
        value = v[tag]
        try:
            # add to existing list for this tag
            d[tag].append(value)
        except AttributeError:
            # turn existing entry into a list
            d[tag] = [d[tag], value]
        except KeyError:
            # add a new non-list entry
            d[tag] = value
    text = elem.text
    # Decode
    # text = urllib.unquote(text).decode('utf8')
    if text:
        text = urllib.parse.unquote(text)
    tail = elem.tail
    if strip:
        # ignore leading and trailing whitespace
        if text:
            text = text.strip()
        if tail:
            tail = tail.strip()
    if text:
        text = set_type(text)
    if tail:
        d["#tail"] = tail

    if d:
        # use #text element if other attributes exist
        if text:
            d["#text"] = text
    else:
        # text is the value if no attributes
        # d = text or None
        d = text
    elem = elem.tag
    if lower_case:
        elem = elem.lower()
    return {elem: d}


def elem2json(elem, lower_case=True, strip=1):
    """Convert an ElementTree or Element into a JSON string."""
    if hasattr(elem, "getroot"):
        elem = elem.getroot()
    internal_json = elem_to_internal(elem, lower_case=lower_case, strip=strip)
    return internal_json


def convert_string(xmlstring, lower_case=True, strip=1):
    """Convert an XML string into a JSON string."""
    elem = ET.fromstring(xmlstring)
    output = elem2json(elem, lower_case=lower_case, strip=strip)
    return output


def convert_file(xmlfile, lower_case=True, strip=1):
    xmlstring = open(xmlfile).read()
    return convert_string(xmlstring, lower_case, strip)


def main():
    pass


if __name__ == "__main__":
    data = convert_file(sys.argv[1])
    print(data)
