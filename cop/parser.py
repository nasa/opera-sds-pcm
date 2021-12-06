from lxml import etree
from util import xml2json
import os

SCHEMA_FILE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "schema", "cop.xsd")
)


def parse(xml_file, validate=True):
    """
    Parse the given XML file.
    :param xml_file: XML file path.
    :param validate: Indicates whether to perform schema validation on the XML.

    :return:
    """
    xml_doc = etree.parse(xml_file)
    if validate:
        schema_doc = etree.parse(SCHEMA_FILE_PATH)
        schema = etree.XMLSchema(schema_doc)
        try:
            schema.assertValid(xml_doc)
        except etree.DocumentInvalid:
            assert_error = AssertionError()
            assert_error.args += tuple(schema.error_log)
            raise assert_error
    return xml_doc


def convert_to_json(xml_doc, lower_case=True):
    return xml2json.convert_string(
        etree.tostring(xml_doc.getroot(), pretty_print=True).decode("utf-8"),
        lower_case=lower_case,
    )
