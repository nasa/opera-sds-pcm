"""
This extractor basically does the same thing as the FilenameRegexMetExtractor. Only difference
is that this will formulate the GranuleName metadata through configuration. This is needed for files
like the Quaternion files, where the FileCreationDateTime is in the middle of the file name.

"""
import os
import json
import sys
import re

from commons.constants import product_metadata
from util.common_util import convert_datetime

from extractor.FilenameRegexMetExtractor import FilenameRegexMetExtractor


class TemplateMetExtractor(FilenameRegexMetExtractor):
    _product_metadata = [
        product_metadata.GRANULE_NAME,
        product_metadata.PCM_RETRIEVAL_ID,
    ]

    def extract(self, product, match_pattern, extractor_config):
        metadata = super(TemplateMetExtractor, self).extract(
            product, match_pattern, extractor_config
        )
        pattern = re.compile(match_pattern)
        match = pattern.search(product)
        match_group = match.groupdict()

        # Get date time formats, if specified
        template_date_time_formats = extractor_config.get("Template_Date_Time_Formats", {})

        # add catalog metadata
        match_group["catalog_metadata"] = extractor_config.get("catalog_metadata", {})

        # If the datetime metadata key is defined in the Template_Date_Time_Formats config,
        # convert the value to the given string
        for key in match_group["catalog_metadata"].keys():
            if key in template_date_time_formats.keys():
                dt_value = convert_datetime(match_group["catalog_metadata"][key])
                match_group["catalog_metadata"][key] = convert_datetime(dt_value,
                                                                        strformat=template_date_time_formats.get(key))
        for pd in self._product_metadata:
            template = extractor_config.get(pd, None)
            if template is None:
                continue
            try:
                val = template.format(**match_group)
                metadata[pd] = val
            except KeyError as ke:
                raise Exception(
                    "Could not formulate the {}: Template={}, Substitution_Map={}. "
                    "Missing '{}'".format(
                        pd, template, match.groupdict(), str(ke)
                    )
                )

        return metadata


def main():
    """
    Main entry point
    """
    product = os.path.abspath(sys.argv[1])
    match_pattern = sys.argv[2]
    extractor_config = json.loads(sys.argv[3])
    extractor = TemplateMetExtractor()
    metadata = extractor.extract(product, match_pattern, extractor_config)
    print(json.dumps(metadata))


if __name__ == "__main__":
    main()
