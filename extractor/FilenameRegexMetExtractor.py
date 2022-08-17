"""
Filename Metadata Extractor. This extracts metadata from the product
filename.

@author: mcayanan
"""
from __future__ import print_function

from builtins import str
import sys
import os
import json
import re
from datetime import datetime
from extractor.CoreMetExtractor import CoreMetExtractor
from util.type_util import set_type

from commons.constants import product_metadata as pm


class FilenameRegexMetExtractor(CoreMetExtractor):
    _FILE_DATE_PATTERN = "%Y%m%d"
    _ISO_DATE_PATTERN = "%Y-%m-%d"

    _NEN_FILE_DATETIME_PATTERN = "%Y%j%H%M%S%f"
    _FILE_DATETIME_PATTERN = "%Y%m%dT%H%M%S"
    _ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"

    def extract(self, product, match_pattern, extractor_config):
        fill_time_field = False
        file_date_time_patterns = [
            self._FILE_DATETIME_PATTERN,
            self._ISO_DATETIME_PATTERN,
            self._NEN_FILE_DATETIME_PATTERN,
        ]
        file_date_patterns = [self._FILE_DATE_PATTERN, self._ISO_DATE_PATTERN]
        if "Date_Time_Patterns" in extractor_config:
            file_date_time_patterns = extractor_config["Date_Time_Patterns"]
        if "Date_Patterns" in extractor_config:
            file_date_patterns = extractor_config["Date_Patterns"]
        if "fill_time_field" in extractor_config:
            if extractor_config["fill_time_field"]:
                fill_time_field = True
        metadata = {}
        core_met = super(FilenameRegexMetExtractor, self).get_core_metadata(product)
        metadata.update(core_met)
        pattern = re.compile(match_pattern)
        match = pattern.search(product)
        if match:
            for key in list(match.groupdict().keys()):
                value = match.groupdict()[key]
                date = None
                if (
                    key.endswith("DateTime")
                    or key.endswith("Time")
                    or key.endswith("Time_Tag")
                    or (key in extractor_config.get("Date_Time_Keys", []))
                ):
                    for datetimePattern in file_date_time_patterns:
                        try:
                            date = datetime.strptime(value, datetimePattern)
                            break
                        except ValueError:
                            """ Ignore. Pattern does not match the value"""
                    if date is None:
                        message = (
                            "Cannot parse datetime value '{}' "
                            "from '{}' with the following patterns: {}".format(
                                value,
                                os.path.basename(product),
                                str(file_date_patterns),
                            )
                        )
                        raise ValueError(message)
                    else:
                        if fill_time_field:
                            if "Begin" in key:
                                date = date.replace(hour=0, minute=0, second=0)
                            elif "End" in key:
                                date = date.replace(
                                    hour=23, minute=59, second=59, microsecond=999999
                                )
                            else:
                                raise ValueError(
                                    "Cannot determine how to fill in the time "
                                    "field for metadata key " + key
                                )

                        metadata[key] = date.strftime(self._ISO_DATETIME_PATTERN) + "Z"
                        if key == pm.RANGE_BEGINNING_DATE_TIME:
                            metadata[pm.RANGE_BEGINNING_YEAR] = date.year
                            metadata[pm.RANGE_BEGINNING_MONTH] = "%02d" % date.month
                            metadata[pm.RANGE_BEGINNING_DAY] = "%02d" % date.day
                        if key == pm.VALIDITY_START_DATE_TIME:
                            metadata[pm.VALIDITY_START_YEAR] = date.year
                            metadata[pm.VALIDITY_START_MONTH] = "%02d" % date.month
                            metadata[pm.VALIDITY_START_DAY] = "%02d" % date.day
                elif key.endswith("Date"):
                    for datePattern in file_date_patterns:
                        try:
                            date = datetime.strptime(value, datePattern)
                            break
                        except ValueError:
                            """Ignore. Pattern does not match the value"""
                    if date is None:
                        message = (
                            "Cannot parse date value '{}' from product "
                            "'{}' with the following patterns: {}".format(
                                value,
                                os.path.basename(product),
                                str(file_date_patterns),
                            )
                        )
                        raise ValueError(message)
                    else:
                        metadata[key] = date.strftime(self._ISO_DATE_PATTERN)
                elif key == "Year":
                    if len(value) == 2:
                        metadata[key] = int("20" + value)
                    else:
                        metadata[key] = int(value)
                elif extractor_config.get("Dataset_Version_Key") and key == extractor_config.get("Dataset_Version_Key"):
                    metadata["dataset_version"] = value
                else:
                    if value:
                        metadata[key] = set_type(value)

        return metadata


def main():
    """
    Main entry point
    """
    product = os.path.abspath(sys.argv[1])
    match_pattern = sys.argv[2]
    extractor_config = json.loads(sys.argv[3])
    extractor = FilenameRegexMetExtractor()
    metadata = extractor.extract(product, match_pattern, extractor_config)
    print(json.dumps(metadata))


if __name__ == "__main__":
    main()
