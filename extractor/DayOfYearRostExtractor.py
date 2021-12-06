'''
Metadata extractor that will convert the day of year to a start and end time.
This inherits from the RostFilenameMetExtractor so that we can do the
hex to decimal conversion of the Kind value extracted from the ROST file names.

@author: mcayanan
'''
from __future__ import print_function

import sys
import os
import json

from extractor.RostFilenameMetExtractor import RostFilenameMetExtractor

from util.common_util import convert_datetime

DATE_KEY = "Date_Key"
START_DATE_TIME_KEY = "Start_Date_Time_Key"
END_DATE_TIME_KEY = "End_Date_Time_Key"


class DayOfYearRostExtractor(RostFilenameMetExtractor):
    def extract(self, product, match_pattern, extractor_config):
        metadata = {}
        core_met = super(DayOfYearRostExtractor, self).extract(product, match_pattern, extractor_config)
        metadata.update(core_met)

        date_value = metadata.get(extractor_config.get(DATE_KEY), None)
        if date_value is None:
            raise RuntimeError("Could not find {} in the metadata: {}".format(DATE_KEY, json.dumps(metadata,
                                                                                                   indent=2)))

        date = convert_datetime(date_value, RostFilenameMetExtractor._ISO_DATE_PATTERN)

        metadata['Year'] = date.strftime('%Y')
        metadata['Month'] = date.strftime('%m')
        metadata['Day'] = date.strftime('%d')
        metadata[extractor_config.get(START_DATE_TIME_KEY)] = convert_datetime(
            date.replace(hour=0, minute=0, second=0, microsecond=0))
        metadata[extractor_config.get(END_DATE_TIME_KEY)] = convert_datetime(
            date.replace(hour=23, minute=59, second=59, microsecond=999999))

        return metadata


def main():
    """
    Main entry point
    """
    product = os.path.abspath(sys.argv[1])
    match_pattern = sys.argv[2]
    extractor_config = json.loads(sys.argv[3])
    extractor = DayOfYearRostExtractor()
    metadata = extractor.extract(product, match_pattern, extractor_config)
    print(json.dumps(metadata))


if __name__ == "__main__":
    main()
