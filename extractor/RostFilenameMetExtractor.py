"""
This extractor basically does the same thing as the FilenameRegexMetExtractor. Only difference
is that this will convert the Kind value extracted from ROST files from hex to decimal.

"""
import os
import json
import sys

from extractor.FilenameRegexMetExtractor import FilenameRegexMetExtractor


class RostFilenameMetExtractor(FilenameRegexMetExtractor):
    def extract(self, product, match_pattern, extractor_config):
        metadata = super().extract(product, match_pattern, extractor_config)

        if "Kind" in metadata:
            kind_val = metadata["Kind"]

            if isinstance(kind_val, int):
                kind_val = str(kind_val)
            elif isinstance(kind_val, float):
                kind_val = str(kind_val)

            kind_val_base_10 = int(kind_val, 16)
            metadata["Kind"] = kind_val_base_10
        if "ValidityDate" in metadata:
            validity_val = metadata["ValidityDate"]

            if isinstance(validity_val, int):
                validity_val = str(validity_val)
            elif isinstance(validity_val, float):
                validity_val = str(validity_val)
            metadata["validity"] = validity_val

        if "CycleNumber" in metadata:
            cycle_val = metadata["CycleNumber"]

            if isinstance(cycle_val, int):
                cycle_val = str(cycle_val)
            elif isinstance(cycle_val, float):
                cycle_val = str(cycle_val)
            metadata["cycle"] = cycle_val

        if "FileCorrelationId" in metadata:
            file_correlation_id = metadata["FileCorrelationId"]
            metadata["file_correlation_id"] = file_correlation_id

        return metadata


def main():
    """
    Main entry point
    """
    product = os.path.abspath(sys.argv[1])
    match_pattern = sys.argv[2]
    extractor_config = json.loads(sys.argv[3])
    extractor = RostFilenameMetExtractor()
    metadata = extractor.extract(product, match_pattern, extractor_config)
    print(json.dumps(metadata))


if __name__ == "__main__":
    main()
