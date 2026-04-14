from pathlib import PurePath
from typing import Optional
from util.os_util import norm_path
from urllib.parse import urlparse
import os
import json


class DatasetsJson:
    """Parses conf/sds/files/datasets.json and makes access easier"""

    def __init__(self, file: Optional[str] = None):
        """Constructor. Parses datasets.json

        :param file: filepath to datasets.json. Defaults to checking Mozart, Verdi, 
                     and then relative paths.
        """

        # Intercept if file is None OR if it's the hardcoded string from dataset_util.py
        if file is None or (file == "datasets.json" and not os.path.exists(file)):
            mozart_path = os.path.expanduser("~/mozart/ops/opera-pcm/conf/sds/files/datasets.json")
            verdi_path = os.path.expanduser("~/verdi/etc/datasets.json")
            
            if os.path.exists(mozart_path):
                file = mozart_path
            elif os.path.exists(verdi_path):
                file = verdi_path
            else:
                # Fallback to the original relative path logic
                file = norm_path(
                    os.path.join(os.path.dirname(__file__), "..", "conf", "sds", "files", "datasets.json")
                )

        # Open up the datasets.json file and create a dictionary of datasets keyed by dataset type
        with open(file) as f:
            datasets = json.load(f)["datasets"]
            self._datasets_json = {dataset["type"]: dataset for dataset in datasets}

    def get(self, key):
        '''Returns the dataset with the given key. Key is the dataset type.'''
        return self._datasets_json[key]


# TODO: Refactor so that all the functions below are methods of DatasetsJson

def find_publish_location_s3(datasets_json, dataset_type):
    """Example location: "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
    """
    publish_location = None
    for dataset in datasets_json["datasets"]:
        if dataset["type"] == dataset_type:
            # Example location: "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
            publish_location = dataset["publish"]["location"]
            break

    if publish_location is None:
        raise Exception(f"s3 bucket not found for dataset type {dataset_type}")
    return PurePath(publish_location)


def find_dataset_s3_endpoint(datasets_json, dataset_type):
    """Example location: "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
    """
    publish_location = find_publish_location_s3(datasets_json, dataset_type)
    return PurePath(publish_location).parts[1]


def find_s3_bucket(datasets_json, dataset_type):
    """Example location: "s3://{{ DATASET_S3_ENDPOINT }}:80/{{ DATASET_BUCKET }}/products/{id}"
    """
    publish_location = find_publish_location_s3(datasets_json, dataset_type)
    return PurePath(publish_location).parts[2]


def find_region(datasets_json, dataset_type):
    """Extracts the region from the publish location. See find_publish_location_s3
    """
    publish_location = find_publish_location_s3(datasets_json, dataset_type)
    region_fragment = PurePath(publish_location.parts[1].split()[0]).with_suffix("").with_suffix("")  # e.g. "s3-us-west-2"
    return str(region_fragment).removeprefix("s3-")


def find_s3_url(datasets_json, dataset_type):
    """Example url: "http://{{ DATASET_BUCKET }}.{{ DATASET_S3_WEBSITE_ENDPOINT }}/products/{id}"
    """
    s3_publish_url = None
    for dataset in datasets_json["datasets"]:
        if dataset["type"] == dataset_type:
            url: str
            for url in dataset["publish"]["urls"]:
                if url.startswith("http"):
                    s3_publish_url = url
                    break

    if s3_publish_url is None:
        raise Exception("No s3 URL found in datasets.json")
    return s3_publish_url


def normalize_to_s3_uri(url: str) -> str:
    """
    Convert various S3 URL formats into canonical: s3://bucket/key

    Handles:
    - s3://bucket/key
    - s3://s3-region.amazonaws.com/bucket/key
    - https://s3-region.amazonaws.com/bucket/key
    - malformed cases like 3:// or s3:///

    Returns:
        s3://bucket/key
    """
    if not url:
        raise ValueError("Empty URL")

    # --- Fix common malformed prefixes ---
    url = url.strip()

    if url.startswith("3://"):
        url = "s" + url

    if url.startswith("s3:///"):
        url = url.replace("s3:///", "s3://", 1)

    # --- Parse ---
    parsed = urlparse(url)

    # Case 1: endpoint-style (s3.amazonaws.com/bucket/key)
    if "amazonaws.com" in parsed.netloc:
        parts = parsed.path.lstrip("/").split("/", 1)
        if not parts or parts[0] == "":
            raise ValueError(f"Invalid S3 path: {url}")

        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""

    # Case 2: already clean (s3://bucket/key)
    else:
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

    return f"s3://{bucket}/{key}" if key else f"s3://{bucket}"
