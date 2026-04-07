import json
import logging
import os
import re
from os import PathLike
from pathlib import PurePath, Path
from shutil import move
from typing import Union

from util.datasets_json_util import DatasetsJson
from util.job_util import is_running_outside_verdi_worker_context

logger = logging.getLogger(__name__)


def create_dataset(dataset_id: str, ds_dataset_json: Union[str, PathLike[str]], ds_met_json: Union[str, PathLike[str]] = None, dataset_type: str = None):
    """
    Creates a dataset, including creating the necessary directory and listing.
    :return: the path of the directory representing the new dataset. The name of the directory is prefixed by the dataset ID.
    """
    validate_dataset_type(dataset_type)
    validate_dataset_id(dataset_id, dataset_type)
    return _create_dataset_dir(dataset_id, ds_dataset_json, ds_met_json=ds_met_json, dataset_type=dataset_type)


def _create_dataset_dir(dataset_id: str, ds_dataset_json: Union[str, PathLike[str]], ds_met_json: Union[str, PathLike[str]] = None, dataset_type: str = None):
    """
    Creates the dataset directory, moving the specified input files into it.
    :return: the dataset directory path.
    """
    make_dataset_dir(dataset_id, dataset_type)
    move(ds_dataset_json, PurePath(dataset_id) / f"{dataset_id}.dataset.json")
    if ds_met_json:
        move(ds_met_json, PurePath(dataset_id) / f"{dataset_id}.met.json")
    return Path(dataset_id).resolve()


def make_dataset_dir(dataset_id, dataset_type: str = None):
    validate_dataset_id(dataset_id, dataset_type)
    os.makedirs(dataset_id, exist_ok=True)
    return Path(dataset_id).resolve()


def validate_dataset_type(dataset_type):
    if is_running_outside_verdi_worker_context():
        datasets_json = DatasetsJson()
    else:
        datasets_json = DatasetsJson(file="datasets.json")
    try:
        datasets_json.get(dataset_type)
    except KeyError as e:
        raise Exception(f"Invalid {dataset_type=}. Compare against datasets.json") from e


def is_valid_dataset_type(dataset_type = None):
    if is_running_outside_verdi_worker_context():
        datasets_json = DatasetsJson()
    else:
        datasets_json = DatasetsJson(file="datasets.json")
    try:
        datasets_json.get(dataset_type)
        return True
    except KeyError:
        return False


def validate_dataset_id(dataset_id, dataset_type=None):
    validate_dataset_type(dataset_type)

    datasets_json = DatasetsJson(file="datasets.json")
    dataset_regex = datasets_json.get(dataset_type)["match_pattern"]
    dataset_regex = dataset_regex.removeprefix("/")
    match = re.fullmatch(dataset_regex, dataset_id)
    # match = re.match(dataset_regex, dataset_id)
    if not match:
        raise ValueError(f"{dataset_type=} {dataset_id=} not a valid dataset. Compare against datasets.json")


def create_ds_dataset_json(version=None, label=None, location=None, starttime=None, endtime=None):
    """
    Create the JSON contents of a dataset's dataset.json file, typically named `<dataset_id>.dataset.json`
    :param version: the dataset's version. Typically, a string-encoded float, with an optional "v" prefix. E.g. "1.0" or "v1.0"
    :poram label: Tosca label
    :param location: GeoJSON geometry -formated JSON.
    :param starttime: Used in Tosca table view.
    :param endtime: Used in Tosca table view.
    """
    ds_dataset_dict = {}
    req_fields = {
        **({"version": version} if version else {})
    }
    ds_dataset_dict.update(req_fields)

    opt_fields = {
        **({"label": label} if label else {}),
        **({"location": location} if location else {}),
        **({"starttime": starttime} if starttime else {}),
        **({"endtime":endtime} if endtime else {})
    }
    ds_dataset_dict.update(opt_fields)

    return ds_dataset_dict


def write_ds_met_json(dataset_metadata, dataset_id: str, output_dir: str = None) -> Path:
    output_dir = output_dir if output_dir else "."
    ds_met_json_path = Path(output_dir, f"{dataset_id}.met.json").resolve()
    with ds_met_json_path.open("w") as fp:
        json.dump(dataset_metadata, fp)
    return ds_met_json_path


def write_ds_dataset_json(dataset_metadata: dict, dataset_id: str, output_dir: str = None) -> Path:
    output_dir = output_dir if output_dir else "."
    ds_dataset_json_path = Path(output_dir, f"{dataset_id}.dataset.json").resolve()
    with ds_dataset_json_path.open("w") as fp:
        json.dump(dataset_metadata, fp)
    return ds_dataset_json_path
