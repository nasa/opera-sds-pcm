from typing import Dict


def find_param(job_json_dict, param_name) -> Dict:
    for param in job_json_dict["context"]["job_specification"]["params"]:
        if param["name"] == param_name:
            return param


def find_param_value(job_json_dict, param_name):
    param = find_param(job_json_dict, param_name)
    return param["value"]


def get_pge_container_image_name(job_json_dict) -> str:
    # e.g. "opera_pge/dswx_hls:1.0.0-rc.1.0"
    return job_json_dict["context"]["job_specification"]["dependency_images"][0]["container_image_name"]


def get_pge_container_image_version(job_json_dict):
    # e.g. "opera_pge/dswx_hls:1.0.0-rc.1.0"
    container_image_name: str = get_pge_container_image_name(job_json_dict)

    # e.g. "1.0.0-rc.1.0"
    return container_image_name.split(":")[1]


def get_pcm_version(job_json_dict) -> str:
    return job_json_dict["context"]["container_specification"]["version"]
