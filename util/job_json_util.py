def find_param(job_json_dict, param_name):
    for param in job_json_dict["context"]["job_specification"]["params"]:
        if param["name"] == param_name:
            return param


def find_param_value(job_json_dict, param_name):
    param = find_param(job_json_dict, param_name)
    return param["value"]
