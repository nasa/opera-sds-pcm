import os
import json
import hashlib


def validate(sf_context):
    """
    :param sf_context: _context.json of the SciFlo job
    :return:
    """
    context = {}
    if isinstance(sf_context, str):
        context = json.load(open(sf_context, 'r'))
    elif isinstance(sf_context, dict):
        context = sf_context

    work_dir = os.path.dirname(sf_context)
    files = [os.path.basename(
        f) for f in context['job_specification']['params'][0]['value']]
    checksums = context['checksum']
    for i, path in enumerate(files):
        checksum = checksums[i]
        abs_path = os.path.join(work_dir, path)
        validate_checksum(abs_path, checksum)


def validate_checksum(filepath, checksum):
    """
    Validates checksum of a file with the given checksum
    :param checksum:
    :param filepath
    :return:
    """
    observed_checksum = hashlib.md5(open(filepath, 'rb').read()).hexdigest()
    if checksum != observed_checksum:
        raise ValueError("Invalid checksum. observed:{0}, submitted:{1}".format(
            observed_checksum, checksum))
