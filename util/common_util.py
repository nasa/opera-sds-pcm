import re
import datetime
import json
import os
import backoff

from commons.constants import product_metadata as pm
from commons.logger import logger

INCOMPATIBLE_TIMESTAMP_RE = re.compile(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{6})?)\d+(Z?)$')


def convert_datetime(datetime_obj, strformat="%Y-%m-%dT%H:%M:%S.%fZ"):
    """
    Converts from a datetime string to a datetime object or vice versa
    """
    if isinstance(datetime_obj, datetime.datetime):
        return datetime_obj.strftime(strformat)
    return datetime.datetime.strptime(str(datetime_obj), strformat)


def to_datetime(input_object, strformat="%Y-%m-%dT%H:%M:%S.%fZ"):
    """
    Takes as input either a datetime object or an ISO datetime string in UTC
    and returns a datetime object
    """
    if isinstance(input_object, str):
        return convert_datetime(input_object, strformat)
    if not isinstance(input_object, datetime.datetime):
        raise ValueError("Do not know how to convert type {} to datetime".format(
                         str(type(input_object))))
    return input_object


def get_product_metadata(product_metadata):
    if "metadata" in product_metadata:
        metadata = product_metadata.get("metadata")
    else:
        metadata = product_metadata

    return metadata


def get_working_dir(work_unit_filename="workunit.json"):
    # read in SciFlo work unit json file and extract the working directory
    work_unit_file = os.path.abspath(work_unit_filename)

    with open(work_unit_file) as f:
        work_unit = json.load(f)

    working_dir = os.path.dirname(work_unit['args'][0])

    return working_dir


def lower_keys(x):
    if isinstance(x, list):
        return [lower_keys(v) for v in x]
    elif isinstance(x, dict):
        return dict((k.lower(), lower_keys(v)) for k, v in x.items())
    else:
        return x


def get_latest_product_sort_list():
    sort_list = [
        "metadata.{}.keyword:desc".format(pm.COMPOSITE_RELEASE_ID),
        "metadata.{}:desc".format(pm.PRODUCT_COUNTER)
    ]
    return sort_list


def get_data_date_times(records, begin_time_key, end_time_key):
    """
    Gets the min max date times of the inputs. In the case of multiple inputs, it'll return
    the min range begin time and the max range end time.

    :return:
    """
    min_begin_date_time = None
    max_end_date_time = None

    for record in records:
        metadata = record.get("_source", {}).get("metadata", {})
        try:
            rbt = metadata.get(begin_time_key, None)
            rbt = convert_datetime(rbt)
        except Exception:
            raise RuntimeError('{} does not exist in the metadata of this record: {}'.format(
                begin_time_key, json.dumps(metadata, indent=2)))
        try:
            ret = metadata.get(end_time_key, None)
            ret = convert_datetime(ret)
        except Exception:
            raise RuntimeError('{} does not exist in the product_metadata of the context: {}'.format(
                end_time_key, json.dumps(metadata, indent=2)))

        if min_begin_date_time is None:
            min_begin_date_time = rbt
        else:
            if rbt < min_begin_date_time:
                min_begin_date_time = rbt

        if max_end_date_time is None:
            max_end_date_time = ret
        else:
            if ret > max_end_date_time:
                max_end_date_time = ret

    return convert_datetime(min_begin_date_time), convert_datetime(max_end_date_time)


def get_source_includes():
    # Default source_includes so we don't return everything from a query
    source_includes = [
        "id",
        "_id",
        "urls",
        "metadata.{}".format(pm.FILE_NAME),
        "metadata.{}".format(pm.PRODUCT_RECEIVED_TIME),
        "starttime",
        "endtime"
    ]
    return source_includes


def fix_timestamp(ts):
    """Detect incompatible ISO8601 timestamp, truncate it to support
       python3 (microsecond - 6 digits) and ElasticSearch 7.1 (microsecond)
       and return. Otherwise, the input value is passed through."""

    if match := INCOMPATIBLE_TIMESTAMP_RE.search(ts):
        return "{}{}".format(*match.groups())
    else:
        return ts


def create_expiration_time(latency):
    """
    Calculates an expiration time based on a given latency in minutes.

    :param latency:
    :return: a datetime string in ISO 8601 format.
    """
    return convert_datetime(datetime.datetime.utcnow() + datetime.timedelta(minutes=latency))


def create_state_config_dataset(dataset_name, metadata, start_time, end_time=None, geojson=None,
                                expiration_time=None):
    """
    Creates a state config dataset.

    :param dataset_name: dataset name
    :param metadata: metadata associated with the state config
    :param start_time: start time
    :param end_time: optional end time
    :param geojson: optional geojson location coordinates
    :param expiration_time: optional expiration time

    :return:
    """
    os.makedirs(dataset_name)

    met_json_file = "{}/{}.met.json".format(dataset_name, dataset_name)
    logger.info("Creating {} with the following content: {}".format(met_json_file, json.dumps(metadata, indent=2)))
    with open(met_json_file, "w") as f:
        json.dump(metadata, f, indent=2)

    dataset_info = {
        "version": "1",
        pm.STATE_CONFIG_CREATION_TIME: convert_datetime(datetime.datetime.utcnow()),
        pm.START_TIME: start_time
    }
    if end_time:
        dataset_info[pm.END_TIME] = end_time

    if geojson:
        dataset_info[pm.LOCATION] = geojson

    if expiration_time:
        dataset_info[pm.EXPIRATION_TIME] = expiration_time

    ds_json_file = "{}/{}.dataset.json".format(dataset_name, dataset_name)
    logger.info("Creating {} with the following content: {}".format(ds_json_file,
                                                                    json.dumps(dataset_info, indent=2)))
    with open(ds_json_file, "w") as f:
        json.dump(dataset_info, f, indent=2)


@backoff.on_exception(
    backoff.expo, Exception, max_value=13, max_time=34
)
def backoff_wrapper(func, *args, **kwargs):
    """
    Run a function wrapped in exponential backoff.

    :param func: function or method object
    :param args: args to pass to function
    :param kwargs: keyword args to pass to function

    :return:
    """
    return func(*args, **kwargs)


def create_info_message_files(msg=None, msg_details=None):
    """
    Function that will create the _alt_msg.txt and _alt_msg_details.txt
    files for PCM Core to pick up and display out to the UI.

    :param msg: The short message. Should be less than 35 characters.
    Otherwise, UI will truncate it.
    :param msg_details: The message details. This is intended to give a more
     descriptive informational message.
    :return:
    """
    if msg:
        with open('_alt_msg.txt', 'w') as f:
            if isinstance(msg, list):
                for m in msg:
                    f.write("%s\n" % str(m))
            else:
                f.write("%s\n" % str(msg))

    if msg_details:
        with open('_alt_msg_details.txt', 'w') as f:
            f.write("%s\n" % msg_details)
