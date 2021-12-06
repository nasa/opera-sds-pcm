import os
import re
import click
import requests
import time
import json
import yaml

from datetime import datetime, timedelta
from lxml import etree

from hysds.celery import app


GRQ_URL = ":".join(app.conf['GRQ_URL'].split(":")[0:-1])


def get_json_metadata(response_text):
    json_data = json.loads(response_text)
    if "HEADER" in json_data:
        return json_data["HEADER"]
    else:
        return json_data["header"]


def get_xml_metadata(response_text):
    try:
        root = etree.fromstring(response_text)
    except ValueError:
        root = bytes(bytearray(response_text, encoding='utf-8'))
        root = etree.XML(root)
    except Exception as e:
        raise Exception(e)

    if root.find("HEADER") is not None:
        header = root.find("HEADER")
    else:
        header = root.find("header")

    metadata = {}
    for item in header.iterchildren():
        metadata[item.tag] = item.text

    return metadata


def write_oad_report(metadata, response_text, format_type):
    file_name = "oad_{}_{}_{}.{}".format(
        metadata["CONTENT_TYPE"],
        metadata["START_DATETIME"],
        metadata["END_DATETIME"],
        format_type
    )

    with open(file_name, "w") as f:
        f.write(response_text)

    return file_name


def write_dar_report(metadata, response_text, format_type):
    file_name = "dar_{venue}_{time_of_report}.{format_type}".format(
        venue=metadata["venue"],
        time_of_report=metadata["time_of_report"],
        format_type=format_type
    )

    with open(file_name, "w") as f:
        f.write(response_text)

    return file_name


def create_report(format_type, start, end, processing_mode, report_name, vcid, venue="local", use_click=True):
    reports_url = GRQ_URL + ":8876/1.0/reports/{}?startDateTime={}&endDateTime={}&mime={}&processingMode={}&venue={}" \
                            "&vcid={}".format(report_name, start, end, format_type, processing_mode, venue, vcid)

    response = requests.get(reports_url)  # wait for the response
    # waited = response.elapsed  # retrieved response
    time.sleep(0.5)  # TODO: why are we sleeping here? the code wont finish until the request is finished (not async)

    # TODO: no point in this while loop, it will never run b/c waited == response.elapsed
    # while waited < response.elapsed:
    #     if use_click is True:
    #         click.echo(".", nl=False)
    #     waited = response.elapsed
    #     time.sleep(0.5)

    if use_click is True:
        click.echo(".", nl=True)

    if format_type == "xml":
        metadata = get_xml_metadata(response.text)
    elif format_type == "json":
        metadata = get_json_metadata(response.text)
    else:
        if use_click is True:
            click.echo("Not Implemented")
        raise NotImplementedError("%s data format not implemented" % format_type)

    return response, metadata


@click.command()
@click.option('--format_type', default="xml", help='format to return the result (xml or json)')
@click.option('--start', default=datetime.utcnow().isoformat(), type=str,
              help='UTC start datetime in iso format (YYYY-MM-DDTHH:mm:ssZ)')
@click.option('--end', default=(datetime.utcnow() + timedelta(days=365 * 3)).isoformat(), type=str,
              help='UTC end datetime in iso format (YYYY-MM-DDTHH:mm:ssZ)')
@click.option('--processing_mode', default="", help='Try and filter by processingMode')
@click.option('--vcid', default="", help='grab data with a specific vcid')
@click.argument('report_name')
def get_report(format_type, start, end, processing_mode, report_name, vcid):
    """
    Reporting CLI

    Report Options:
    - ObservationAccountabilityReport\n
    - DataAccountabilityReport
    """
    current_dir = os.getcwd()
    # could not open file simply with ~/.sds :(
    # go to the config location
    path = os.path.expanduser('~/mozart/etc/settings.yaml')
    venue = "local"  # get the venue

    with open(path, "r") as conf:
        yaml.SafeLoader.add_constructor(
            u'tag:yaml.org,2002:python/regexp', lambda l, n: re.compile(l.construct_scalar(n)))
        config = yaml.load(conf, Loader=yaml.SafeLoader)
        if "VENUE" in config and config["VENUE"]:
            venue = config["VENUE"]

    os.chdir(current_dir)

    if "Z" not in start:
        start = "{}Z".format(start)

    if "Z" not in end:
        end = "{}Z".format(end)

    response, metadata = create_report(format_type, start, end, processing_mode, report_name, vcid, venue)

    if response.status_code == "501":
        raise NotImplementedError("%s report not implemented" % report_name)
    if report_name == "ObservationAccountabilityReport":
        filename = write_oad_report(metadata, response.text, format_type)
        click.echo("wrote out %s" % filename)
    elif report_name == "DataAccountabilityReport":
        filename = write_dar_report(metadata, response.text, format_type)
        click.echo("wrote out %s" % filename)
    else:
        raise NotImplementedError("%s report not implemented" % report_name)


if __name__ == '__main__':
    get_report()
