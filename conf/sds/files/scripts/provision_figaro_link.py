#!/bin/env python
"""
This script fixes the links in Figaro
"""

import os
import collections
from os.path import expanduser


def fix_data(line):
    if line.startswith("RABBITMQ_ADMIN_URL"):
        return 'RABBITMQ_ADMIN_URL = "https://{{ MOZART_RABBIT_FQDN }}:15673"'
    elif line.startswith("ES_HEAD_URL"):
        return 'ES_HEAD_URL = "https://{{ MOZART_ES_FQDN }}/es/_plugin/head/"'
    elif line.startswith("ES_KOPF_URL"):
        return 'ES_KOPF_URL = "https://{{ MOZART_ES_FQDN }}/es/_plugin/kopf/"'
    elif line.startswith("FLOWER_URL"):
        return 'FLOWER_URL = "http://{{ MOZART_FQDN }}/flower/"'
    else:
        return line


def parse_file(data):
    present_key = "Default"
    data_dict = collections.OrderedDict()
    for line in data:
        line = line.strip()
        if line.startswith("#"):
            present_key = line.split("#")[1]
        elif line:
            data_list = []
            if present_key in list(data_dict.keys()):
                data_list = data_dict[present_key]
            data_list.append(fix_data(line))
            data_dict[present_key] = data_list

    return data_dict


def write_file(filename, data):
    with open(filename, "w") as fw:
        for key, value in list(data.items()):
            if key != "Default":
                fw.write("%s\n" % ("#" + key))
            for line in value:
                fw.write("%s\n" % line)
            fw.write("\n")


def read_file(filename):
    with open(filename, "r") as fp:
        return parse_file(fp)


home = expanduser("~")

figaro_settings_tmpl_file = os.path.join(
    home, ".sds", "files", "figaro_settings.cfg.tmpl"
)
print(figaro_settings_tmpl_file)
data = read_file(figaro_settings_tmpl_file)
write_file(figaro_settings_tmpl_file, data)
