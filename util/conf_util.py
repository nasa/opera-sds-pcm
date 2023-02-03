#!/usr/bin/env python
from __future__ import absolute_import
from builtins import object
import os
import re
import json
from typing import Optional

import yaml
import logging

# Ignore if import fails. Some of our test scripts leverage other classes that are found in this file where yamale
# isn't needed. Furthermore, yamale is not installed as part of HySDS Core.
try:
    import yamale
except ImportError:
    pass

from .os_util import norm_path
from jinja2 import Environment, FileSystemLoader

log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

# have yaml parse regular expressions
yaml.SafeLoader.add_constructor(
    u"tag:yaml.org,2002:python/regexp", lambda l, n: re.compile(l.construct_scalar(n))
)


class YamlConfEncoder(json.JSONEncoder):
    """Custom encoder for YamlConf."""

    def default(self, obj):
        if isinstance(obj, type(re.compile(r""))):
            return obj.pattern
        return super(YamlConfEncoder, self).default(obj)


class YamlConfError(Exception):
    """Exception class for YamlConf class."""

    pass


class YamlConf(object):
    """YAML configuration class."""

    def __init__(self, file: str):
        """Construct YamlConf instance.

        :param file: filepath to the YAML file.
        """

        logger.info("file: {}".format(file))
        self._file = file
        with open(self._file) as f:
            self._cfg = yaml.safe_load(f)

    @property
    def file(self):
        return self._file

    @property
    def cfg(self):
        return self._cfg

    def get(self, key):
        try:
            return self._cfg[key]
        except KeyError:
            raise YamlConfError

    def __repr__(self):
        return json.dumps(self._cfg, cls=YamlConfEncoder, indent=2)


class SettingsConf(YamlConf):
    """Settings YAML configuration class."""

    def __init__(self, file: Optional[str] = None):
        """Construct SettingsConf instance.

        :param file: filepath to the settings config file. Defaults to "../conf/settings.yaml", relative to this module.
        """

        if file is None:
            file = norm_path(
                os.path.join(os.path.dirname(__file__), "..", "conf", "settings.yaml")
            )
        super(SettingsConf, self).__init__(file)


class RunConfig(object):
    """PGE run configuration class."""

    def __init__(self, rc_data, template_type):
        """
        Construct RunConfig instance.

        :param rc_data: Run Config content.
        :param template_type: The template type to use.
        """
        self._rc_data = rc_data
        if template_type is None:
            raise ValueError("Must specify a template type.")

        tmpl_file_dir = norm_path(os.path.join(os.path.dirname(__file__), "..", "conf"))
        file_loader = FileSystemLoader(tmpl_file_dir)
        env = Environment(loader=file_loader)
        template = env.get_template(
            "RunConfig.yaml.{}.jinja2.tmpl".format(template_type)
        )
        self._template_type = template_type
        self._rendered_rc = template.render(data=rc_data)

    def validate(self, rc_file, template_type):

        try:
            schema_file = os.path.join(os.path.dirname(__file__), "..", "conf", "schema",
                                       "RunConfig_schema.{}.yaml".format(template_type))
            schema = yamale.make_schema(schema_file)
            # Create a Data object
            data = yamale.make_data(rc_file)
            yamale.validate(schema, data)
        except yamale.YamaleError as e:
            logger.error(e.message)
            raise RuntimeError(e.message)
        return True

    def dump(self, output_file=None, validate=True):
        """
        Dump rc.xml contents.
        ::param output_file: Output file. If None, contents will go to
        standard out.
        ::param validate: Flag to do validation of a generated rc.xml file.
        """
        if output_file is not None:
            with open(output_file, "w") as f:
                f.write("{}\n".format(self._rendered_rc))
            if validate:
                self.validate(output_file, self._template_type)
        else:
            return self._rendered_rc


class PGEOutputsConf(YamlConf):
    """PGE Output YAML configuration class. """

    def __init__(self, file=None):
        """ Construct PGEOutputConf instance. """
        if file is None:
            file = norm_path(
                os.path.join(
                    os.path.dirname(__file__), "..", "conf", "pge_outputs.yaml"
                )
            )
        super(PGEOutputsConf, self).__init__(file)
