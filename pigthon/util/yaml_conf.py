""" Logic for working with yaml. """


from dateutil import parser
from yaml import load
import yaml


try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    # noinspection PyUnresolvedReferences
    from yaml import Loader, Dumper


def _datetime_constructor(_, node):
    """
    Overload for handling datetimes with support of timezones.

    See: http://stackoverflow.com/a/13295663/435460

    :param _:
    :param node:
    """
    return parser.parse(node.value)

yaml.add_constructor(u'tag:yaml.org,2002:timestamp', _datetime_constructor)


def load_yaml(filename):
    """
    Loads a yaml file.

    :param str filename: the name of the yaml file to load
    """
    with open(filename, 'r') as yaml_file:
        return load(yaml_file, Loader=Loader)
