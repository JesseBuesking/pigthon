""" Implements the base class that unit tests should inherit from. """


from genericpath import exists
import logging.config
from os.path import normpath, dirname
import unittest
import yaml


class TestBase(unittest.TestCase):
    """ Base class for unit tests. """

    @classmethod
    def setUpClass(cls):
        """ Sets up the class. """
        path = normpath(dirname(dirname(__file__)) + '/conf/logging.yaml')
        if exists(path):
            with open(path, 'rt') as f:
                config = yaml.load(f.read())
                logging.config.dictConfig(config)
