""" Unit tests for the main pigthon file. """


from os.path import abspath, normpath, dirname
from test.test_base import TestBase
from pigthon.main import Pigthon, PigOptions


class Test(TestBase):
    """ Test cases for Pigthon. """

    def test_dir(self):
        """ Gets the full path to the root test directory. """
        return normpath(dirname(abspath(__file__)))

    def config(self):
        """ Gets the path to the test config file. """
        return normpath(self.test_dir() + '/data/test.yaml')

    def test_load_config(self):
        """ Tests loading a config file. """
        p = Pigthon(self.config())
        self.assertTrue('key' in p._config)
        self.assertEqual('value', p._config['key'])

    def test_run(self):
        """ Tests the run command. """
        p = Pigthon(self.config())
        output, error = p.run([
            'ls',
            normpath(self.test_dir() + '/empty')
        ])
        self.assertEqual('', error)
        self.assertEqual('empty.txt', output.strip())

    def test_pig_help(self):
        """ Test requesting the help menu from pig. """
        po = PigOptions(help=True)
        p = Pigthon()
        output, error = p.pig(po)
        self.assertTrue('Display this message' in output)

    def test_cmd_array_local(self):
        """ A very simple cmd array test for PigOptions. """
        po = PigOptions(exectype='local')
        self.assertEqual(['-exectype', 'local'], po.to_cmd_array())

    def test_cmd_array_custom_param(self):
        """ A more complex cmd array test for PigOptions. """
        po = PigOptions(params={'cake': 'IMPORT "/path/to/import";'})
        self.assertEqual(
            ['-param', 'cake="IMPORT \'"\'"\'/path/to/import\'"\'"\';"'],
            po.to_cmd_array()
        )
