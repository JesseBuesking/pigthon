""" Unit tests for util.cmd. """


from pigthon.util import cmd
from test.test_base import TestBase


class Test(TestBase):
    """ Test cases for util.cmd. """

    def test_cmd_safe_quote_simple(self):
        """ Tests safe quoting on a simple string. """
        value = cmd.safe_quote('value')
        self.assertEqual('"value"', value)

    def test_cmd_safe_quote_has_double_quote(self):
        """ Tests safe quoting on a string containing a double quote. """
        value = cmd.safe_quote('set key "value";')
        self.assertEqual('"set key \'"\'"\'value\'"\'"\';"', value)
