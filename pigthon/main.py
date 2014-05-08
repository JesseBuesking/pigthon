""" Manages running pig from python. """


from genericpath import exists
from os import environ
from os.path import normpath
from subprocess import Popen, PIPE
from pigthon.util.ptlog import PtLog
from pigthon.util.yaml_conf import load_yaml
logger = PtLog(__name__)


class PigOptions(object):
    """ Manages command line options for pig script execution. """

    # noinspection PyShadowingBuiltins
    def __init__(self, log4jconf=None, brief=None, check=None, debug=None,
                 execute=None, file=None, embedded=None, help=None,
                 version=None, logfile=None, param_file=None, params=None,
                 dryrun=None, verbose=None, warning=None, exectype=None,
                 stop_on_failure=None, no_multiquery=None, property_file=None):
        """
        :param log4jconf: Log4j configuration file, overrides log conf
        :param brief: brief logging (no timestamps)
        :param check: syntax check
        :param debug: debug level, INFO is default
        :param execute: commands to execute (within quotes)
        :param file: path to the script to execute
        :param embedded: scriptEngine classname or keyword for the ScriptEngine
        :param help: display this message. You can specify topic to get help for
         that topic. properties is the only topic currently supported: -h
         properties
        :param version: display version information
        :param logfile: path to client side log file; default is current working
         directory
        :param param_file: path to the parameter file
        :param params: key value pairs to supply to pig
        :param dryrun: produces script with substituted parameters. Script is
         not executed
        :param verbose: print all error messages to screen
        :param warning: turn warning logging on; also turns warning aggregation
         off
        :param exectype: set execution mode - local|mapreduce, default is
         mapreduce
        :param stop_on_failure: aborts execution on the first failed job;
         default is off
        :param no_multiquery: turn multiquery optimization off; default is on
        :param property_file: Path to property file
        """
        assert log4jconf in {str, unicode, None}
        assert brief in {True, False, None}
        assert check in {True, False, None}
        assert debug in {'DEBUG', 'WARN', 'INFO', None}
        # TODO make sure it's in quotes?
        assert execute in {str, unicode, None}
        assert file in {str, unicode, None}
        assert embedded in {str, unicode, None}
        assert help in {True, False, None}
        assert version in {True, False, None}
        assert logfile in {str, unicode, None}
        assert param_file in {str, unicode, None}
        if isinstance(params, dict):
            for k, v in params.iteritems():
                assert k in {str, unicode}
                assert v in {str, unicode}
        else:
            assert params is None
        assert dryrun in {True, False, None}
        assert verbose in {True, False, None}
        assert warning in {True, False, None}
        assert exectype in {'local', 'mapreduce', None}
        assert stop_on_failure in {True, False, None}
        assert no_multiquery in {True, False, None}
        assert property_file in {str, unicode, None}
        self._options = {
            'log4jconf': log4jconf,
            'brief': brief,
            'check': check,
            'debug': debug,
            'execute': execute,
            'file': file,
            'embedded': embedded,
            'help': help,
            'version': version,
            'logfile': logfile,
            'param_file': param_file,
            'params': params,
            'dryrun': dryrun,
            'verbose': verbose,
            'warning': warning,
            'exectype': exectype,
            'stop_on_failure': stop_on_failure,
            'no_multiquery': no_multiquery,
            'property_file': property_file
        }

    def log4jconf(self):
        """ Log4j configuration file, overrides log conf. """
        return self._options.get('log4jconf', None)

    def brief(self):
        """ Brief logging (no timestamps). """
        return self._options.get('brief', None)

    def check(self):
        """ Syntax check. """
        return self._options.get('check', None)

    def debug(self):
        """ Debug level, INFO is default. """
        return self._options.get('debug', None)

    def execute(self):
        """ Commands to execute (within quotes). """
        return self._options.get('execute', None)

    def file(self):
        """ Path to the script to execute. """
        return self._options.get('file', None)

    def embedded(self):
        """ ScriptEngine classname or keyword for the ScriptEngine. """
        return self._options.get('embedded', None)

    def help(self):
        """
        Display this message. You can specify topic to get help for that topic.
        properties is the only topic currently supported: -h properties.
        """
        return self._options.get('help', None)

    def version(self):
        """ Display version information. """
        return self._options.get('version', None)

    def logfile(self):
        """
        Path to client side log file; default is current working directory.
        """
        return self._options.get('logfile', None)

    def param_file(self):
        """ Path to the parameter file. """
        return self._options.get('param_file', None)

    def params(self):
        """ Key value pairs to supply to pig. """
        return self._options.get('params', None)

    def dryrun(self):
        """
        Produces script with substituted parameters. Script is not executed.
        """
        return self._options.get('dryrun', None)

    def verbose(self):
        """ Print all error messages to screen. """
        return self._options.get('verbose', None)

    def warning(self):
        """ Turn warning logging on; also turns warning aggregation off. """
        return self._options.get('warning', None)

    def exectype(self):
        """ Set execution mode: local|mapreduce, default is mapreduce. """
        return self._options.get('exectype', None)

    def stop_on_failure(self):
        """ Aborts execution on the first failed job; default is off. """
        return self._options.get('stop_on_failure', None)

    def no_multiquery(self):
        """ Turn multiquery optimization off; default is on. """
        return self._options.get('no_multiquery', None)

    def property_file(self):
        """ Path to property file. """
        return self._options.get('propertyFile', None)

    def to_cmd_array(self):
        """
        Converts the options into an array of values to be passed on the
        command line.
        """
        # if help is set, just return the help flag and throw away all other
        # options
        if self.help() is not None and self.help():
            return ['-help']
        if self.version() is not None and self.version():
            return ['-version']


class PigTestOptions(PigOptions):
    """ Manages command line options when running pig tests. """
    pass


class PigTest(object):
    """ Base class for running pig tests. """

    def __init__(self):
        self.pigthon = Pigthon()

    def script(self):
        """ The pig script to test. """
        pass

    def options(self):
        """ The PigTestOptions to use. """
        return None

    def run_script(self):
        """ Runs the pig script test. """
        options = self.options()
        if options is None:
            options = PigTestOptions(file=self.script())
        else:
            options._options['file'] = self.script()
        output, error = self.pigthon.test(options)
        return output, error


class Pigthon(object):
    """ Encapsulates the logic necessary for running pig. """

    def __init__(self, filename=None, is_jar=None):
        self._config = dict()
        if filename is not None:
            self._config = load_yaml(filename)

        if is_jar is not None:
            assert is_jar in {True, False}
            self._config['is_jar'] = is_jar
        else:
            # is a valid jar path set in the environment variable?
            pj = environ.get('PIG_JAR', None)
            if pj is not None and exists(pj):
                self._config['is_jar'] = True

    def run(self, args):
        """
        Runs stuff on the command line.

        :param list args: the arguments to run
        :returns: the output and error output from the command that was ran
        :rtype: tuple(output, error)
        """
        logger.info('Running command: ')
        logger.info(' '.join(args))
        output, error = Popen(
            args,
            stdout=PIPE,
            stderr=PIPE,
            shell=True
        ).communicate()
        return output, error

    def pig_cmd(self):
        """
        Gets the command to call to run pig on the command line.

        :returns: the command for pig
        :rtype: list()
        """
        is_jar = self._config.get('is_jar', None)
        if is_jar is not None and is_jar:
            return ['java', '-jar', normpath(environ.get('PIG_JAR'))]
        else:
            return [environ.get('PIG')]

    def pig(self, options=None):
        """
        Runs pig with the arguments supplied.

        :param options: all of the command line options to supply to the pig
         command
        :type options: PigOptions, PigTestOptions or None
        """
        if options is None:
            options = PigOptions()
        args = self.pig_cmd() + options.to_cmd_array()
        output, error = self.run(args)
        logger.debugHeader('error')
        logger.debug(error)
        logger.debugHeader('output')
        logger.debug(output)
        return output, error

    def test(self, options=None):
        """
        Runs pig with the arguments supplied, defaulting to the defaults for
        running tests.

        :param options: all of the command line options to supply to the pig
         command
        :type options: PigTestOptions or None
        """
        if options is None:
            options = PigTestOptions()
        output, error = self.pig(options)
        return output, error
