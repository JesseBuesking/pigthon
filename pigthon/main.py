""" Manages running pig from python. """


from genericpath import exists
from os import environ, name
from pigthon.util import cmd
from pigthon.util.processreader import ProcessReader
from pigthon.util.ptlog import PtLog
from pigthon.util.yaml_conf import load_yaml
from subprocess import Popen, PIPE
import os
import sys


logger = PtLog(__name__)


ON_POSIX = 'posix' in sys.builtin_module_names


class PigOptions(object):
    """ Manages command line options for pig script execution. """

    # noinspection PyShadowingBuiltins
    def __init__(self, log4jconf=None, brief=None, check=None, debug=None,
                 execute=None, file=None, embedded=None, help=None,
                 version=None, logfile=None, param_file=None, params=None,
                 dryrun=None, verbose=None, warning=None, exectype=None,
                 stop_on_failure=None, no_multiquery=None,
                 property_file=None, dparams=None):
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
        :param dparams: key value pairs to supply to pig as -D params
        """
        assert type(log4jconf) in {str, unicode} or log4jconf is None
        assert brief in {True, False, None}
        assert check in {True, False, None}
        assert debug in {'DEBUG', 'WARN', 'INFO', None}
        assert type(execute) in {str, unicode} or execute is None
        assert type(file) in {str, unicode} or file is None
        assert type(embedded) in {str, unicode} or embedded is None
        assert help in {True, False, None}
        assert version in {True, False, None}
        assert type(logfile) in {str, unicode} or logfile is None
        assert type(param_file) in {str, unicode} or param_file is None
        if isinstance(params, dict):
            for k, v in params.iteritems():
                assert type(k) in {str, unicode}
        else:
            assert params is None, 'params must be a dictionary or None, ' \
                                   'but is {}'.format(type(params))
        assert dryrun in {True, False, None}
        assert verbose in {True, False, None}
        assert warning in {True, False, None}
        assert exectype in {'local', 'mapreduce', None}
        assert stop_on_failure in {True, False, None}
        assert no_multiquery in {True, False, None}
        assert type(property_file) in {str, unicode} or property_file is None
        if isinstance(dparams, dict):
            for k, v in dparams.iteritems():
                assert type(k) in {str, unicode}
        else:
            assert dparams is None, 'dparams must be a dictionary or None, ' \
                                   'but is {}'.format(type(dparams))
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
            'property_file': property_file,
            'dparams': dparams
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

    def dparams(self):
        """ Key value pairs to supply to pig as -D params. """
        return self._options.get('dparams', None)

    def to_cmd_array(self):
        """
        Converts the options into an array of values to be passed on the
        command line.
        """
        if self.help() is not None and self.help():
            return ['-help']

        if self.version() is not None and self.version():
            return ['-version']

        args = []
        if self.dparams() is not None and isinstance(self.dparams(), dict):
            for key, value in self.dparams().iteritems():
                value = str(value)
                if value == '':
                    args += ['-D{}=""'.format(key)]
                else:
                    args += ['-D{}={}'.format(key, cmd.safe_quote(value))]

        if self.exectype() is not None:
            args += ['-exectype', self.exectype()]

        if self.file() is not None:
            args += ['-file', self.file()]

        if self.log4jconf() is not None:
            args += ['-log4jconf', self.log4jconf()]

        if self.brief() is not None and self.brief():
            args += ['-brief']

        if self.check() is not None and self.check():
            args += ['-check']

        if self.debug() is not None:
            args += ['-debug', self.debug()]

        if self.execute() is not None:
            args += ['-execute', cmd.safe_quote(self.execute())]

        if self.embedded() is not None:
            args += ['-embedded', self.embedded()]

        if self.logfile() is not None:
            args += ['-logfile', self.logfile()]

        if self.param_file() is not None:
            args += ['-param_file', self.logfile()]

        if self.params() is not None and isinstance(self.params(), dict):
            for key, value in self.params().iteritems():
                value = str(value)
                if value == '':
                    args += ['-param', '{}=""'.format(key)]
                else:
                    args += [
                        '-param',
                        '{}={}'.format(key, cmd.safe_quote(value))
                    ]

        if self.dryrun() is not None and self.dryrun():
            args += ['-dryrun']

        if self.verbose() is not None and self.verbose():
            args += ['-verbose']

        if self.warning() is not None and self.warning():
            args += ['-warning']

        if self.stop_on_failure() is not None and self.stop_on_failure():
            args += ['-stop_on_failure']

        if self.no_multiquery() is not None and self.no_multiquery():
            args += ['-no_multiquery']

        if self.property_file() is not None:
            args += ['-propertyFile', self.property_file()]

        return args


class PigTestOptions(PigOptions):
    """ Manages command line options when running pig tests. """

    def __init__(self, *args, **kwargs):
        super(PigTestOptions, self).__init__(*args, **kwargs)

    def exectype(self):
        """ Set execution mode: local|mapreduce, default is local. """
        value = self._options.get('exectype', None)
        return value if value is not None else 'local'

    def debug(self):
        """ Debug level, DEBUG is default. """
        value = self._options.get('debug', None)
        return value if value is not None else 'DEBUG'


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
        is_windows = name == 'nt'
        p = Popen(
            args,
            stdout=PIPE,
            stderr=PIPE,
            shell=is_windows,
            env=environ,
            bufsize=1,
            close_fds=ON_POSIX
        )

        pr = ProcessReader(p, p_out=False, p_err=False)
        return pr.results()

    def pig_cmd(self):
        """
        Gets the command to call to run pig on the command line.

        :returns: the command for pig
        :rtype: list()
        """
        return ['pig']

        # is_jar = self._config.get('is_jar', None)
        # if is_jar is not None and is_jar:
        #     return ['pig']
        # else:
        #     return [environ.get('PIG')]

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
        code, output, error = self.run(args)
        logger.debugHeader('error')
        logger.debug(os.linesep + os.linesep.join(error))
        logger.debugHeader('output')
        logger.debug(os.linesep + os.linesep.join(output))
        return code, output, error

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
        code, output, error = self.pig(options)
        return code, output, error
