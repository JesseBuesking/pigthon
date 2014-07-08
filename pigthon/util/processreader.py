

from pigthon.util.nbstreamreader import NonBlockingStreamReader
import time


class ProcessReader(object):
    """
    Asynchronously reads info from a subprocess' error and output streams.
    """

    def __init__(self, process, p_out=True, p_err=True):
        """

        :param subprocess.Popen process:
        :param p_out:
        :param p_err:
        :return:
        """
        self._p = process
        self._o = []
        self._e = []
        self._c = 255
        self._p_out = p_out
        self._p_err = p_err
        self._ostream = NonBlockingStreamReader(self._p.stdout)
        self._estream = NonBlockingStreamReader(self._p.stderr)

        # loop until the process ends
        while self._p.poll() is None:
            if not self._try_read():
                # minor delay to prevent cpu-cycling, but only if we were
                # unable to read anything.
                #
                # note: this actually makes the process block, removing
                # asynchronous-ness
                time.sleep(.01)

        # now that the subprocess has ended, flush out any remaining output
        self._try_flush(self._p.stdout)
        self._try_flush(self._p.stderr)

        # read whatever may have been flushed
        while self._try_read():
            pass

        # make sure to close out any open streams
        self._try_close(self._p.stdout)
        self._try_close(self._p.stderr)

        # snag the return code
        self._c = self._p.returncode

    def _try_flush(self, stream):
        if not stream.closed:
            stream.flush()

    def _try_read(self):
        out = self._ostream.readline()
        if out is not None:
            if self._p_out:
                print(out)
            self._o.append(out)

        err = self._estream.readline()
        if err is not None:
            if self._p_err:
                print(err)
            self._e.append(err)

        # true if something was read
        return out is not None or err is not None

    def _try_close(self, stream):
        while not stream.closed:
            try:
                stream.close()
                break
            except IOError, e:
                if 'concurrent operation on the same file object' in str(e):
                    # more to read?
                    while self._try_read():
                        pass
                    continue
                raise

    def results(self):
        """
        Returns a tuple of results.

        :return: a tuple containing the result code, output, and errors
        :rtype: tuple
        """
        return self._c, self._o, self._e
