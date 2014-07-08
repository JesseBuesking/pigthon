

from threading import Thread

try:
    from Queue import Queue, Empty
except ImportError:
    # Python 3.x
    # noinspection PyUnresolvedReferences
    from queue import Queue, Empty


class NonBlockingStreamReader(object):
    """ Reads from a stream on a separate thread. """

    def __init__(self, stream):
        """
        :param stream: the stream to read from. Usually a process' stdout or
         stderr.
        """

        self._s = stream
        self._q = Queue()

        def _populateQueue(stream, queue):
            """ Collect lines from `stream` and put them in `queue`. """
            for line in iter(stream.readline, b''):
                if line:
                    queue.put(line)
                else:
                    raise UnexpectedEndOfStream
            stream.close()

        self._t = Thread(
            target=_populateQueue,
            args=(self._s, self._q)
        )

        # thread dies with the program
        self._t.daemon = True

        # start collecting lines from the stream
        self._t.start()

    def readline(self, timeout=None):
        """
        Attempts to read a line from the stream. If a timeout is not
        supplied, it will do so in a non-blocking fashion.

        :param float timeout: amount of time to block and wait for output
        :return: the next line of output, or None
        :rtype: str or None
        """
        line = None
        try:
            if timeout is None:
                line = self._q.get_nowait()
            else:
                line = self._q.get(
                    block=timeout is not None,
                    timeout=timeout
                )
            # remove any preceding \f
            if line is not None:
                line = line.lstrip('\f').rstrip('\r\n').rstrip('\n')
        except Empty:
            pass

        return line


# noinspection PyDocstring
class UnexpectedEndOfStream(Exception):
    pass
