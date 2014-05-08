import logging


class PtLog(object):

    def __init__(self, name, width=80):
        self.logger = logging.getLogger(name)
        self.width = width

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def infoHeader(self, msg, *args, **kwargs):
        self.header(logging.INFO, msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def debugHeader(self, msg, *args, **kwargs):
        self.header(logging.DEBUG, msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)

    def criticalHeader(self, msg, *args, **kwargs):
        self.header(logging.CRITICAL, msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def errorHeader(self, msg, *args, **kwargs):
        self.header(logging.ERROR, msg, *args, **kwargs)

    def log(self, level, msg, *args, **kwargs):
        self.logger.log(level, msg, *args, **kwargs)

    def setLevel(self, level):
        self.logger.setLevel(level)

    def header(self, level, msg, *args, **kwargs):
        width = self.width
        diff = (width - len(msg)) - 4
        diff = int(diff / 2) if 0 < diff else 0
        msg = '--' + ' ' * diff + msg
        msg += ' ' * ((width - len(msg)) - 2) + '--'
        self.log(level, '-' * width)
        self.log(level, msg, *args, **kwargs)
        self.log(level, '-' * width)
