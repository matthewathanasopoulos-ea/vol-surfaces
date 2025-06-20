import logging
import sys
from .max_logging_entry_handler import MaxLoggingEntriesHandler

class Logger(object):
    r"""A class that creates a logger using the package logging with the levels INFO, DEBUG and ERROR and which outputs
    any returned messages on the system's standard output. The format of the logger is as follows\:

        %(asctime)s - %(name)s - %(levelname)s - %(message)s

    If limit != -1, then a restricted version of logger is created which outputs by max the limit number of logs.

    By default, Logger is restricted. 
    """
    def __init__(self, name: str, limit: int = 10_000):
        """
        Initialises the logger using the name value that was passed.

        Args:
            name: The name you want to set as your logger's output name.
            limit (optional, default: 10000): max number of log lines allowed for logger to produce, -1 to disable limit
        """
        
        self.logger = self._set_logger(name) if limit <= 0 else self._set_restricted_logger(name, limit)

    @staticmethod
    def _set_logger(name: str) -> logging.getLogger:
        """
        Initialize logger_tool with appropriate Level and Formatting

        Args:
            name: The name that the logger will report as. Used in the logger's formated output.

        Returns:
            The constructed logger.
        """
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
    
    @staticmethod
    def _set_restricted_logger(name: str, limit:int) -> logging.getLogger:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = MaxLoggingEntriesHandler(max_entries=limit)
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger
