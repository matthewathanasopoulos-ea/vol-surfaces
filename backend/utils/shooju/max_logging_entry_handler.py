import sys
import logging

class MaxLoggingEntriesHandler(logging.StreamHandler):
    """Logging handler to limit the number of logs produced by a logger.

    This important in cases where specific scripts are deployed to a production environment, and large number of logs cost $$$.

    """
    def __init__(self, max_entries: int):
        super().__init__(sys.stdout)
        self.max_entries = max_entries
        self.current_entries = 0

    def emit(self, record):
        # Check if maximum entries have been reached
        if self.current_entries < self.max_entries:
            print(self.format(record))
            self.current_entries += 1