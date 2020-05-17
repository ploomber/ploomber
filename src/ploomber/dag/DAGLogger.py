import logging


class DAGLogger:
    """Set up logging when calling DAG.build()

    DAGLogger logger works by simply adding a logger handler to the passed
    logger when DAG.build() starts and removing it when it finishes. This class
    is not intended to be used directly.

    """
    def __init__(self, handler, logger=None):
        self.handler = handler

        # if no logger is passed, set it to the root logger with debug level
        if logger is None:
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger

    def __enter__(self):
        # can be None
        if self.handler:
            self.logger.addHandler(self.handler)

    def __exit__(self, exc_type, exc_value, traceback):
        if self.handler:
            self.logger.removeHandler(self.handler)
