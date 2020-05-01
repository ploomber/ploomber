import logging


class DAGLogger:
    """Set up logging when calling DAG.build()

    DAGLogger logger works by simply adding a logger handler to the root logger
    when DAG.build() starts and removing it when it finishes. This class is
    not intended to be used directly.

    """
    def __init__(self, handler):
        self.handler = handler
        self.root_logger = logging.getLogger()

        # both logger and handlers have can set logging levels. Since the
        # use does not have access to the logger level itself, we set it
        # to DEBUG so logging is effectively tuned by the handler level
        self.root_logger.setLevel(logging.DEBUG)

    def __enter__(self):
        # can be None
        if self.handler:
            self.root_logger.addHandler(self.handler)

    def __exit__(self, exc_type, exc_value, traceback):
        if self.handler:
            self.root_logger.removeHandler(self.handler)
