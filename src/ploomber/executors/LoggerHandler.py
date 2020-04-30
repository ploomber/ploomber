import datetime
import logging
from pathlib import Path


class DAGLogger:
    """Set up logging when calling DAG.build()

    DAGLogger logger works by simply adding a logger handler to the root logger
    when DAG.build() starts and removing it when it finishes. This class is
    not intended to be used directly.

    Examples
    --------
    >>> import logging
    >>> from ploomber import DAGConfigurator
    >>> configurator = DAGConfigurator()
    >>> handler = logging.FileHandler('my_pipeline.log')
    >>> handler.setLevel(logging.INFO)
    >>> configurator.cfg.logging_handler = handler
    >>> dag = configurator.create()
    """
    def __init__(self, handler):
        self.handler = handler
        self.root_logger = logging.getLogger()

    def __enter__(self):
        # can be None
        if self.handler:
            self.root_logger.addHandler(self.handler)

    def __exit__(self, exc_type, exc_value, traceback):
        self.root_logger.removeHandler(self.handler)


class LoggerHandler:
    """Add a remove a handler to configure logging during a DAG run
    """
    def __init__(self, dag_name, directory, logging_level=logging.INFO):
        self.directory = Path(directory)
        self.logging_level = logging_level
        self.dag_name = dag_name

    def add(self):
        self.logger = logging.getLogger()
        timestamp = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
        filename = '{}-{}.log'.format(self.dag_name, timestamp)
        self.handler = logging.FileHandler(self.directory / filename)
        # docs
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        formatter = logging.Formatter(
            '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)
        self.logger.setLevel(self.logging_level)

    def remove(self):
        self.logger.removeHandler(self.handler)
