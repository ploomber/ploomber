import datetime
import logging
from pathlib import Path


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
