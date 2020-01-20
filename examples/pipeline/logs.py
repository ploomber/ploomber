import logging
from pathlib import Path

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber import executors


executor = executors.Parallel(processes=6,
                              logging_directory='./')
dag = DAG(executor=executor)


def _create_file(product):
    logger = logging.getLogger(__name__)
    Path(str(product)).touch()
    logger.info('Created file %s', str(product))


def _create_file_up(upstream, product):
    logger = logging.getLogger(__name__)
    Path(str(product)).touch()
    logger.info('Created file %s', str(product))


t1 = PythonCallable(_create_file, File('a_file'), dag,
                    name='create_file')

t2 = PythonCallable(_create_file, File('a_file2'), dag,
                    name='create_file2')

t3 = PythonCallable(_create_file_up, File('a_file3'), dag,
                    name='create_file3')

t4 = PythonCallable(_create_file_up, File('a_file4'), dag,
                    name='create_file4')


t1 >> t3
t2 >> t4

dag.build(force=True)
