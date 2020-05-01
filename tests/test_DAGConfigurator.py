import logging
from pathlib import Path

from ploomber import DAG
from ploomber.dag.DAGConfigurator import DAGConfigurator
from ploomber.tasks import PythonCallable
from ploomber.products import File


def touch_root(product):
    logging.info('Logging...')
    Path(str(product)).touch()


def touch_root_modified(product):
    1 + 1
    Path(str(product)).touch()


def handler_factory():
    handler = logging.FileHandler('dag.log')
    handler.setLevel(logging.INFO)
    return handler


def test_turn_off_outdated_by_code(tmp_directory):
    # build a dag that generates a file
    dag = DAG()
    PythonCallable(touch_root, File('file.txt'), dag)
    dag.build()

    # same dag, but with modified source code and set checking code to false
    configurator = DAGConfigurator()
    configurator.cfg.outdated_by_code = False
    dag2 = configurator.create()
    PythonCallable(touch_root_modified, File('file.txt'), dag2)
    report = dag2.build()

    # tasks should not run
    assert report['Ran?'] == [False]


def test_from_dict():
    configurator = DAGConfigurator.from_dict({'outdated_by_code': False})
    assert not configurator.cfg.outdated_by_code


def test_logging_handler(tmp_directory):
    configurator = DAGConfigurator()

    configurator.cfg.logging_handler_factory = handler_factory
    dag = configurator.create()
    PythonCallable(touch_root, File('file.txt'), dag)
    dag.build()

    assert 'Logging...' in Path('dag.log').read_text()
