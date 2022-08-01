from collections.abc import Mapping
from unittest.mock import Mock
from pathlib import Path

import pytest
import pandas as pd
from debuglater.pydump import debug_dump

from test_pkg import functions
from ploomber import DAG, DAGConfigurator, tasks
from ploomber.tasks import PythonCallable, task_factory
from ploomber.products import File
from ploomber.exceptions import (DAGBuildError, TaskRenderError,
                                 DAGRenderError, TaskBuildError)
from ploomber.executors import Serial


@pytest.fixture
def dag():
    dag = DAG()

    t1 = PythonCallable(touch,
                        File('1.txt'),
                        dag=dag,
                        name='without_dependencies')
    t2 = PythonCallable(touch_with_upstream,
                        File('2.txt'),
                        dag=dag,
                        name='with_dependencies',
                        params={'param': 42})
    t1 >> t2

    return dag


@pytest.fixture
def dag_with_unserializer():
    dag = DAG()

    t1 = PythonCallable(fn_data_frame,
                        File('t1.parquet'),
                        dag,
                        name='t1',
                        serializer=df_serializer)
    t2 = PythonCallable(fn_adds_one,
                        File('t2.parquet'),
                        dag,
                        unserializer=df_unserializer,
                        serializer=df_serializer,
                        name='t2')
    t1 >> t2

    return dag


class MyException(Exception):
    pass


def fn(product, a):
    Path(str(product)).write_text('things')


def fn_w_exception(product):
    raise MyException


def fn_data_frame():
    return pd.DataFrame({'x': [1, 2, 3]})


def fn_adds_one(upstream):
    return upstream['t1'] + 1


def df_serializer(output, product):
    output.to_parquet(str(product))


def df_unserializer(product):
    return pd.read_parquet(str(product))


def metaproduct_unserializer(product):
    if isinstance(product, Mapping):
        # FIXME: we only need this bc the current MetaProduct implementation
        # iterates over values (instead of keys like a dict does) - we should
        # make it work like a regular dict and deprecate support for
        # MetaProduct initialized from lists
        products_d = product.products.to_json_serializable()

        return {k: Path(v).read_text() for k, v in products_d.items()}
    else:
        return Path(product).read_text()


def touch(product):
    Path(str(product)).touch()


def touch_meta(product):
    for path in product:
        Path(path).touch()


def touch_with_first_as_upstream(upstream, product):
    upstream['first']
    Path(product).touch()


def touch_with_upstream(upstream, product, param):
    _ = upstream['without_dependencies']
    Path(str(product)).touch()


def test_params_are_accesible_after_init():
    dag = DAG()
    t = PythonCallable(fn, File('file.txt'), dag, 'callable', params=dict(a=1))
    assert t.params == dict(a=1)


def test_upstream_and_me_are_added():
    dag = DAG()
    product = File('file.txt')
    t = PythonCallable(fn, product, dag, 'callable', params=dict(a=1))
    dag.render()

    assert t.params['product'] is product


def test_can_execute_python_callable(tmp_directory):
    dag = DAG()
    PythonCallable(fn, File('file.txt'), dag, 'callable', params=dict(a=1))
    assert dag.build()


def test_exceptions_are_raised_with_serial_executor():
    dag = DAG()
    PythonCallable(fn_w_exception, File('file.txt'), dag, 'callable')

    with pytest.raises(DAGBuildError):
        dag.build()


def test_catches_signature_errors_at_render_time():
    dag = DAG()
    t = PythonCallable(fn,
                       File('file.txt'),
                       dag,
                       'callable',
                       params=dict(non_param=1))

    with pytest.raises(TaskRenderError):
        t.render()

    with pytest.raises(DAGRenderError):
        dag.render()


def test_hot_reload(backup_test_pkg, tmp_directory):
    cfg = DAGConfigurator()
    cfg.params.hot_reload = True
    dag = cfg.create()

    t1 = PythonCallable(functions.touch_root, File('file1.txt'), dag)
    t2 = PythonCallable(functions.touch_upstream, File('file2.txt'), dag)
    t1 >> t2

    path_to_functions = Path(backup_test_pkg, 'functions.py')
    source_new = """
from pathlib import Path

def touch_root(product):
    Path(str(product)).write_text("hi")

def touch_upstream(product, upstream):
    Path(str(product)).write_text("hello")
    """
    path_to_functions.write_text(source_new)

    dag.build()

    assert Path('file1.txt').read_text() == 'hi'
    assert Path('file2.txt').read_text() == 'hello'


def test_serialize_unserialize(tmp_directory, dag_with_unserializer):
    dag_with_unserializer.build()
    assert pd.read_parquet('t1.parquet')['x'].tolist() == [1, 2, 3]
    assert pd.read_parquet('t2.parquet')['x'].tolist() == [2, 3, 4]


def test_load_with_unserializer_function(tmp_directory, dag_with_unserializer,
                                         monkeypatch):
    dag_with_unserializer.build()

    mock_t1 = Mock(wraps=dag_with_unserializer['t1']._unserializer)
    monkeypatch.setattr(dag_with_unserializer['t1'], '_unserializer', mock_t1)
    mock_t2 = Mock(wraps=dag_with_unserializer['t2']._unserializer)
    monkeypatch.setattr(dag_with_unserializer['t2'], '_unserializer', mock_t2)

    dag_with_unserializer['t1'].load()
    mock_t1.assert_called_once_with('t1.parquet')

    dag_with_unserializer['t2'].load()['x'].tolist() == [2, 3, 4]
    mock_t2.assert_called_once_with('t2.parquet')


def test_load_error_if_task_has_metaproduct_and_no_key():
    dag = DAG()

    task = PythonCallable(touch_meta, {'a': File('a'), 'b': File('b')}, dag)

    with pytest.raises(ValueError):
        task.load()


def test_load_from_metaproduct(tmp_directory):
    Path('a.csv').write_text('a,b\n1,2')

    dag = DAG()

    task = PythonCallable(touch_meta, {
        'a': File('a.csv'),
        'b': File('b')
    }, dag)

    assert task.load(key='a') is not None


def test_uses_default_serializer_and_deserializer():
    dag = DAG()

    def _serializer():
        pass

    def _unserializer():
        pass

    dag.serializer = _serializer
    dag.unserializer = _unserializer

    t = PythonCallable(fn_data_frame,
                       File('t1.parquet'),
                       dag,
                       name='root',
                       serializer=None,
                       unserializer=None)

    assert t._serializer is _serializer
    assert t._unserializer is _unserializer


def test_uses_override_default_serializer_and_deserializer():
    dag = DAG()

    def _serializer():
        pass

    def _unserializer():
        pass

    def _new_serializer():
        pass

    def _new_unserializer():
        pass

    dag.serializer = _serializer
    dag.unserializer = _unserializer

    t = PythonCallable(fn_data_frame,
                       File('t1.parquet'),
                       dag,
                       name='root',
                       serializer=_new_serializer,
                       unserializer=_new_unserializer)

    assert t._serializer is _new_serializer
    assert t._unserializer is _new_unserializer


def test_unserializes_upstream_metaproduct(tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=False))
    dag.unserializer = metaproduct_unserializer
    t1 = PythonCallable(touch_meta, {
        'one': File('one'),
        'another': File('another')
    },
                        dag=dag,
                        name='first')
    t2 = PythonCallable(touch_with_first_as_upstream, File('last'), dag=dag)
    t1 >> t2

    dag.build()


@pytest.mark.parametrize(
    'task_name',
    ['without_dependencies', 'with_dependencies'],
)
def test_develop(dag, task_name, monkeypatch):
    mock = Mock()
    monkeypatch.setattr(tasks.tasks.subprocess, 'run', mock)
    dag.render()
    dag[task_name].develop()

    mock.assert_called_once()
    assert mock.call_args[0][0][:2] == ['jupyter', 'notebook']


@pytest.mark.parametrize('app', ['notebook', 'lab'])
def test_develop_with_custom_args(app, dag, monkeypatch):
    mock = Mock()
    monkeypatch.setattr(tasks.tasks.subprocess, 'run', mock)
    dag.render()
    dag['without_dependencies'].develop(app=app,
                                        args='--port=8081 --no-browser')

    mock.assert_called_once()
    assert mock.call_args[0][0][:2] == ['jupyter', app]
    # make sure args are quoted
    assert mock.call_args[0][0][3:] == ['--port=8081', '--no-browser']


def test_develop_unknown_app(dag):
    dag.render()

    with pytest.raises(ValueError) as excinfo:
        dag['without_dependencies'].develop(app='unknown')

    assert '"app" must be one of' in str(excinfo.value)


@pytest.mark.parametrize(
    'task_name',
    ['without_dependencies', 'with_dependencies'],
)
@pytest.mark.parametrize(
    'kind, module',
    [
        ['pdb', tasks.tasks.pdb],
        ['ipdb', tasks.tasks.Pdb],
    ],
)
def test_debug(kind, module, dag, task_name, monkeypatch):
    mock = Mock()
    monkeypatch.setattr(module, 'runcall', mock)
    dag.render()
    dag[task_name].debug(kind=kind)

    mock.assert_called_once()

    assert mock.call_args[0][0] is dag[task_name].source.primitive
    assert mock.call_args[1] == dag[task_name].params.to_dict()


@pytest.mark.parametrize(
    'kind, module',
    [
        ['pdb', tasks.tasks.pdb],
        ['ipdb', tasks.tasks.Pdb],
    ],
)
def test_debug_with_userializer(tmp_directory, dag_with_unserializer,
                                monkeypatch, kind, module):
    mock = Mock()
    monkeypatch.setattr(module, 'runcall', mock)

    dag_with_unserializer.build()
    dag_with_unserializer['t2'].debug(kind=kind)

    assert (mock.call_args[0][0] is
            dag_with_unserializer['t2'].source.primitive)
    assert mock.call_args[1]['upstream']['t1'].to_dict(orient='list') == {
        'x': [1, 2, 3]
    }


def test_debug_unknown_kind(dag):
    dag.render()

    with pytest.raises(ValueError) as excinfo:
        dag['without_dependencies'].debug(kind='unknown')

    assert '"kind" must be one of' in str(excinfo.value)


@pytest.mark.parametrize('method', ['debug', 'develop'])
def test_calling_unrendered_task(method):
    dag = DAG()
    t = PythonCallable(touch, File('1.txt'), dag)

    msg = f'Cannot call task.{method}() on a task that has'

    with pytest.raises(TaskBuildError) as excinfo:
        getattr(t, method)()

    assert msg in str(excinfo.value)


def test_task_factory():
    dag = DAG()

    @task_factory(product=File('file.txt'))
    def touch(product):
        Path(str(product)).touch()

    touch(dag=dag)

    assert list(dag) == ['touch']
    assert str(dag['touch'].product) == 'file.txt'


def test_task_factory_override_params():
    dag = DAG()

    @task_factory(product=File('file.txt'))
    def touch(product):
        Path(str(product)).touch()

    touch(dag=dag, product=File('another.txt'))

    assert list(dag) == ['touch']
    assert str(dag['touch'].product) == 'another.txt'


def test_creates_parent_dirs(tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=False))

    PythonCallable(touch, File('some/nested/product.txt'), dag=dag)

    dag.build()

    return dag


def test_creates_parent_dirs_meta_product(tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=False))

    PythonCallable(touch_meta, {
        'one': File('some/nested/product.txt'),
        'another': File('some/another/product.txt')
    },
                   dag=dag)

    dag.build()

    return dag


def test_validates_debug_mode_in_constructor():
    with pytest.raises(ValueError) as excinfo:
        PythonCallable(touch,
                       File('file.txt'),
                       dag=DAG(),
                       debug_mode='something')

    msg = "'something' is an invalid value for 'debug_mode'. Valid values:"
    assert msg in str(excinfo.value)


def test_validates_debug_mode_property():
    task = PythonCallable(touch, File('file.txt'), dag=DAG(), debug_mode=None)

    with pytest.raises(ValueError) as excinfo:
        task.debug_mode = 'something'

    msg = "'something' is an invalid value for 'debug_mode'. Valid values:"
    assert msg in str(excinfo.value)


def test_debug_now():

    raise NotImplementedError


def test_debug_later(tmp_directory, monkeypatch, capsys):

    def crash(product):
        x, y = 1, 0
        return x / y

    task = PythonCallable(crash,
                          File('file.txt'),
                          dag=DAG(),
                          debug_mode='later')

    with pytest.raises(TaskBuildError) as excinfo:
        task.build()

    assert "dltr crash.dump" in str(excinfo.getrepr())

    mock = Mock(side_effect=["print(f'x={x}')", 'quit'])

    with monkeypatch.context() as m:
        m.setattr('builtins.input', mock)
        debug_dump('crash.dump')

    captured = capsys.readouterr()
    assert "x=1" in captured.out
