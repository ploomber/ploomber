import sys
import logging
from itertools import product
import warnings
from pathlib import Path
from unittest.mock import Mock, MagicMock
import sqlite3

import pytest
import tqdm.auto
import pandas as pd
import numpy as np

from tests_util import executors_w_exception_logging
from ploomber import DAG
from ploomber.dag import dag as dag_module
from ploomber.tasks import PythonCallable, SQLDump, SQLScript
from ploomber.products import File, SQLiteRelation
from ploomber.constants import TaskStatus, DAGStatus
from ploomber.exceptions import (DAGBuildError, DAGRenderError,
                                 DAGBuildEarlyStop, DAGCycle)
from ploomber.executors import Serial, Parallel, serial
from ploomber.clients import SQLAlchemyClient
from ploomber.dag.dagclients import DAGClients
from ploomber.dag import plot as dag_plot_module
from ploomber.util import util as ploomber_util

# TODO: a lot of these tests should be in a test_executor file
# since they test Errored or Executed status and the output errors, which
# is done by the executor

# parametrize tests over these executors
_executors = [
    Serial(build_in_subprocess=False),
    Serial(build_in_subprocess=True),
    Parallel()
]


class FailedTask(Exception):
    pass


class WarningA(Warning):
    pass


class WarningB(Warning):
    pass


def hook():
    if hasattr(hook, 'count'):
        hook.count += 1


def hook_crashing():
    if hasattr(hook_crashing, 'count'):
        hook_crashing.count += 1
    raise Exception('crash!')


def touch_root(product):
    Path(str(product)).touch()


def touch_root_w_param(product, some_param):
    Path(str(product)).touch()


def touch(upstream, product):
    Path(str(product)).touch()


def touch_root_w_warning(product):
    warnings.warn('This is a warning', WarningA)
    Path(str(product)).touch()


def touch_w_warning(upstream, product):
    warnings.warn('This is another warning', WarningA)
    Path(str(product)).touch()


def failing_root(product):
    raise FailedTask('Bad things happened')


def early_stop_root(product):
    raise DAGBuildEarlyStop('Ending gracefully')


def early_stop():
    raise DAGBuildEarlyStop('Ending gracefully')


def failing(upstream, product):
    raise FailedTask('Bad things happened')


@pytest.fixture
def dag():
    def fn1(product):
        pass

    def fn2(upstream, product):
        pass

    dag = DAG()
    t1 = PythonCallable(fn1, File('file1.txt'), dag, name='first')
    t2 = PythonCallable(fn2, File('file2.txt'), dag, name='second')
    t1 >> t2

    return dag


@pytest.fixture
def monkeypatch_plot(monkeypatch):
    """
    Monkeypatch logic for making the DAG.plot() work without calling
    pygraphviz and checking calls are done with the right arguments
    """
    image_out = object()
    mock_Image = Mock(return_value=image_out)
    mock_to_agraph = Mock()

    def touch(*args, **kwargs):
        Path(args[0]).touch()

    # create file, then make sure it's deleted
    mock_to_agraph.draw.side_effect = touch

    def to_agraph(*args, **kwargs):
        return mock_to_agraph

    monkeypatch.setattr(dag_module, 'Image', mock_Image)
    monkeypatch.setattr(dag_module.nx.nx_agraph, 'to_agraph',
                        Mock(side_effect=to_agraph))

    yield mock_Image, mock_to_agraph, image_out


def test_init():
    dag = DAG()
    assert isinstance(dag.clients, DAGClients)


def test_errror_on_invalid_executor():
    with pytest.raises(TypeError):
        DAG(executor=None)


def test_plot_embed(dag, monkeypatch_plot):
    mock_Image, mock_to_agraph, image_out = monkeypatch_plot

    img = dag.plot()

    kwargs = mock_Image.call_args[1]
    mock_Image.assert_called_once()
    assert set(kwargs) == {'filename'}
    # file should not exist, it's just temporary
    assert not Path(kwargs['filename']).exists()
    assert img is image_out
    mock_to_agraph.draw.assert_called_once()


def test_plot_include_products(dag, monkeypatch):
    mock = Mock(wraps=dag._to_graph)
    monkeypatch.setattr(DAG, '_to_graph', mock)
    monkeypatch.setattr(dag_module.nx.nx_agraph, 'to_agraph', Mock())

    dag.plot(include_products=True)

    mock.assert_called_with(fmt='pygraphviz', include_products=True)


def test_plot_path(dag, tmp_directory, monkeypatch_plot):
    mock_Image, mock_to_agraph, image_out = monkeypatch_plot

    img = dag.plot(output='pipeline.png')

    # user may select a format that is not compatible with
    # IPython.display.image, hence, we should not call it
    mock_Image.assert_not_called()
    assert Path('pipeline.png').exists()
    assert img == 'pipeline.png'
    mock_to_agraph.draw.assert_called_once()


def test_plot_validates_backend(dag, tmp_directory):
    with pytest.raises(ValueError) as excinfo:
        dag.plot(backend='unknown')

    expected = ("Expected backend to be: None, 'd3' or "
                "'pygraphviz', but got: 'unknown'")
    assert expected == str(excinfo.value)


def test_plot_validates_html_extension_if_d3(dag, tmp_directory):
    with pytest.raises(ValueError) as excinfo:
        dag.plot(backend='d3', output='pipeline.png')

    expected = "expected a path with extension .html"
    assert expected in str(excinfo.value)


@pytest.mark.parametrize('backend', [None, 'd3'])
def test_plot_with_d3_embed(dag, tmp_directory, monkeypatch, backend):
    # simulate pygraphviz isnt installed
    monkeypatch.setattr(dag_plot_module, 'find_spec', lambda _: None)
    output = dag.plot(backend=backend)

    # test the svg tag has the rendered content
    assert '<svg id="dag" viewBox=' in output.data
    # and the js message is hidden
    assert '<div id="js-message" style="display: none;">' in output.data


def test_plot_with_d3_embed_error_if_missing_dependency(
        dag, tmp_directory, monkeypatch):
    monkeypatch.setattr(ploomber_util.importlib.util, 'find_spec',
                        lambda _: None)

    with pytest.raises(ImportError) as excinfo:
        dag.plot(backend='d3')

    expected = ("'requests-html' 'nest_asyncio' are "
                "required to use 'embedded HTML with D3 backend'. "
                "Install with: pip install 'requests-html' 'nest_asyncio'")
    assert expected == str(excinfo.value)


@pytest.mark.parametrize('backend', [None, 'd3'])
def test_plot_with_d3_file(dag, tmp_directory, monkeypatch, backend):
    # simulate pygraphviz isnt installed
    monkeypatch.setattr(dag_plot_module, 'find_spec', lambda _: None)
    dag.plot(backend=backend, output='some-pipeline.html')

    html = Path('some-pipeline.html').read_text()
    # check the js message appears
    assert '<div id="js-message">' in html


def test_plot_error_if_d3_and_include_products(dag):
    with pytest.raises(ValueError) as excinfo:
        dag.plot(backend='d3', include_products=True)

    expected = "'include_products' is not supported when using the d3 backend."
    assert expected in str(excinfo.value)


@pytest.mark.parametrize('fmt', ['html', 'md'])
@pytest.mark.parametrize('sections', [None, 'plot', 'status', 'source'])
def test_to_markup(fmt, sections, dag, monkeypatch_plot):
    dag.to_markup(fmt=fmt, sections=sections)


def test_error_on_invalid_markup_format(dag):
    with pytest.raises(ValueError):
        dag.to_markup(fmt='invalid format')


def test_ipython_key_completions(dag):
    assert dag._ipython_key_completions_() == ['first', 'second']


def test_error_when_adding_task_with_existing_name(dag):
    with pytest.raises(ValueError) as excinfo:
        PythonCallable(touch_root, File('another.txt'), dag, name='first')

    assert "DAG already has a task with name 'first'" in str(excinfo.value)


def test_error_if_invalid_to_graph_fmt(dag):
    with pytest.raises(ValueError) as excinfo:
        dag._to_graph(fmt='unknown')

    assert "Invalid format" in str(excinfo.value)


def test_to_graph_networkx(dag):
    graph = dag._to_graph(fmt='networkx')

    assert set(graph.nodes) == {dag['first'], dag['second']}
    assert list(graph.edges) == [(dag['first'], dag['second'])]


def test_to_graph_d3(dag):
    graph = dag._to_graph(fmt='d3')

    assert set(graph.nodes) == {'first', 'second'}
    assert list(graph.edges) == [('first', 'second')]


def test_to_graph_prepare_for_graphviz(dag):
    graph = dag._to_graph(fmt='pygraphviz')

    assert set(n.attr['id'] for n in graph) == {'first', 'second'}
    assert set(n.attr['label'] for n in graph) == {"first", "second"}

    assert len(graph) == 2


def test_to_graph_prepare_for_graphviz_include_products(dag):
    graph = dag._to_graph(fmt='pygraphviz', include_products=True)

    assert set(n.attr['id'] for n in graph) == {'first', 'second'}
    assert set(n.attr['label'] for n in graph) == {
        "first -> \nFile('file1.txt')", "second -> \nFile('file2.txt')"
    }

    assert len(graph) == 2


def test_graphviz_graph_with_clashing_task_str(dag):
    def fn1(product):
        pass

    def fn2(upstream, product):
        pass

    dag = DAG()
    # both products have the same str representation - this should not happen
    # with files but might happen with sql tables (e.g. when a pipeline
    # connects to two different dbs and generates tables with the same names)
    t1 = PythonCallable(fn1, File('file1.txt'), dag, name='first')
    t2 = PythonCallable(fn2, File('file1.txt'), dag, name='second')
    t1 >> t2

    graph = dag._to_graph(fmt='pygraphviz', include_products=True)

    # check the representation of the graph still looks fine
    assert set(n.attr['id'] for n in graph) == {'first', 'second'}
    assert set(n.attr['label'] for n in graph) == {
        "first -> \nFile('file1.txt')", "second -> \nFile('file1.txt')"
    }
    assert len(graph) == 2


def test_count_in_progress_bar(monkeypatch, tmp_directory):
    dag = DAG()
    dag.executor = Serial()
    t1 = PythonCallable(touch_root, File('1.txt'), dag, name=1)
    t2 = PythonCallable(touch, File('2.txt'), dag, name=2)
    t3 = PythonCallable(touch, File('3.txt'), dag, name=3)
    t1 >> t2 >> t3

    mock = Mock(wraps=tqdm.auto.tqdm)

    monkeypatch.setattr(serial, 'tqdm', mock)

    # first time it builds everything
    dag.build()
    # this time it doesn't run anything
    dag.build()

    first, second = mock.call_args_list

    assert first[1] == dict(total=3)
    assert len(first[0][0]) == 3

    assert second[1] == dict(total=0)
    assert len(second[0][0]) == 0


def test_mapping_interface():
    dag = DAG()
    t1 = PythonCallable(touch_root, File('1.txt'), dag, name=1)
    t2 = PythonCallable(touch, File('2.txt'), dag, name=2)
    t3 = PythonCallable(touch, File('3.txt'), dag, name=3)
    t1 >> t2 >> t3

    assert list(dag) == [1, 2, 3]
    assert list(dag.keys()) == [1, 2, 3]
    assert list(dag.values()) == [t1, t2, t3]

    del dag[3]
    assert list(dag) == [1, 2]

    with pytest.raises(KeyError) as excinfo:
        dag[100]

    # check no quotes are added to distinguish between dag[10] and dag["10"]
    assert '100' in str(excinfo.value)

    with pytest.raises(KeyError) as excinfo:
        dag['100']

    assert "'100'" in str(excinfo.value)


@pytest.mark.parametrize('executor', _executors)
def test_forced_build(executor, tmp_directory):
    dag = DAG(executor=executor)
    PythonCallable(touch_root, File('1.txt'), dag, name=1)

    dag.build()

    report = dag.build(force=True)

    assert report['Ran?'] == [True]


def test_forced_render_does_not_call_is_outdated(monkeypatch):
    """
    For products whose metadata is stored remotely, checking status is an
    expensive operation. Make dure forced render does not call
    Product._is_oudated
    """
    dag = DAG()
    t1 = PythonCallable(touch_root, File('1.txt'), dag, name=1)
    t2 = PythonCallable(touch, File('2.txt'), dag, name=2)
    t1 >> t2

    def _is_outdated(self, outdated_by_code):
        raise ValueError(f'Called _is_outdated on {self}')

    monkeypatch.setattr(File, '_is_outdated', _is_outdated)

    dag.render(force=True)


@pytest.mark.parametrize('show_progress', [True, False])
def test_hide_progress(show_progress, capsys):
    dag = DAG()
    dag.render(show_progress=show_progress)
    captured = capsys.readouterr()
    assert bool(captured.err) is show_progress


@pytest.mark.parametrize('executor', _executors)
def test_build_partially(tmp_directory, executor):
    dag = DAG(executor=executor)
    PythonCallable(touch_root, File('a.txt'), dag, name='a')
    PythonCallable(touch_root, File('b.txt'), dag, name='b')

    report = dag.build_partially('b')

    # check it only ran task b
    assert report['Ran?'] == [True]
    assert report['name'] == ['b']

    # task status in original dag are the same
    assert (set(t.exec_status
                for t in dag.values()) == {TaskStatus.WaitingRender})

    # this triggers metadata loading for the first time
    dag.render()

    # new task status reflect partial execution
    assert ({n: t.exec_status
             for n, t in dag.items()} == {
                 'a': TaskStatus.WaitingExecution,
                 'b': TaskStatus.Skipped,
             })


def test_build_partially_with_wildcard(tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=False))
    PythonCallable(touch_root, File('a-1.txt'), dag, name='a-1')
    PythonCallable(touch_root, File('a-2.txt'), dag, name='a-2')
    PythonCallable(touch_root, File('b.txt'), dag, name='b')

    dag.build_partially('a-*')

    assert Path('a-1.txt').exists()
    assert Path('a-2.txt').exists()
    assert not Path('b.txt').exists()


def test_build_partially_with_wildcard_that_has_upstream(tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=False))
    root = PythonCallable(touch_root, File('root.txt'), dag, name='root')
    a1 = PythonCallable(touch, File('a-1.txt'), dag, name='a-1')
    root >> a1
    PythonCallable(touch_root, File('a-2.txt'), dag, name='a-2')
    PythonCallable(touch_root, File('b.txt'), dag, name='b')

    dag.build_partially('a-*')

    assert Path('root.txt').exists()
    assert Path('a-1.txt').exists()
    assert Path('a-2.txt').exists()
    assert not Path('b.txt').exists()


def test_build_partially_with_wildcard_skip_upstream(tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=False))
    root = PythonCallable(touch_root, File('root.txt'), dag, name='root')
    a1 = PythonCallable(touch, File('a-1.txt'), dag, name='a-1')
    root >> a1
    PythonCallable(touch_root, File('a-2.txt'), dag, name='a-2')
    PythonCallable(touch_root, File('b.txt'), dag, name='b')

    dag.build_partially('a-*', skip_upstream=True)

    assert not Path('root.txt').exists()
    assert Path('a-1.txt').exists()
    assert Path('a-2.txt').exists()
    assert not Path('b.txt').exists()


def test_build_partially_diff_sessions(tmp_directory):
    def make():
        dag = DAG()
        a = PythonCallable(touch_root, File('a.txt'), dag, name='a')
        b = PythonCallable(touch, File('b.txt'), dag, name='b')
        a >> b
        return dag

    # build dag
    make().build()

    # force outdated "a" task by deleting metadata
    Path('.a.txt.metadata').unlink()

    # create another dag
    dag = make()

    # render, this triggers metadata loading, which is going to be empty for
    # "a", hence, this dag thinks it has to run "a"
    dag.render()

    # build partially - this copies the dag and runs "a", this must trigger
    # the "clear metadata" procedure in the original dag object to let it know
    # that it has to load the metadata again before building
    dag.build_partially('a')

    # dag should reload metadata and realize it only has to run "b"
    report = dag.build()

    df = report.to_pandas().set_index('name')
    assert not df.loc['a']['Ran?']
    assert df.loc['b']['Ran?']


@pytest.mark.parametrize('function_name', ['render', 'build', 'plot'])
@pytest.mark.parametrize('executor', _executors)
def test_dag_functions_do_not_fetch_metadata(function_name, executor,
                                             tmp_directory, monkeypatch_plot):
    """
    these function should not look up metadata, since the products do not
    exist, the status can be determined without it
    """
    product = File('1.txt')
    dag = DAG(executor=executor)
    PythonCallable(touch_root, product, dag, name=1)

    m = Mock(wraps=product.fetch_metadata)

    # to make this work with pickle
    m.__reduce__ = lambda self: (MagicMock, ())

    product.fetch_metadata = m

    getattr(dag, function_name)()

    # not called
    product.fetch_metadata.assert_not_called()

    if function_name == 'build':
        # if building, we should still see the metadata
        assert product.metadata._data['stored_source_code']
        assert product.metadata._data['timestamp']


def test_can_get_upstream_and_downstream_tasks():
    dag = DAG('dag')

    ta = PythonCallable(touch_root, File('a.txt'), dag, 'ta')
    tb = PythonCallable(touch, File('b.txt'), dag, 'tb')
    tc = PythonCallable(touch, File('c.txt'), dag, 'tc')

    ta >> tb >> tc

    assert set(ta.upstream) == set()
    assert set(tb.upstream) == {'ta'}
    assert set(tc.upstream) == {'tb'}

    assert dag.get_downstream('ta') == ['tb']
    assert dag.get_downstream('tb') == ['tc']
    assert not dag.get_downstream('tc')


def test_can_access_sub_dag():
    sub_dag = DAG('sub_dag')

    ta = PythonCallable(touch_root, File('a.txt'), sub_dag, 'ta')
    tb = PythonCallable(touch, File('b.txt'), sub_dag, 'tb')
    tc = PythonCallable(touch, File('c.txt'), sub_dag, 'tc')

    ta >> tb >> tc

    dag = DAG('dag')

    fd = Path('d.txt')
    td = PythonCallable(touch, File(fd), dag, 'td')

    td.set_upstream(sub_dag)

    assert 'sub_dag' in td.upstream


def test_can_access_tasks_inside_dag_using_getitem():
    dag = DAG('dag')
    dag2 = DAG('dag2')

    ta = PythonCallable(touch, File(Path('a.txt')), dag, 'ta')
    tb = PythonCallable(touch, File(Path('b.txt')), dag, 'tb')
    tc = PythonCallable(touch, File(Path('c.txt')), dag, 'tc')

    # td is still discoverable from dag even though it was declared in dag2,
    # since it is a dependency for a task in dag
    td = PythonCallable(touch_root, File(Path('c.txt')), dag2, 'td')
    # te is not discoverable since it is not a dependency for any task in dag
    te = PythonCallable(touch, File(Path('e.txt')), dag2, 'te')

    td >> ta >> tb >> tc >> te

    assert set(dag) == {'ta', 'tb', 'tc', 'td'}


def test_partial_build(tmp_directory):
    dag = DAG('dag')

    ta = PythonCallable(touch_root, File(Path('a.txt')), dag, 'ta')
    tb = PythonCallable(touch, File(Path('b.txt')), dag, 'tb')
    tc = PythonCallable(touch, File(Path('c.txt')), dag, 'tc')
    td = PythonCallable(touch, File(Path('d.txt')), dag, 'td')
    te = PythonCallable(touch, File(Path('e.txt')), dag, 'te')

    ta >> tb >> tc
    tb >> td >> te

    table = dag.build_partially('tc')

    assert set(table['name']) == {'ta', 'tb', 'tc'}
    assert all(table['Ran?'])


@pytest.mark.parametrize('executor', _executors)
def test_dag_task_status_life_cycle(executor, tmp_directory):
    """
    Check dag and task status along calls to DAG.render and DAG.build.
    Although DAG and Task status are automatically updated and propagated
    downstream upon calls to render and build, we have to parametrize this
    over executors since the object that gets updated might not be the same
    one that we declared here (this happens when a task runs in a different
    process), hence, it is the executor's responsibility to notify tasks
    on sucess/fail scenarios so downstream tasks are updated correctly
    """
    dag = DAG(executor=executor)
    t1 = PythonCallable(touch_root, File('ok.txt'), dag, name='t1')
    t2 = PythonCallable(failing_root, File('a_file.txt'), dag, name='t2')
    t3 = PythonCallable(touch, File('another_file.txt'), dag, name='t3')
    t4 = PythonCallable(touch, File('yet_another_file.txt'), dag, name='t4')
    t5 = PythonCallable(touch_root, File('file.txt'), dag, name='t5')
    t2 >> t3 >> t4

    assert dag._exec_status == DAGStatus.WaitingRender
    assert {TaskStatus.WaitingRender
            } == set([t.exec_status for t in dag.values()])

    dag.render()

    assert dag._exec_status == DAGStatus.WaitingExecution
    assert t1.exec_status == TaskStatus.WaitingExecution
    assert t2.exec_status == TaskStatus.WaitingExecution
    assert t3.exec_status == TaskStatus.WaitingUpstream
    assert t4.exec_status == TaskStatus.WaitingUpstream
    assert t5.exec_status == TaskStatus.WaitingExecution

    try:
        dag.build()
    except DAGBuildError:
        pass

    assert dag._exec_status == DAGStatus.Errored
    assert t1.exec_status == TaskStatus.Executed
    assert t2.exec_status == TaskStatus.Errored
    assert t3.exec_status == TaskStatus.Aborted
    assert t4.exec_status == TaskStatus.Aborted
    assert t5.exec_status == TaskStatus.Executed

    dag.render()

    assert dag._exec_status == DAGStatus.WaitingExecution
    assert t1.exec_status == TaskStatus.Skipped
    assert t2.exec_status == TaskStatus.WaitingExecution
    assert t3.exec_status == TaskStatus.WaitingUpstream
    assert t4.exec_status == TaskStatus.WaitingUpstream
    assert t5.exec_status == TaskStatus.Skipped

    # TODO: add test when trying to Execute dag with task status
    # other than WaitingExecution anf WaitingUpstream


@pytest.mark.parametrize('executor', _executors)
def test_executor_keeps_running_until_no_more_tasks_can_run(
        executor, tmp_directory):
    dag = DAG(executor=executor)
    t_fail = PythonCallable(failing_root, File('t_fail'), dag, name='t_fail')
    t_fail_downstream = PythonCallable(failing,
                                       File('t_fail_downstream'),
                                       dag,
                                       name='t_fail_downstream')
    t_touch_aborted = PythonCallable(touch,
                                     File('t_touch_aborted'),
                                     dag,
                                     name='t_touch_aborted')

    t_fail >> t_fail_downstream >> t_touch_aborted

    PythonCallable(touch_root, File('t_ok'), dag, name='t_ok')

    try:
        dag.build(force=True)
    except DAGBuildError:
        pass

    assert not Path('t_fail').exists()
    assert not Path('t_fail_downstream').exists()
    assert Path('t_ok').exists()


def test_status_on_render_source_fail():
    def make():
        mock_client = Mock()
        dag = DAG()
        SQLDump('SELECT * FROM my_table',
                File('ok.txt'),
                dag,
                name='t1',
                client=mock_client)
        t2 = SQLDump('SELECT * FROM {{table}}',
                     File('a_file.txt'),
                     dag,
                     name='t2',
                     client=mock_client)
        t3 = SQLDump('SELECT * FROM another',
                     File('another_file.txt'),
                     dag,
                     name='t3',
                     client=mock_client)
        t4 = SQLDump('SELECT * FROM something',
                     File('yet_another'),
                     dag,
                     name='t4',
                     client=mock_client)
        SQLDump('SELECT * FROM my_table_2',
                File('ok_2'),
                dag,
                name='t5',
                client=mock_client)
        t2 >> t3 >> t4
        return dag

    dag = make()

    with pytest.raises(DAGRenderError):
        dag.render()

    assert dag._exec_status == DAGStatus.ErroredRender
    assert dag['t1'].exec_status == TaskStatus.WaitingExecution
    assert dag['t2'].exec_status == TaskStatus.ErroredRender
    assert dag['t3'].exec_status == TaskStatus.AbortedRender
    assert dag['t4'].exec_status == TaskStatus.AbortedRender
    assert dag['t5'].exec_status == TaskStatus.WaitingExecution

    # building directly should also raise render error
    dag = make()

    with pytest.raises(DAGRenderError):
        dag.build()


def test_status_on_product_source_fail():
    def make():
        mock_client = Mock()

        dag = DAG()
        SQLDump('SELECT * FROM my_table',
                File('ok.txt'),
                dag,
                name='t1',
                client=mock_client)
        t2 = SQLDump('SELECT * FROM my_table',
                     File('{{unknown}}'),
                     dag,
                     name='t2',
                     client=mock_client)
        t3 = SQLDump('SELECT * FROM another',
                     File('another_file.txt'),
                     dag,
                     name='t3',
                     client=mock_client)
        t4 = SQLDump('SELECT * FROM something',
                     File('yet_another'),
                     dag,
                     name='t4',
                     client=mock_client)
        SQLDump('SELECT * FROM my_table_2',
                File('ok_2'),
                dag,
                name='t5',
                client=mock_client)
        t2 >> t3 >> t4
        return dag

    dag = make()

    with pytest.raises(DAGRenderError):
        dag.render()

    assert dag._exec_status == DAGStatus.ErroredRender
    assert dag['t1'].exec_status == TaskStatus.WaitingExecution
    assert dag['t2'].exec_status == TaskStatus.ErroredRender
    assert dag['t3'].exec_status == TaskStatus.AbortedRender
    assert dag['t4'].exec_status == TaskStatus.AbortedRender
    assert dag['t5'].exec_status == TaskStatus.WaitingExecution

    dag = make()

    # building directly should also raise render error
    with pytest.raises(DAGRenderError):
        dag.build()


def test_tracebacks_are_shown_for_all_on_render_failing_tasks():
    dag = DAG()
    mock_client = Mock()
    SQLDump('SELECT * FROM {{one_table}}',
            File('one_table'),
            dag,
            name='t1',
            client=mock_client)
    SQLDump('SELECT * FROM {{another_table}}',
            File('another_table'),
            dag,
            name='t2',
            client=mock_client)

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    assert "SQLDump: t2 -> File('another_table')" in str(excinfo.value)
    assert "SQLDump: t1 -> File('one_table')" in str(excinfo.value)


@pytest.mark.parametrize('executor', _executors)
def test_tracebacks_are_shown_for_all_on_build_failing_tasks(executor):
    dag = DAG(executor=executor)
    PythonCallable(failing_root, File('a_file.txt'), dag, name='t1')
    PythonCallable(failing_root, File('another_file.txt'), dag, name='t2')

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    # excinfo.getrepr() returns full text of chained exceptions
    assert "PythonCallable: t1 -> File('a_file.txt')" in str(excinfo.getrepr())
    assert ("PythonCallable: t2 -> File('another_file.txt')"
            in str(excinfo.getrepr()))


@pytest.mark.parametrize('executor', _executors)
def test_sucessful_execution(executor, tmp_directory):
    dag = DAG(executor=executor)
    t1 = PythonCallable(touch_root, File('ok.txt'), dag, name='t1')
    t2 = PythonCallable(touch, File('a_file.txt'), dag, name='t2')
    t3 = PythonCallable(touch, File('another_file.txt'), dag, name='t3')
    t4 = PythonCallable(touch, File('yet_another_file.txt'), dag, name='t4')
    PythonCallable(touch_root, File('file.txt'), dag, name='t5')
    t1 >> t2
    t1 >> t3
    (t2 + t3) >> t4

    dag.build()

    assert Path('ok.txt').exists()
    assert Path('a_file.txt').exists()
    assert Path('another_file.txt').exists()
    assert Path('yet_another_file.txt').exists()
    assert Path('file.txt').exists()

    assert set(t.exec_status for t in dag.values()) == {TaskStatus.Executed}
    assert set(t.product._is_outdated() for t in dag.values()) == {False}

    # nothing executed cause everything is up-to-date
    dag.build()

    assert set(t.exec_status for t in dag.values()) == {TaskStatus.Skipped}


@pytest.mark.parametrize('executor', _executors)
def test_status_cleared_after_reporting_status(executor, tmp_directory):
    # this is a pesky scenario, we try to avoid retrieving metdata when we
    # don't have to because it's slow, so we keep a local copy, but this means
    # we have to keep an eye on conditions where we must retrieve again, here's
    # one edge case
    dag = DAG(executor=executor)
    PythonCallable(touch_root, File('ok.txt'), dag, name='t1')

    # dag status requires retrieving metadata, we have a local copy now...
    dag.status()

    # building a task means saving metadata again, if the task was executed
    # in the process where the dag lives, metadata is still up-to-date because
    # to save metadata, we first have to override the local copy, the edge
    # case happens when task is executed in a child process, which means
    # the local copy in the DAG process is now outdated and should be cleared
    # up
    dag.build()

    # this should not trigger any execution, because we just built
    dag.build()

    assert set(t.exec_status for t in dag.values()) == {TaskStatus.Skipped}


def test_warnings_are_shown(tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=False))
    t1 = PythonCallable(touch_root_w_warning, File('file.txt'), dag)
    t2 = PythonCallable(touch_w_warning, File('file2.txt'), dag)
    t1 >> t2

    with pytest.warns(None) as record:
        dag.build()

    assert len(record) == 1
    assert 'This is a warning' in str(record[0].message)
    assert 'This is another warning' in str(record[0].message)
    # assert isinstance(record[0], WarningA)
    # assert isinstance(record[1], WarningB)


@pytest.mark.parametrize('executor', [
    Serial(build_in_subprocess=True, catch_exceptions=False),
    Serial(build_in_subprocess=False, catch_exceptions=False)
])
def test_exception_is_not_masked_if_not_catching_them(executor):
    dag = DAG(executor=executor)
    PythonCallable(failing_root, File('file.txt'), dag)

    with pytest.raises(FailedTask):
        dag.build()


# this feture only works for the serial executor
_bools = (False, True)

_serial = [
    Serial(build_in_subprocess=a, catch_exceptions=b, catch_warnings=c)
    for a, b, c in product(_bools, _bools, _bools)
]


@pytest.mark.parametrize('executor', _serial)
def test_early_stop(executor, tmp_directory):
    dag = DAG(executor=executor)
    PythonCallable(early_stop_root, File('file.txt'), dag)
    assert dag.build() is None


@pytest.mark.parametrize('executor', _serial)
def test_early_stop_from_task_level_on_finish(executor, tmp_directory):
    dag = DAG(executor=executor)
    t = PythonCallable(touch_root, File('file.txt'), dag)
    t.on_finish = early_stop
    assert dag.build() is None


# test early stop when registered an on_failure hook, maybe don't run hook?


def test_metadata_is_synced_when_executing_in_subprocess(tmp_directory):
    dag = DAG(executor=Serial(build_in_subprocess=True))
    t = PythonCallable(touch_root, File('file.txt'), dag)

    dag.build()

    assert t.product.metadata._data is not None


@pytest.mark.parametrize('executor', executors_w_exception_logging)
def test_task_errors_are_logged(executor, caplog):
    dag = DAG(executor=executor)
    PythonCallable(failing_root, File('file.txt'), dag, name='t')

    with caplog.at_level(logging.ERROR):
        with pytest.raises(DAGBuildError):
            dag.build()

    assert 'Error building task "t"' in caplog.text


def test_on_render_hook_is_executed(tmp_directory):
    hook.count = 0

    dag = DAG()
    PythonCallable(touch_root, File('file.txt'), dag, name='t')
    dag.on_render = hook

    dag.render()

    assert hook.count == 1


def test_on_render_crashes(tmp_directory):
    dag = DAG()
    PythonCallable(touch_root, File('file.txt'), dag, name='t')
    dag.on_render = hook_crashing

    with pytest.raises(DAGRenderError) as excinfo:
        dag.build()

    msg = ('Exception when running on_render for DAG "No name": crash!'
           '\nNeed help? https://ploomber.io/community')
    assert str(excinfo.value) == msg
    assert 'crash!' in str(excinfo.getrepr())
    assert dag._exec_status == DAGStatus.ErroredRender


def test_on_finish_hook_is_executed(tmp_directory):
    hook.count = 0

    dag = DAG()
    PythonCallable(touch_root, File('file.txt'), dag, name='t')
    dag.on_finish = hook

    dag.build()

    assert hook.count == 1


def test_on_finish_crashes(tmp_directory):
    dag = DAG()
    PythonCallable(touch_root, File('file.txt'), dag, name='t')
    dag.on_finish = hook_crashing

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    msg = ('Exception when running on_finish for DAG "No name": crash!'
           '\nNeed help? https://ploomber.io/community')
    assert str(excinfo.value) == msg
    assert 'crash!' in str(excinfo.getrepr())
    assert dag._exec_status == DAGStatus.Errored


def test_on_finish_crashes_gracefully(tmp_directory):
    dag = DAG()
    PythonCallable(touch_root, File('file.txt'), dag, name='t')
    dag.on_finish = early_stop

    assert dag.build() is None
    assert dag._exec_status == DAGStatus.Errored


def test_on_failure(caplog):
    hook.count = 0

    dag = DAG(name='dag')
    PythonCallable(failing_root, File('file.txt'), dag, name='t')
    dag.on_failure = hook

    with pytest.raises(DAGBuildError):
        dag.build()

    assert hook.count == 1


def test_on_failure_crashes(caplog):
    hook_crashing.count = 0

    dag = DAG(name='dag')
    PythonCallable(failing_root, File('file.txt'), dag, name='t')
    dag.on_failure = hook_crashing

    with pytest.raises(DAGBuildError) as excinfo:
        with caplog.at_level(logging.ERROR):
            dag.build()

    assert hook_crashing.count == 1
    msg = ('Exception when running on_failure for DAG "dag": crash!'
           '\nNeed help? https://ploomber.io/community')
    assert str(excinfo.value) == msg
    assert 'crash!' in str(excinfo.getrepr())
    assert dag._exec_status == DAGStatus.Errored


def test_on_failure_crashes_gracefully(caplog):
    dag = DAG(name='dag')
    PythonCallable(failing_root, File('file.txt'), dag, name='t')
    dag.on_failure = early_stop

    with caplog.at_level(logging.ERROR):
        out = dag.build()

    assert out is None is None
    assert 'Exception when running on_failure for DAG "dag"' in caplog.text


@pytest.mark.parametrize('method', ['build', 'build_partially'])
def test_build_debug(dag, method, monkeypatch):
    m = Mock()
    monkeypatch.setattr(dag_module, 'debug_if_exception', m)

    fake_executor = Mock()
    dag.executor = fake_executor

    if method == 'build':
        getattr(dag, method)(debug=True)
    else:
        getattr(dag, method)('first', debug=True)

    m.assert_called_once()
    partial = m.call_args[0][0]

    assert partial.func.__name__ == '_build'
    assert partial.keywords == {'force': False, 'show_progress': True}
    # debug has to modify the executor but must restore it back to the original
    # value
    assert dag.executor is fake_executor


def test_clients_are_closed_after_build(tmp_directory):
    # TODO: same test but when the dag breaks (make sure clients are closed
    # even on that case)
    tmp = Path(tmp_directory)

    # create a db
    conn = sqlite3.connect(str(tmp / "database.db"))
    uri = 'sqlite:///{}'.format(tmp / "database.db")

    # make some data and save it in the db
    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn)
    conn.close()

    # create the task and run it
    dag = DAG()

    def mock_client():
        m = Mock(wraps=SQLAlchemyClient(uri))
        m.split_source = ';'
        return m

    clients = [mock_client() for _ in range(4)]

    dag.clients[SQLScript] = clients[0]
    dag.clients[SQLiteRelation] = clients[1]

    t1 = SQLScript("""
    CREATE TABLE {{product}} AS SELECT * FROM numbers
    """,
                   SQLiteRelation(('another', 'table')),
                   dag=dag,
                   name='t1')

    t2 = SQLScript("""
    CREATE TABLE {{product}} AS SELECT * FROM {{upstream['t1']}}
    """,
                   SQLiteRelation(('yet_another', 'table'), client=clients[2]),
                   dag=dag,
                   name='t2',
                   client=clients[3])

    t1 >> t2

    dag.build()

    assert all(client.close.called for client in clients)


def test_task_grouping():
    dag = DAG()
    t1 = PythonCallable(touch_root, File('1.txt'), dag, name='first')
    t2 = PythonCallable(touch_root, File('2.txt'), dag, name='second')
    t3 = PythonCallable(touch, File('3.txt'), dag, name='third')
    t3.set_upstream(t1, group_name='group')
    t3.set_upstream(t2, group_name='group')
    dag.render()

    assert set(t3.upstream) == {'first', 'second'}

    assert set(t3._upstream_product_grouped) == {'group'}
    assert set(t3._upstream_product_grouped['group']) == {'first', 'second'}

    assert set(t3.params['upstream']) == {'group'}

    assert t3.params['upstream']['group']['first'] is t1.product
    assert t3.params['upstream']['group']['second'] is t2.product


def test_outdated_if_different_params(tmp_directory):
    def make(some_param):
        dag = DAG(executor=Serial(build_in_subprocess=False))
        PythonCallable(touch_root_w_param,
                       File('1.txt'),
                       dag,
                       name='first',
                       params={'some_param': some_param})
        return dag

    make(some_param=1).build()

    dag = make(some_param=2).render()

    assert {t.exec_status
            for t in dag.values()} == {TaskStatus.WaitingExecution}


def test_up_to_date_status_when_unserializable_params(tmp_directory):
    def make():
        dag = DAG(executor=Serial(build_in_subprocess=False))
        PythonCallable(touch_root_w_param,
                       File('1.txt'),
                       dag,
                       name='first',
                       params={'some_param': object()})
        return dag

    make().build()

    dag = make().render()

    assert {t.exec_status for t in dag.values()} == {TaskStatus.Skipped}


def test_cycle_exception():
    dag = DAG()
    ta = PythonCallable(touch_root, File(Path("a.txt")), dag, "ta")
    tb = PythonCallable(touch, File(Path("b.txt")), dag, "tb")
    ta >> tb >> ta
    with pytest.raises(DAGCycle):
        dag.build()


def test_error_if_missing_pypgraphviz(monkeypatch, dag):
    monkeypatch.setattr(dag_plot_module, 'find_spec', lambda _: None)

    with pytest.raises(ImportError) as excinfo:
        dag.plot(backend='pygraphviz')

    if sys.version_info < (3, 8):
        assert ("'pygraphviz<1.8' is required to use 'plot'. Install "
                "with: pip install 'pygraphviz<1.8'" in str(excinfo.value))
        assert ("conda install 'pygraphviz<1.8' -c conda-forge"
                in str(excinfo.value))
    else:
        assert ("'pygraphviz' is required to use 'plot'. "
                "Install with: pip install 'pygraphviz'" in str(excinfo.value))
        assert ("conda install pygraphviz -c conda-forge"
                in str(excinfo.value))
