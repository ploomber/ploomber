import logging
from itertools import product
import warnings
from pathlib import Path

import pytest

from tests_util import executors_w_exception_logging
from ploomber import DAG
from ploomber.tasks import ShellScript, PythonCallable, SQLDump
from ploomber.products import File
from ploomber.constants import TaskStatus, DAGStatus
from ploomber.exceptions import (DAGBuildError, DAGRenderError,
                                 DAGBuildEarlyStop)
from ploomber.executors import Serial, Parallel

# TODO: a lot of these tests should be in a test_executor file
# since they test Errored or Executed status and the output errors, which
# is done by the executor
# TODO: check build successful execution does not run anything if tried a
# # second time
# TODO: once a successful dag build happens, check task.should_execute
# TODO: check skipped status
# TODO: test once a task is skipped, downstream tasks go from WaitingUpstream
# to WaitingExecution
# TODO: test dag.plot(), create a function that returns an object and test
# such function, to avoid comparing images

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
    raise Exception


def touch_root(product):
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


def early_stop_on_finish():
    raise DAGBuildEarlyStop('Ending gracefully')


def failing(upstream, product):
    raise FailedTask('Bad things happened')


# can test this since this uses dag.plot(), which needs dot for plotting
# def test_to_html():
#     def fn1(product):
#         pass

#     def fn2(product):
#         pass

#     dag = DAG()
#     t1 = PythonCallable(fn1, File('file1.txt'), dag)
#     t2 = PythonCallable(fn2, File('file2.txt'), dag)
#     t1 >> t2

#     dag.to_html('a.html')


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


@pytest.mark.parametrize('executor', _executors)
def test_forced_build(executor, tmp_directory):
    dag = DAG(executor=executor)
    PythonCallable(touch_root, File('1.txt'), dag, name=1)

    dag.build()

    report = dag.build(force=True)

    assert report['Ran?'] == [True]


@pytest.mark.parametrize('executor', _executors)
def test_build_partially(tmp_directory, executor):
    dag = DAG(executor=executor)
    PythonCallable(touch_root, File('a.txt'), dag, name='a')
    PythonCallable(touch_root, File('b.txt'), dag, name='b')

    report = dag.build_partially('b')

    # check it only ran task 2
    assert report['Ran?'] == [True]
    assert report['name'] == ['b']

    # task status in original dag are the same
    assert (set(t.exec_status
                for t in dag.values()) == {TaskStatus.WaitingRender})

    dag.render()

    # new task status reflect partial execution
    assert ({n: t.exec_status
             for n, t in dag.items()} == {
                 'a': TaskStatus.WaitingExecution,
                 'b': TaskStatus.Skipped
             })


@pytest.mark.parametrize('function_name',
                         ['render', 'build', 'to_markup', 'plot'])
def test_dag_functions_clear_up_product_status(function_name, tmp_directory):
    dag = DAG()
    t = PythonCallable(touch_root, File('1.txt'), dag, name=1)
    getattr(dag, function_name)()

    assert t.product._outdated_code_dependency_status is None
    assert t.product._outdated_data_dependencies_status is None


def test_dag_build_clears_cached_status(tmp_directory):
    dag = DAG()
    t = PythonCallable(touch_root, File('my_file.txt'), dag)

    assert t.product._outdated_data_dependencies_status is None
    assert t.product._outdated_code_dependency_status is None

    dag.status()

    assert t.product._outdated_data_dependencies_status is not None
    assert t.product._outdated_code_dependency_status is not None

    dag.build()

    assert t.product._outdated_data_dependencies_status is None
    assert t.product._outdated_code_dependency_status is None


# def test_can_use_null_task(tmp_directory):
#     dag = DAG('dag')

#     Path('a.txt').write_text('hello')

#     ta = Null(File('a.txt'), dag, 'ta')
#     tb = ShellScript('cat {{upstream["ta"]}} > {{product}}', File('b.txt'),
#                      dag, 'tb')

#     ta >> tb

#     dag.build()

#     assert Path('b.txt').read_text() == 'hello'


def test_can_get_upstream_and_downstream_tasks():
    dag = DAG('dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}', File('b.txt'),
                     dag, 'tb')
    tc = ShellScript('cat {{upstream["tb"]}} > {{product}}', File('c.txt'),
                     dag, 'tc')

    ta >> tb >> tc

    assert set(ta.upstream) == set()
    assert set(tb.upstream) == {'ta'}
    assert set(tc.upstream) == {'tb'}

    assert dag.get_downstream('ta') == ['tb']
    assert dag.get_downstream('tb') == ['tc']
    assert not dag.get_downstream('tc')


def test_can_access_sub_dag():
    sub_dag = DAG('sub_dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), sub_dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}', File('b.txt'),
                     sub_dag, 'tb')
    tc = ShellScript('tcat {{upstream["tb"]}} > {{product}}', File('c.txt'),
                     sub_dag, 'tc')

    ta >> tb >> tc

    dag = DAG('dag')

    fd = Path('d.txt')
    td = ShellScript('touch {{product}}', File(fd), dag, 'td')

    td.set_upstream(sub_dag)

    assert 'sub_dag' in td.upstream


def test_can_access_tasks_inside_dag_using_getitem():
    dag = DAG('dag')
    dag2 = DAG('dag2')

    ta = ShellScript('touch {{product}}', File(Path('a.txt')), dag, 'ta')
    tb = ShellScript('touch {{product}}', File(Path('b.txt')), dag, 'tb')
    tc = ShellScript('touch {{product}}', File(Path('c.txt')), dag, 'tc')

    # td is still discoverable from dag even though it was declared in dag2,
    # since it is a dependency for a task in dag
    td = ShellScript('touch {{product}}', File(Path('c.txt')), dag2, 'td')
    # te is not discoverable since it is not a dependency for any task in dag
    te = ShellScript('touch {{product}}', File(Path('e.txt')), dag2, 'te')

    td >> ta >> tb >> tc >> te

    assert set(dag) == {'ta', 'tb', 'tc', 'td'}


def test_partial_build(tmp_directory):
    dag = DAG('dag')

    ta = ShellScript('echo "hi" >> {{product}}', File(Path('a.txt')), dag,
                     'ta')
    code = 'cat {{upstream.first}} >> {{product}}'
    tb = ShellScript(code, File(Path('b.txt')), dag, 'tb')
    tc = ShellScript(code, File(Path('c.txt')), dag, 'tc')
    td = ShellScript(code, File(Path('d.txt')), dag, 'td')
    te = ShellScript(code, File(Path('e.txt')), dag, 'te')

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
        dag = DAG()
        SQLDump('SELECT * FROM my_table',
                File('ok.txt'),
                dag,
                name='t1',
                client=object())
        t2 = SQLDump('SELECT * FROM {{table}}',
                     File('a_file.txt'),
                     dag,
                     name='t2',
                     client=object())
        t3 = SQLDump('SELECT * FROM another',
                     File('another_file.txt'),
                     dag,
                     name='t3',
                     client=object())
        t4 = SQLDump('SELECT * FROM something',
                     File('yet_another'),
                     dag,
                     name='t4',
                     client=object())
        SQLDump('SELECT * FROM my_table_2',
                File('ok_2'),
                dag,
                name='t5',
                client=object())
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
        dag = DAG()
        SQLDump('SELECT * FROM my_table',
                File('ok.txt'),
                dag,
                name='t1',
                client=object())
        t2 = SQLDump('SELECT * FROM my_table',
                     File('{{unknown}}'),
                     dag,
                     name='t2',
                     client=object())
        t3 = SQLDump('SELECT * FROM another',
                     File('another_file.txt'),
                     dag,
                     name='t3',
                     client=object())
        t4 = SQLDump('SELECT * FROM something',
                     File('yet_another'),
                     dag,
                     name='t4',
                     client=object())
        SQLDump('SELECT * FROM my_table_2',
                File('ok_2'),
                dag,
                name='t5',
                client=object())
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


def test_tracebacks_are_shown_for_all_on_render_failing_tasks():
    dag = DAG()
    SQLDump('SELECT * FROM {{one_table}}',
            File('one_table'),
            dag,
            name='t1',
            client=object())
    SQLDump('SELECT * FROM {{another_table}}',
            File('another_table'),
            dag,
            name='t2',
            client=object())

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    assert "SQLDump: t2 -> File(another_table)" in str(excinfo.value)
    assert "SQLDump: t1 -> File(one_table)" in str(excinfo.value)


@pytest.mark.parametrize('executor', _executors)
def test_tracebacks_are_shown_for_all_on_build_failing_tasks(executor):
    dag = DAG(executor=executor)
    PythonCallable(failing_root, File('a_file.txt'), dag, name='t1')
    PythonCallable(failing_root, File('another_file.txt'), dag, name='t2')

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    # need this to get chained exceptions:
    # https://docs.pytest.org/en/latest/reference.html#_pytest._code.ExceptionInfo.getrepr
    assert "PythonCallable: t1 -> File(a_file.txt)" in str(excinfo.getrepr())
    assert ("PythonCallable: t2 -> File(another_file.txt)"
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
def test_early_stop_from_on_finish(executor, tmp_directory):
    dag = DAG(executor=executor)
    t = PythonCallable(touch_root, File('file.txt'), dag)
    t.on_finish = early_stop_on_finish
    assert dag.build() is None


# test early stop when registered an on_failure hook, maybe don't run hook?


def test_reporting_status_triggers_metadata_reload(tmp_directory):
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


def test_on_finish_hook_is_executed(tmp_directory):
    hook.count = 0

    dag = DAG()
    PythonCallable(touch_root, File('file.txt'), dag, name='t')
    dag.on_finish = hook

    dag.build()

    assert hook.count == 1


def test_on_failure(caplog):
    hook.count = 0

    dag = DAG(name='dag')
    PythonCallable(failing_root, File('file.txt'), dag, name='t')
    dag.on_failure = hook

    with pytest.raises(DAGBuildError):
        with caplog.at_level(logging.ERROR):
            dag.build()

    assert hook.count == 1
    assert 'Failure when building DAG "dag"' in caplog.text


def test_on_failure_exception(caplog):
    hook_crashing.count = 0

    dag = DAG(name='dag')
    PythonCallable(failing_root, File('file.txt'), dag, name='t')
    dag.on_failure = hook_crashing

    with pytest.raises(DAGBuildError):
        with caplog.at_level(logging.ERROR):
            dag.build()

    assert hook_crashing.count == 1
    assert 'Exception when running on_failure for DAG "dag"' in caplog.text
