from unittest.mock import Mock
from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.tasks import ShellScript, PythonCallable, SQLDump
from ploomber.products import File
from ploomber.constants import TaskStatus, DAGStatus
from ploomber.exceptions import DAGBuildError, DAGRenderError

# TODO: a lot of these tests should be in a test_executor file
# since they test Errored or Executed status and the output errors, which
# is done by the executor
# TODO: check build successful execution does not run anything if tried a
# # second time
# TODO: once a successful dag build happens, check task.should_execute
# TODO: check skipped status
# TODO: test once a task is skipped, downstream tasks go from WaitingUpstream
# to WaitingExecution


class FailedTask(Exception):
    pass


def touch_root(product):
    Path(str(product)).touch()


def touch(upstream, product):
    Path(str(product)).touch()


def failing_root(product):
    raise FailedTask('Bad things happened')


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


@pytest.mark.parametrize('executor', ['parallel', 'serial'])
def test_forced_build(executor, tmp_directory):
    dag = DAG(executor=executor)
    PythonCallable(touch_root, File('1.txt'), dag, name=1)

    dag.build()

    report = dag.build(force=True)

    assert report['Ran?'] == [True]


@pytest.mark.parametrize('executor', ['parallel', 'serial'])
def test_build_partially(tmp_directory, executor):
    dag = DAG(executor=executor)
    PythonCallable(touch_root, File('1.txt'), dag, name=1)
    PythonCallable(touch_root, File('2.txt'), dag, name=2)

    report = dag.build_partially(2)

    # check it only ran task 2
    assert report['Ran?'] == [True]
    assert report['name'] == [2]

    # task status in original dag are the same
    assert (set(t.exec_status for t in dag.values())
            == {TaskStatus.WaitingRender})

    dag.render()

    # new task status reflect partial execution
    assert ({n: t.exec_status for n, t in dag.items()}
            == {1: TaskStatus.WaitingExecution, 2: TaskStatus.Skipped})


@pytest.mark.parametrize('function_name', ['render', 'build', 'to_markup',
                         'plot'])
def test_dag_functions(function_name):
    dag = DAG()
    getattr(dag, function_name)()


def test_dag_build_clears_cached_status(tmp_directory):
    dag = DAG()
    t = PythonCallable(touch_root, File('my_file'), dag)

    assert t.product._outdated_data_dependencies_status is None
    assert t.product._outdated_code_dependency_status is None

    dag.status()

    assert t.product._outdated_data_dependencies_status is not None
    assert t.product._outdated_code_dependency_status is not None

    dag.build()

    assert t.product._outdated_data_dependencies_status is None
    assert t.product._outdated_code_dependency_status is None


def test_warn_on_python_missing_docstrings():
    def fn1(product):
        pass

    dag = DAG()
    PythonCallable(fn1, File('file1.txt'), dag, name='fn1')

    with pytest.warns(UserWarning):
        dag.diagnose()


def test_does_not_warn_on_python_docstrings():
    def fn1(product):
        """This is a docstring
        """
        pass

    dag = DAG()
    PythonCallable(fn1, File('file1.txt'), dag, name='fn1')

    with pytest.warns(None) as warn:
        dag.diagnose()

    assert not warn


def test_warn_on_sql_missing_docstrings():
    dag = DAG()

    sql = 'SELECT * FROM table'
    SQLDump(sql, File('file1.txt'), dag, client=Mock(), name='sql')

    with pytest.warns(UserWarning):
        dag.diagnose()


def test_does_not_warn_on_sql_docstrings():
    dag = DAG()

    sql = '/* get data from table */\nSELECT * FROM table'
    SQLDump(sql, File('file1.txt'), dag, client=Mock(), name='sql')

    with pytest.warns(None) as warn:
        dag.diagnose()

    assert not warn


# def test_can_use_null_task(tmp_directory):
#     dag = DAG('dag')

#     Path('a.txt').write_text('hello')

#     ta = Null(File('a.txt'), dag, 'ta')
#     tb = ShellScript('cat {{upstream["ta"]}} > {{product}}', File('b.txt'),
#                      dag, 'tb')

#     ta >> tb

#     dag.build()

#     assert Path('b.txt').read_text() == 'hello'


def test_can_get_upstream_tasks():
    dag = DAG('dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}',
                     File('b.txt'), dag, 'tb')
    tc = ShellScript('cat {{upstream["tb"]}} > {{product}}',
                     File('c.txt'), dag, 'tc')

    ta >> tb >> tc

    assert set(ta.upstream) == set()
    assert set(tb.upstream) == {'ta'}
    assert set(tc.upstream) == {'tb'}


def test_can_access_sub_dag():
    sub_dag = DAG('sub_dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), sub_dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}',
                     File('b.txt'), sub_dag, 'tb')
    tc = ShellScript('tcat {{upstream["tb"]}} > {{product}}',
                     File('c.txt'), sub_dag, 'tc')

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

    ta = ShellScript('echo "hi" >> {{product}}',
                     File(Path('a.txt')), dag, 'ta')
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


@pytest.mark.parametrize('executor', ['parallel', 'serial'])
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
    t1 = PythonCallable(touch_root, File('ok'), dag, name='t1')
    t2 = PythonCallable(failing_root, File('a_file'), dag, name='t2')
    t3 = PythonCallable(touch, File('another_file'), dag, name='t3')
    t4 = PythonCallable(touch, File('yet_another_file'), dag, name='t4')
    t5 = PythonCallable(touch_root, File('file'), dag, name='t5')
    t2 >> t3 >> t4

    assert dag._exec_status == DAGStatus.WaitingRender
    assert {TaskStatus.WaitingRender} == set([t.exec_status
                                              for t in dag.values()])

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


@pytest.mark.parametrize('executor', ['parallel', 'serial'])
def test_executor_keeps_running_until_no_more_tasks_can_run(executor,
                                                            tmp_directory):
    dag = DAG(executor=executor)
    t_fail = PythonCallable(failing_root, File('t_fail'), dag, name='t_fail')
    t_fail_downstream = PythonCallable(failing, File('t_fail_downstream'),
                                       dag, name='t_fail_downstream')
    t_touch_aborted = PythonCallable(touch, File('t_touch_aborted'),
                                     dag, name='t_touch_aborted')

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
        SQLDump('SELECT * FROM my_table', File('ok'), dag, name='t1',
                client=object())
        t2 = SQLDump('SELECT * FROM {{table}}', File('a_file'), dag, name='t2',
                     client=object())
        t3 = SQLDump('SELECT * FROM another', File('another_file'), dag,
                     name='t3',
                     client=object())
        t4 = SQLDump('SELECT * FROM something', File('yet_another'), dag,
                     name='t4', client=object())
        SQLDump('SELECT * FROM my_table_2', File('ok_2'), dag,
                name='t5', client=object())
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
        SQLDump('SELECT * FROM my_table', File('ok'), dag, name='t1',
                client=object())
        t2 = SQLDump('SELECT * FROM my_table', File('{{unknown}}'), dag,
                     name='t2', client=object())
        t3 = SQLDump('SELECT * FROM another', File('another_file'), dag,
                     name='t3',
                     client=object())
        t4 = SQLDump('SELECT * FROM something', File('yet_another'), dag,
                     name='t4', client=object())
        SQLDump('SELECT * FROM my_table_2', File('ok_2'), dag,
                name='t5', client=object())
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
    SQLDump('SELECT * FROM {{one_table}}', File('one_table'), dag,
            name='t1', client=object())
    SQLDump('SELECT * FROM {{another_table}}', File('another_table'), dag,
            name='t2',         client=object())

    with pytest.raises(DAGRenderError) as excinfo:
        dag.render()

    assert "SQLDump: t2 -> File(another_table)" in str(excinfo.value)
    assert "SQLDump: t1 -> File(one_table)" in str(excinfo.value)


@pytest.mark.parametrize('executor', ['parallel', 'serial'])
def test_tracebacks_are_shown_for_all_on_build_failing_tasks(executor):
    dag = DAG(executor=executor)
    PythonCallable(failing_root, File('a_file'), dag, name='t1')
    PythonCallable(failing_root, File('another_file'), dag, name='t2')

    with pytest.raises(DAGBuildError) as excinfo:
        dag.build()

    # need this to get chained exceptions:
    # https://docs.pytest.org/en/latest/reference.html#_pytest._code.ExceptionInfo.getrepr
    assert "PythonCallable: t1 -> File(a_file)" in str(excinfo.getrepr())
    assert ("PythonCallable: t2 -> File(another_file)"
            in str(excinfo.getrepr()))


@pytest.mark.parametrize('executor', ['parallel', 'serial'])
def test_sucessful_execution(executor, tmp_directory):
    dag = DAG(executor=executor)
    t1 = PythonCallable(touch_root, File('ok'), dag, name='t1')
    t2 = PythonCallable(touch, File('a_file'), dag, name='t2')
    t3 = PythonCallable(touch, File('another_file'), dag, name='t3')
    t4 = PythonCallable(touch, File('yet_another_file'), dag, name='t4')
    PythonCallable(touch_root, File('file'), dag, name='t5')
    t1 >> t2
    t1 >> t3
    (t2 + t3) >> t4

    dag.build()

    assert Path('ok').exists()
    assert Path('a_file').exists()
    assert Path('another_file').exists()
    assert Path('yet_another_file').exists()
    assert Path('file').exists()

    assert set(t.exec_status for t in dag.values()) == {TaskStatus.Executed}
    assert set(t.product._is_outdated() for t in dag.values()) == {False}

    dag.build()

    assert set(t.exec_status for t in dag.values()) == {TaskStatus.Skipped}
