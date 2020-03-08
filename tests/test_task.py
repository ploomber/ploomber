from pathlib import Path

from ploomber.exceptions import RenderError, TaskBuildError
from ploomber import DAG
from ploomber.products import File, PostgresRelation
from ploomber.tasks import PythonCallable, SQLScript, ShellScript, SQLDump
from ploomber.constants import TaskStatus
from ploomber.templates.Placeholder import Placeholder

import pytest


# TODO: if there is only one product class supported, infer from a tuple?
# TODO: make PostgresRelation accept three parameters instead of a tuple
# TODO: provide a way to manage locations in products, so a relation
# is fulle specified

class Dummy:
    pass


def my_fn(product, upstream):
    pass


# have to declare this here, otherwise it won't work with pickle
def touch(product):
    Path(str(product)).touch()


def on_finish(task):
    Path(str(task)).write_text('Written from on_finish')


def on_render(task):
    Path(str(task)).write_text('Written from on_render')


def on_finish_fail(task):
    raise Exception


def test_task_can_infer_name_from_source():
    dag = DAG()
    t = PythonCallable(my_fn, File('/path/to/{{name}}'), dag,
                       params=dict(name='file'))
    assert t.name == 'my_fn'


def test_task_raises_error_if_name_cannot_be_infered():
    dag = DAG()

    with pytest.raises(AttributeError):
        SQLDump('SELECT * FROM my_table', File('/path/to/data'), dag)


def test_python_callable_with_file():
    dag = DAG()
    t = PythonCallable(my_fn, File('/path/to/{{name}}'), dag, name='name',
                       params=dict(name='file'))
    t.render()

    assert str(t.product) == '/path/to/file'
    assert str(t.source) == 'def my_fn(product, upstream):\n    pass\n'


def test_postgresscript_with_relation():
    dag = DAG()
    t = SQLScript('CREATE TABLE {{product}} AS SELECT * FROM {{name}}',
                  PostgresRelation(('user', 'table', 'table'),
                                   client=Dummy()),
                  dag,
                  name='name',
                  params=dict(name='some_table'),
                  client=Dummy())

    t.render()

    assert str(t.product) == 'user.table'
    assert (str(t.source)
            == 'CREATE TABLE user.table AS SELECT * FROM some_table')


def test_task_change_in_status():
    # NOTE: there are some similar tests in test_dag.py - maybe move them?
    dag = DAG('dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}',
                     File('b.txt'), dag, 'tb')
    tc = ShellScript('cat {{upstream["tb"]}} > {{product}}',
                     File('c.txt'), dag, 'tc')

    assert all([t.exec_status == TaskStatus.WaitingRender for t
                in [ta, tb, tc]])

    ta >> tb >> tc

    dag.render()

    assert (ta.exec_status == TaskStatus.WaitingExecution
            and tb.exec_status == TaskStatus.WaitingUpstream
            and tc.exec_status == TaskStatus.WaitingUpstream)

    ta.build()

    assert (ta.exec_status == TaskStatus.Executed
            and tb.exec_status == TaskStatus.WaitingExecution
            and tc.exec_status == TaskStatus.WaitingUpstream)

    tb.build()

    assert (ta.exec_status == TaskStatus.Executed
            and tb.exec_status == TaskStatus.Executed
            and tc.exec_status == TaskStatus.WaitingExecution)

    tc.build()

    assert all([t.exec_status == TaskStatus.Executed for t in [ta, tb, tc]])


def test_raises_render_error_if_missing_param_in_code():
    dag = DAG('my dag')

    ta = ShellScript('{{command}} "a" > {{product}}', File('a.txt'), dag,
                     name='my task')

    with pytest.raises(RenderError):
        ta.render()


def test_raises_render_error_if_missing_param_in_product():
    dag = DAG('my dag')

    ta = ShellScript('echo "a" > {{product}}', File('a_{{name}}.txt'), dag,
                     name='my task')

    with pytest.raises(RenderError):
        ta.render()


def test_raises_render_error_if_non_existing_dependency_used():
    dag = DAG('my dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag, name='bash')
    tb = ShellScript('cat {{upstream.not_valid}} > {{product}}',
                     File('b.txt'), dag, name='bash2')
    ta >> tb

    with pytest.raises(RenderError):
        tb.render()


def test_raises_render_error_if_extra_param_in_code():
    dag = DAG('my dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag,
                     name='my task',
                     params=dict(extra_param=1))

    with pytest.raises(RenderError):
        ta.render()


def test_shows_warning_if_unused_dependencies():
    dag = DAG('dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}',
                     File('b.txt'), dag, 'tb')
    tc = ShellScript('cat {{upstream["tb"]}} > {{product}}',
                     File('c.txt'), dag, 'tc')

    ta >> tb >> tc
    ta >> tc

    ta.render()
    tb.render()

    with pytest.warns(UserWarning):
        tc.render()


def test_on_finish(tmp_directory):
    dag = DAG()

    t = PythonCallable(touch, File('file'), dag, name='touch')
    t.on_finish = on_finish

    dag.build()

    assert Path('file').read_text() == 'Written from on_finish'


def test_on_render(tmp_directory):
    dag = DAG()

    t = PythonCallable(touch, File('file'), dag, name='touch')
    t.on_render = on_render

    dag.render()

    assert Path('file').read_text() == 'Written from on_render'


def test_lineage():
    dag = DAG('dag')

    ta = ShellScript('touch {{product}}', File(Path('a.txt')), dag, 'ta')
    tb = ShellScript('touch {{product}}', File(Path('b.txt')), dag, 'tb')
    tc = ShellScript('touch {{product}}', File(Path('c.txt')), dag, 'tc')

    ta >> tb >> tc

    assert ta._lineage is None
    assert tb._lineage == {'ta'}
    assert tc._lineage == {'ta', 'tb'}


def test_params_are_copied_upon_initialization():
    dag = DAG()

    params = {'a': 1}
    t1 = PythonCallable(touch, File('file'), dag, name='t1', params=params)
    t2 = PythonCallable(touch, File('file'), dag, name='t2', params=params)

    assert t1.params is not t2.params


def test_placeholder_is_copied_upon_initialization():
    dag = DAG()
    dag.clients[SQLScript] = object()
    dag.clients[PostgresRelation] = object()

    p = Placeholder('CREATE TABLE {{product}} AS SELECT * FROM TABLE')

    t1 = SQLScript(p, PostgresRelation(('schema', 'a_table', 'table')),
                   dag, name='t1')
    t2 = SQLScript(p, PostgresRelation(('schema', 'another_table', 'table')),
                   dag, name='t2')

    assert t1.source.value is not t2.source.value


def test_task_is_re_executed_if_on_finish_fails(tmp_directory):
    dag = DAG()

    t = PythonCallable(touch, File('file'), dag, name='t1')
    t.on_finish = on_finish_fail

    # first time it runs, fails..
    try:
        dag.build(clear_cached_status=True)
    except TaskBuildError:
        pass

    # if we attempt to run, it will fail again (since no metadata is saved)
    with pytest.raises(TaskBuildError):
        dag.build(clear_cached_status=True)
