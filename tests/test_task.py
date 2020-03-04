from pathlib import Path

from ploomber.exceptions import RenderError
from ploomber import DAG
from ploomber.products import File, PostgresRelation
from ploomber.tasks import PythonCallable, SQLScript, ShellScript
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
    Path('file').touch()


def on_finish(task):
    Path('file').write_text('hello')


# NOTE: this feature was removed
# def test_task_can_infer_name_from_product():
#     dag = DAG()
#     t = PythonCallable(my_fn, File('/path/to/{{name}}'), dag,
#                        params=dict(name='file'), name='my_fn')
#     assert t.name == 'file'


# NOTE: this feature was removed
# def test_task_raises_error_if_name_cannot_be_infered():
#     dag = DAG()

#     with pytest.raises(RenderError):
#         PythonCallable(my_fn, File('/path/to/{{upstream["t1"]}}_2'), dag,
#                        name='my_fn')


# NOTE: this feature was removed
# def test_task_can_infer_name_if_product_does_not_depend_on_upstream():
#     dag = DAG()
#     t1 = PythonCallable(my_fn, File('/path/to/{{name}}'), dag,
#                         params=dict(name='file'), name='my_fn')
#     t2 = PythonCallable(my_fn, File('/path/to/{{name}}'), dag,
#                         params=dict(name='file2'), name='my_fn2')
#     assert t1.name == 'file' and t2.name == 'file2'


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
    dag = DAG('dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}',
                     File('b.txt'), dag, 'tb')
    tc = ShellScript('cat {{upstream["tb"]}} > {{product}}',
                     File('c.txt'), dag, 'tc')

    assert all([t._status == TaskStatus.WaitingRender for t in [ta, tb, tc]])

    ta >> tb >> tc

    dag.render()

    assert (ta._status == TaskStatus.WaitingExecution
            and tb._status == TaskStatus.WaitingUpstream
            and tc._status == TaskStatus.WaitingUpstream)

    ta.build()

    assert (ta._status == TaskStatus.Executed
            and tb._status == TaskStatus.WaitingExecution
            and tc._status == TaskStatus.WaitingUpstream)

    tb.build()

    assert (ta._status == TaskStatus.Executed
            and tb._status == TaskStatus.Executed
            and tc._status == TaskStatus.WaitingExecution)

    tc.build()

    assert all([t._status == TaskStatus.Executed for t in [ta, tb, tc]])


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
