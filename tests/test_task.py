from pathlib import Path

from ploomber.exceptions import RenderError, TaskBuildError
from ploomber import DAG
from ploomber.products import (File, PostgresRelation, GenericProduct,
                               GenericSQLRelation)
from ploomber.tasks import (PythonCallable, SQLScript, ShellScript, SQLDump,
                            SQLTransfer, SQLUpload, PostgresCopyFrom,
                            NotebookRunner)
from ploomber.constants import TaskStatus
from ploomber.placeholders.Placeholder import Placeholder

import pytest

# TODO: if there is only one product class supported, infer from a tuple?
# TODO: make PostgresRelation accept three parameters instead of a tuple
# TODO: provide a way to manage locations in products, so a relation
# is fulle specified


class Dummy:
    pass


def touch(product):
    Path(str(product)).touch()


def touch_w_upstream(product, upstream):
    Path(str(product)).touch()


@pytest.mark.parametrize('class_,kwargs', [
    [
        NotebookRunner,
        dict(source='# + tags = ["parameters"]\n1 + 1',
             ext_in='py',
             kernelspec_name=None,
             static_analysis=False,
             kwargs={})
    ],
    [
        SQLScript,
        dict(source='CREATE TABLE {{product}} FROM some_table', kwargs={})
    ],
    [SQLDump, dict(source='SELECT * FROM some_tablle', kwargs={})],
])
def test_init_source(class_, kwargs):
    assert class_._init_source(**kwargs)


@pytest.mark.parametrize('Task, prod, source', [
    (ShellScript, GenericProduct('file.txt'), 'touch {{product}}'),
    (SQLScript, GenericSQLRelation(
        ('name', 'table')), 'CREATE TABLE {{product}}'),
    (SQLDump, GenericProduct('file.txt'), 'SELECT * FROM {{upstream["key"]}}'),
    (SQLTransfer, GenericSQLRelation(
        ('name', 'table')), 'SELECT * FROM {{upstream["key"]}}'),
    (SQLUpload, GenericSQLRelation(('name', 'table')), 'some_file.txt'),
    (PostgresCopyFrom, PostgresRelation(('name', 'table')), 'file.parquet')
])
def test_task_init_source_with_placeholder_obj(Task, prod, source):
    """
    Testing we can initialize a task with a Placeholder as the source argument
    """
    dag = DAG()
    dag.clients[Task] = object()
    dag.clients[type(prod)] = object()

    Task(Placeholder(source), prod, dag, name='task')


def test_task_build_clears_cached_status(tmp_directory):
    dag = DAG()
    t = PythonCallable(touch, File('my_file'), dag)
    t.render()

    assert t.product._outdated_data_dependencies_status is None
    assert t.product._outdated_code_dependency_status is None

    t.status()

    assert t.product._outdated_data_dependencies_status is not None
    assert t.product._outdated_code_dependency_status is not None

    t.build()

    assert t.product._outdated_data_dependencies_status is None
    assert t.product._outdated_code_dependency_status is None


def test_task_can_infer_name_from_source():
    dag = DAG()
    t = PythonCallable(touch, File('file.txt'), dag)
    assert t.name == 'touch'


def test_task_raises_error_if_name_cannot_be_infered():
    dag = DAG()

    with pytest.raises(AttributeError):
        SQLDump('SELECT * FROM my_table', File('/path/to/data'), dag)


def test_python_callable_with_file():
    dag = DAG()
    t = PythonCallable(touch, File('file.txt'), dag, name='name')
    t.render()

    assert str(t.product) == 'file.txt'
    assert str(t.source) == ('def touch(product):\n    '
                             'Path(str(product)).touch()\n')


def test_postgresscript_with_relation(pg_client_and_schema):
    client, _ = pg_client_and_schema
    dag = DAG()
    t = SQLScript('CREATE TABLE {{product}} AS SELECT * FROM {{name}}',
                  PostgresRelation(('user', 'table', 'table'), client=client),
                  dag,
                  name='name',
                  params=dict(name='some_table'),
                  client=client)

    t.render()

    assert str(t.product) == 'user.table'
    assert (str(
        t.source) == 'CREATE TABLE user.table AS SELECT * FROM some_table')


def test_task_change_in_status(tmp_directory):
    # NOTE: there are some similar tests in test_dag.py - maybe move them?
    dag = DAG('dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}', File('b.txt'),
                     dag, 'tb')
    tc = ShellScript('cat {{upstream["tb"]}} > {{product}}', File('c.txt'),
                     dag, 'tc')

    assert all(
        [t.exec_status == TaskStatus.WaitingRender for t in [ta, tb, tc]])

    ta >> tb >> tc

    dag.render()

    assert (ta.exec_status == TaskStatus.WaitingExecution
            and tb.exec_status == TaskStatus.WaitingUpstream
            and tc.exec_status == TaskStatus.WaitingUpstream)

    # ta.build()

    # assert (ta.exec_status == TaskStatus.Executed
    #         and tb.exec_status == TaskStatus.WaitingExecution
    #         and tc.exec_status == TaskStatus.WaitingUpstream)

    # tb.build()

    # assert (ta.exec_status == TaskStatus.Executed
    #         and tb.exec_status == TaskStatus.Executed
    #         and tc.exec_status == TaskStatus.WaitingExecution)

    # tc.build()

    dag.build()

    assert all([t.exec_status == TaskStatus.Executed for t in [ta, tb, tc]])


def test_raises_render_error_if_missing_param_in_code():
    dag = DAG('my dag')

    ta = ShellScript('{{command}} "a" > {{product}}',
                     File('a.txt'),
                     dag,
                     name='my task')

    with pytest.raises(RenderError):
        ta.render()


def test_raises_render_error_if_missing_param_in_product():
    dag = DAG('my dag')

    ta = ShellScript('echo "a" > {{product}}',
                     File('a_{{name}}.txt'),
                     dag,
                     name='my task')

    with pytest.raises(RenderError):
        ta.render()


def test_raises_render_error_if_non_existing_dependency_used():
    dag = DAG('my dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag, name='bash')
    tb = ShellScript('cat {{upstream.not_valid}} > {{product}}',
                     File('b.txt'),
                     dag,
                     name='bash2')
    ta >> tb

    with pytest.raises(RenderError):
        tb.render()


def test_raises_render_error_if_extra_param_in_code():
    dag = DAG('my dag')

    ta = ShellScript('echo "a" > {{product}}',
                     File('a.txt'),
                     dag,
                     name='my task',
                     params=dict(extra_param=1))

    with pytest.raises(RenderError):
        ta.render()


def test_shows_warning_if_unused_dependencies():
    dag = DAG('dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}', File('b.txt'),
                     dag, 'tb')
    tc = ShellScript('cat {{upstream["tb"]}} > {{product}}', File('c.txt'),
                     dag, 'tc')

    ta >> tb >> tc
    ta >> tc

    ta.render()
    tb.render()

    with pytest.warns(UserWarning):
        tc.render()


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

    t1 = SQLScript(p,
                   PostgresRelation(('schema', 'a_table', 'table')),
                   dag,
                   name='t1')
    t2 = SQLScript(p,
                   PostgresRelation(('schema', 'another_table', 'table')),
                   dag,
                   name='t2')

    assert t1.source._placeholder is not t2.source._placeholder


def test_build_a_single_task(tmp_directory):
    dag = DAG()
    t = PythonCallable(touch, File('1.txt'), dag)
    assert t.build()


def test_building_a_single_task_when_has_unrendered_upstream():
    dag = DAG()
    t1 = PythonCallable(touch, File('1.txt'), dag, name=1)
    t2 = PythonCallable(touch_w_upstream, File('2.txt'), dag, name=2)

    t1 >> t2

    with pytest.raises(TaskBuildError) as excinfo:
        t2.build()

    msg = ('Cannot directly build task "2" as it has upstream dependencies'
           ', call dag.render() first')
    assert msg == str(excinfo.value)


def test_building_a_single_task_when_rendered_upstream():
    dag = DAG()
    t1 = PythonCallable(touch, File('1.txt'), dag, name=1)
    t2 = PythonCallable(touch_w_upstream, File('2.txt'), dag, name=2)

    t1 >> t2

    dag.render()
    t2.build()
