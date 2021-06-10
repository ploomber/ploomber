from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.executors import Serial
from ploomber.tasks import PythonCallable, TaskGroup
from ploomber.products import File, SQLRelation


def touch(product, param):
    pass


def touch_a_b(product, a, b):
    pass


def test_from_params():
    dag = DAG()
    group = TaskGroup.from_params(PythonCallable,
                                  File,
                                  'dir/file.txt', {'source': touch},
                                  dag,
                                  name='task_group',
                                  params_array=[{
                                      'param': 1
                                  }, {
                                      'param': 2
                                  }])

    dag.render()

    assert len(group) == 2
    assert dag['task_group0'].source.primitive is touch
    assert dag['task_group1'].source.primitive is touch
    assert str(dag['task_group0'].product) == str(Path('dir', 'file-0.txt'))
    assert str(dag['task_group1'].product) == str(Path('dir', 'file-1.txt'))


def test_from_params_resolves_paths():
    dag = DAG()
    TaskGroup.from_params(PythonCallable,
                          File,
                          'dir/file.txt', {'source': touch},
                          dag,
                          name='task_group',
                          params_array=[{
                              'param': 1
                          }, {
                              'param': 2
                          }],
                          resolve_relative_to='')

    dag.render()

    assert Path(dag['task_group0'].product) == Path('dir',
                                                    'file-0.txt').resolve()
    assert Path(dag['task_group1'].product) == Path('dir',
                                                    'file-1.txt').resolve()


def test_from_params_with_namer():
    def namer(params):
        return 'param={}'.format(params['param'])

    dag = DAG()
    group = TaskGroup.from_params(PythonCallable,
                                  File,
                                  '{{name}}.txt', {'source': touch},
                                  dag,
                                  namer=namer,
                                  params_array=[{
                                      'param': 1
                                  }, {
                                      'param': 2
                                  }])

    dag.render()

    assert len(group) == 2
    assert dag['param=1'].source.primitive is touch
    assert dag['param=2'].source.primitive is touch
    assert str(dag['param=1'].product) == 'param=1.txt'
    assert str(dag['param=2'].product) == 'param=2.txt'


def test_from_params_error_if_name_and_namer_are_nome():
    dag = DAG()

    with pytest.raises(ValueError) as excinfo:
        TaskGroup.from_params(PythonCallable,
                              File,
                              'dir/file.txt', {'source': touch},
                              dag,
                              name=None,
                              namer=None,
                              params_array=[{
                                  'param': 1
                              }, {
                                  'param': 2
                              }])
    expected = 'Only one of name and namer can be None, but not both'
    assert expected == str(excinfo.value)


def test_from_grid():
    dag = DAG()
    group = TaskGroup.from_grid(PythonCallable,
                                File,
                                'file.txt', {
                                    'source': touch_a_b,
                                },
                                dag,
                                name='task_group',
                                grid={
                                    'a': [1, 2],
                                    'b': [3, 4]
                                })

    assert len(group) == 4


def test_from_grid_resolve_relative_to():
    dag = DAG()
    TaskGroup.from_grid(PythonCallable,
                        File,
                        'file.txt', {
                            'source': touch_a_b,
                        },
                        dag,
                        name='task_group',
                        grid={
                            'a': [1, 2],
                            'b': [3, 4]
                        },
                        resolve_relative_to='')

    assert str(dag['task_group0'].product) == str(Path('file-0.txt').resolve())
    assert str(dag['task_group1'].product) == str(Path('file-1.txt').resolve())
    assert str(dag['task_group2'].product) == str(Path('file-2.txt').resolve())
    assert str(dag['task_group3'].product) == str(Path('file-3.txt').resolve())


def test_metaproduct():
    dag = DAG()
    group = TaskGroup.from_params(PythonCallable,
                                  File, {
                                      'one': 'one.txt',
                                      'another': 'another.txt'
                                  }, {'source': touch},
                                  dag,
                                  name='task_group',
                                  params_array=[{
                                      'param': 1
                                  }, {
                                      'param': 2
                                  }])

    assert str(dag['task_group0'].product['one']) == 'one-0.txt'
    assert str(dag['task_group0'].product['another']) == 'another-0.txt'
    assert str(dag['task_group1'].product['one']) == 'one-1.txt'
    assert str(dag['task_group1'].product['another']) == 'another-1.txt'
    assert len(group) == 2


def test_from_params_resolves_paths_in_metaproduct(tmp_directory):
    def touch(product, param):
        Path(product['one']).touch()
        Path(product['another']).touch()

    dag = DAG(executor=Serial(build_in_subprocess=False))
    TaskGroup.from_params(PythonCallable,
                          File, {
                              'one': 'one.txt',
                              'another': 'another.txt'
                          }, {'source': touch},
                          dag,
                          name='task_group',
                          params_array=[{
                              'param': 1
                          }, {
                              'param': 2
                          }],
                          resolve_relative_to='')

    # on windows, paths do not resolve if the file doesn't exist, so we run
    # the pipeline to ensure they do
    dag.build()

    assert Path(dag['task_group0'].product['one']).resolve() == Path(
        'one-0.txt').resolve()
    assert Path(dag['task_group0'].product['another']).resolve() == Path(
        'another-0.txt').resolve()
    assert Path(dag['task_group1'].product['one']).resolve() == Path(
        'one-1.txt').resolve()
    assert Path(dag['task_group1'].product['another']).resolve() == Path(
        'another-1.txt').resolve()


def test_sql_product():
    dag = DAG()
    TaskGroup.from_params(PythonCallable,
                          SQLRelation, ['schema', 'one', 'table'],
                          {'source': touch},
                          dag=dag,
                          name='task_group',
                          params_array=[{
                              'param': 1
                          }, {
                              'param': 2
                          }])

    id_ = dag['task_group0'].product
    assert (id_.schema, id_.name, id_.kind) == ('schema', 'one-0', 'table')
    id_ = dag['task_group1'].product
    assert (id_.schema, id_.name, id_.kind) == ('schema', 'one-1', 'table')


def test_sql_meta_product():
    dag = DAG()
    TaskGroup.from_params(PythonCallable,
                          SQLRelation, {
                              'one': ['schema', 'one', 'table'],
                              'another': ['another', 'view']
                          }, {'source': touch},
                          dag=dag,
                          name='task_group',
                          params_array=[{
                              'param': 1
                          }, {
                              'param': 2
                          }])

    id_ = dag['task_group0'].product['one']
    assert (id_.schema, id_.name, id_.kind) == ('schema', 'one-0', 'table')
    id_ = dag['task_group0'].product['another']
    assert (id_.schema, id_.name, id_.kind) == (None, 'another-0', 'view')
    id_ = dag['task_group1'].product['one']
    assert (id_.schema, id_.name, id_.kind) == ('schema', 'one-1', 'table')
    id_ = dag['task_group1'].product['another']
    assert (id_.schema, id_.name, id_.kind) == (None, 'another-1', 'view')


@pytest.mark.parametrize('key', ['dag', 'name', 'params'])
def test_error_if_non_permitted_key_in_task_kwargs(key):
    dag = DAG()

    with pytest.raises(KeyError) as excinfo:
        TaskGroup.from_params(PythonCallable,
                              File,
                              'file.txt', {key: None},
                              dag,
                              name='task_group',
                              params_array=[{
                                  'param': 1
                              }, {
                                  'param': 2
                              }])

    assert 'should not be part of task_kwargs' in str(excinfo.value)


def test_error_if_required_keys_not_in_task_kwargs():
    dag = DAG()

    with pytest.raises(KeyError) as excinfo:
        TaskGroup.from_params(PythonCallable,
                              File,
                              'file.txt',
                              dict(),
                              dag,
                              name='task_group',
                              params_array=[{
                                  'param': 1
                              }, {
                                  'param': 2
                              }])

    assert 'should be in task_kwargs' in str(excinfo.value)


def test_error_if_wrong_product_primitive():
    dag = DAG()

    with pytest.raises(NotImplementedError):
        TaskGroup.from_params(PythonCallable,
                              File,
                              b'wrong-type.txt', {'source': touch},
                              dag,
                              name='task_group',
                              params_array=[{
                                  'param': 1
                              }])
