import pytest

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.helpers import make_task_group


def touch(product, name, param):
    pass


def test_create_group():
    dag = DAG()
    group = make_task_group(PythonCallable, {
        'source': touch,
        'product': File('file_{{name}}.txt')
    },
                            dag,
                            name='task_group',
                            params_array=[{
                                'param': 1
                            }, {
                                'param': 2
                            }])

    assert len(group) == 2

    dag.render()

    assert dag['task_group0'].source.primitive is touch
    assert dag['task_group1'].source.primitive is touch
    assert str(dag['task_group0'].product) == 'file_0.txt'
    assert str(dag['task_group1'].product) == 'file_1.txt'


@pytest.mark.parametrize('key', ['dag', 'name', 'params'])
def test_error_if_non_permitted_key_in_task_kwargs(key):
    dag = DAG()

    with pytest.raises(KeyError) as excinfo:
        make_task_group(PythonCallable, {key: None},
                        dag,
                        name='task_group',
                        params_array=[{
                            'param': 1
                        }, {
                            'param': 2
                        }])

    assert 'should not be part of task_kwargs' in str(excinfo.value)


@pytest.mark.parametrize('key', ['product'])
def test_error_if_required_keys_not_in_task_kwargs(key):
    dag = DAG()

    kwargs = {'product': None, 'source': None}

    kwargs.pop(key)

    with pytest.raises(KeyError) as excinfo:
        make_task_group(PythonCallable,
                        kwargs,
                        dag,
                        name='task_group',
                        params_array=[{
                            'param': 1
                        }, {
                            'param': 2
                        }])

    assert 'should be in task_kwargs' in str(excinfo.value)
