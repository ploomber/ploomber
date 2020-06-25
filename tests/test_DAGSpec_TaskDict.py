import pytest
from ploomber.spec.TaskDict import TaskDict
from ploomber.spec.DAGSpec import DAGSpec


@pytest.mark.parametrize('task', [
    {'upstream': 'some_task'},
    {'product': 'report.ipynb'}
])
def test_error_if_extract_upstream_but_upstream_declared(task):

    spec = DAGSpec({'tasks': []})
    spec['meta']

    with pytest.raises(ValueError):
        TaskDict(task, spec['meta'])
