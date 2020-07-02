import pytest
from ploomber.spec.TaskDict import TaskDict


@pytest.mark.parametrize('key', ['source', 'product'])
def test_validate_missing_source(key):
    with pytest.raises(KeyError):
        TaskDict({key: None},
                 {'extract_product': False, 'extract_upstream': False})


@pytest.mark.parametrize('task, meta', [
    ({'upstream': 'some_task', 'product': None, 'source': None},
        {'extract_upstream': True, 'extract_product': False}),
    ({'product': 'report.ipynb', 'source': None}, {
     'extract_product': True, 'extract_upstream': False}),
])
def test_error_if_extract_but_keys_declared(task, meta):
    with pytest.raises(ValueError):
        TaskDict(task, meta)
