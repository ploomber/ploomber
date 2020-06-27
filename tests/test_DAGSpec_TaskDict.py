import pytest
from ploomber.spec.TaskDict import TaskDict


@pytest.mark.parametrize('task, meta', [
    ({'upstream': 'some_task'}, {'extract_upstream': True, 'extract_product': False}),
    ({'product': 'report.ipynb'}, {'extract_product': True, 'extract_upstream': False}),
])
def test_error_if_extract_but_keys_declared(task, meta):

    with pytest.raises(ValueError):
        TaskDict(task, meta)
