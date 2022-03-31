import pytest

from ploomber.tasks.abc import Task
from ploomber._testing_utils import assert_no_extra_attributes_in_class


@pytest.mark.parametrize('concrete_class', Task.__subclasses__())
def test_interface(concrete_class):
    """
    Look for unnecessary implemented methods/attributes in Tasks concrete
    classes, this helps us keep the API up-to-date
    """
    allowed_mapping = {
        'Input': {'_true', '_null_update_metadata'},
        'Link': {'_false'},
        'PythonCallable': {'load', '_interactive_developer'},
        'SQLScript': {'load'},
        'NotebookRunner': {'static_analysis'},
        'ScriptRunner': {'static_analysis'}
    }

    allowed = allowed_mapping.get(concrete_class.__name__, {})

    assert_no_extra_attributes_in_class(Task, concrete_class, allowed=allowed)
