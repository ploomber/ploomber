import pytest

from ploomber.tasks.Task import Task
from ploomber._testing_utils import assert_no_extra_attributes_in_class


@pytest.mark.parametrize('concrete_class', Task.__subclasses__())
def test_interface(concrete_class):
    """
    Look for unnecessary implemeneted methods/attributes in MetaProduct,
    this helps us keep the API up-to-date if the Product interface changes
    """
    allowed_mapping = {
        'Input': {'_true', '_null_update_metadata'},
        'Link': {'_false'},
        'PythonCallable': {'load', '_interactive_developer'},
        'SQLScript': {'load'},
    }

    allowed = allowed_mapping.get(concrete_class.__name__, {})

    assert_no_extra_attributes_in_class(Task, concrete_class, allowed=allowed)
