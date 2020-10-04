import pytest

from ploomber.products import MetaProduct, Product
from ploomber._testing_utils import assert_no_extra_attributes_in_class


@pytest.mark.parametrize('concrete_class',
                         Product.__subclasses__() + [MetaProduct])
def test_interface(concrete_class):
    """
    Look for unnecessary implemeneted methods/attributes in MetaProduct,
    this helps us keep the API up-to-date if the Product interface changes
    """
    allowed_mapping = {
        'SQLRelation': {'schema', 'name', 'kind', 'client'},
        'SQLiteRelation': {'schema', 'name', 'kind', 'client'},
        'PostgresRelation': {'schema', 'name', 'kind', 'client'},
        'MetaProduct': {'get', 'keys', 'items', 'values'},
        'GenericProduct': {'client', 'name'}
    }

    allowed = allowed_mapping.get(concrete_class.__name__, {})

    # look for extra attrs, but allow the ones we get from
    # collections.abc.Mapping
    assert_no_extra_attributes_in_class(Product,
                                        concrete_class,
                                        allowed=allowed)
