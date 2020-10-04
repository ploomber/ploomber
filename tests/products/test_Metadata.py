from unittest.mock import Mock

import pytest

from ploomber.products.Metadata import Metadata
from ploomber.products import Product


class FakeProduct(Product):
    def _init_identifier(self, identifier):
        return identifier

    def fetch_metadata(self):
        pass

    def save_metadata(self, metadata):
        pass

    def exists(self):
        pass

    def delete(self, force=False):
        pass


def test_clear():
    prod = Mock(wraps=FakeProduct(identifier='fake-product'))
    # we need this because if it doesn't exist, fetch_metata is skipped
    prod.exists.return_value = True
    prod.fetch_metadata.return_value = dict(timestamp=None,
                                            stored_source_code=None)
    metadata = Metadata(prod)

    # this should trigger one fetch call
    metadata.timestamp

    assert prod.fetch_metadata.call_count == 1

    # clear in memory copy
    metadata.clear()
    # this should trigger another fetch
    metadata.timestamp

    assert prod.fetch_metadata.call_count == 2


@pytest.mark.xfail(reason='delete_metadata is not part of the abstract class')
def test_delete():
    prod = Mock(wraps=FakeProduct(identifier='fake-product'))
    metadata = Metadata(prod)
    assert not prod.delete_metadata.call_count

    metadata.delete()

    assert prod.delete_metadata.call_count == 1


def test_update():
    prod = FakeProduct(identifier='fake-product')

    metadata = Metadata(prod)

    metadata.update('new code')

    # check code was updated
    assert metadata._data['stored_source_code'] == 'new code'


@pytest.mark.parametrize(
    'method, kwargs',
    [['clear', dict()], ['update', dict(source_code='')],
     ['update_locally', dict(data=dict())]])
def test_cache_flags_are_cleared_up(method, kwargs):
    prod = FakeProduct(identifier='fake-product')
    prod._outdated_data_dependencies_status = 1
    prod._outdated_code_dependency_status = 1

    metadata = Metadata(prod)
    getattr(metadata, method)(**kwargs)

    # check cache flags were cleared up
    assert prod._outdated_data_dependencies_status is None
    assert prod._outdated_code_dependency_status is None
