from pathlib import Path
from datetime import datetime
from unittest.mock import Mock

import pytest

from ploomber.products.metadata import (Metadata, AbstractMetadata,
                                        MetadataCollection)
from ploomber.products import Product, File
from ploomber._testing_utils import assert_no_extra_attributes_in_class


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

    def _delete_metadata(self):
        pass


class ConcreteMetadata(AbstractMetadata):
    """
    Class to test AbstractMetadata implementation
    """
    def timestamp(self):
        pass

    def _data(self):
        pass

    def stored_source_code(self):
        pass

    def params(self):
        pass

    def update(self, source_code):
        pass

    def delete(self):
        pass

    def _fetch(self):
        pass

    def _get(self):
        pass

    def clear(self):
        pass

    def update_locally(self):
        pass


def test_eq_in_abstract_metadata_using_concrete_class(monkeypatch):
    metadata = ConcreteMetadata(product=Mock())
    monkeypatch.setattr(metadata, '_data', {'a': 1})

    assert metadata == {'a': 1}


@pytest.mark.parametrize('concrete_class', AbstractMetadata.__subclasses__())
def test_interfaces(concrete_class):
    assert_no_extra_attributes_in_class(AbstractMetadata, concrete_class)


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


def test_delete():
    prod = Mock(wraps=FakeProduct(identifier='fake-product'))
    metadata = Metadata(prod)
    assert not prod._delete_metadata.call_count

    metadata.delete()

    assert prod._delete_metadata.call_count == 1


def test_update():
    prod = FakeProduct(identifier='fake-product')
    metadata = Metadata(prod)

    metadata.update('new code', params={'a': 1})

    # check code was updated
    assert metadata.stored_source_code == 'new code'
    assert metadata.params == {'a': 1}


def test_update_with_non_string_keys():
    prod = FakeProduct(identifier='fake-product')
    metadata = Metadata(prod)

    metadata.update('new code', params={1: 1})

    assert metadata.params == {1: 1}


@pytest.mark.parametrize(
    'method, kwargs',
    [['clear', dict()], ['update', dict(source_code='', params={})],
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


@pytest.mark.parametrize(
    't1, t2, expected, should_warn',
    [
        [1, 2, 1, False],
        [None, None, None, False],
        [1, None, None, True],
    ],
)
def test_metadata_collection_timestamp(t1, t2, expected, should_warn):
    p1 = Mock()
    p1.metadata.timestamp = t1

    p2 = Mock()
    p2.metadata.timestamp = t2

    m = MetadataCollection([p1, p2])

    with pytest.warns(None) as record:
        ts = m.timestamp

    assert bool(record) is should_warn
    assert ts == expected


@pytest.mark.parametrize(
    'c1, c2, expected, should_warn',
    [
        ['code', 'code', 'code', False],
        [None, None, None, False],
        ['code', 'other code', None, True],
    ],
)
def test_metadata_collection_stored_source_code(c1, c2, expected, should_warn):
    p1 = Mock()
    p1.metadata.stored_source_code = c1

    p2 = Mock()
    p2.metadata.stored_source_code = c2

    m = MetadataCollection([p1, p2])

    with pytest.warns(None) as record:
        code = m.stored_source_code

    assert bool(record) is should_warn
    assert code == expected


@pytest.mark.parametrize('method', ['_get', 'clear', 'delete'])
def test_metadata_collection_forwards_calls_to_all_products(method):
    p1 = Mock()
    p2 = Mock()

    m = MetadataCollection([p1, p2])

    getattr(m, method)()

    getattr(p1.metadata, method).assert_called_once()
    getattr(p1.metadata, method).assert_called_once()


def test_metadata_collection_update_forwards_to_all_products():
    p1 = Mock()
    p2 = Mock()
    arg = Mock()
    arg2 = Mock()

    m = MetadataCollection([p1, p2])

    m.update(arg, arg2)

    p1.metadata.update.assert_called_once_with(arg, arg2)
    p2.metadata.update.assert_called_once_with(arg, arg2)


def test_metadata_collection_update_locally_forwards_to_all_products():
    p1 = Mock()
    p2 = Mock()
    arg = Mock()

    m = MetadataCollection([p1, p2])

    m.update_locally(arg)

    p1.metadata.update_locally.assert_called_once_with(arg)
    p2.metadata.update_locally.assert_called_once_with(arg)


_METADATA_CASES = [
    [
        # all the same
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1).timestamp()
        },
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1).timestamp()
        },
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1).timestamp()
        },
        False,
    ],
    [
        # different code
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1).timestamp()
        },
        {
            'stored_source_code': 'other code',
            'timestamp': datetime(2021, 1, 1).timestamp()
        },
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1).timestamp()
        },
        True,
    ],
    [
        # slightly different timestamp (1 second)
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1, minute=1, second=0).timestamp()
        },
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1, minute=0, second=59).timestamp()
        },
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1, minute=1, second=0).timestamp()
        },
        False,
    ],
    [
        # slightly different timestamp (1 second), inverted
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1, minute=0, second=59).timestamp()
        },
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1, minute=1, second=0).timestamp()
        },
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1, minute=0, second=59).timestamp()
        },
        False,
    ],
    [
        # both different (code + >5 seconds  timestamp difference)
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1).timestamp()
        },
        {
            'stored_source_code': 'another code',
            'timestamp': datetime(2021, 1, 2).timestamp()
        },
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1).timestamp()
        },
        True,
    ],
    [
        # large difference in timestamp (> 5 seconds)
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1, second=0).timestamp()
        },
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1, second=6).timestamp()
        },
        {
            'stored_source_code': 'code',
            'timestamp': datetime(2021, 1, 1, second=0).timestamp()
        },
        True,
    ]
]


class FakeMetadata(Metadata):
    """Helper testing class to directly pass metadata values
    """
    def __init__(self, stored_source_code, timestamp):
        self._Metadata__data = dict(stored_source_code=stored_source_code,
                                    timestamp=timestamp)
        self._did_fetch = True


@pytest.mark.parametrize('d1, d2, expected, should_warn', _METADATA_CASES)
def test_metadata_collection_to_dict(d1, d2, expected, should_warn):
    p1, p2 = Mock(), Mock()
    p1.metadata = FakeMetadata(**d1)
    p2.metadata = FakeMetadata(**d2)
    m = MetadataCollection([p1, p2])

    m = MetadataCollection([p1, p2])

    with pytest.warns(None) as record:
        d = m.to_dict()

    assert bool(record) is should_warn
    assert d == expected


@pytest.mark.parametrize('d1, d2, expected, should_warn', _METADATA_CASES)
def test_metadata_collection_underscore_data(d1, d2, expected, should_warn):
    p1, p2 = Mock(), Mock()
    p1.metadata = FakeMetadata(**d1)
    p2.metadata = FakeMetadata(**d2)
    m = MetadataCollection([p1, p2])

    with pytest.warns(None) as record:
        d = m._data

    assert bool(record) is should_warn
    assert d == expected


def test_file(tmp_directory):
    Path('file').touch()
    product = File('file')

    m = Metadata(product)

    m.update('some_source_code', {'a': 1})

    m2 = Metadata(product)

    assert m2.stored_source_code == 'some_source_code'
    assert m2.timestamp
    assert m2.params == {'a': 1}


@pytest.mark.parametrize(
    'num_warnings, params',
    [
        [
            1,  # number of instances of object()
            # Single parameter with an unserializable object
            {
                'foo': object()
            }
        ],
        [
            2,
            # Single parameter with multiple unserializable objects
            {
                'foo': [1, 2, object(), object()]
            }
        ],
        [
            1,
            # Single parameter with a nested unserializable object
            {
                'foo': ['bar', {
                    'another': object()
                }]
            }
        ],
        [
            4,
            # Multiple parameters with unserializable parameters
            {
                'foo':
                ['bar',
                 object(), {
                     'another': [1, 2, object(), object()]
                 }],
                'stuff': object(),
            }
        ]
    ])
def test_warns_on_unserializable_params(tmp_directory, num_warnings, params):
    params.update(final_metadata='this')

    Path('file').touch()
    product = File('file')

    m = Metadata(product)

    with pytest.warns(UserWarning) as records:
        m.update('some_source_code', params)

    assert len(records) == num_warnings
    assert all([
        'contains an unserializable object' in record.message.args[0]
        for record in records
    ])
    assert m.stored_source_code == 'some_source_code'
    assert m.params == {'final_metadata': 'this'}


# still missing implementation - check note on Metadata@._get
@pytest.mark.skip
def test_warns_on_corruped_metadata(tmp_directory):
    Path('file').touch()
    product = File('file')
    mock = Mock(side_effect=ValueError)
    product.fetch_metadata = mock

    m = Metadata(product)

    with pytest.warns(UserWarning) as record:
        m._get()

    assert len(record) == 1
    assert 'corrupted metadata, ignoring' in record[0].message.args[0]
    assert m.stored_source_code is None
    assert m.timestamp is None
    assert m.params is None


def test_update_with_resource(tmp_directory):
    Path('file.txt').touch()

    prod = FakeProduct(identifier='fake-product')
    metadata = Metadata(prod)

    metadata.update('new code', params={'resources_': {'file': 'file.txt'}})

    assert metadata.stored_source_code == 'new code'
    assert metadata.params == {
        'resources_': {
            'file': 'd41d8cd98f00b204e9800998ecf8427e'
        }
    }
