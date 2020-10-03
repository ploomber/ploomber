from unittest.mock import MagicMock
from ploomber.products.Metadata import Metadata


def test_clear():
    prod = MagicMock()
    prod.exists.return_value = True
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
    prod = MagicMock()
    metadata = Metadata(prod)
    assert not prod.delete_metadata.call_count

    metadata.delete()

    assert prod.delete_metadata.call_count == 1
