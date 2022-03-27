from unittest.mock import Mock
from pathlib import Path

import pytest

from ploomber.cloud import io


@pytest.mark.parametrize('file_size, max_size, expected', [
    [2, 1, [(0, 1), (1, 2)]],
    [7, 2, [(0, 2), (2, 4), (4, 6), (6, 7)]],
])
def test_yield_index(file_size, max_size, expected):
    assert list(io.yield_index(file_size, max_size)) == expected


@pytest.mark.parametrize('i, j, expected', [
    [4, 6, [4, 5]],
    [10, 13, [10, 11, 12]],
])
def test_read_from_index(tmp_directory, i, j, expected):
    Path('file').write_bytes(bytearray(range(256)))

    out = io.read_from_index('file', i, j)

    assert out == bytearray(expected)


def test_yield_parts(tmp_directory):
    Path('file').write_bytes(bytearray(range(10)))

    assert list(io.yield_parts('file', 3)) == [
        bytearray([0, 1, 2]),
        bytearray([3, 4, 5]),
        bytearray([6, 7, 8]),
        bytearray([9])
    ]


@pytest.mark.parametrize('n_bytes, max_size, expected', [
    [10, 1, 10],
    [10, 2, 5],
    [10, 3, 4],
])
def test_n_parts(tmp_directory, n_bytes, max_size, expected):
    Path('file').write_bytes(bytearray(range(n_bytes)))

    assert io.n_parts('file', max_size) == expected


def test_generate_links(monkeypatch):
    monkeypatch.setattr(io.boto3, 'client', Mock())

    links = io.generate_links('bucket', 'file.csv', 'someid', 10)

    assert len(links) == 10
