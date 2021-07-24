from pathlib import Path

import pytest

from ploomber.products._resources import process_resources


@pytest.mark.parametrize('params, expected', [
    [{
        'a': 1
    }, {
        'a': 1
    }],
    [{
        'resource__file': 'file.txt'
    }, {
        'resource__file': '7a00ddbdae0c81b8824e2b0aaf548df7'
    }],
    [{
        'resource__file': 'file.txt',
        'a': 1
    }, {
        'resource__file': '7a00ddbdae0c81b8824e2b0aaf548df7',
        'a': 1
    }],
])
def test_process_resources(tmp_directory, params, expected):
    Path('file.txt').write_text('my resource file')
    assert process_resources(params) == expected


def test_error_on_incorrect_process_resources_value_type():
    with pytest.raises(TypeError) as excinfo:
        process_resources({'resource__file': 1})

    expected = ("Error reading params resource with key 'resource__file'. "
                "Expected value 1 to be a str, bytes or os.PathLike, not int")
    assert str(excinfo.value) == expected


def test_error_on_missing_file_in_process_resources_value(tmp_directory):
    with pytest.raises(FileNotFoundError) as excinfo:
        process_resources({'resource__file': 'non-existing-file'})

    expected = ("Error reading params resource with key 'resource__file'. "
                "Expected value 'non-existing-file' to be an existing file.")
    assert str(excinfo.value) == expected
