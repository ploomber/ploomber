from pathlib import Path

import pytest

from ploomber.products._resources import process_resources, resolve_resources


@pytest.mark.parametrize('params, expected', [
    [{
        'a': 1
    }, {
        'a': 1
    }],
    [{
        'resources_': {
            'file': 'file.txt'
        }
    }, {
        'resources_': {
            'file': '7a00ddbdae0c81b8824e2b0aaf548df7'
        }
    }],
    [{
        'resources_': {
            'file': 'file.txt'
        },
        'a': 1
    }, {
        'resources_': {
            'file': '7a00ddbdae0c81b8824e2b0aaf548df7'
        },
        'a': 1
    }],
])
def test_process_resources(tmp_directory, params, expected):
    Path('file.txt').write_text('my resource file')
    assert process_resources(params) == expected


@pytest.mark.parametrize('fn, kwargs', [
    [process_resources, dict()],
    [resolve_resources, dict(relative_to='')],
])
def test_error_on_incorrect_resources_value_type(fn, kwargs):
    with pytest.raises(TypeError) as excinfo:
        fn({'resources_': {'file': 1}}, **kwargs)

    expected = ("Error reading params.resources_ with key 'file'. "
                "Expected value 1 to be a str, bytes or os.PathLike, not int")
    assert str(excinfo.value) == expected


def test_error_on_missing_file_in_process_resources_value(tmp_directory):
    with pytest.raises(FileNotFoundError) as excinfo:
        process_resources({'resources_': {'file': 'non-existing-file'}})

    expected = ("Error reading params.resources_ with key 'file'. "
                "Expected value 'non-existing-file' to be an existing file.")
    assert str(excinfo.value) == expected


@pytest.mark.parametrize('fn, kwargs', [
    [process_resources, dict()],
    [resolve_resources, dict(relative_to='')],
])
def test_error_if_resources_key_isnt_a_dictionary(fn, kwargs):
    with pytest.raises(TypeError) as excinfo:
        fn({'resources_': 1}, **kwargs)

    expected = ("Error reading params.resources_. 'resources_' must be a "
                "dictionary with paths to files to track, but got a value "
                "1 with type int")
    assert str(excinfo.value) == expected


@pytest.mark.parametrize('fn, kwargs', [
    [process_resources, dict()],
    [resolve_resources, dict(relative_to='')],
])
def test_returned_object_is_a_deep_copy(fn, kwargs):
    nested = {'a': 1}
    in_ = {'key': nested}
    out = fn(in_, **kwargs)

    assert out is not in_
    assert out['key'] is not nested


@pytest.mark.parametrize('fn, kwargs', [
    [process_resources, dict()],
    [resolve_resources, dict(relative_to='')],
])
def test_returns_none_if_input_is_none(fn, kwargs):
    assert fn(None, **kwargs) is None


def test_resolve_resources(tmp_directory):
    Path('file.txt').touch()

    out = resolve_resources({'resources_': {
        'some_file': 'file.txt'
    }},
                            relative_to='.')

    assert out == {
        'resources_': {
            'some_file': str(Path('file.txt').resolve())
        }
    }


def test_resolve_resources_error_if_missing_file(tmp_directory):
    with pytest.raises(FileNotFoundError) as excinfo:
        resolve_resources({'resources_': {
            'some_file': 'file.txt'
        }},
                          relative_to='.')

    expected = ("Error reading params.resources_ with key 'some_file'. "
                "Expected value 'file.txt' to be an existing file.")
    assert str(excinfo.value) == expected
