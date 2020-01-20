from pathlib import Path
from ploomber.products import File


def test_file_initialized_with_str():
    f = File('/path/to/file')
    f.render({})
    assert str(f) == '/path/to/file'


def test_file_initialized_with_path():
    f = File(Path('/path/to/file'))
    f.render({})
    assert str(f) == '/path/to/file'


def test_file_is_rendered_correctly():
    f = File('/path/to/{{name}}')
    f.render(params=dict(name='file'))
    assert str(f) == '/path/to/file'
