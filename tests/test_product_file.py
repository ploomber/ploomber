from pathlib import Path

from ploomber.products import File


def test_file_initialized_with_str():
    f = File('/path/to/file')
    f.render({})
    assert str(f) == '/path/to/file'


def test_file_initialized_with_path():
    path = Path('/path/to/file')
    f = File(path)
    f.render({})
    assert str(f) == str(path)


def test_file_is_rendered_correctly():
    f = File('/path/to/{{name}}')
    f.render(params=dict(name='file'))
    assert str(f) == '/path/to/file'


def test_file_delete(tmp_directory):
    f = Path('file')
    f.touch()
    File('file').delete()

    assert not f.exists()


def test_file_delete_directory(tmp_directory):
    d = Path('dir')
    d.mkdir()
    (d / 'file.txt').touch()

    File('dir').delete()

    assert not d.exists()


def test_delete_non_existing_metadata(tmp_directory):
    File('some_file')._delete_metadata()
    assert not Path('some_file.source').exists()


def test_delete_metadata(tmp_directory):
    Path('some_file.source').touch()
    File('some_file')._delete_metadata()
    assert not Path('some_file.source').exists()


def test_repr_relative():
    assert repr(File('a/b/c')) == "File('a/b/c')"


def test_repr_absolute():
    assert repr(File('/a/b/c')) == "File('/a/b/c')"


def test_repr_absolute_shows_as_relative_if_possible():
    path = Path('.').resolve() / 'a'
    assert repr(File(path)) == "File('a')"
