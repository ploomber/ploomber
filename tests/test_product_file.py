from pathlib import Path
from ploomber.products import File


def test_initializes_metadata():
    f = File('/path/to/file')
    assert f.metadata == {'timestamp': None, 'stored_source_code': None}


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


def test_suffix():
    assert File('some/file.txt').suffix == '.txt'


def test_suffix_when_tags_present():
    assert File('{{some}}/{{file}}.txt').suffix == '.txt'


def test_delete_non_existing_metadata(tmp_directory):
    File('some_file').delete_metadata()
    assert not Path('some_file.source').exists()


def test_delete_metadata(tmp_directory):
    Path('some_file.source').touch()
    File('some_file').delete_metadata()
    assert not Path('some_file.source').exists()
