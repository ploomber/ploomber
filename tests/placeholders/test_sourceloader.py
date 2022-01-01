import copy
from unittest.mock import Mock
import tempfile
from pathlib import Path

import pytest
from jinja2.exceptions import TemplateRuntimeError, TemplateNotFound

from ploomber import SourceLoader
from ploomber.tasks import SQLTransfer
from ploomber.products import SQLiteRelation
from ploomber import DAG


@pytest.mark.parametrize(
    'processor',
    [
        # should work with strings and paths
        lambda s: s,
        lambda s: Path(s),
    ])
def test_load_template(processor):
    tmp_directory = tempfile.mkdtemp()
    Path(tmp_directory, 'template.sql').write_text('{{file}}')
    source_loader = SourceLoader(str(tmp_directory))

    t = source_loader[processor('template.sql')]
    assert t.render({'file': 'some file'})

    assert source_loader.get('non_existing_template.sql') is None


def test_path_to():
    tmp_directory = tempfile.mkdtemp()
    Path(tmp_directory, 'template.sql').write_text('{{file}}')
    source_loader = SourceLoader(str(tmp_directory))

    assert source_loader.path_to('template.sql') == Path(
        tmp_directory, 'template.sql')
    assert source_loader.path_to('non_template.sql') == Path(
        tmp_directory, 'non_template.sql')


def test_multiple_paths():
    tmp_directory1 = tempfile.mkdtemp()
    tmp_directory2 = tempfile.mkdtemp()
    Path(tmp_directory1, 'template1.sql').write_text('{{file}}')
    Path(tmp_directory2, 'template2.sql').write_text('{{file}}')
    source_loader = SourceLoader([str(tmp_directory1), str(tmp_directory2)])

    assert source_loader['template1.sql']
    assert source_loader['template2.sql']


def test_multiple_paths_mixed_types():
    tmp_directory1 = tempfile.mkdtemp()
    tmp_directory2 = tempfile.mkdtemp()
    Path(tmp_directory1, 'template1.sql').write_text('{{file}}')
    Path(tmp_directory2, 'template2.sql').write_text('{{file}}')
    source_loader = SourceLoader([str(tmp_directory1), Path(tmp_directory2)])

    assert source_loader['template1.sql']
    assert source_loader['template2.sql']


def test_load_from_module():
    source_loader = SourceLoader(module='test_pkg', path='templates')
    assert source_loader['query.sql']


def test_source_loader_and_task(sqlite_client_and_tmp_dir):
    client, tmp_dir = sqlite_client_and_tmp_dir

    Path(tmp_dir, 'data_query.sql').write_text('SELECT * FROM data')
    source_loader = SourceLoader(str(tmp_dir))

    dag = DAG()
    dag.clients[SQLTransfer] = client
    dag.clients[SQLiteRelation] = client

    SQLTransfer(source_loader['data_query.sql'],
                product=SQLiteRelation((None, 'data2', 'table')),
                dag=dag,
                name='transfer')

    dag.build()


def test_raise(tmp_directory):
    Path('template.sql').write_text("{% raise 'some error message' %}")

    loader = SourceLoader(path='.')

    with pytest.raises(TemplateRuntimeError) as excinfo:
        loader['template.sql'].render({})

    assert str(excinfo.value) == 'some error message'


def test_error_template_not_found(tmp_directory):
    Path('templates').mkdir()

    loader = SourceLoader(path='templates')

    with pytest.raises(TemplateNotFound) as excinfo:
        loader['unknown.py']

    path = str(Path('templates', 'unknown.py'))
    assert str(excinfo.value) == ('\'unknown.py\' template does not exist. '
                                  'Based on your configuration, if should '
                                  f'be located at: {path!r}')


def test_error_template_not_found_but_found_in_current_dir(tmp_directory):
    Path('templates').mkdir()
    Path('unknown.py').touch()

    loader = SourceLoader(path='templates')

    with pytest.raises(TemplateNotFound) as excinfo:
        loader['unknown.py']

    assert str(excinfo.value) == (
        "'unknown.py' template does not exist. "
        "However such a file exists in the current working directory, "
        "if you want to load it as a template, move it to 'templates' "
        "or remove the source_loader")


def test_get_item_calls_get_template(monkeypatch):
    # loader[key] is not short for loader.get_template(key), we keep both
    # to keep a jinja-like API. get_template implements the actual logic
    loader = SourceLoader(path='.')

    mock = Mock()
    monkeypatch.setattr(loader, 'get_template', mock)

    loader['some_template.sql']

    mock.assert_called_once_with('some_template.sql')


def test_get_template_nested_with_path(tmp_directory):
    Path('dir').mkdir()
    path = Path('dir', 'template.txt')
    path.write_text('something')

    loader = SourceLoader(path='.')

    assert str(loader[path]) == 'something'


def test_deepcopy(tmp_directory):
    Path('template.sql').write_text('SELECT * FROM table')
    loader = SourceLoader('.')

    # once we load a template, the jinja Environment caches it, and it will
    # fail if we dont correctly implement the deepcopy method
    loader.get_template('template.sql')

    loader_copy = copy.deepcopy(loader)

    # ensure the copy works
    assert loader_copy.get_template('template.sql')
