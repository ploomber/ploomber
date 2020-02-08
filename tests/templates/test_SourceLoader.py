import tempfile
from pathlib import Path

from ploomber.templates import SourceLoader
from ploomber.tasks import SQLTransfer
from ploomber.products import SQLiteRelation
from ploomber import DAG


def test_load_template():
    tmp_directory = tempfile.mkdtemp()
    Path(tmp_directory, 'template.sql').write_text('{{file}}')
    source_loader = SourceLoader(str(tmp_directory))

    t = source_loader['template.sql']
    assert t.render({'file': 'some file'})


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
    source_loader = SourceLoader(module='ploomber')

    assert source_loader['dag.py']


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
