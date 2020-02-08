import tempfile
from pathlib import Path

from ploomber.templates import SQLStore


def test_load_template():
    tmp_directory = tempfile.mkdtemp()
    Path(tmp_directory, 'template.sql').write_text('{{file}}')
    source_loader = SQLStore(str(tmp_directory))

    t = source_loader['template.sql']
    assert t.render({'file': 'some file'})


def test_multiple_paths():
    tmp_directory1 = tempfile.mkdtemp()
    tmp_directory2 = tempfile.mkdtemp()
    Path(tmp_directory1, 'template1.sql').write_text('{{file}}')
    Path(tmp_directory2, 'template2.sql').write_text('{{file}}')
    source_loader = SQLStore([str(tmp_directory1), str(tmp_directory2)])

    assert source_loader['template1.sql']
    assert source_loader['template2.sql']


def test_multiple_paths_mixed_types():
    tmp_directory1 = tempfile.mkdtemp()
    tmp_directory2 = tempfile.mkdtemp()
    Path(tmp_directory1, 'template1.sql').write_text('{{file}}')
    Path(tmp_directory2, 'template2.sql').write_text('{{file}}')
    source_loader = SQLStore([str(tmp_directory1), Path(tmp_directory2)])

    assert source_loader['template1.sql']
    assert source_loader['template2.sql']


def test_load_from_module():
    source_loader = SQLStore(module='ploomber')

    assert source_loader['dag.py']
