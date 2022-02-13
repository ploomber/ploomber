import pytest
import nbconvert

from pathlib import Path

from ploomber.tasks.notebook import NotebookConverter
from ploomber.exceptions import TaskInitializationError


@pytest.mark.parametrize('file, exporter_name', [
    ['file.md', 'markdown'],
    ['file.html', 'html'],
    ['file.tex', 'latex'],
    ['file.pdf', 'pdf'],
    ['file.rst', 'rst'],
])
def test_infer_exporter_from_extension(file, exporter_name):
    converter = NotebookConverter(file, exporter_name=None)
    assert converter._exporter is nbconvert.get_exporter(exporter_name)


def test_exporter_name():
    converter = NotebookConverter('file', exporter_name='latex')
    assert converter._exporter is nbconvert.get_exporter('latex')


def test_error_if_cannot_infer_from_extension_and_missing_exporter_name():
    with pytest.raises(TaskInitializationError) as excinfo:
        NotebookConverter('out.extension')

    assert ("Could not determine format for product 'out.extension'"
            in str(excinfo.value))


def test_error_if_cannot_infer_from_extension_suggests_pipeline_yaml(
        tmp_directory):
    Path('pipeline.yaml').touch()

    with pytest.raises(TaskInitializationError) as excinfo:
        NotebookConverter('out.extension')

    assert ("Could not determine format for product 'out.extension'"
            in str(excinfo.value))
    assert 'nb: products/output-notebook.ipynb' in str(excinfo.value)


def test_error_if_missing_exporter_name_and_extension():
    with pytest.raises(TaskInitializationError) as excinfo:
        NotebookConverter('file')

    assert ("Could not determine format for product 'file'"
            in str(excinfo.value))


def test_error_if_missing_exporter_name_and_extension_suggests_pipeline_yaml(
        tmp_directory):
    Path('pipeline.yaml').touch()

    with pytest.raises(TaskInitializationError) as excinfo:
        NotebookConverter('file')

    assert ("Could not determine format for product 'file'"
            in str(excinfo.value))
    assert 'nb: products/output-notebook.ipynb' in str(excinfo.value)


def test_error_if_wrong_exporter_name():
    with pytest.raises(TaskInitializationError) as excinfo:
        NotebookConverter('file.ext', exporter_name='something')

    expected = "'something' is not a valid 'nbconvert_exporter_name' value."
    assert expected in str(excinfo.value)
