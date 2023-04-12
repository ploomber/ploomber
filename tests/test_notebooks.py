"""
Runs notebooks under doc/

NOTE: we cannot run them using papermill.execute_notebook as some notebooks
might run ploomber.NotebookRunner tasks inside them, which will cause
a nested call to papermill.execute_notebook.
"""
import subprocess
from pathlib import Path
from glob import glob
import pytest
from conftest import _path_to_tests

import jupytext
import nbformat


PATH_TO_DOC = str(_path_to_tests().parent / "doc")
# ignore auto-generated notebooks
NBS = [
    nb for nb in glob(str(Path(PATH_TO_DOC, "**/*.ipynb"))) if "auto_examples" not in nb
]


@pytest.mark.parametrize("path", NBS)
def test_notebooks(tmp_directory, path):
    path = Path(path)
    nb = nbformat.reads(path.read_text(), as_version=nbformat.NO_CONVERT)
    py = jupytext.writes(nb, fmt="py")
    Path(path.name).write_text(py)
    assert subprocess.call(["ipython", path.name]) == 0
