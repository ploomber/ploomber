"""
Runs notebooks under doc/
"""
from pathlib import Path
from glob import glob
import pytest
from conftest import _path_to_tests

import papermill as pm


PATH_TO_DOC = str(_path_to_tests().parent / 'doc')
# ignore auto-generated notebooks
NBS = [nb for nb in glob(str(Path(PATH_TO_DOC, '**/*.ipynb')))
       if 'auto_examples' not in nb]


@pytest.mark.parametrize('path', NBS)
def test_notebook(tmp_directory, path):
    pm.execute_notebook(path, Path(path).name, cwd=tmp_directory)
