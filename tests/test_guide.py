"""
Test notebooks in doc/
"""
import subprocess
from pathlib import Path
from glob import glob

# we have to use this, nbconvert removes cells that execute shell comands
import jupytext

import pytest
from conftest import _path_to_tests

path_to_doc = _path_to_tests().parent / 'doc'

nbs = [
    f for f in glob(str(Path(path_to_doc, '**', '*.ipynb')))
    if 'auto_examples' not in f
]


# we cannot use papermill since some notebooks use papermill via NotebookRunner
# there is an issue when this happens, so we just run it as scripts using
# ipython directly
def run_notebook(nb):
    print('Running %s' % nb)

    out = 'nb.py'
    jupytext.write(jupytext.read(nb), out)

    # jupytext keeps shell commands but adds them as comments, fix
    lines = []

    for line in Path(out).read_text().splitlines():
        # https://stackoverflow.com/a/29262880/709975
        if line.startswith('# !'):
            line = 'get_ipython().magic("sx %s")' % line[2:]

        lines.append(line)

    Path(out).write_text('\n'.join(lines))

    exit_code = subprocess.call(['ipython', 'nb.py'])

    return exit_code


@pytest.mark.parametrize('nb', nbs, ids=[Path(nb).name for nb in nbs])
def test_examples(nb, tmp_directory):
    # TODO: add timeout
    assert run_notebook(nb) == 0
