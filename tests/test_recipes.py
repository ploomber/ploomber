"""
Runs examples form examples/
"""
import os
import pytest
import subprocess
from conftest import _path_to_tests


_recipes = [f for f in os.listdir(str(_path_to_tests().parent / 'recipes'))
            if f.endswith('.py')]


@pytest.mark.parametrize('name', _recipes)
def test_examples(tmp_recipes_directory, name):
    # TODO: add timeout
    assert subprocess.call(['python', name]) == 0
