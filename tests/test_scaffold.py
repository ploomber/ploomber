from pathlib import Path
import ast

import pytest

from ploomber import tasks
from ploomber import scaffold


@pytest.mark.parametrize('name', ['task.py', 'task.ipynb'])
@pytest.mark.parametrize('extract_upstream', [False, True])
@pytest.mark.parametrize('extract_product', [False, True])
def test_load_scaffold_script(name, extract_product, extract_upstream):
    loader = scaffold.ScaffoldLoader('ploomber_add')
    out = loader.render(name,
                        params=dict(extract_product=extract_product,
                                    extract_upstream=extract_upstream))

    # make sure it generates valid python code, except for the sql template
    if not name.endswith('.sql'):
        ast.parse(out)


def test_load_scaffold_function():
    loader = scaffold.ScaffoldLoader('ploomber_add')
    out = loader.render('function.py',
                        params=dict(function_name='some_function'))
    module = ast.parse(out)

    assert module.body[0].name == 'some_function'


def test_create(backup_test_pkg, tmp_directory):
    loader = scaffold.ScaffoldLoader('ploomber_add')

    loader.create('test_pkg.functions.new_function', {}, tasks.PythonCallable)

    code = Path(backup_test_pkg, 'functions.py').read_text()
    module = ast.parse(code)

    function_names = {
        element.name
        for element in module.body if hasattr(element, 'name')
    }

    assert 'new_function' in function_names


def test_add_scaffold(backup_test_pkg, tmp_directory):
    yaml = """
    meta:
        source_loader:
            module: test_pkg
    tasks:
        - source: notebook.ipynb
        - source: notebook.py
        - source: test_pkg.functions.my_new_function
    """

    Path('pipeline.yaml').write_text(yaml)

    scaffold.add()
    code = Path(backup_test_pkg, 'functions.py').read_text()
    module = ast.parse(code)

    function_names = {
        element.name
        for element in module.body if hasattr(element, 'name')
    }

    assert 'my_new_function' in function_names
    assert Path(backup_test_pkg, 'notebook.ipynb').exists()
    assert Path(backup_test_pkg, 'notebook.py').exists()
