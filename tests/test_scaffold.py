import ast

import pytest

from ploomber.scaffold.ScaffoldLoader import ScaffoldLoader


@pytest.mark.parametrize('name', ['task.py', 'task.ipynb'])
@pytest.mark.parametrize('extract_upstream', [False, True])
@pytest.mark.parametrize('extract_product', [False, True])
def test_load_scaffold(name, extract_product, extract_upstream):
    loader = ScaffoldLoader('ploomber_add')
    out = loader.render(name,
                        params=dict(extract_product=extract_product,
                                    extract_upstream=extract_upstream))

    # make sure it generates valid python code, except for the sql template
    if not name.endswith('.sql'):
        ast.parse(out)
