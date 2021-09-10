from unittest.mock import Mock

import nbformat
import jupytext
import pytest

from ploomber.static_analysis import pyflakes
from ploomber.exceptions import RenderError


@pytest.mark.parametrize('code', [
    """
if
""",
    """
while
""",
])
def test_check_source_syntax_error(code):
    nb = jupytext.reads(code)

    with pytest.raises(SyntaxError):
        pyflakes.check_source(nb)


def test_check_source_ignores_non_code_cells():
    v = nbformat.versions[nbformat.current_nbformat]
    nb = v.new_notebook()
    nb.cells = [
        v.new_code_cell('1 + 1'),
        v.new_markdown_cell('Some markdown'),
        v.new_raw_cell('Some raw cell')
    ]

    assert pyflakes.check_source(nb) is None


def test_check_source_warns_on_unexpected_error(monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(pyflakes.pyflakes_api.ast, 'parse',
                  Mock(side_effect=ValueError))

        with pytest.warns(UserWarning) as record:
            pyflakes.check_source(jupytext.reads(''))

    assert len(record) == 1
    expected = ("An unexpected error happened when analyzing code: ': "
                "problem decoding source'")
    assert record[0].message.args[0] == expected


@pytest.mark.parametrize('code', [
    """
x = 1

if y:
    pass
""",
    """
def x():
    df = pd.read_csv()
""",
    """
def fn(a, a):
    pass
""",
    'return',
    'yield',
    'continue',
    'break',
],
                         ids=[
                             'undefined-name',
                             'undefined-local',
                             'duplicate-argument',
                             'return-outside-fn',
                             'yield-outside-fn',
                             'continue-outside-fn',
                             'break-outside-fn',
                         ])
def test_check_source_errors(code):
    nb = jupytext.reads(code)

    with pytest.raises(RenderError):
        pyflakes.check_source(nb)


@pytest.mark.parametrize('params, source, first, second', [
    [
        dict(a=1),
        '',
        "Unexpected params: 'a'",
        "to fix this, add 'a'",
    ],
    [
        dict(a=1, b=2),
        '',
        "Unexpected params: 'a', and 'b'",
        "to fix this, add them",
    ],
    [
        dict(),
        'a = None\n b = None',
        "Missing params: 'a', and 'b'",
        "to fix this, pass them",
    ],
    [
        dict(),
        'a = None',
        "Missing params: 'a'",
        "to fix this, pass 'a'",
    ],
    [
        dict(a=1),
        'b = None',
        "Missing params: 'b' (to fix this, pass 'b' in the 'params' "
        "argument).",
        "Unexpected params: 'a' (to fix this, add 'a' to the "
        "'parameters' cell and assign the value as None. e.g., a = None).",
    ],
],
                         ids=[
                             'one-unexpected',
                             'many-unexpected',
                             'many-missing',
                             'one-missing',
                             'many-unexpected-and-many-missing',
                         ])
def test_check_params(params, source, first, second):
    with pytest.raises(TypeError) as excinfo:
        pyflakes.check_params(params, source, 'script.py')

    assert first in str(excinfo.value)
    assert second in str(excinfo.value)
    assert 'script.py' in str(excinfo.value)
