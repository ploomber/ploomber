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


@pytest.mark.parametrize('code', [
    """
%debug
""", """
# some comment
%line_magic
""", """
# +
x = 1

# +
y = 2

# +
%debug

# +
%%sh
""", """
# +
%%time
df = 1

# +
df
"""
])
def test_check_source_ignores_ipython_magics(code):
    pyflakes.check_source(jupytext.reads(code))


@pytest.mark.parametrize(
    'code, expected',
    [
        [
            """%%html
some html""",
            """# %%html
# some html""",
        ],
        [
            """%%html
some html
more html""",
            """# %%html
# some html
# more html""",
        ],
        # cell magics cannot have comments
        # so we shouldn't change the content
        [
            """# some comment
%%html
some html""", """# some comment
# %%html
# some html"""
        ],
        [
            """
# some comment
%%html
some html""", """
# some comment
# %%html
# some html"""
        ],
        ["""\
   %%html
some html\
""", """\
#    %%html
# some html\
"""],
        ['%cd', '# %cd'],
        ['   %cd', '#    %cd'],
        ['%cd\n%cd', '# %cd\n# %cd'],
        ['\n%cd', '\n# %cd'],
        ['1 + 1\n%cd', '1 + 1\n# %cd'],
        ['1 + 1\n   %cd', '1 + 1\n#    %cd'],
        ['! mkdir stuff', '# ! mkdir stuff'],
        ['   ! mkdir stuff', '#    ! mkdir stuff'],
        # this contains inline python, only the magic should be commented
        ['%%time\n1+1', '# %%time\n1+1'],
        ['%%timeit\n1+1', '# %%timeit\n1+1'],
        ['%%capture\n1+1', '# %%capture\n1+1'],
    ])
# TODO: test with leading spaces
def test_comment_if_ipython_magic(code, expected):
    assert pyflakes._comment_if_ipython_magic(code) == expected


@pytest.mark.parametrize('code, expected', [
    ['%debug', True],
    ['%%sh', False],
    ['%%sh --no-raise-error', False],
    ['# %debug', False],
    ['% debug', False],
    ['%%%debug', False],
])
def test_is_ipython_line_magic(code, expected):
    assert pyflakes._is_ipython_line_magic(code) is expected


@pytest.mark.parametrize(
    'code, expected',
    [
        ['%debug', False],
        ['%%sh', '%%sh'],
        # space after the %% is not allowed
        ['%% sh', False],
        ['%%sh --no-raise-error', '%%sh'],
        ['# %debug', False],
        ['% debug', False],
        ['%%%debug', False],
        # cell magics cannot contain comments
        ['# comment\n%%html\nhello', False],
        # cell magics may contain whitespace
        ['\n\n%%html\nhello', '%%html'],
        ['\n\n   %%html\nhello', '%%html'],
        ['  %%html\nhello', '%%html'],
    ])
def test_is_ipython_cell_magic(code, expected):
    assert pyflakes._is_ipython_cell_magic(code) == expected


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


def test_check_params_warns_if_warn_flag_is_on():
    params = {'a', 'b'}
    source = """
a = None
"""
    with pytest.warns(UserWarning) as record:
        pyflakes.check_params(params, source, 'script.py', warn=True)

    assert len(record) == 1
    assert ("Parameters declared in "
            "the 'parameters' cell do not match task params"
            ) in record[0].message.args[0]


@pytest.mark.parametrize('passed, params_source', [
    [set(), 'raise Exception'],
    [set(), """
def x():
    pass
    """],
])
def test_check_params_ignores_non_variable_assignment(passed, params_source):
    pyflakes.check_params(passed, params_source, 'script.py')


syntax_error = """
# + tags=["parameters"]
a = 1

# +
if
"""

undefined_name = """
# + tags=["parameters"]
a = 1

# +
c = a + b
"""


@pytest.mark.parametrize('code, error, msg', [
    [syntax_error, SyntaxError, 'invalid syntax'],
    [undefined_name, RenderError, 'undefined name'],
],
                         ids=[
                             'syntax-error',
                             'undefined-name',
                         ])
def test_check_notebook_raises_if_pyflakes_error(code, error, msg):
    nb = jupytext.reads(code)

    with pytest.raises(error) as excinfo:
        pyflakes.check_notebook(nb, {}, 'file.py', raise_=True)

    assert 'An error happened' in str(excinfo.value)
    assert msg in str(excinfo.value)


@pytest.mark.parametrize('code, msg', [
    [syntax_error, 'invalid syntax'],
    [undefined_name, 'undefined name'],
])
def test_check_notebook_warns_if_pyflakes_error(code, msg):
    nb = jupytext.reads(code)

    with pytest.warns(UserWarning) as record:
        pyflakes.check_notebook(nb, {},
                                'file.py',
                                raise_=False,
                                check_signature=True)

    assert len(record) == 2
    first = record[0].message.args[0]
    assert 'An error happened' in first
    assert msg in first
    second = record[1].message.args[0]
    assert "Parameters declared in the 'parameters' cell" in second


@pytest.mark.parametrize('code, msg', [
    [syntax_error, 'invalid syntax'],
    [undefined_name, 'undefined name'],
])
def test_check_notebook_warns_with_check_signature_disabled(code, msg):
    nb = jupytext.reads(code)

    with pytest.warns(UserWarning) as record:
        pyflakes.check_notebook(nb, {},
                                'file.py',
                                raise_=False,
                                check_signature=False)

    assert len(record) == 1
    first = record[0].message.args[0]
    assert 'An error happened' in first
    assert msg in first


def test_check_notebook_raises_if_signature_mismatch():
    code = """
# + tags=["parameters"]
a = 1
"""

    nb = jupytext.reads(code)

    with pytest.raises(TypeError) as excinfo:
        pyflakes.check_notebook(nb, {}, 'file.py', raise_=True)

    expected = (
        "Parameters declared in the 'parameters' cell do not match task "
        "params. Missing params: 'a'")
    assert expected in str(excinfo.value)
