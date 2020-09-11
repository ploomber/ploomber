import pytest
from ploomber.sources import docstring
import nbformat


@pytest.mark.parametrize('code, doc', [
    ['/*hello*/', 'hello'],
    ['\n/*\nhello\n*/\n', '\nhello\n'],
    ['\n\n\n/*\nhello\n*/\n SELECT * FROM some_table', '\nhello\n'],
    ['SELECT * FROM some_table', ''],
    ['-- not a docstring\nSELECT * FROM some_table', ''],
])
def test_extract_from_sql(code, doc):
    assert docstring.extract_from_sql(code) == doc


@pytest.mark.parametrize('code, doc', [
    ["'''hello'''", 'hello'],
    ['"""hello"""', 'hello'],
    ["\n'''\nhello\n'''\n", '\nhello\n'],
    ['\n\n\n"""\nhello\n"""\n x = 1', '\nhello\n'],
    ['x = 1', ''],
    ['# not a docstring\nx=1', ''],
])
def test_extract_from_triple_quotes(code, doc):
    assert docstring.extract_from_triple_quotes(code) == doc


def nb_with_top_md_cell():
    current = nbformat.versions[nbformat.current_nbformat]
    nb = current.new_notebook()
    md = current.new_markdown_cell(source='This is a *md* docstring')
    nb.cells.append(md)
    return nb


def nb_with_top_doc_code_cell():
    current = nbformat.versions[nbformat.current_nbformat]
    nb = current.new_notebook()
    md = current.new_code_cell(source='"""\ndocstring\n"""\n')
    nb.cells.append(md)
    return nb


def nb_with_top_code_cell():
    current = nbformat.versions[nbformat.current_nbformat]
    nb = current.new_notebook()
    md = current.new_code_cell(source='x = 1')
    nb.cells.append(md)
    return nb


@pytest.mark.parametrize('nb, doc', [
    [nb_with_top_md_cell(), 'This is a *md* docstring'],
    [nb_with_top_doc_code_cell(), '\ndocstring\n'],
    [nb_with_top_code_cell(), ''],
])
def test_extract_from_nb(nb, doc):
    assert docstring.extract_from_nb(nb) == doc
