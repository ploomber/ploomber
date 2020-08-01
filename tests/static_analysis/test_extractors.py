import pytest
from ploomber.static_analysis.python import PythonNotebookExtractor
from ploomber.static_analysis.r import RNotebookExtractor

# TODO: move SQLExtractor tests from test_static_analysis.py to here


@pytest.mark.parametrize('extractor', [
    PythonNotebookExtractor(
        "upstream = ['a', 'b', 'c']\nproduct={'d': 'path/d.csv'}"),
    PythonNotebookExtractor(
        "upstream = ['a', 'b', 'c']\n\nproduct={'d': 'path/d.csv'}"),
    PythonNotebookExtractor(
        "product={'d': 'path/d.csv'}\n\nupstream = ['a', 'b', 'c']\n\n"),
    RNotebookExtractor(
        "upstream <- list('a', 'b', 'c')\n\nproduct <- list(d='path/d.csv')"),
    RNotebookExtractor(
        "upstream = list('a', 'b', 'c')\n\nproduct = list(d='path/d.csv')"),
    RNotebookExtractor(
        "\nproduct <- list(d='path/d.csv')\n\nupstream <- list('a', 'b', 'c')"
    ),
])
def test_extract_variables(extractor):
    assert extractor.extract_upstream() == set(['a', 'b', 'c'])
    assert extractor.extract_product() == {'d': 'path/d.csv'}
