import pytest
from ploomber.static_analysis.python import PythonNotebookExtractor
from ploomber.static_analysis.r import RNotebookExtractor

# TODO: move SQLExtractor tests from test_static_analysis.py to here


@pytest.mark.parametrize('extractor', [
    PythonNotebookExtractor(
        "upstream = ['a', 'b', 'c']\nproduct={'d': 'path/d.csv'}"),
    RNotebookExtractor(
        "upstream <- list('a', 'b', 'c')\nproduct <- list(d='path/d.csv')"),
])
def test_extract_variables(extractor):
    assert extractor.extract_upstream() == set(['a', 'b', 'c'])
    assert extractor.extract_product() == {'d': 'path/d.csv'}
