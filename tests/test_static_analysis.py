import pytest
from ploomber.static_analysis import notebook

case_1 = """
upstream = {'some_key': 'some_value'}
""", ['some_key']

case_2 = """
upstream = {'a': None, 'b': None}
product, some_variable = None, None

if upstream:
    pass

for i in range(10):
    print(i)

if True:
    upstream = {'x': None, 'y': None}
""", ['a', 'b']


@pytest.mark.parametrize('code, expected',
                         [case_1, case_2])
def test_infer_from_code_str(code, expected):
    assert notebook.infer_dependencies_from_code_str(code) == expected


case_error_1 = "some_variable = 1"
case_error_2 = """
upstreammm = {'a': 1}
"""
case_error_3 = "upstream = 1"


@pytest.mark.parametrize('code', [case_error_1, case_error_2, case_error_3])
def test_error_from_code_str(code):
    with pytest.raises(ValueError):
        notebook.infer_dependencies_from_code_str(code)
