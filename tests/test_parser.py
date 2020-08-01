import pytest
from ploomber.static_analysis.parser.parser import Parser


@pytest.mark.parametrize('code, expected', [
    ('upstream <- list(1, 2, 3)', [1, 2, 3]),
    ("upstream <- list('a', 'b', 'c')", ['a', 'b', 'c']),
])
def test_parse_list(code, expected):
    parser = Parser(code)
    expression = parser.parse()
    assert expression.left.value == 'upstream'
    assert expression.right.to_python() == expected


@pytest.mark.parametrize('code, expected', [
    ('product <- list(a=1, b=2, c=3)', {
        'a': 1,
        'b': 2,
        'c': 3
    }),
    ("product <- list(a='d', b='e', c='f')", {
        'a': 'd',
        'b': 'e',
        'c': 'f',
    }),
])
def test_parse_namedlist(code, expected):
    parser = Parser(code)
    expression = parser.parse()
    assert expression.left.value == 'product'
    assert expression.right.to_python() == expected
