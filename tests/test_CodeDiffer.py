import pytest
from ploomber.CodeDiffer import CodeDiffer


def test_python_differ_ignores_docstrings():

    a = '''
def x():
    """This is some docstring
    """
    pass
'''

    b = '''
def x():
    """This is a docstring and should be ignored
    """
    pass
'''

    differ = CodeDiffer()
    res, _ = differ.is_different(a, b, extension='py')
    assert not res


def test_python_differ_ignores_comments():

    a = '''
def x():
    # this is a comment
    # another comment
    var = 100
'''

    b = '''
def x():
    # one comment
    var = 100 # this is a comment
'''

    differ = CodeDiffer()
    res, _ = differ.is_different(a, b, extension='py')
    assert not res


def test_sql_is_normalized():
    a = """
    SELECT * FROM TABLE
    """

    b = """
    SELECT *
    FROM table
    """
    differ = CodeDiffer()
    different, _ = differ.is_different(a, b, extension='sql')
    assert not different


@pytest.mark.parametrize('extension', ['py', 'sql', None])
def test_get_diff(extension):
    differ = CodeDiffer()
    a = 'some code...'
    b = 'some other code...'
    differ.get_diff(a, b, extension=extension)
