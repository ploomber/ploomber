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
    assert not differ.code_is_different(a, b, language='python')


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
    assert not differ.code_is_different(a, b, language='python')
