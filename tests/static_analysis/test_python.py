from ploomber.static_analysis import python


def test_extract_variable():
    found, value = python.extract_variable("""
a = 1
b = None
c = 100
""", 'c')

    assert found
    assert value == 100
