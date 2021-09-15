import pytest

from ploomber.exceptions import MyException


def test_show(capsys):
    MyException('something').show()
    captured = capsys.readouterr()
    assert captured.err == "Error: something\n"


@pytest.mark.parametrize('class_', [MyException, Exception])
def test_show_chained_exceptions(class_, capsys):
    first = class_('first')
    second = MyException('second')

    try:
        raise second from first
    except Exception as e:
        ex = e

    ex.show()

    captured = capsys.readouterr()
    assert captured.err == 'Error: second\nfirst\n'
