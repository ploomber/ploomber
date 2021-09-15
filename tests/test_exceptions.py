import pytest

from ploomber.exceptions import BaseException


def test_show(capsys):
    BaseException('something').show()
    captured = capsys.readouterr()
    assert captured.err == "Error: something\n"


@pytest.mark.parametrize('class_', [BaseException, Exception])
def test_show_chained_exceptions(class_, capsys):
    first = class_('first')
    second = BaseException('second')

    try:
        raise second from first
    except Exception as e:
        ex = e

    ex.show()

    captured = capsys.readouterr()
    assert captured.err == 'Error: second\nfirst\n'
