from unittest.mock import Mock

import pytest

from ploomber.dag.dagclients import DAGClients
from ploomber.tasks import SQLScript
from ploomber.products import File


def test_error_if_setting_invalid_key():

    clients = DAGClients()

    with pytest.raises(ValueError) as excinfo:
        clients[object] = Mock()

    assert ('DAG client keys must be Tasks or '
            'Products, value <class \'object\'> is not') == str(excinfo.value)


def test_iter():
    assert list(DAGClients()) == []


def test_setitem_and_getitem_with_str():
    clients = DAGClients()

    mock = Mock()

    clients['SQLScript'] = mock

    assert clients[SQLScript] is mock
    assert clients['SQLScript'] is mock


def test_error_setitem_invalid_str():
    clients = DAGClients()

    mock = Mock()

    with pytest.raises(ValueError) as excinfo:
        clients['invalid_name'] = mock

    expected = (f"Could not set DAG-level client {mock!r}. 'invalid_name' "
                "is not a valid Task or Product class name")
    assert str(excinfo.value) == expected


@pytest.mark.parametrize('typo, expected', [
    ['sqlscript', 'SQLScript'],
    ['SQLSCRIPT', 'SQLScript'],
    ['sql_script', 'SQLScript'],
    ['sql-script', 'SQLScript'],
    ['sql script', 'SQLScript'],
    ['file', 'File'],
])
def test_error_setitem_invalid_str_with_typo(typo, expected):
    clients = DAGClients()

    mock = Mock()

    with pytest.raises(ValueError) as excinfo:
        clients[typo] = mock

    assert f"Did you mean {expected!r}?" in str(excinfo.value)


@pytest.mark.parametrize('typo, expected, class_', [
    ['sqlscript', 'SQLScript', SQLScript],
    ['SQLSCRIPT', 'SQLScript', SQLScript],
    ['sql_script', 'SQLScript', SQLScript],
    ['sql-script', 'SQLScript', SQLScript],
    ['sql script', 'SQLScript', SQLScript],
    ['file', 'File', File],
])
def test_error_getitem_invalid_str_with_typo(typo, expected, class_):
    clients = DAGClients()

    mock = Mock()

    clients[class_] = mock

    with pytest.raises(KeyError) as excinfo:
        clients[typo]

    expected = f"{typo!r}. Did you mean {expected!r}?"
    assert expected in str(excinfo.value)


def test_error_does_not_suggest_if_key_does_not_exist():
    clients = DAGClients()

    with pytest.raises(KeyError) as excinfo:
        clients['sqlscript']

    assert "Did you mean 'SQLScript'?" not in str(excinfo.value)


def test_repr():
    clients = DAGClients()
    clients[SQLScript] = 1

    expected = "DAGClients({<class 'ploomber.tasks.sql.SQLScript'>: 1})"
    assert repr(clients) == expected
