from unittest.mock import Mock

import pytest

from ploomber import DAG
from ploomber.tasks import PythonCallable, SQLDump
from ploomber.products import File
from ploomber.qa import DAGQualityChecker


def test_warn_on_python_missing_docstrings():
    def fn1(product):
        pass

    dag = DAG()
    PythonCallable(fn1, File("file1.txt"), dag, name="fn1")

    qc = DAGQualityChecker()

    with pytest.warns(UserWarning):
        qc(dag)


def test_does_not_warn_on_python_docstrings():
    def fn1(product):
        """This is a docstring"""
        pass

    dag = DAG()
    PythonCallable(fn1, File("file1.txt"), dag, name="fn1")

    qc = DAGQualityChecker()

    with pytest.warns(None) as warn:
        qc(dag)

    assert not warn


def test_warn_on_sql_missing_docstrings():
    dag = DAG()

    sql = "SELECT * FROM table"
    SQLDump(sql, File("file1.txt"), dag, client=Mock(), name="sql")

    qc = DAGQualityChecker()

    with pytest.warns(UserWarning):
        qc(dag)


def test_does_not_warn_on_sql_docstrings():
    dag = DAG()

    sql = "/* get data from table */\nSELECT * FROM table"
    SQLDump(sql, File("file1.txt"), dag, client=Mock(), name="sql")

    qc = DAGQualityChecker()

    with pytest.warns(None) as warn:
        qc(dag)

    assert not warn
