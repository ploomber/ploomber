from unittest.mock import Mock
from pathlib import Path

import pytest

from ploomber.dag import DAG
from ploomber.tasks import ShellScript, PythonCallable, SQLDump
from ploomber.products import File


# can test this since this uses dag.plot(), which needs dot for plotting
# def test_to_html():
#     def fn1(product):
#         pass

#     def fn2(product):
#         pass

#     dag = DAG()
#     t1 = PythonCallable(fn1, File('file1.txt'), dag)
#     t2 = PythonCallable(fn2, File('file2.txt'), dag)
#     t1 >> t2

#     dag.to_html('a.html')


def test_warn_on_python_missing_docstrings():
    def fn1(product):
        pass

    dag = DAG()
    PythonCallable(fn1, File('file1.txt'), dag, name='fn1')

    with pytest.warns(UserWarning):
        dag.diagnose()


def test_does_not_warn_on_python_docstrings():
    def fn1(product):
        """This is a docstring
        """
        pass

    dag = DAG()
    PythonCallable(fn1, File('file1.txt'), dag, name='fn1')

    with pytest.warns(None) as warn:
        dag.diagnose()

    assert not warn


def test_warn_on_sql_missing_docstrings():
    dag = DAG()

    sql = 'SELECT * FROM table'
    SQLDump(sql, File('file1.txt'), dag, client=Mock(), name='sql')

    with pytest.warns(UserWarning):
        dag.diagnose()


def test_does_not_warn_on_sql_docstrings():
    dag = DAG()

    sql = '/* get data from table */\nSELECT * FROM table'
    SQLDump(sql, File('file1.txt'), dag, client=Mock(), name='sql')

    with pytest.warns(None) as warn:
        dag.diagnose()

    assert not warn


# def test_can_use_null_task(tmp_directory):
#     dag = DAG('dag')

#     Path('a.txt').write_text('hello')

#     ta = Null(File('a.txt'), dag, 'ta')
#     tb = ShellScript('cat {{upstream["ta"]}} > {{product}}', File('b.txt'),
#                      dag, 'tb')

#     ta >> tb

#     dag.build()

#     assert Path('b.txt').read_text() == 'hello'


def test_can_get_upstream_tasks():
    dag = DAG('dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}',
                     File('b.txt'), dag, 'tb')
    tc = ShellScript('cat {{upstream["tb"]}} > {{product}}',
                     File('c.txt'), dag, 'tc')

    ta >> tb >> tc

    assert set(ta.upstream) == set()
    assert set(tb.upstream) == {'ta'}
    assert set(tc.upstream) == {'tb'}


def test_can_access_sub_dag():
    sub_dag = DAG('sub_dag')

    ta = ShellScript('echo "a" > {{product}}', File('a.txt'), sub_dag, 'ta')
    tb = ShellScript('cat {{upstream["ta"]}} > {{product}}',
                     File('b.txt'), sub_dag, 'tb')
    tc = ShellScript('tcat {{upstream["tb"]}} > {{product}}',
                     File('c.txt'), sub_dag, 'tc')

    ta >> tb >> tc

    dag = DAG('dag')

    fd = Path('d.txt')
    td = ShellScript('touch {{product}}', File(fd), dag, 'td')

    td.set_upstream(sub_dag)

    assert 'sub_dag' in td.upstream


def test_can_access_tasks_inside_dag_using_getitem():
    dag = DAG('dag')
    dag2 = DAG('dag2')

    ta = ShellScript('touch {{product}}', File(Path('a.txt')), dag, 'ta')
    tb = ShellScript('touch {{product}}', File(Path('b.txt')), dag, 'tb')
    tc = ShellScript('touch {{product}}', File(Path('c.txt')), dag, 'tc')

    # td is still discoverable from dag even though it was declared in dag2,
    # since it is a dependency for a task in dag
    td = ShellScript('touch {{product}}', File(Path('c.txt')), dag2, 'td')
    # te is not discoverable since it is not a dependency for any task in dag
    te = ShellScript('touch {{product}}', File(Path('e.txt')), dag2, 'te')

    td >> ta >> tb >> tc >> te

    assert set(dag) == {'ta', 'tb', 'tc', 'td'}


def test_partial_build(tmp_directory):
    dag = DAG('dag')

    ta = ShellScript('echo "hi" >> {{product}}',
                     File(Path('a.txt')), dag, 'ta')
    code = 'cat {{upstream.first}} >> {{product}}'
    tb = ShellScript(code, File(Path('b.txt')), dag, 'tb')
    tc = ShellScript(code, File(Path('c.txt')), dag, 'tc')
    td = ShellScript(code, File(Path('d.txt')), dag, 'td')
    te = ShellScript(code, File(Path('e.txt')), dag, 'te')

    ta >> tb >> tc
    tb >> td >> te

    table = dag.build_partially('tc')

    assert {row['name'] for row in table} == {'ta', 'tb', 'tc'}
    assert all(row['Ran?'] for row in table)
