"""
Testing that upstream tasks metadata is available
"""
import subprocess
from pathlib import Path

from ploomber.dag import DAG
from ploomber.tasks import BashCommand, SQLScript
from ploomber.products import File, PostgresRelation
from ploomber.clients import SQLAlchemyClient


def test_passing_upstream_and_product_in_bashcommand(tmp_directory):
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    kwargs = {'stderr': subprocess.PIPE,
              'stdout': subprocess.PIPE,
              'shell': True}

    ta = BashCommand(('echo a > {{product}} '), File(fa), dag,
                     'ta', {}, kwargs, False)
    tb = BashCommand(('cat {{upstream["ta"]}} > {{product}}'
                     '&& echo b >> {{product}} '), File(fb), dag,
                     'tb', {}, kwargs, False)
    tc = BashCommand(('cat {{upstream["tb"]}} > {{product}} '
                     '&& echo c >> {{product}}'), File(fc), dag,
                     'tc', {}, kwargs, False)

    ta >> tb >> tc

    dag.build()

    assert fc.read_text() == 'a\nb\nc\n'


def test_passing_upstream_and_product_in_postgres(pg_client, db_credentials):
    dag = DAG()

    client = SQLAlchemyClient(db_credentials['uri'])

    dag.clients[SQLScript] = client
    dag.clients[PostgresRelation] = client

    conn = pg_client.connection
    cur = conn.cursor()
    cur.execute('drop table if exists series;')
    conn.commit()
    conn.close()

    ta_t = """begin;
              drop table if exists {{product}};
              create table {{product}} as
              select * from generate_series(0, 15) as n;
              commit;"""
    ta_rel = PostgresRelation((None, 'series', 'table'))
    ta = SQLScript(ta_t, ta_rel, dag, 'ta')

    dag.build()

    assert ta_rel.exists()
