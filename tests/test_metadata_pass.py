"""
Testing that upstream tasks metadata is available
"""
from pathlib import Path

from ploomber import DAG
from ploomber.tasks import ShellScript, SQLScript
from ploomber.products import File, PostgresRelation


def test_passing_upstream_and_product_in_shellscript(tmp_directory):
    dag = DAG()

    fa = Path("a.txt")
    fb = Path("b.txt")
    fc = Path("c.txt")

    ta = ShellScript(("echo a > {{product}}"), File(fa), dag, "ta")
    tb = ShellScript(
        ('cat {{upstream["ta"]}} > {{product}}' " && echo b >> {{product}}"),
        File(fb),
        dag,
        "tb",
    )
    tc = ShellScript(
        ('cat {{upstream["tb"]}} > {{product}}' " && echo c >> {{product}}"),
        File(fc),
        dag,
        "tc",
    )

    ta >> tb >> tc

    dag.render()

    assert str(ta.source) == "echo a > a.txt"
    assert str(tb.source) == "cat a.txt > b.txt && echo b >> b.txt"
    assert str(tc.source) == "cat b.txt > c.txt && echo c >> c.txt"


def test_passing_upstream_and_product_in_postgres(pg_client_and_schema):
    client, _ = pg_client_and_schema
    dag = DAG()
    dag.clients[SQLScript] = client
    dag.clients[PostgresRelation] = client

    client.execute("drop table if exists series;")

    ta_t = """begin;
              drop table if exists {{product}};
              create table {{product}} as
              select * from generate_series(0, 15) as n;
              commit;"""
    ta_rel = PostgresRelation((None, "series", "table"))
    SQLScript(ta_t, ta_rel, dag, "ta")

    dag.build()

    assert ta_rel.exists()
