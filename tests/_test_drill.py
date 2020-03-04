"""
Testing a drill pipeline
"""
from ploomber.clients import DrillClient
from ploomber import DAG
from ploomber.tasks import SQLScript
from ploomber.products import File


def test_drill_client():
    drill = DrillClient()
    drill.run('SELECT * FROM cp.`employee.json` LIMIT 1')


def test_drill_dag():
    code = """
    -- {{product.name}}
    CREATE TABLE dfs.tmp.`/test/` AS
    SELECT * FROM cp.`employee.json` LIMIT 1
    """

    drill = DrillClient()
    dag = DAG()

    SQLScript(code, File('/tmp/test/'), dag, client=drill)

    dag.build()
