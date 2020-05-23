from pathlib import Path
from ploomber.dag import DAGSpec
import yaml


def test_notebook_spec(tmp_nbs):
    Path('output').mkdir()

    with open('pipeline.yaml') as f:
        dag_dict = yaml.load(f, Loader=yaml.SafeLoader)

    dag = DAGSpec.init_dag(dag_dict)
    dag.build()
