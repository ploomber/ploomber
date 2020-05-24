from pathlib import Path
import pytest
from ploomber.dag import DAGSpec
import yaml


@pytest.mark.parametrize('spec', ['pipeline.yaml',
                                  'pipeline-w-location.yaml'])
def test_notebook_spec(spec, tmp_nbs, add_current_to_sys_path):

    Path('output').mkdir()

    with open(spec) as f:
        dag_spec = yaml.load(f, Loader=yaml.SafeLoader)

    dag = DAGSpec.init_dag(dag_spec)
    dag.build()
