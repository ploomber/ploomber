from pathlib import Path

import yaml
import pandas as pd
import pytest

from ploomber.dag.OnlineDAG import OnlineDAG
from ploomber.spec import DAGSpec


class BaseModel(OnlineDAG):
    @staticmethod
    def terminal_params():
        return {'offset': 0.1}

    @staticmethod
    def terminal_task(upstream, offset):
        return upstream['square'].sum().sum()


class ModelFromSpec(BaseModel):
    @staticmethod
    def get_partial():
        return 'pipeline-features.yaml'


class ModelFromDAG(BaseModel):
    @staticmethod
    def get_partial():
        with open('pipeline-features.yaml') as f:
            tasks = yaml.safe_load(f)

        meta = {'extract_product': False, 'extract_upstream': False}
        spec = DAGSpec({'tasks': tasks, 'meta': meta})

        return spec.to_dag()


class ModelFromInvalidType(BaseModel):
    @staticmethod
    def get_partial():
        return dict()


@pytest.mark.parametrize('class_', [ModelFromSpec, ModelFromDAG])
def test_online_dag_from_partial_spec(class_, backup_online,
                                      add_current_to_sys_path):
    get = pd.DataFrame({'x': [1, 2, 3]})
    out = class_().predict(get=get)

    assert set(out) == {'cube', 'get', 'square', 'terminal'}
    assert out['terminal'] == 56


def test_online_dag_from_partial_spec_with_source_and_product_only(
        backup_online, add_current_to_sys_path):
    spec = yaml.safe_load(Path('pipeline-features.yaml').read_text())

    for task in spec:
        task.pop('serializer')
        task.pop('unserializer')

    Path('pipeline-features.yaml').write_text(yaml.dump(spec))

    get = pd.DataFrame({'x': [1, 2, 3]})
    out = ModelFromSpec().predict(get=get)

    assert set(out) == {'cube', 'get', 'square', 'terminal'}
    assert out['terminal'] == 56


def test_error_if_invalid_predict_kwargs(backup_online,
                                         add_current_to_sys_path):
    model = ModelFromSpec()

    with pytest.raises(KeyError):
        model.predict(invalid=42)


def test_error_if_init_with_invalid_type():
    with pytest.raises(TypeError) as excinfo:
        ModelFromInvalidType()

    expected = ('Expected ModelFromInvalidType.get_partial() to '
                'return a str, pathlib.Path or ploomber.DAG, got dict')
    assert str(excinfo.value) == expected
