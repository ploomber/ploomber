import pickle
from unittest.mock import Mock
from pathlib import Path

import test_pkg
import yaml
import pandas as pd
import pytest

from ploomber.dag import onlinedag
from ploomber.dag.onlinedag import OnlineDAG, OnlineModel
from ploomber.spec import DAGSpec
from ploomber.exceptions import ValidationError


class FakePredictor:
    def predict(self, x):
        return x


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

    with pytest.raises(ValidationError):
        model.predict(invalid=42)


def test_error_if_init_with_invalid_type():
    with pytest.raises(TypeError) as excinfo:
        ModelFromInvalidType()

    expected = ('Expected ModelFromInvalidType.get_partial() to '
                'return a str, pathlib.Path or ploomber.DAG, got dict')
    assert str(excinfo.value) == expected


def test_online_model(monkeypatch):

    mock_read = Mock(return_value=pickle.dumps(FakePredictor()))
    monkeypatch.setattr(onlinedag.importlib_resources, 'read_binary',
                        mock_read)

    model = OnlineModel(test_pkg)

    assert model.predict(root=41) == 42
    mock_read.assert_called_once_with(test_pkg, 'model.pickle')


def test_online_model_missing_model_file():

    with pytest.raises(FileNotFoundError) as excinfo:
        OnlineModel(test_pkg)

    assert 'Error initializing OnlineModel' in str(excinfo.value)


def test_online_model_without_features_task(monkeypatch, backup_test_pkg):
    mock_read = Mock(return_value=pickle.dumps(FakePredictor()))
    monkeypatch.setattr(onlinedag.importlib_resources, 'read_binary',
                        mock_read)

    # change task name
    path = Path(backup_test_pkg, 'pipeline-features.yaml')
    tasks = yaml.safe_load(path.read_text())
    tasks[0]['name'] = 'another_name'
    Path(path).write_text(yaml.dump(tasks))

    with pytest.raises(ValueError) as excinfo:
        OnlineModel(test_pkg)

    assert 'Error initializing OnlineModel' in str(excinfo.value)


def test_error_initializing_if_partial_is_not_a_list(tmp_directory):
    class ModelFromInvalidYAML(BaseModel):
        @staticmethod
        def get_partial():
            return 'not-a-list.yaml'

    Path('not-a-list.yaml').write_text('{"a": 1}')

    with pytest.raises(ValueError) as excinfo:
        ModelFromInvalidYAML()

    expected = ("Expected partial 'not-a-list.yaml' to be a list of "
                "tasks, but got a dict instead")
    assert str(excinfo.value) == expected
