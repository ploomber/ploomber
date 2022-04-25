from pathlib import Path

import pytest
import yaml

from ploomber.config import Config


class MyConfig(Config):
    number: int = 42
    string: str = 'value'

    def path(self):
        return Path('myconfig.yaml')


class AnotherConfig(Config):
    uuid: str
    another: str = 'default'

    def path(self):
        return Path('another.yaml')

    def uuid_default(self):
        return 'some-value'


def test_stores_defaults(tmp_directory):
    MyConfig()

    content = yaml.safe_load(Path('myconfig.yaml').read_text())

    assert content == {'number': 42, 'string': 'value'}


def test_reads_existing(tmp_directory):
    path = Path('myconfig.yaml')
    path.write_text(yaml.dump({'number': 100}))

    cfg = MyConfig()

    assert cfg.number == 100
    assert cfg.string == 'value'


def test_ignores_extra(tmp_directory):
    path = Path('myconfig.yaml')
    path.write_text(
        yaml.dump({
            'number': 200,
            'string': 'another',
            'unknown': 2000,
        }))

    cfg = MyConfig()

    assert cfg.number == 200
    assert cfg.string == 'another'


def test_stores_on_update(tmp_directory):
    cfg = MyConfig()

    cfg.number = 500

    content = yaml.safe_load(Path('myconfig.yaml').read_text())

    assert content == {'number': 500, 'string': 'value'}


def test_uses_default_if_missing(tmp_directory):
    path = Path('myconfig.yaml')
    path.write_text(yaml.dump({'number': 100}))

    cfg = MyConfig()

    assert cfg.number == 100
    assert cfg.string == 'value'


@pytest.mark.parametrize('content', ['not yaml', '[[]'])
def test_uses_defaults_if_corrupted(tmp_directory, content):
    path = Path('myconfig.yaml')
    path.write_text(content)

    with pytest.warns(UserWarning) as records:
        cfg = MyConfig()

    content = yaml.safe_load(Path('myconfig.yaml').read_text())

    assert len(records) == 1
    assert 'Error loading' in records[0].message.args[0]
    assert cfg.number == 42
    assert cfg.string == 'value'
    assert content == {'number': 42, 'string': 'value'}


def test_config_with_factory(tmp_directory):
    cfg = AnotherConfig()

    assert cfg.uuid == 'some-value'
    assert cfg.another == 'default'


def test_factory_keeps_existing_values(tmp_directory):
    path = Path('another.yaml')
    values = {'uuid': 'my-uuid', 'another': 'some-value'}
    path.write_text(yaml.dump(values))

    cfg = AnotherConfig()
    content = yaml.safe_load(Path('another.yaml').read_text())

    assert cfg.uuid == 'my-uuid'
    assert cfg.another == 'some-value'
    assert content == values


@pytest.mark.parametrize('content, number, string', [
    [
        {
            'number': 'not-a-number'
        },
        42,
        'value',
    ],
    [
        {
            'string': 1
        },
        42,
        'value',
    ],
])
def test_restores_values_if_wrong_type(tmp_directory, content, number, string):
    path = Path('myconfig.yaml')
    path.write_text(yaml.dump(content))

    with pytest.warns(UserWarning) as records:
        cfg = MyConfig()

    assert len(records) == 1
    assert 'Corrupted config' in records[0].message.args[0]
    assert cfg.number == number
    assert cfg.string == string
