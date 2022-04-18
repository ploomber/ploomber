from pathlib import Path

import pytest
import yaml

from ploomber.config import Config


class MyConfig(Config):
    number: int = 42
    string: str = 'value'

    def path(self):
        return Path('myconfig.yaml')


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

    cfg = MyConfig()
    content = yaml.safe_load(Path('myconfig.yaml').read_text())

    assert cfg.number == 42
    assert cfg.string == 'value'
    assert content == {'number': 42, 'string': 'value'}
