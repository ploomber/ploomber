from pathlib import Path
import sys
from argparse import ArgumentParser

import yaml
import pytest

from ploomber.cli.parsers import (_add_args_from_callable,
                                  _process_file_or_entry_point, CustomParser)


def fn(a: int, b: float, c: str, d: bool, e):
    pass


def test_add_type_to_arg_parser():
    parser = ArgumentParser()

    _add_args_from_callable(parser, fn)

    actions = {action.dest: action for action in parser._actions}

    assert actions['a'].type is int
    assert actions['b'].type is float
    assert actions['c'].type is str
    assert actions['d'].type is bool


@pytest.mark.parametrize('argv, expected', [
    [['ploomber'], 'some_value'],
    [['ploomber', '--env__tag', 'another_value'], 'another_value'],
])
def test_process_file_or_entry_point_param_replace(argv, expected, monkeypatch,
                                                   tmp_directory):
    d = {
        'meta': {
            'extract_product': False,
            'extract_upstream': False
        },
        'tasks': [{
            'source': 'plot.py',
            'params': {
                'some_param': '{{tag}}',
            },
            'product': 'output/plot.ipynb',
            'name': 'plot'
        }]
    }

    Path('plot.py').write_text('# + tags=["parameters"]')

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(d, f)

    with open('env.yaml', 'w') as f:
        yaml.dump({'tag': 'some_value'}, f)

    monkeypatch.setattr(sys, 'argv', argv)
    parser = CustomParser()

    with parser:
        pass

    dag, args = _process_file_or_entry_point(parser)

    assert dag['plot'].params['some_param'] == expected
