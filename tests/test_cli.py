from pathlib import Path
import subprocess

import pytest
import yaml
import click
from ploomber.cli import _new, _add


@pytest.mark.parametrize('answer', [False, True])
def test_ploomber_new(answer, tmp_directory, monkeypatch):
    monkeypatch.setattr(click, 'confirm', lambda x: answer)
    _new()
    assert not subprocess.call(['ploomber', 'entry', 'pipeline.yaml'])


@pytest.mark.parametrize('file, header, extract_flag',
                         [
                             ('task.py', 'Python task', False),
                             ('task.py', 'Python task', True),
                             ('task.sql', 'SQL task', False),
                             ('task.sql', 'SQL task', True)
                         ])
def test_ploomber_add(file, header, extract_flag, tmp_directory):
    sample_spec = {'meta':
                   {'extract_upstream': extract_flag,
                    'extract_product': extract_flag},
                   'tasks': []}

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(sample_spec, f)

    _add(file)

    content = Path(file).read_text()

    assert header in content
    assert ('extract_upstream is set to {} '
            'in your pipeline.yaml'.format(extract_flag) in content)
    assert ('extract_product is set to {} '
            'in your pipeline.yaml'.format(extract_flag) in content)


def test_ploomber_add_unknown_extension(tmp_directory, capsys):
    sample_spec = {'meta':
                   {'extract_upstream': False,
                    'extract_product': False},
                   'tasks': []}

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(sample_spec, f)

    _add('task.txt')

    captured = capsys.readouterr()
    out = ('Error: This command does not support adding tasks with '
           'extension ".txt"')
    assert out in captured.out


def test_ploomber_add_missing_spec(tmp_directory, capsys):
    _add('task.py')

    captured = capsys.readouterr()
    assert 'Error: No pipeline.yaml spec found...' in captured.out


def test_ploomber_add_file_exists(tmp_directory, capsys):
    sample_spec = {'meta':
                   {'extract_upstream': False,
                    'extract_product': False},
                   'tasks': []}

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(sample_spec, f)

    Path('task.py').touch()

    _add('task.py')

    captured = capsys.readouterr()
    out = 'Error: File "task.py" already exists'
    assert out in captured.out
