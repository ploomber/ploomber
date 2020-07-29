import os
from pathlib import Path
import subprocess

import pytest
import yaml
import click
from ploomber.cli.cli import _new, _add, _is_valid_name


@pytest.mark.parametrize('name,valid',
                         [('project', True), ('project123', True),
                          ('pro_jec_t', True), ('pro-ject', True),
                          ('1234', False), ('a project', False)])
def test_project_name(name, valid):
    assert _is_valid_name(name) is valid


@pytest.mark.parametrize('answer', [False, True])
def test_ploomber_new(answer, tmp_directory, monkeypatch):
    monkeypatch.setattr(click, 'confirm', lambda x: answer)
    monkeypatch.setattr(click, 'prompt', lambda x, type: 'my-project')
    _new()
    os.chdir('my-project')
    assert not subprocess.call(['ploomber', 'build'])


@pytest.mark.parametrize(
    'file, header, extract_flag',
    [
        ('task.py', 'Python task', False),
        ('task.py', 'Python task', True),
        ('task.sql', 'SQL task', False),
        ('task.sql', 'SQL task', True),
        # test file with sub-directories
        ('sql/task.sql', 'SQL task', True)
    ])
def test_ploomber_add(file, header, extract_flag, tmp_directory):
    sample_spec = {
        'meta': {
            'extract_upstream': extract_flag,
            'extract_product': extract_flag
        }
    }

    task = {'source': file}

    if not extract_flag:
        task['product'] = 'nb.ipynb'

    sample_spec['tasks'] = [task]

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(sample_spec, f)

    _add()

    content = Path(file).read_text()

    assert header in content
    assert ('extract_upstream is set to {} '
            'in your pipeline.yaml'.format(extract_flag) in content)
    assert ('extract_product is set to {} '
            'in your pipeline.yaml'.format(extract_flag) in content)


def test_ploomber_add_unknown_extension(tmp_directory, capsys):
    sample_spec = {
        'meta': {
            'extract_upstream': False,
            'extract_product': False
        },
        'tasks': [{
            'source': 'task.txt',
            'product': 'nb.ipynb'
        }]
    }

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(sample_spec, f)

    _add()

    captured = capsys.readouterr()
    out = ('Error: This command does not support adding tasks with '
           'extension ".txt"')
    assert out in captured.out


def test_ploomber_add_missing_spec(tmp_directory, capsys):
    _add()

    captured = capsys.readouterr()
    assert 'Error: No pipeline.yaml spec found...' in captured.out


def test_ploomber_add_skip_if_file_exists(tmp_directory, capsys):
    sample_spec = {
        'meta': {
            'extract_upstream': False,
            'extract_product': False
        },
        'tasks': []
    }

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(sample_spec, f)

    Path('task.py').touch()

    _add()

    assert Path('task.py').read_text() == ''
