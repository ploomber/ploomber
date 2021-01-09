import os
from pathlib import Path
import subprocess

import pytest
import yaml
import click
from click.testing import CliRunner

from ploomber.cli.cli import _new, add, _is_valid_name


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
    'file_, extract_flag',
    [
        ('task.py', False),
        ('task.py', True),
        ('task.sql', False),
        ('task.sql', True),
        # test file with sub-directories
        ('sql/task.sql', True)
    ])
def test_ploomber_add(file_, extract_flag, tmp_directory):
    sample_spec = {
        'meta': {
            'extract_upstream': extract_flag,
            'extract_product': extract_flag
        }
    }

    task = {'source': file_}

    if not extract_flag:
        task['product'] = 'nb.ipynb'

    sample_spec['tasks'] = [task]

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(sample_spec, f)

    runner = CliRunner()
    result = runner.invoke(add)

    content = Path(file_).read_text()

    assert result.exit_code == 0
    assert 'Add description here' in content
    assert ('extract_upstream={} '
            'in your pipeline.yaml'.format(extract_flag) in content)

    # task.sql does not output this part
    if not file_.endswith('.sql'):
        assert ('extract_product={} '
                'in your pipeline.yaml'.format(extract_flag) in content)


def test_ploomber_add_unknown_extension(tmp_directory):
    sample_spec = {
        'meta': {
            'extract_upstream': False,
            'extract_product': False
        },
        'tasks': [{
            'source': 'task.txt',
            'product': 'nb.ipynb',
            'class': 'NotebookRunner',
        }]
    }

    with open('pipeline.yaml', 'w') as f:
        yaml.dump(sample_spec, f)

    runner = CliRunner()
    result = runner.invoke(add)

    out = ('Error: This command does not support adding tasks with '
           'extension ".txt"')

    assert result.exit_code == 0
    assert out in result.output


def test_ploomber_add_missing_spec(tmp_directory):
    runner = CliRunner()
    result = runner.invoke(add)

    assert result.exit_code == 0
    assert 'Error: No pipeline.yaml spec found...' in result.output


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

    runner = CliRunner()
    result = runner.invoke(add)

    assert result.exit_code == 0
    assert Path('task.py').read_text() == ''
