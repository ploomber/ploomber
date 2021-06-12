from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from click.testing import CliRunner

from ploomber.cli.cli import scaffold
from ploomber.cli import cli


@pytest.mark.parametrize('args, conda, package', [
    [[], False, False],
    [['--conda'], True, False],
    [['--package'], False, True],
    [['--conda', '--package'], True, True],
])
def test_ploomber_scaffold(tmp_directory, monkeypatch, args, conda, package):
    """
    Testing scaffold for creating a new project
    """
    mock = Mock()
    monkeypatch.setattr(cli.scaffold_project, 'cli', mock)

    runner = CliRunner()
    result = runner.invoke(scaffold, args=args, catch_exceptions=False)

    assert not result.exit_code
    mock.assert_called_once_with(project_path=None,
                                 conda=conda,
                                 package=package)


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
def test_ploomber_scaffold_task_template(file_, extract_flag, tmp_directory):
    """Test scaffold when project already exists (add task templates)
    """
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
    result = runner.invoke(scaffold)

    content = Path(file_).read_text()

    assert result.exit_code == 0
    assert 'Add description here' in content
    assert ('extract_upstream={} '
            'in your pipeline.yaml'.format(extract_flag) in content)

    # task.sql does not output this part
    if not file_.endswith('.sql'):
        assert ('extract_product={} '
                'in your pipeline.yaml'.format(extract_flag) in content)


def test_ploomber_scaffold_unknown_extension(tmp_directory):
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
    result = runner.invoke(scaffold)

    out = ('Error: This command does not support adding tasks with '
           'extension ".txt"')

    assert result.exit_code == 0
    assert out in result.output


def test_ploomber_scaffold_skip_if_file_exists(tmp_directory, capsys):
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
    result = runner.invoke(scaffold)

    assert result.exit_code == 0
    assert Path('task.py').read_text() == ''
