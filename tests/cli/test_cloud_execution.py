"""
Tests for cloud execution
"""
from pathlib import Path

import pytest
from click.testing import CliRunner

from ploomber_cli.cli import cli


@pytest.fixture
def runid():
    return 'afabd29e-2622-4349-a398-c330f39e9a95'


@pytest.fixture
def taskid():
    return '3827ee34-7048-44d2-9507-79544d0f0e43'


def test_cloud_build():
    pass


def test_cloud_list():
    runner = CliRunner()
    result = runner.invoke(cli, ['cloud', 'list'], catch_exceptions=False)

    assert 'created_at' in result.output
    assert 'runid' in result.output
    assert 'status' in result.output
    assert len(result.output.splitlines()) == 7
    assert result.exit_code == 0


def test_cloud_status(runid):
    runner = CliRunner()
    result = runner.invoke(cli, ['cloud', 'status', runid])

    assert 'taskid' in result.output
    assert 'name' in result.output
    assert 'runid' in result.output
    assert 'status' in result.output
    assert len(result.output.splitlines()) == 16
    assert result.exit_code == 0


def test_cloud_products():
    runner = CliRunner()
    result = runner.invoke(cli, ['cloud', 'products'])

    assert 'path' in result.output
    assert result.exit_code == 0


def test_cloud_download(tmp_directory):
    runner = CliRunner()
    result = runner.invoke(cli, ['cloud', 'download', '*.html'])

    assert 'Downloading' in result.output
    assert result.exit_code == 0


def test_cloud_logs(runid):
    runner = CliRunner()
    result = runner.invoke(cli, ['cloud', 'logs', runid])

    assert 'START OF LOGS FOR TASK' in result.output
    assert 'END OF LOGS FOR TASK' in result.output
    assert result.exit_code == 0


def test_cloud_abort(runid):
    runner = CliRunner()
    result = runner.invoke(cli, ['cloud', 'abort', runid])

    assert 'Aborted.' in result.output
    assert result.exit_code == 0


def test_cloud_data_list():
    runner = CliRunner()
    result = runner.invoke(cli, ['cloud', 'data'])

    assert result.exit_code == 0


def test_cloud_data_upload_delete(tmp_directory):
    Path('file.txt').write_text('some text')

    runner = CliRunner()
    result = runner.invoke(cli, ['cloud', 'data', '--upload', 'file.txt'])
    assert result.exit_code == 0

    result = runner.invoke(cli, ['cloud', 'data', '--delete', 'file.txt'])
    assert result.exit_code == 0
