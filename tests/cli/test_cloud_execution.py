"""
Tests for cloud execution
"""
from pathlib import Path

import pytest
from click.testing import CliRunner

from ploomber_cli.cli import cli


@pytest.fixture
def runid():
    return '1e8224b0-07e9-4c29-bb81-393817926c12'


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
    assert len(result.output.splitlines()) == 10
    assert result.exit_code == 0


def test_cloud_status_finish_with_no_task_run(monkeypatch, runid):

    class MockResponse:
        exit_code = 0

        @staticmethod
        def output():
            return 'Pipeline finished... \n\
                Pipeline finished due to no newly triggered tasks, \
                    try build with --force'

    def mock_status(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(CliRunner, 'invoke', mock_status)
    runner = CliRunner()
    result = runner.invoke(cli, ['cloud', 'status', runid])

    assert 'taskid' not in result.output()
    assert 'name' not in result.output()
    assert 'Pipeline finished' in result.output()
    assert '--force' in result.output()
    assert len(result.output().splitlines()) == 2
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
