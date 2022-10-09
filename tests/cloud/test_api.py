from unittest.mock import Mock
import zipfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from ploomber.cloud.api import PloomberCloudAPI
from ploomber.cloud.api import zip_project
from ploomber.cloud import api
from ploomber_core.exceptions import BaseException


@pytest.fixture
def sample_project():
    Path('a').touch()
    Path('b').mkdir()
    Path('b', 'b1').touch()
    Path('c', 'c1').mkdir(parents=True)
    Path('c', 'c1', 'c2').touch()


@pytest.fixture
def tmp_project(monkeypatch, tmp_nbs):
    monkeypatch.setenv('PLOOMBER_CLOUD_KEY', 'some-key')


def test_zip_project(tmp_directory, sample_project):
    zip_project(force=False,
                runid='runid',
                github_number='number',
                verbose=False)

    with zipfile.ZipFile('project.zip') as zip:
        files = zip.namelist()

    assert set(files) == {
        'a',
        'c/',
        'b/',
        'c/c1/',
        'c/c1/c2',
        'b/b1',
        '.ploomber-cloud',
    }


def test_zip_project_with_base_dir(tmp_directory, sample_project):
    Path('c', 'c1', 'nested').mkdir()
    Path('c', 'c1', 'nested', 'file').touch()

    zip_project(force=False,
                runid='runid',
                github_number='number',
                verbose=False,
                base_dir='c/c1')

    with zipfile.ZipFile('c/c1/project.zip') as zip:
        files = zip.namelist()

    assert set(files) == {'c2', '.ploomber-cloud', 'nested/', 'nested/file'}


@pytest.mark.parametrize('ignore_prefixes, base_dir, expected', [
    [
        ['a'],
        '',
        {
            'b/',
            'b/b1',
            'c/',
            'c/c1/',
            'c/c1/c2',
            '.ploomber-cloud',
        },
    ],
    [
        ['a', 'b'],
        '',
        {
            'c/',
            'c/c1/',
            'c/c1/c2',
            '.ploomber-cloud',
        },
    ],
    [
        None,
        'c',
        {
            'c1/',
            'c1/c2',
            '.ploomber-cloud',
        },
    ],
    [
        ['c1'],
        'c',
        {
            '.ploomber-cloud',
        },
    ],
])
def test_zip_project_ignore_prefixes(tmp_directory, sample_project,
                                     ignore_prefixes, base_dir, expected):
    zip_project(force=False,
                runid='runid',
                github_number='number',
                verbose=False,
                ignore_prefixes=ignore_prefixes,
                base_dir=base_dir)

    with zipfile.ZipFile(Path(base_dir, 'project.zip')) as zip:
        files = zip.namelist()

    assert set(files) == expected


@pytest.mark.skip(reason="no way of currently testing this")
def test_runs_new():
    PloomberCloudAPI().runs_new(metadata=dict(a=1))


def test_upload_project_errors_if_missing_reqs_lock_txt(tmp_project):
    with pytest.raises(BaseException) as excinfo:
        PloomberCloudAPI().build()

    assert 'requirements.lock.txt' in str(excinfo.value)
    assert 'environment.lock.yml' in str(excinfo.value)


def test_upload_project_errors_if_invalid_cloud_yaml(tmp_project):
    Path('requirements.lock.txt').touch()
    Path('cloud.yaml').write_text("""
key: value
""")

    with pytest.raises(ValidationError):
        PloomberCloudAPI().build()


def test_upload_project_ignores_product_prefixes(monkeypatch, tmp_nbs):
    monkeypatch.setenv('PLOOMBER_CLOUD_KEY', 'some-key')
    monkeypatch.setattr(PloomberCloudAPI, 'runs_new',
                        Mock(return_value='runid'))
    monkeypatch.setattr(PloomberCloudAPI, 'get_presigned_link', Mock())
    monkeypatch.setattr(api, 'upload_zipped_project', Mock())
    monkeypatch.setattr(PloomberCloudAPI, 'trigger', Mock())
    Path('requirements.lock.txt').touch()

    Path('output').mkdir()
    Path('output', 'should-not-appear').touch()

    PloomberCloudAPI().build()

    with zipfile.ZipFile('project.zip') as zip:
        files = zip.namelist()

    assert 'output/should-not-appear' not in files


def test_run_detailed_print_finish_no_task(monkeypatch, capsys):

    def mock_return(self, runid):
        return {'run': {'status': 'finished'}, 'tasks': []}

    monkeypatch.setattr(PloomberCloudAPI, 'run_detail', mock_return)
    PloomberCloudAPI().run_detail_print('some-key')
    captured = capsys.readouterr()

    assert captured.out.splitlines()[0] == 'Pipeline finished...'
    assert captured.out.splitlines()[1] == 'Pipeline finished due ' \
        'to no newly triggered tasks, try running ploomber cloud build --force'


def test_run_detailed_print_finish_with_tasks(monkeypatch, capsys):

    def mock_return(self, runid):
        return {
            'run': {
                'status': 'finished'
            },
            'tasks': [{
                'taskid': 'mock-id',
                'name': 'mock',
                'runid': 'some-key',
                'status': 'finished'
            }]
        }

    monkeypatch.setattr(PloomberCloudAPI, 'run_detail', mock_return)
    PloomberCloudAPI().run_detail_print('some-key')
    captured = capsys.readouterr()

    assert captured.out.splitlines()[0] == 'Pipeline finished...'
    assert 'taskid' in captured.out.splitlines()[1]
    assert 'name' in captured.out.splitlines()[1]
    assert 'runid' in captured.out.splitlines()[1]
    assert 'status' in captured.out.splitlines()[1]


def test_run_detailed_print_abort(monkeypatch, capsys):

    def mock_return(self, runid):
        return {
            'run': {
                'status': 'aborted'
            },
            'tasks': [{
                'taskid': 'mock-id',
                'name': 'mock',
                'runid': 'some-key',
                'status': 'aborted'
            }]
        }

    monkeypatch.setattr(PloomberCloudAPI, 'run_detail', mock_return)
    PloomberCloudAPI().run_detail_print('some-key')
    captured = capsys.readouterr()

    assert captured.out.splitlines()[0] == 'Pipeline aborted...'
    assert 'taskid' in captured.out.splitlines()[1]
    assert 'name' in captured.out.splitlines()[1]
    assert 'runid' in captured.out.splitlines()[1]
    assert 'status' in captured.out.splitlines()[1]


def test_run_detailed_print_fail(monkeypatch, capsys):

    def mock_return(self, runid):
        return {
            'run': {
                'status': 'failed'
            },
            'tasks': [{
                'taskid': 'mock-id',
                'name': 'mock',
                'runid': 'some-key',
                'status': 'failed'
            }]
        }

    monkeypatch.setattr(PloomberCloudAPI, 'run_detail', mock_return)
    PloomberCloudAPI().run_detail_print('some-key')
    captured = capsys.readouterr()

    assert captured.out.splitlines()[0] == 'Pipeline failed...'
    assert 'taskid' in captured.out.splitlines()[1]
    assert 'name' in captured.out.splitlines()[1]
    assert 'runid' in captured.out.splitlines()[1]
    assert 'status' in captured.out.splitlines()[1]


def test_run_detailed_print_unknown(monkeypatch, capsys):

    def mock_return(self, runid):
        return {
            'run': {
                'status': 'error'
            },
            'tasks': [{
                'taskid': 'mock-id',
                'name': 'mock',
                'runid': 'some-key',
                'status': 'error'
            }]
        }

    monkeypatch.setattr(PloomberCloudAPI, 'run_detail', mock_return)
    PloomberCloudAPI().run_detail_print('some-key')
    captured = capsys.readouterr()

    assert captured.out.splitlines()[0] == 'Unknown status: error'
    assert 'taskid' in captured.out.splitlines()[1]
    assert 'name' in captured.out.splitlines()[1]
    assert 'runid' in captured.out.splitlines()[1]
    assert 'status' in captured.out.splitlines()[1]
