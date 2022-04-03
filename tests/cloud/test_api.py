from unittest.mock import Mock
import zipfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from ploomber.cloud import api
from ploomber.exceptions import BaseException


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
    api.zip_project(force=False,
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


def test_zip_project_ignore_prefixes(tmp_directory, sample_project):
    api.zip_project(force=False,
                    runid='runid',
                    github_number='number',
                    verbose=False,
                    ignore_prefixes=['a', 'b'])

    with zipfile.ZipFile('project.zip') as zip:
        files = zip.namelist()

    assert set(files) == {
        'c/',
        'c/c1/',
        'c/c1/c2',
        '.ploomber-cloud',
    }


@pytest.mark.skip(reason="no way of currently testing this")
def test_runs_new():
    api.runs_new(metadata=dict(a=1))


def test_upload_project_errors_if_missing_reqs_lock_txt(tmp_project):
    with pytest.raises(BaseException) as excinfo:
        api.upload_project()

    assert 'Missing requirements.lock.txt' in str(excinfo.value)


def test_upload_project_errors_if_invalid_cloud_yaml(tmp_project):
    Path('requirements.lock.txt').touch()
    Path('cloud.yaml').write_text("""
key: value
""")

    with pytest.raises(ValidationError):
        api.upload_project()


def test_upload_project_ignores_product_prefixes(monkeypatch, tmp_nbs):
    monkeypatch.setenv('PLOOMBER_CLOUD_KEY', 'some-key')
    monkeypatch.setattr(api, 'runs_new', Mock(return_value='runid'))
    monkeypatch.setattr(api, 'get_presigned_link', Mock())
    monkeypatch.setattr(api, 'upload_zipped_project', Mock())
    monkeypatch.setattr(api, 'trigger', Mock())
    Path('requirements.lock.txt').touch()

    Path('output').mkdir()
    Path('output', 'should-not-appear').touch()

    api.upload_project()

    with zipfile.ZipFile('project.zip') as zip:
        files = zip.namelist()

    assert 'output/should-not-appear' not in files
