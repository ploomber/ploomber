"""
Configuration file for doctests (under src/ploomber)
"""
from unittest.mock import Mock
from pathlib import Path
import sys

import pytest
from ploomber.clients.storage import gcloud

path_to_src = str(Path(__file__, '..', '..', 'testutils').resolve())
sys.path.insert(0, path_to_src)

from testutils import fixture_tmp_dir, _path_to_tests  # noqa: E402


@pytest.fixture(autouse=True)
def mock_gcloud(monkeypatch):
    mock = Mock()
    # mock the storage module to prevent making API calls to google cloud
    # so the doctest that shows how to use the google cloud storage client
    # works
    monkeypatch.setattr(gcloud, 'storage', mock)


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'doctests', autouse=True)
def doctests():
    pass
