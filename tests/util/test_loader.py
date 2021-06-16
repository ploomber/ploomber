from unittest.mock import Mock
from pathlib import Path

import pytest

from ploomber.util import loader


@pytest.mark.parametrize('root_path', ['.', 'subdir'])
def test_default_spec_load_searches_in_default_locations(
        monkeypatch, tmp_nbs, root_path):
    root_path = Path(root_path).resolve()
    Path('subdir').mkdir()

    mock = Mock(wraps=loader.default.entry_point)
    monkeypatch.setattr(loader.default, 'entry_point', mock)

    loader._default_spec_load(starting_dir=root_path)

    mock.assert_called_once_with(root_path=root_path)
