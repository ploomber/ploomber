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


def test_lazily_load_entry_point(tmp_directory, add_current_to_sys_path,
                                 no_sys_modules_cache):
    Path('pipeline.yaml').write_text("""
tasks:
    - source: tasks_for_testing.some_task
      product: output.csv
""")

    Path('tasks_for_testing.py').write_text("""
import unknown_package

def some_task(product):
    pass
""")

    # this should not fail because it should not try to import unknown_package
    loader.lazily_load_entry_point(starting_dir='.', reload=False)


def test_top_level_importable():
    """
    Must be importable from the top-level package since users may
    use it to debug errors when using the Jupyter integration
    """
    from ploomber import lazily_load_entry_point
    assert lazily_load_entry_point
