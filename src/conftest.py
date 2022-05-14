"""
Configuration file for doctests (under src/ploomber)
"""
from pathlib import Path
import sys

path_to_src = str(Path(__file__, '..', '..', 'testutils').resolve())
sys.path.insert(0, path_to_src)

from testutils import fixture_tmp_dir, _path_to_tests  # noqa: E402


@fixture_tmp_dir(_path_to_tests() / 'assets' / 'doctests', autouse=True)
def doctests():
    pass
