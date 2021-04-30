from pathlib import Path

from ploomber.io._commander import Commander


def test_empty_workspace():
    Commander(workspace=None)


def test_creates_workpace(tmp_directory):
    with Commander('workspace'):
        pass

    assert Path('workspace').is_dir()
