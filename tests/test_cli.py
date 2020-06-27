from pathlib import Path
import subprocess
import click
from ploomber.cli import _new


def test_ploomber_new(tmp_directory, monkeypatch):
    monkeypatch.setattr(click, 'confirm', lambda x: True)
    _new()
    Path('output').mkdir()
    assert not subprocess.call(['ploomber', 'entry', 'pipeline.yaml'])
