from pathlib import Path

from ploomber.io._commander import Commander, CommanderStop, to_pascal_case


def test_to_pascal_case():
    assert to_pascal_case('ml_online') == 'MlOnline'


def test_empty_workspace():
    Commander(workspace=None)


def test_creates_workpace(tmp_directory):
    with Commander('workspace'):
        pass

    assert Path('workspace').is_dir()


def test_commander_stop(capsys):
    msg = 'Stopping because of reasons'

    with Commander():
        raise CommanderStop(msg)

    captured = capsys.readouterr()
    assert msg in captured.out
