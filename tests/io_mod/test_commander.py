from pathlib import Path

import pytest

from ploomber.io._commander import (Commander, CommanderException,
                                    CommanderStop, to_pascal_case)


def test_to_pascal_case():
    assert to_pascal_case('ml_online') == 'MlOnline'


def test_empty_workspace():
    Commander(workspace=None)


def test_creates_workpace(tmp_directory):
    with Commander('workspace'):
        pass

    assert Path('workspace').is_dir()


def test_get_template(tmp_directory):
    Path('workspace').mkdir()
    Path('workspace', 'template').touch()

    with Commander('workspace',
                   templates_path=('test_pkg', 'templates')) as cmdr:
        cmdr.copy_template('simple.sql')

    assert Path('workspace', 'simple.sql').read_text() == 'SELECT * FROM data'


def test_get_template_nested(tmp_directory):
    Path('workspace').mkdir()
    Path('workspace', 'template').touch()

    with Commander('workspace',
                   templates_path=('test_pkg', 'templates')) as cmdr:
        cmdr.copy_template('nested/simple.sql')

    assert Path('workspace', 'simple.sql').read_text() == 'SELECT * FROM data'


def test_commander_stop(capsys):
    msg = 'Stopping because of reasons'

    with Commander():
        raise CommanderStop(msg)

    captured = capsys.readouterr()
    assert msg in captured.out


def test_hide_command(capsys):
    with Commander() as cmdr:
        cmdr.run('echo', 'hello', show_cmd=False, description='Do something')

    captured = capsys.readouterr()
    assert 'echo hello' not in captured.out


def test_show_command(capsys):
    with Commander() as cmdr:
        cmdr.run('echo', 'hello', show_cmd=False, description='Do something')

    captured = capsys.readouterr()
    assert '==Do something: echo hello==' not in captured.out


def test_hide_command_on_error():
    with pytest.raises(CommanderException) as excinfo:
        with Commander() as cmdr:
            cmdr.run('pip', 'something', show_cmd=False)

    lines = str(excinfo.value).splitlines()
    assert lines[0] == 'An error occurred.'
    assert 'returned non-zero exit status 1.' in lines[1]
    assert len(lines) == 2


def test_show_command_on_error():
    with pytest.raises(CommanderException) as excinfo:
        with Commander() as cmdr:
            cmdr.run('pip', 'something', show_cmd=True)

    lines = str(excinfo.value).splitlines()
    assert lines[
        0] == 'An error occurred when executing command: pip something'
    assert 'returned non-zero exit status 1.' in lines[1]
    assert len(lines) == 2


def test_show_hint():
    with pytest.raises(CommanderException) as excinfo:
        with Commander() as cmdr:
            cmdr.run('pip', 'something', show_cmd=False, hint='Try this')

    lines = str(excinfo.value).splitlines()
    assert lines[0] == 'An error occurred.'
    assert 'returned non-zero exit status 1.' in lines[1]
    assert lines[2] == 'Hint: Try this.'
    assert len(lines) == 3


def test_commander_custom_environment(tmp_directory):
    Path('workspace').mkdir()

    cmdr = Commander(workspace='workspace',
                     templates_path=('test_pkg', 'templates'),
                     environment_kwargs=dict(variable_start_string='[[',
                                             variable_end_string=']]'))

    cmdr.copy_template('square-brackets.sql', placeholder='value')

    assert Path('workspace',
                'square-brackets.sql').read_text() == 'value {{another}}'
