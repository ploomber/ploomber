import shutil
import subprocess
from pathlib import Path
import sys

import nbformat
import jupytext
import pytest

from ploomber.cli import cli


def git_init():
    subprocess.check_call(['git', 'init'])


def git_commit():
    subprocess.check_call(['git', 'config', 'user.email', 'ci@ploomberio'])
    subprocess.check_call(['git', 'config', 'user.name', 'Ploomber'])
    subprocess.check_call(['git', 'add', '--all'])
    subprocess.check_call(['git', 'commit', '-m', 'commit'])


def test_inject_remove(monkeypatch, tmp_nbs):
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--inject'])
    expected = 'tags=["injected-parameters"]'

    assert expected not in Path('load.py').read_text()
    assert expected not in Path('clean.py').read_text()
    assert expected not in Path('plot.py').read_text()

    cli.cmd_router()

    assert expected in Path('load.py').read_text()
    assert expected in Path('clean.py').read_text()
    assert expected in Path('plot.py').read_text()

    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--remove'])

    cli.cmd_router()

    assert expected not in Path('load.py').read_text()
    assert expected not in Path('clean.py').read_text()
    assert expected not in Path('plot.py').read_text()


def test_format(monkeypatch, tmp_nbs):
    monkeypatch.setattr(sys, 'argv',
                        ['ploomber', 'nb', '--format', 'py:percent'])

    expected = '%% tags=["parameters"]'

    assert expected not in Path('load.py').read_text()
    assert expected not in Path('clean.py').read_text()
    assert expected not in Path('plot.py').read_text()

    cli.cmd_router()

    assert expected in Path('load.py').read_text()
    assert expected in Path('clean.py').read_text()
    assert expected in Path('plot.py').read_text()


def test_format_with_extension_change(monkeypatch, tmp_nbs):
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--format', 'ipynb'])
    cli.cmd_router()

    assert not Path('load.py').exists()
    assert not Path('clean.py').exists()
    assert not Path('plot.py').exists()
    assert jupytext.read('load.ipynb')
    assert jupytext.read('clean.ipynb')
    assert jupytext.read('plot.ipynb')


def test_format_skips_non_notebooks(monkeypatch, backup_simple,
                                    no_sys_modules_cache):
    monkeypatch.setattr(sys, 'argv',
                        ['ploomber', 'nb', '--format', 'py:percent'])
    cli.cmd_router()


def test_pair_sync(monkeypatch, tmp_nbs):
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--pair', 'nbs'])

    cli.cmd_router()

    def get_formats(nb):
        return nb.metadata.jupytext.formats

    expected_fmt = 'nbs//ipynb,py:light'
    assert get_formats(jupytext.read('load.py')) == expected_fmt
    assert get_formats(jupytext.read('clean.py')) == expected_fmt
    assert get_formats(jupytext.read('plot.py')) == expected_fmt
    assert get_formats(jupytext.read(Path('nbs',
                                          'load.ipynb'))) == expected_fmt
    assert get_formats(jupytext.read(Path('nbs',
                                          'clean.ipynb'))) == expected_fmt
    assert get_formats(jupytext.read(Path('nbs',
                                          'plot.ipynb'))) == expected_fmt

    # modify one and sync
    nb = jupytext.read('load.py')
    current = nbformat.versions[nbformat.current_nbformat]
    cell = current.new_code_cell(source='# this is a new cell')
    nb.cells.append(cell)
    jupytext.write(nb, 'load.py')

    assert '# this is a new cell' not in Path('nbs', 'load.ipynb').read_text()

    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--sync'])
    cli.cmd_router()

    assert '# this is a new cell' in Path('nbs', 'load.ipynb').read_text()


def test_install_hook_error_if_missing_git(monkeypatch, tmp_nbs, capsys):
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--install-hook'])

    with pytest.raises(SystemExit):
        cli.cmd_router()

    captured = capsys.readouterr()
    assert 'Error: Expected a .git/ directory' in captured.err


@pytest.mark.xfail(sys.platform == "win32",
                   reason="Windows can't run git hook")
def test_install_hook(monkeypatch, tmp_nbs):
    # inject cells
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--inject'])
    cli.cmd_router()

    # init repo
    git_init()

    # install hook
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--install-hook'])
    cli.cmd_router()

    # commit (should remove injected cells before committing and re-add them
    # after committing)
    git_commit()

    injected_tag = '# + tags=["injected-parameters"]'
    assert injected_tag in Path('load.py').read_text()

    # check last commit files
    subprocess.check_call(['git', 'stash'])

    assert injected_tag not in Path('load.py').read_text()

    assert ('ploomber nb --entry-point pipeline.yaml --remove'
            in Path('.git', 'hooks', 'pre-commit').read_text())
    assert ('ploomber nb --entry-point pipeline.yaml --inject'
            in Path('.git', 'hooks', 'post-commit').read_text())


def test_install_hook_custom_entry_point(monkeypatch, tmp_nbs):
    shutil.copy('pipeline.yaml', 'pipeline.another.yaml')

    # inject cells
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--inject'])
    cli.cmd_router()

    # init repo
    git_init()

    # install hook
    monkeypatch.setattr(sys, 'argv', [
        'ploomber',
        'nb',
        '--install-hook',
        '--entry-point',
        'pipeline.another.yaml',
    ])
    cli.cmd_router()

    assert ('ploomber nb --entry-point pipeline.another.yaml --remove'
            in Path('.git', 'hooks', 'pre-commit').read_text())
    assert ('ploomber nb --entry-point pipeline.another.yaml --inject'
            in Path('.git', 'hooks', 'post-commit').read_text())


def test_uninstall_hook(monkeypatch, tmp_nbs):
    git_init()

    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--install-hook'])
    cli.cmd_router()

    assert Path('.git', 'hooks', 'pre-commit').is_file()
    assert Path('.git', 'hooks', 'post-commit').is_file()

    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--uninstall-hook'])
    cli.cmd_router()

    assert not Path('.git', 'hooks', 'pre-commit').exists()
    assert not Path('.git', 'hooks', 'post-commit').exists()
