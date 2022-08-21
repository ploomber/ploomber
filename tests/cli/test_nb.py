import json
import shutil
import subprocess
from pathlib import Path
import sys

import nbformat
import jupytext
import pytest

from ploomber_cli import cli
from ploomber.cli import nb


def git_init():
    subprocess.check_call(['git', 'init'])
    subprocess.check_call(['git', 'config', 'commit.gpgsign', 'false'])
    subprocess.check_call(['git', 'config', 'user.email', 'ci@ploomberio'])
    subprocess.check_call(['git', 'config', 'user.name', 'Ploomber'])


def git_commit():
    subprocess.check_call(['git', 'add', '--all'])
    subprocess.check_call(['git', 'commit', '-m', 'commit'])


def get_nb_injected_params(template_path):
    updated_template = Path(template_path).read_text()
    nb = json.loads(updated_template)
    parameters_cell_index = 1
    some_param_index = 1
    injected_cell = nb['cells'][parameters_cell_index]
    some_param_string = injected_cell['source'][some_param_index]
    some_param = some_param_string.split()[2]
    return some_param


def test_inject_single_task_parameters_with_same_template(
        monkeypatch, capsys, path_to_assets):
    # ploomber nb --inject --priority task-a
    # ploomber nb --inject --priority task-b
    nb_inject_assets_path = f'{path_to_assets}/test-nb-inject-assets'
    template_path = f'{nb_inject_assets_path}/template.ipynb'
    test_pipeline = f'{nb_inject_assets_path}/pipeline.yaml'

    Path(test_pipeline).write_text(f"""
tasks:
  - source: {template_path}
    name: task-a
    product: {nb_inject_assets_path}/report-a.ipynb
    params:
      some_param: a

  - source: {template_path}
    name: task-b
    product: {nb_inject_assets_path}/report-b.ipynb
    params:
      some_param: b
    """)

    inject_results = []

    params_to_inject = ['a', 'b']

    for param_to_inject in params_to_inject:
        task_to_inject = f'task-{param_to_inject}'
        monkeypatch.setattr(
            sys, 'argv',
            ['ploomber', 'nb', '--entry-point', test_pipeline,
             '--inject', '--priority', task_to_inject])
        cli.cmd_router()
        out, err = capsys.readouterr()
        if err:
            inject_results.append(False)
        else:
            injected_params = get_nb_injected_params(template_path)
            inject_results.append(injected_params == f'"{param_to_inject}"')

    assert all(inject_results)


def test_inject_multiple_tasks_parameters_with_different_templates(
        monkeypatch, capsys, path_to_assets):
    # ploomber nb --inject --priority task-a --priority task-c
    nb_inject_assets_path = f'{path_to_assets}/test-nb-inject-assets'
    template_path = f'{nb_inject_assets_path}/template.ipynb'
    template_b_path = f'{nb_inject_assets_path}/template_b.ipynb'
    test_pipeline = f'{nb_inject_assets_path}/pipeline.yaml'

    Path(test_pipeline).write_text(f"""
tasks:
  - source: {template_path}
    name: task-a
    product: {nb_inject_assets_path}/report-a.ipynb
    params:
      some_param: a

  - source: {template_b_path}
    name: task-c
    product: {nb_inject_assets_path}/report-c.ipynb
    params:
      some_param: c
    """)

    inject_results = []
    param_to_inject_a = 'a'
    param_to_inject_b = 'c'

    monkeypatch.setattr(sys, 'argv', [
        'ploomber', 'nb', '--entry-point', test_pipeline,
        '--inject', '--priority',
        f'task-{param_to_inject_a}', '--priority', f'task-{param_to_inject_b}'
    ])

    cli.cmd_router()
    out, err = capsys.readouterr()
    if err:
        inject_results.append(False)
    else:
        injected_params_a = get_nb_injected_params(template_path)
        inject_results.append(injected_params_a == f'"{param_to_inject_a}"')

        injected_params_b = get_nb_injected_params(template_b_path)
        inject_results.append(injected_params_b == f'"{param_to_inject_b}"')

    assert all(inject_results)


def test_inject_multiple_task_parameters_that_use_the_same_template(
        monkeypatch, capsys, path_to_assets):
    # ploomber nb --inject --priority task-a --priority task-b
    nb_inject_assets_path = f'{path_to_assets}/test-nb-inject-assets'
    template_path = f'{nb_inject_assets_path}/template.ipynb'
    test_pipeline = f'{nb_inject_assets_path}/pipeline.yaml'

    Path(test_pipeline).write_text(f"""
tasks:
  - source: {template_path}
    name: task-a
    product: {nb_inject_assets_path}/report-a.ipynb
    params:
      some_param: a

  - source: {template_path}
    name: task-b
    product: {nb_inject_assets_path}/report-b.ipynb
    params:
      some_param: b
    """)

    param_to_inject_a = 'a'
    param_to_inject_b = 'b'

    monkeypatch.setattr(sys, 'argv', [
        'ploomber', 'nb', '--entry-point', test_pipeline,
        '--inject', '--priority',
        f'task-{param_to_inject_a}', '--priority', f'task-{param_to_inject_b}'
    ])

    with pytest.raises(BaseException):
        cli.cmd_router()

    out, err = capsys.readouterr()
    assert 'Values are correspond to the same task' in err


def test_inject_invalid_prioritized_task_single_task(monkeypatch, capsys,
                                                     path_to_assets):
    # ploomber nb --inject --priority this-task-doesnt-exist
    nb_inject_assets_path = f'{path_to_assets}/test-nb-inject-assets'
    template_path = f'{nb_inject_assets_path}/template.ipynb'
    test_pipeline = f'{nb_inject_assets_path}/pipeline.yaml'

    Path(test_pipeline).write_text(f"""
tasks:
  - source: {template_path}
    name: task-a
    product: {nb_inject_assets_path}/report-a.ipynb
    params:
      some_param: a

  - source: {template_path}
    name: task-b
    product: {nb_inject_assets_path}/report-b.ipynb
    params:
      some_param: b
    """)

    task_name_to_inject = 'this-task-doesnt-exist'

    monkeypatch.setattr(sys, 'argv', [
        'ploomber', 'nb', '--entry-point', test_pipeline,
        '--inject', '--priority',
        f'task-{task_name_to_inject}'
    ])

    with pytest.raises(BaseException):
        cli.cmd_router()

    out, err = capsys.readouterr()
    assert 'Invalid task' in err


def test_inject_with_priority_without_task(monkeypatch, capsys,
                                           path_to_assets):
    # ploomber nb --inject --priority

    nb_inject_assets_path = f'{path_to_assets}/test-nb-inject-assets'
    template_path = f'{nb_inject_assets_path}/template.ipynb'
    test_pipeline = f'{nb_inject_assets_path}/pipeline.yaml'

    Path(test_pipeline).write_text(f"""
tasks:
  - source: {template_path}
    name: task-a
    product: {nb_inject_assets_path}/report-a.ipynb
    params:
      some_param: a

  - source: {template_path}
    name: task-b
    product: {nb_inject_assets_path}/report-b.ipynb
    params:
      some_param: b
    """)

    monkeypatch.setattr(sys, 'argv',
                        ['ploomber', 'nb', '--entry-point', test_pipeline,
                         '--inject', '--priority'])

    with pytest.raises(BaseException):
        cli.cmd_router()

    out, err = capsys.readouterr()
    assert 'expected one argument' in err


def test_inject_default_task_when_no_priority_given(monkeypatch,
                                                    path_to_assets):
    # ploomber nb --inject --priority task-a --priority task-b

    nb_inject_assets_path = f'{path_to_assets}/test-nb-inject-assets'
    template_path = f'{nb_inject_assets_path}/template.ipynb'
    test_pipeline = f'{nb_inject_assets_path}/pipeline.yaml'

    Path(test_pipeline).write_text(f"""
tasks:
  - source: {template_path}
    name: task-a
    product: {nb_inject_assets_path}/report-a.ipynb
    params:
      some_param: a

  - source: {template_path}
    name: task-b
    product: {nb_inject_assets_path}/report-b.ipynb
    params:
      some_param: b
    """)

    expected_default_value = 'a'

    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb',
                                      '--entry-point', test_pipeline,
                                      '--inject'])

    with pytest.warns(UserWarning) as warning:
        cli.cmd_router()

    injected_params = get_nb_injected_params(template_path)

    assert (len(warning) > 0)
    assert ('appears more than once in your pipeline'
            in warning[0].message.args[0])
    assert (injected_params == f'"{expected_default_value}"')


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


def test_format_adjusts_pipeline(monkeypatch, tmp_nbs):
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--format', 'ipynb'])
    assert Path('load.py').exists()
    cli.cmd_router()

    assert jupytext.read('load.ipynb')
    assert '.py' not in Path('pipeline.yaml').read_text()


def test_format_same_pipeline(monkeypatch, tmp_nbs):
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--format', 'py'])
    pipeline = Path('pipeline.yaml').read_text()
    cli.cmd_router()

    assert pipeline == Path('pipeline.yaml').read_text()


def test_format_no_entry_point(monkeypatch, tmp_nbs_factory, capsys,
                               tmp_imports):
    monkeypatch.setattr(sys, 'argv', [
        'ploomber',
        'nb',
        '--entry-point',
        'nbs_factory.make',
        '--format',
        'ipynb',
    ])
    cli.cmd_router()
    out, _ = capsys.readouterr()
    assert 'entry-point is not a valid file' in out


def test_format_missing_file_in_entry_point(monkeypatch, tmp_directory,
                                            capsys):
    Path('pipeline.yaml').write_text("""tasks:
  - source: '{{root}}'
    product: 'some_file.ipynb'""")
    file_name = 'get.py'
    Path('env.yaml').write_text(f"""root: {file_name}""")
    Path(file_name).write_text("""# %% tags=["parameters"]
                               \nupstream = None\nproduct = None\n
                               # %% section \nprint("test")""")

    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--format', 'ipynb'])
    cli.cmd_router()
    out, err = capsys.readouterr()
    assert f'{file_name} does not appear in entry-point' in out


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
    Path('another').touch()
    git_commit()

    injected_tag = '# + tags=["injected-parameters"]'
    assert injected_tag in Path('load.py').read_text()

    # check out last committed files
    subprocess.check_call(['git', 'stash'])

    # committed version should not have the injected cell
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


@pytest.fixture
def mock_nb_single_click(monkeypatch):
    parent = Path('.jupyter', 'labconfig')
    path = parent / 'default_setting_overrides.json'

    def MockPath(*args):
        return (parent if args == ('~/.jupyter', 'labconfig') else Path(*args))

    monkeypatch.setattr(nb, 'Path', MockPath)

    return path


@pytest.fixture
def mock_nb_single_click_enable(monkeypatch, mock_nb_single_click):
    monkeypatch.setattr(sys, 'argv', ['ploomber', 'nb', '--single-click'])
    return mock_nb_single_click


@pytest.fixture
def mock_nb_single_click_disable(monkeypatch, mock_nb_single_click):
    monkeypatch.setattr(sys, 'argv',
                        ['ploomber', 'nb', '--single-click-disable'])
    return mock_nb_single_click


def test_single_click(mock_nb_single_click_enable, tmp_directory):
    cli.cmd_router()

    current = json.loads(mock_nb_single_click_enable.read_text())
    expected = json.loads(nb._jupyterlab_default_settings_overrides)
    assert current == expected


def test_single_click_updates_existing(mock_nb_single_click_enable,
                                       tmp_directory):
    parent = mock_nb_single_click_enable.parent
    parent.mkdir(parents=True)
    mock_nb_single_click_enable.write_text(json.dumps(dict(key='value')))

    cli.cmd_router()

    expected = json.loads(nb._jupyterlab_default_settings_overrides)
    expected['key'] = 'value'

    current = json.loads(mock_nb_single_click_enable.read_text())
    assert current == expected


def test_single_click_disable(mock_nb_single_click_disable, tmp_directory):
    cli.cmd_router()


@pytest.mark.parametrize('existing, expected', [
    [
        {
            'key': 'value',
            '@jupyterlab/docmanager-extension:plugin': {
                'defaultViewers': {
                    "python": "Jupytext Notebook",
                }
            }
        },
        {
            'key': 'value'
        },
    ],
    [
        {
            'key': 'value',
            '@jupyterlab/docmanager-extension:plugin': {
                'defaultViewers': {
                    "python": "Jupytext Notebook",
                },
                'another': 'key'
            }
        },
        {
            'key': 'value',
            '@jupyterlab/docmanager-extension:plugin': {
                'another': 'key'
            }
        },
    ],
])
def test_single_click_disable_updates_existing(mock_nb_single_click_disable,
                                               tmp_directory, existing,
                                               expected):
    parent = mock_nb_single_click_disable.parent
    parent.mkdir(parents=True)
    mock_nb_single_click_disable.write_text(json.dumps(existing))

    cli.cmd_router()

    current = json.loads(mock_nb_single_click_disable.read_text())
    assert current == expected
