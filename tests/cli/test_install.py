import subprocess
import os
import sys
from pathlib import Path
from unittest.mock import Mock, call, ANY
import shutil

import yaml
import datetime
import pytest
from click.testing import CliRunner

from ploomber.cli import install as install_module
from ploomber.cli.cli import install
from ploomber.cli.install import _pip_install
from ploomber.exceptions import BaseException
from conftest import (_write_sample_conda_env, _prepare_files,
                      _write_sample_pip_req, _write_sample_conda_files,
                      _write_sample_pip_files)

setup_py = """
from setuptools import setup, find_packages

setup(
    name='sample_package',
    version='1.0',
    extras_require={'dev': []}
)
"""


@pytest.fixture
def pkg_manager():
    mamba = shutil.which('mamba')
    conda = shutil.which('conda')
    return mamba if mamba else conda


@pytest.fixture
def error_if_calling_locate_pip_inside_conda(monkeypatch):
    # to make it fail if it attempts to look for pip, see docstring in the
    # '_locate_pip_inside_conda' method for details
    mock_locate = Mock(side_effect=ValueError)
    monkeypatch.setattr(install_module, '_locate_pip_inside_conda',
                        mock_locate)


@pytest.fixture
def mock_cmdr_wrapped(monkeypatch):
    mock = Mock(wraps=install_module.Commander().run)
    monkeypatch.setattr(install_module.Commander, 'run', mock)
    return mock


@pytest.fixture
def cleanup_conda_tmp_env():
    if os.name == 'nt':
        conda = shutil.which('conda')
        subprocess.run([conda, 'env', 'remove', '--name', 'my_tmp_env'])


def _get_venv_and_pip():
    name = Path().resolve().name
    venv = f'venv-{name}'
    pip = str(
        Path(venv, 'Scripts' if os.name == 'nt' else 'bin',
             'pip.exe' if os.name == 'nt' else 'pip'))
    return venv, pip


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, reqs_lock',
                         [
                             [0, 0, 0, 0, 0, 0],
                             [0, 0, 0, 1, 0, 0],
                             [0, 0, 0, 0, 0, 1],
                             [0, 0, 0, 1, 0, 1],
                             [1, 0, 0, 0, 0, 0],
                             [1, 0, 0, 1, 0, 0],
                             [1, 0, 0, 0, 0, 1],
                             [1, 0, 0, 1, 0, 1],
                         ])
def test_missing_both_files(tmp_directory, has_conda, use_lock, env, env_lock,
                            reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        install,
        args=['--use-lock'] if use_lock else ['--no-use-lock'],
        catch_exceptions=False)
    expected = ('Expected an environment.yaml (conda)'
                ' or requirements.txt (pip) in the current directory.'
                ' Add one of them and try again.')

    assert f'Error: {expected}\n' == result.stdout


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, reqs_lock',
                         [
                             [0, 1, 1, 0, 1, 0],
                             [1, 1, 1, 0, 1, 0],
                             [0, 1, 0, 0, 1, 0],
                             [1, 1, 0, 0, 1, 0],
                             [0, 1, 1, 0, 0, 0],
                             [1, 1, 1, 0, 0, 0],
                             [0, 1, 0, 0, 0, 0],
                             [1, 1, 0, 0, 0, 0],
                         ])
def test_missing_both_lock_files(tmp_directory, has_conda, use_lock, env,
                                 env_lock, reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    runner = CliRunner()
    result = runner.invoke(install,
                           args=['--use-lock'] if use_lock else [],
                           catch_exceptions=False)
    expected = (
        'Expected an environment.lock.yaml '
        '(conda) or requirements.lock.txt (pip) in the current directory. '
        'Add one of them and try again.')

    assert f'Error: {expected}\n' == result.stdout


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, reqs_lock',
                         [
                             [0, 1, 1, 1, 1, 0],
                             [0, 1, 0, 1, 1, 0],
                             [0, 1, 1, 1, 0, 0],
                             [0, 1, 0, 1, 0, 0],
                         ])
def test_missing_env_lock(tmp_directory, has_conda, use_lock, env, env_lock,
                          reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        install,
        args=['--use-lock'] if use_lock else ['--no-use-lock'],
        catch_exceptions=False)
    expected = ('Found env environment.lock.yaml '
                'but conda is not installed. Install conda or add a '
                'requirements.lock.txt to use pip instead')

    assert f'Error: {expected}\n' == result.stdout


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, '
                         'reqs_lock', [
                             [0, 0, 1, 0, 0, 1],
                             [0, 0, 1, 1, 0, 1],
                             [0, 0, 1, 0, 0, 0],
                             [0, 0, 1, 1, 0, 0],
                         ])
def test_missing_env(tmp_directory, has_conda, use_lock, env, env_lock, reqs,
                     reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    runner = CliRunner()
    result = runner.invoke(
        install,
        args=['--use-lock'] if use_lock else ['--no-use-lock'],
        catch_exceptions=False)
    expected = ('Found environment.yaml but conda '
                'is not installed. Install conda or add a '
                'requirements.txt to use pip instead')

    assert f'Error: {expected}\n' == result.stdout


@pytest.fixture
def mock_main(monkeypatch):
    main_pip, main_conda = Mock(), Mock()
    monkeypatch.setattr(install_module, 'main_pip', main_pip)
    monkeypatch.setattr(install_module, 'main_conda', main_conda)
    return main_pip, main_conda


def test_use_lock_none_with_pip_lock_exists(tmp_directory, monkeypatch,
                                            mock_main):
    main_pip, main_conda = mock_main
    # simulate no conda
    monkeypatch.setattr(install_module.shutil, 'which', lambda _: None)

    Path('requirements.lock.txt').touch()

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert result.exit_code == 0
    main_pip.assert_called_once_with(use_lock=True, create_env=ANY)
    main_conda.assert_not_called()


def test_use_lock_none_with_pip_regular_exists(tmp_directory, monkeypatch,
                                               mock_main):
    main_pip, main_conda = mock_main
    # simulate no conda
    monkeypatch.setattr(install_module.shutil, 'which', lambda _: None)

    Path('requirements.txt').touch()

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert result.exit_code == 0
    main_pip.assert_called_once_with(use_lock=False, create_env=ANY)
    main_conda.assert_not_called()


def test_use_lock_none_with_conda_lock_exists(tmp_directory, mock_main):
    main_pip, main_conda = mock_main

    Path('environment.lock.yml').touch()

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert result.exit_code == 0
    main_conda.assert_called_once_with(use_lock=True, create_env=ANY)
    main_pip.assert_not_called()


def test_use_lock_none_with_conda_regular_exists(tmp_directory, mock_main):
    main_pip, main_conda = mock_main
    Path('environment.yml').touch()

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert result.exit_code == 0
    main_conda.assert_called_once_with(use_lock=False, create_env=ANY)
    main_pip.assert_not_called()


def test_use_lock_none_with_conda_wrong_lock_exists(tmp_directory, monkeypatch,
                                                    mock_main):
    main_pip, main_conda = mock_main
    # simulate no conda
    monkeypatch.setattr(install_module.shutil, 'which', lambda _: None)

    Path('environment.lock.yml').touch()

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert result.exit_code == 1
    expected = 'Expected an environment.yaml (conda) or requirements.txt (pip)'
    assert expected in result.output


def test_use_lock_none_with_pip_wrong_lock_exists(tmp_directory, mock_main):
    main_pip, main_conda = mock_main

    Path('requirements.lock.txt').touch()

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert result.exit_code == 1
    expected = 'Expected an environment.yaml (conda) or requirements.txt (pip)'
    assert expected in result.output


def test_use_venv_even_if_conda_installed(tmp_directory, mock_main):
    main_pip, main_conda = mock_main

    Path('requirements.lock.txt').touch()

    runner = CliRunner()
    result = runner.invoke(install,
                           args=['--use-venv'],
                           catch_exceptions=False)

    assert result.exit_code == 0
    main_pip.assert_called_once()
    main_conda.assert_not_called()


def mocked_get_now():
    dt = datetime.datetime(2021, 1, 1, 10, 10, 10)
    return dt


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, reqs_lock',
                         [
                             [1, 1, 1, 1, 1, 0],
                             [1, 1, 1, 1, 1, 1],
                             [1, 1, 1, 1, 0, 0],
                             [1, 1, 1, 1, 0, 1],
                             [1, 1, 0, 1, 1, 0],
                             [1, 1, 0, 1, 1, 1],
                             [1, 1, 0, 1, 0, 0],
                             [1, 1, 0, 1, 0, 1],
                             [1, 0, 1, 0, 1, 0],
                             [1, 0, 1, 1, 1, 0],
                             [1, 0, 1, 0, 1, 1],
                             [1, 0, 1, 1, 1, 1],
                             [1, 0, 1, 0, 0, 0],
                             [1, 0, 1, 1, 0, 0],
                             [1, 0, 1, 0, 0, 1],
                             [1, 0, 1, 1, 0, 1],
                         ])
def test_install_with_conda(tmp_directory, has_conda, use_lock, env, env_lock,
                            reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    mock = Mock()
    mock.patch(install_module, 'datetime', side_effect=mocked_get_now)
    monkeypatch.setattr(install_module, 'main_conda', mock)
    start = mocked_get_now()
    install_module.main_conda(start, True if use_lock else False)
    inputs_args, kwargs = mock.call_args

    assert inputs_args[0] == start
    assert inputs_args[1] == bool(use_lock)


@pytest.mark.parametrize('has_conda, use_lock, env, env_lock, reqs, reqs_lock',
                         [
                             [1, 1, 1, 0, 1, 1],
                             [1, 1, 0, 0, 1, 1],
                             [1, 1, 1, 0, 0, 1],
                             [1, 1, 0, 0, 0, 1],
                             [0, 1, 1, 0, 1, 1],
                             [0, 1, 1, 1, 1, 1],
                             [0, 1, 0, 0, 1, 1],
                             [0, 1, 0, 1, 1, 1],
                             [0, 1, 1, 0, 0, 1],
                             [0, 1, 1, 1, 0, 1],
                             [0, 1, 0, 0, 0, 1],
                             [0, 1, 0, 1, 0, 1],
                             [1, 0, 0, 0, 1, 0],
                             [1, 0, 0, 1, 1, 0],
                             [1, 0, 0, 0, 1, 1],
                             [1, 0, 0, 1, 1, 1],
                             [0, 0, 1, 0, 1, 0],
                             [0, 0, 1, 1, 1, 0],
                             [0, 0, 1, 0, 1, 1],
                             [0, 0, 1, 1, 1, 1],
                             [0, 0, 0, 0, 1, 0],
                             [0, 0, 0, 1, 1, 0],
                             [0, 0, 0, 0, 1, 1],
                             [0, 0, 0, 1, 1, 1],
                         ])
def test_install_with_pip(tmp_directory, has_conda, use_lock, env, env_lock,
                          reqs, reqs_lock, monkeypatch):

    _prepare_files(has_conda, use_lock, env, env_lock, reqs, reqs_lock,
                   monkeypatch)

    mock = Mock()
    monkeypatch.setattr(install_module, 'main_pip', mock)
    mock.patch(install_module, 'datetime', side_effect=mocked_get_now)
    start = mocked_get_now()
    install_module.main_pip(start, True if use_lock else False)
    inputs_args, kwargs = mock.call_args

    assert inputs_args[0] == start
    assert inputs_args[1] == bool(use_lock)


@pytest.mark.parametrize('args, is_conda, env_name, create_env', [
    [[], True, 'some-env', False],
    [[], True, 'some-env', False],
    [[], True, 'base', False],
    [[], True, 'base', False],
    [[], False, 'some-env', False],
    [[], False, 'some-env', False],
    [['--create-env'], True, 'some-env', True],
])
def test_installs_conda_inline_if_inside_venv(tmp_directory, monkeypatch, args,
                                              is_conda, env_name, create_env):
    _write_sample_conda_files()

    main = Mock()
    monkeypatch.setattr(install_module.shutil, 'which', Mock())
    monkeypatch.setattr(install_module, 'main_conda', main)
    monkeypatch.setattr(install_module.telemetry, 'is_conda', lambda: is_conda)
    monkeypatch.setattr(install_module, '_current_conda_env_name',
                        lambda: env_name)

    runner = CliRunner()
    result = runner.invoke(install, args=args, catch_exceptions=False)

    main.assert_called_once_with(use_lock=ANY, create_env=create_env)
    assert result.exit_code == 0


@pytest.mark.parametrize('args, in_venv, create_env', [
    [[], False, False],
    [[], False, False],
    [[], True, False],
    [[], True, False],
    [['--create-env'], True, True],
])
def test_installs_pip_inline_if_inside_venv(tmp_directory, monkeypatch, args,
                                            in_venv, create_env):
    _write_sample_pip_files()

    main = Mock()
    # simulate no conda
    monkeypatch.setattr(install_module.shutil, 'which', lambda _: None)
    monkeypatch.setattr(install_module, 'main_pip', main)
    monkeypatch.setattr(install_module.telemetry, 'in_virtualenv',
                        lambda: in_venv)

    runner = CliRunner()
    result = runner.invoke(install, args=args, catch_exceptions=False)

    main.assert_called_once_with(use_lock=ANY, create_env=create_env)
    assert result.exit_code == 0


@pytest.mark.parametrize('dev_create, use_lock, expected_call', [
    [
        False, False,
        [
            call('pip',
                 'install',
                 '--requirement',
                 'requirements.txt',
                 description=ANY),
            call('pip',
                 'freeze',
                 '--exclude-editable',
                 description=ANY,
                 capture_output=True)
        ]
    ],
    [
        False, True,
        [
            call('pip',
                 'install',
                 '--requirement',
                 'requirements.lock.txt',
                 description=ANY)
        ]
    ],
    [
        True, False,
        [
            call('pip',
                 'install',
                 '--requirement',
                 'requirements.txt',
                 description=ANY),
            call('pip',
                 'freeze',
                 '--exclude-editable',
                 description=ANY,
                 capture_output=True),
            call('pip',
                 'install',
                 '--requirement',
                 'requirements.dev.txt',
                 description=ANY),
            call('pip',
                 'freeze',
                 '--exclude-editable',
                 description=ANY,
                 capture_output=True)
        ]
    ],
    [
        True, True,
        [
            call('pip',
                 'install',
                 '--requirement',
                 'requirements.lock.txt',
                 description=ANY),
            call('pip',
                 'install',
                 '--requirement',
                 'requirements.dev.lock.txt',
                 description=ANY)
        ]
    ],
])
def test_main_pip_install_inline(tmp_directory, monkeypatch, capsys,
                                 dev_create, use_lock, expected_call):
    _write_sample_pip_files(dev=False)
    _write_sample_pip_files(dev=dev_create)

    mock = Mock(return_value='something')
    monkeypatch.setattr(install_module.Commander, 'run', mock)

    install_module.main_pip(use_lock=use_lock, create_env=False)

    assert mock.call_args_list == expected_call

    captured = capsys.readouterr()
    assert "=\n$ ploomber build\n=" in captured.out


@pytest.mark.parametrize('dev_create, use_lock, expected_calls', [
    [
        False, False,
        [
            call('conda',
                 'env',
                 'update',
                 '--file',
                 'environment.yml',
                 '--name',
                 'some-env',
                 description=ANY),
            call('conda',
                 'env',
                 'export',
                 '--no-build',
                 '--name',
                 'some-env',
                 description=ANY,
                 capture_output=True)
        ]
    ],
    [
        False, True,
        [
            call('conda',
                 'env',
                 'update',
                 '--file',
                 'environment.lock.yml',
                 '--name',
                 'some-env',
                 description=ANY),
        ]
    ],
    [
        True, True,
        [
            call('conda',
                 'env',
                 'update',
                 '--file',
                 'environment.lock.yml',
                 '--name',
                 'some-env',
                 description=ANY),
            call('conda',
                 'env',
                 'update',
                 '--file',
                 'environment.dev.lock.yml',
                 '--name',
                 'some-env',
                 description=ANY)
        ]
    ],
    [
        True, False,
        [
            call('conda',
                 'env',
                 'update',
                 '--file',
                 'environment.yml',
                 '--name',
                 'some-env',
                 description=ANY),
            call('conda',
                 'env',
                 'export',
                 '--no-build',
                 '--name',
                 'some-env',
                 description=ANY,
                 capture_output=True),
            call('conda',
                 'env',
                 'update',
                 '--file',
                 'environment.dev.yml',
                 '--name',
                 'some-env',
                 description=ANY),
            call('conda',
                 'env',
                 'export',
                 '--no-build',
                 '--name',
                 'some-env',
                 description=ANY,
                 capture_output=True)
        ]
    ],
])
def test_main_conda_install_inline(monkeypatch, capsys, tmp_directory,
                                   dev_create, use_lock, expected_calls):
    _write_sample_conda_files()
    _write_sample_conda_files(dev=dev_create)

    def which(arg):
        return arg if arg == 'conda' else None

    mock = Mock(return_value='something')
    monkeypatch.setattr(install_module.Commander, 'run', mock)
    monkeypatch.setattr(install_module.shutil, 'which', which)
    monkeypatch.setattr(install_module, '_current_conda_env_name',
                        lambda: 'some-env')

    install_module.main_conda(use_lock=use_lock, create_env=False)

    assert mock.call_args_list == expected_calls

    captured = capsys.readouterr()
    assert "=\n$ ploomber build\n=" in captured.out


@pytest.mark.parametrize('conda_bin, conda_root',
                         [
                             [('something', 'Miniconda3', 'conda'),
                              ('something', 'Miniconda3')],
                             [('something', 'miniconda3', 'conda'),
                              ('something', 'miniconda3')],
                             [('something', 'Anaconda3', 'conda'),
                              ('something', 'Anaconda3')],
                             [('something', 'anaconda3', 'conda'),
                              ('something', 'anaconda3')],
                             [('one', 'miniconda3', 'dir', 'conda'),
                              ('one', 'miniconda3')],
                             [('one', 'anaconda3', 'dir', 'conda'),
                              ('one', 'anaconda3')],
                             [('one', 'another', 'Miniconda3', 'conda'),
                              ('one', 'another', 'Miniconda3')],
                             [('one', 'another', 'Anaconda3', 'conda'),
                              ('one', 'another', 'Anaconda3')],
                         ])
def test_find_conda_root(conda_bin, conda_root):
    assert install_module._find_conda_root(
        Path(*conda_bin)).parts == conda_root


def test_error_if_unknown_conda_layout():
    with pytest.raises(BaseException):
        install_module._find_conda_root(Path('a', 'b'))


@pytest.mark.parametrize(
    'conda_bin',
    [
        # old versions of conda may have the conda binary in a different
        # location see #319
        ['Users', 'user', 'Miniconda3', 'Library', 'bin', 'conda.BAT'],
        ['Users', 'user', 'Miniconda3', 'condabin', 'conda.BAT'],
    ],
    ids=['location-old', 'location-new'])
def test_locate_pip_inside_conda(monkeypatch, tmp_directory, conda_bin):
    mock = Mock(return_value=str(Path(*conda_bin)))

    path = Path('Users', 'user', 'Miniconda3', 'envs', 'myenv',
                'Scripts' if os.name == 'nt' else 'bin',
                'pip.exe' if os.name == 'nt' else 'pip')

    path.parent.mkdir(parents=True)
    path.touch()

    monkeypatch.setattr(install_module.shutil, 'which', mock)

    assert install_module._locate_pip_inside_conda('myenv') == str(path)


# FIXME: i tested this locally on a windows machine and it works but for some
# reason, the machine running on github actions is unable to locate "conda"
# hence this fails. it's weird because I'm calling conda without issues
# to install dependencies during setup. Same with the next two tests
@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_package_conda(tmp_directory, mock_cmdr_wrapped):
    _write_sample_conda_env()
    Path('setup.py').write_text(setup_py)

    runner = CliRunner()
    runner.invoke(install, catch_exceptions=False)

    # check it calls "pip install --editable ."
    assert mock_cmdr_wrapped.call_args_list[-2][1][
        'description'] == 'Installing project'

    # check first argument is the path to the conda binary instead of just
    # "conda" since we discovered that fails sometimes on Windows
    assert all(
        [Path(c[0][0]).is_file() for c in mock_cmdr_wrapped.call_args_list])

    assert set(os.listdir()) == {
        'environment.yml',
        'environment.lock.yml',
        'sample_package.egg-info',
        'setup.py',
    }


@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_non_package_with_conda(tmp_directory, monkeypatch,
                                        mock_cmdr_wrapped):
    # to make it fail if it attempts to look for pip, see docstring in the
    # '_locate_pip_inside_conda' method for details
    mock_locate = Mock(side_effect=ValueError)
    monkeypatch.setattr(install_module, '_locate_pip_inside_conda',
                        mock_locate)

    _write_sample_conda_env()

    runner = CliRunner()
    runner.invoke(install, catch_exceptions=False)

    # check first argument is the path to the conda binary instead of just
    # "conda" since we discovered that fails sometimes on Windows
    assert all(
        [Path(c[0][0]).is_file() for c in mock_cmdr_wrapped.call_args_list])

    assert set(os.listdir()) == {'environment.yml', 'environment.lock.yml'}


@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_non_package_with_conda_with_dev_deps(tmp_directory):
    _write_sample_conda_env()
    _write_sample_conda_env('environment.dev.yml')
    runner = CliRunner()

    runner.invoke(install, catch_exceptions=False)

    assert set(os.listdir()) == {
        'environment.yml', 'environment.lock.yml', 'environment.dev.yml',
        'environment.dev.lock.yml'
    }


@pytest.mark.parametrize('create_dev_lock', [True, False])
def test_install_lock_non_package_with_conda(
        tmp_directory, monkeypatch, mock_cmdr_wrapped, pkg_manager,
        error_if_calling_locate_pip_inside_conda, cleanup_conda_tmp_env,
        create_dev_lock):
    _write_sample_conda_env('environment.lock.yml')

    if create_dev_lock:
        _write_sample_conda_env('environment.dev.lock.yml')

    runner = CliRunner()
    runner.invoke(install,
                  args=['--use-lock', '--create-env'],
                  catch_exceptions=False)

    expected = [
        call(pkg_manager,
             'env',
             'create',
             '--file',
             'environment.lock.yml',
             '--force',
             description='Creating env'),
        call(pkg_manager,
             'env',
             'update',
             '--file',
             'environment.dev.lock.yml',
             '--name',
             'my_tmp_env',
             description='Installing dev dependencies')
    ]

    # on windows, we expect this call to check if the env exists already
    if os.name == 'nt':
        expected.insert(
            0, call(pkg_manager, 'env', 'list', '--json', capture_output=True))

    # pop the last entry if we dont have dev dependencies
    if not create_dev_lock:
        expected.pop(-1)

    assert mock_cmdr_wrapped.call_args_list == expected
    assert all(
        [Path(c[0][0]).is_file() for c in mock_cmdr_wrapped.call_args_list])


@pytest.mark.parametrize('create_dev_lock', [True, False])
def test_install_lock_package_with_conda(tmp_directory, monkeypatch,
                                         mock_cmdr_wrapped, pkg_manager,
                                         cleanup_conda_tmp_env,
                                         create_dev_lock):
    _write_sample_conda_env('environment.lock.yml')

    if create_dev_lock:
        _write_sample_conda_env('environment.dev.lock.yml')

    Path('setup.py').write_text(setup_py)

    runner = CliRunner()
    runner.invoke(install,
                  args=['--use-lock', '--create-env'],
                  catch_exceptions=False)

    pip = install_module._path_to_pip_in_env_with_name(shutil.which('conda'),
                                                       'my_tmp_env')

    expected = [
        call(pkg_manager,
             'env',
             'create',
             '--file',
             'environment.lock.yml',
             '--force',
             description='Creating env'),
        call(pip,
             'install',
             '--editable',
             '.',
             description='Installing project'),
        call(pkg_manager,
             'env',
             'update',
             '--file',
             'environment.dev.lock.yml',
             '--name',
             'my_tmp_env',
             description='Installing dev dependencies')
    ]

    if os.name == 'nt':
        expected.insert(
            0, call(pkg_manager, 'env', 'list', '--json', capture_output=True))

    if not create_dev_lock:
        expected.pop(-1)

    assert mock_cmdr_wrapped.call_args_list == expected
    assert all(
        [Path(c[0][0]).is_file() for c in mock_cmdr_wrapped.call_args_list])


# FIXME: I tested this locally on a windows machine but breaks on Github
# Actions.
# Problem happens when running pip:
# AssertionError: Egg-link c:\users\runner~1\appdata\local\temp\tmp30cvb5ki
# does not match installed location of sample-package-pip
# (at c:\users\runneradmin\appdata\local\temp\tmp30cvb5ki)
# I think it's because of some weird configuration on github actions
# creates symlinks
@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_pip(tmp_directory):
    _write_sample_pip_req()

    Path('setup.py').write_text(setup_py)
    name = f'venv-{Path(tmp_directory).name}'

    runner = CliRunner()
    result = runner.invoke(install,
                           args='--create-env',
                           catch_exceptions=False)

    if os.name == 'nt':
        expected_command = (
            f'\nIf using cmd.exe: {name}\\Scripts\\activate.bat'
            f'\nIf using PowerShell: {name}\\Scripts\\Activate.ps1')
    else:
        expected_command = f'source {name}/bin/activate'

    assert Path('.gitignore').read_text() == f'\n{name}\n'
    assert expected_command in result.stdout
    assert Path('requirements.lock.txt').exists()
    assert result.exit_code == 0


@pytest.mark.xfail(sys.platform == 'win32',
                   reason='Test not working on Github Actions on Windows')
def test_install_pip_does_not_duplicate_gitignore_entry(tmp_directory):
    _write_sample_pip_req()

    Path('setup.py').write_text(setup_py)
    name = f'venv-{Path(tmp_directory).name}'

    Path('.gitignore').write_text(f'{name}\n')

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    # the entry was already there, should not duplicate
    assert Path('.gitignore').read_text() == f'{name}\n'
    assert result.exit_code == 0


def test_non_package_with_pip(tmp_directory):
    _write_sample_pip_req()

    Path('setup.py').write_text(setup_py)
    name = f'venv-{Path(tmp_directory).name}'

    runner = CliRunner()
    result = runner.invoke(install,
                           args=['--create-env'],
                           catch_exceptions=False)

    assert Path('.gitignore').read_text() == f'\n{name}\n'
    assert Path('requirements.lock.txt').exists()
    assert result.exit_code == 0


def test_non_package_with_pip_with_dev_deps(tmp_directory):
    _write_sample_pip_req()
    _write_sample_pip_req('requirements.dev.txt')

    Path('setup.py').write_text(setup_py)
    name = f'venv-{Path(tmp_directory).name}'

    runner = CliRunner()
    result = runner.invoke(install,
                           args='--create-env',
                           catch_exceptions=False)

    assert Path('.gitignore').read_text() == f'\n{name}\n'
    assert Path('requirements.lock.txt').exists()
    assert Path('requirements.dev.lock.txt').exists()
    assert '# Editable install' not in Path(
        'requirements.lock.txt').read_text()
    assert '# Editable install' not in Path(
        'requirements.dev.lock.txt').read_text()
    assert result.exit_code == 0


@pytest.mark.parametrize('create_setup_py', [True, False])
@pytest.mark.parametrize('create_dev_lock', [True, False])
def test_install_lock_pip(tmp_directory, mock_cmdr_wrapped, create_setup_py,
                          create_dev_lock):
    _write_sample_pip_req('requirements.lock.txt')

    if create_dev_lock:
        _write_sample_pip_req('requirements.dev.lock.txt')

    if create_setup_py:
        Path('setup.py').write_text(setup_py)

    runner = CliRunner()
    result = runner.invoke(install,
                           args=['--use-lock', '--create-env'],
                           catch_exceptions=False)

    venv, pip = _get_venv_and_pip()

    expected = [
        call(sys.executable, '-m', 'venv', venv, description='Creating venv'),
        call(pip, 'install', '--editable', '.', description=ANY),
        call(pip,
             'install',
             '--requirement',
             'requirements.lock.txt',
             description=ANY),
        call(pip,
             'install',
             '--requirement',
             'requirements.dev.lock.txt',
             description=ANY)
    ]

    if not create_setup_py:
        expected.pop(1)

    if not create_dev_lock:
        expected.pop(-1)

    assert mock_cmdr_wrapped.call_args_list == expected
    assert Path('.gitignore').read_text() == f'\n{venv}\n'
    assert result.exit_code == 0


@pytest.mark.parametrize('file', ['requirements.lock.txt', 'requirements.txt'])
def test_suggests_use_pip_if_cmd_fails(tmp_directory, monkeypatch, file):
    # simulate no conda
    monkeypatch.setattr(install_module.shutil, 'which', lambda _: None)
    monkeypatch.setattr(install_module, '_run_pip_commands',
                        Mock(side_effect=Exception('some-error')))

    Path(file).touch()

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert result.exit_code == 1
    assert f'pip install --requirement {file}' in result.output
    assert 'some-error' in result.output


@pytest.mark.parametrize('file', ['environment.yml', 'environment.lock.yml'])
def test_suggests_use_conda_if_cmd_fails(tmp_directory, monkeypatch, file):
    monkeypatch.setattr(install_module, '_run_conda_commands',
                        Mock(side_effect=Exception('some-error')))
    monkeypatch.setattr(install_module, '_current_conda_env_name',
                        lambda: 'current-env')

    Path(file).write_text('name: some-env')

    runner = CliRunner()
    result = runner.invoke(install, catch_exceptions=False)

    assert result.exit_code == 1
    assert (f'conda env update --file {file} --name current-env'
            in result.output)
    assert 'some-error' in result.output


@pytest.mark.parametrize('file', ['environment.yml', 'environment.lock.yml'])
def test_suggests_use_conda_create_if_cmd_fails(tmp_directory, monkeypatch,
                                                file):
    monkeypatch.setattr(install_module, '_run_conda_commands',
                        Mock(side_effect=Exception('some-error')))
    monkeypatch.setattr(install_module, '_current_conda_env_name',
                        lambda: 'current-env')

    Path(file).write_text('name: some-env')

    runner = CliRunner()
    result = runner.invoke(install,
                           args=['--create-env'],
                           catch_exceptions=False)

    assert result.exit_code == 1
    assert f'conda env create --file {file} --force' in result.output
    assert 'some-error' in result.output


empty = """
key: value
"""

no_python = """
dependencies:
 - a
 - b
 - pip:
     - c
"""

with_python = """
dependencies:
 - a
 -  python=3.9
"""


@pytest.mark.parametrize('content, has_python, env_fixed', [
    [empty, False, {
        'key': 'value'
    }],
    [no_python, False, {
        'dependencies': ['a', 'b', {
            'pip': ['c']
        }]
    }],
    [with_python, True, {
        'dependencies': ['a']
    }],
])
def test_conda_install_ignores_python(
    tmp_directory,
    content,
    has_python,
    env_fixed,
):
    Path('environment.yml').write_text(content)
    (has_python_out, env_yml_out
     ) = install_module._environment_yml_has_python('environment.yml')

    assert has_python == has_python_out
    assert env_fixed == env_yml_out


@pytest.mark.parametrize('content, filename, d_to_use', [
    [empty, 'environment.yml', {
        'key': 'value'
    }],
    [
        no_python, 'environment.yml', {
            'dependencies': ['a', 'b', {
                'pip': ['c']
            }]
        }
    ],
    [with_python, '.ploomber-conda-tmp.yml', {
        'dependencies': ['a']
    }],
])
def test_check_environment_yaml(content, filename, d_to_use, tmp_directory):
    Path('environment.yml').write_text(content)
    with install_module.check_environment_yaml('environment.yml') as path:
        assert Path(path).exists()
        assert path == filename

        with open(path) as f:
            d = yaml.safe_load(f)

    assert d_to_use == d
    assert not Path('.ploomber-conda-tmp.yml').exists()


def test_pip_mixed_versions(monkeypatch):
    mock = Mock()
    mock.return_value = """pyflakes==2.4.0\nPygments==2.11.2\n
    pygraphviz @ file:///Users/runner/miniforge3/pygraphviz_1644545996627"""

    monkeypatch.setattr(install_module.Commander, 'run', mock)
    with pytest.warns(UserWarning):
        _pip_install(install_module.Commander, {}, True)
