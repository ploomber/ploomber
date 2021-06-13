import os
from pathlib import Path

import pytest

from ploomber.util import default
from ploomber.exceptions import DAGSpecInvalidError


@pytest.fixture
def pkg_location():
    Path('setup.py').touch()
    parent = Path('src', 'package_a')
    parent.mkdir(parents=True)
    pkg_location = (parent / 'pipeline.yaml')
    pkg_location.touch()
    return str(pkg_location)


def test_entry_point_env_var(monkeypatch, tmp_directory):
    Path('pipeline.yaml').touch()
    Path('pipeline-custom.yaml').touch()
    monkeypatch.setenv('ENTRY_POINT', 'pipeline-custom.yaml')
    assert default.entry_point() == 'pipeline-custom.yaml'


def test_entry_point_env_var_in_pkg(monkeypatch, tmp_directory, pkg_location):
    Path('src', 'package_a', 'pipeline-custom.yaml').touch()
    monkeypatch.setenv('ENTRY_POINT', 'pipeline-custom.yaml')
    assert default.entry_point() == str(
        Path('src', 'package_a', 'pipeline-custom.yaml'))


def test_error_if_env_var_contains_directories(monkeypatch):
    monkeypatch.setenv('ENTRY_POINT', 'path/to/pipeline.yaml')

    with pytest.raises(ValueError) as excinfo:
        default.entry_point()

    assert 'must be a filename' in str(excinfo.value)


def test_entry_point_pkg_location(tmp_directory, pkg_location):
    assert default.entry_point() == str(pkg_location)


def test_entry_point_pkg_location_ignore_egg_info(tmp_directory):
    Path('setup.py').touch()

    for pkg in ['package_a.egg-info', 'package_b']:
        parent = Path('src', pkg)
        parent.mkdir(parents=True)
        pkg_location = (parent / 'pipeline.yaml')
        pkg_location.touch()

    assert default.entry_point() == str(
        Path('src', 'package_b', 'pipeline.yaml'))


def test_entry_point_pkg_location_multiple_pkgs(tmp_directory):
    Path('setup.py').touch()

    for pkg in ['package_a', 'package_b']:
        parent = Path('src', pkg)
        parent.mkdir(parents=True)
        pkg_location = (parent / 'pipeline.yaml')
        pkg_location.touch()

    assert default.entry_point() == str(
        Path('src', 'package_a', 'pipeline.yaml'))


def test_entry_point_error_if_not_exists():
    with pytest.raises(DAGSpecInvalidError):
        default.entry_point()


def test_entry_point_in_parent_folder(tmp_directory):
    Path('dir').mkdir()
    Path('pipeline.yaml').touch()
    os.chdir('dir')
    assert default.entry_point() == str(Path('..', 'pipeline.yaml'))


def test_entry_point_in_src_while_in_sibling_folder(tmp_directory):
    Path('setup.py').touch()
    pkg = Path('src', 'package')
    pkg.mkdir(parents=True)
    (pkg / 'pipeline.yaml').touch()
    Path('tests').mkdir()
    os.chdir('tests')
    assert default.entry_point() == str(
        Path('..', 'src', 'package', 'pipeline.yaml'))


@pytest.mark.parametrize('spec_name, env_name', [
    ['pipeline.yaml', 'env.yaml'],
    ['pipeline.train.yaml', 'env.yaml'],
])
def test_path_to_env_local(tmp_directory, spec_name, env_name):
    Path(env_name).touch()

    Path('dir').mkdir()
    Path('dir', spec_name).touch()

    assert default.path_to_env(Path('dir', spec_name)) == str(
        Path(env_name).resolve())


def test_path_to_env_loads_file_with_same_name(tmp_directory):
    Path('env.train.yaml').touch()

    Path('dir').mkdir()
    Path('dir', 'pipeline.train.yaml').touch()

    assert default.path_to_env(Path('dir', 'pipeline.train.yaml')) == str(
        Path('env.train.yaml').resolve())


def test_path_to_env_prefers_file_wih_name_over_plain_env_yaml(tmp_directory):
    Path('env.train.yaml').touch()
    Path('env.yaml').touch()

    Path('dir').mkdir()
    Path('dir', 'pipeline.train.yaml').touch()

    assert default.path_to_env(Path('dir', 'pipeline.train.yaml')) == str(
        Path('env.train.yaml').resolve())


def test_path_to_env_prefers_env_variable(tmp_directory, monkeypatch):
    monkeypatch.setenv('PLOOMBER_ENV_FILENAME', 'env.local.yaml')

    Path('env.local.yaml').touch()
    Path('env.train.yaml').touch()
    Path('env.yaml').touch()

    Path('dir').mkdir()
    Path('dir', 'pipeline.train.yaml').touch()

    assert default.path_to_env(Path('dir', 'pipeline.train.yaml')) == str(
        Path('env.local.yaml').resolve())


def test_error_if_env_var_has_directories(monkeypatch):
    monkeypatch.setenv('PLOOMBER_ENV_FILENAME', 'path/to/env.local.yaml')

    with pytest.raises(ValueError):
        default.path_to_env('pipeline.yaml')


def test_error_if_env_var_file_missing(monkeypatch):
    monkeypatch.setenv('PLOOMBER_ENV_FILENAME', 'env.local.yaml')

    with pytest.raises(FileNotFoundError):
        default.path_to_env('pipeline.yaml')


def test_path_to_parent_sibling(tmp_directory):
    Path('dir').mkdir()
    Path('dir', 'env.yaml').touch()

    assert default.path_to_env('dir/pipeline.yaml') == str(
        Path('dir', 'env.yaml').resolve())


@pytest.mark.parametrize('arg', ['dir/pipeline.yaml', None])
def test_path_to_env_none(tmp_directory, arg):
    Path('dir').mkdir()

    assert default.path_to_env(arg) is None


def test_path_to_env_error_if_no_extension():
    with pytest.raises(ValueError) as excinfo:
        default.path_to_env('pipeline')

    expected = "Expected path to spec to have an extension but got: 'pipeline'"
    assert str(excinfo.value) == expected


def test_path_to_env_error_if_dir(tmp_directory):
    Path('pipeline.yaml').mkdir()

    with pytest.raises(ValueError) as excinfo:
        default.path_to_env('pipeline.yaml')

    expected = ("Expected path to spec 'pipeline.yaml' to be a file "
                "but got a directory instead")
    assert str(excinfo.value) == expected


def test_finds_pipeline_yaml(tmp_directory):
    expected = Path(tmp_directory).resolve()
    pip = Path('pipeline.yaml').resolve()
    pip.touch()

    dir_ = Path('path', 'to', 'dir')
    dir_.mkdir(parents=True)
    os.chdir(dir_)

    assert expected == default.find_root_recursively().resolve()


def test_finds_setup_py(tmp_directory):
    expected = Path(tmp_directory).resolve()
    pip = Path('setup.py').resolve()
    pip.touch()

    Path('src', 'package').mkdir(parents=True)
    Path('src', 'package', 'pipeline.yaml').touch()

    dir_ = Path('path', 'to', 'dir')
    dir_.mkdir(parents=True)
    os.chdir(dir_)

    assert expected == default.find_root_recursively().resolve()


def test_ignores_src_package_pipeline_if_setup_py(tmp_directory):
    expected = Path(tmp_directory).resolve()
    pip = Path('setup.py').resolve()
    pip.touch()

    dir_ = Path('src', 'package')
    dir_.mkdir(parents=True)
    os.chdir(dir_)
    Path('pipeline.yaml').touch()

    assert expected == default.find_root_recursively().resolve()


def test_error_if_no_pipeline_yaml_and_no_setup_py(tmp_directory):
    with pytest.raises(DAGSpecInvalidError) as excinfo:
        default.find_root_recursively()

    assert ('Looked recursively for a setup.py or '
            'pipeline.yaml in parent folders') in str(excinfo.value)


def test_error_if_setup_py_but_no_src_package_pipeline(tmp_directory):
    pip = Path('setup.py').resolve()
    pip.touch()

    dir_ = Path('path', 'to', 'dir')
    dir_.mkdir(parents=True)
    os.chdir(dir_)

    with pytest.raises(DAGSpecInvalidError) as excinfo:
        default.find_root_recursively()

    assert 'expected to find a pipeline.yaml file' in str(excinfo.value)


def test_error_if_setup_py_and_pipeline_are_siblings(tmp_directory):
    pip = Path('setup.py').resolve()
    Path('pipeline.yaml').touch()
    pip.touch()

    dir_ = Path('path', 'to', 'dir')
    dir_.mkdir(parents=True)
    os.chdir(dir_)

    with pytest.raises(DAGSpecInvalidError) as excinfo:
        default.find_root_recursively()

    assert 'Move the pipeline.yaml' in str(excinfo.value)


def test_error_if_both_setup_py_and_pipeline_yaml_exist(tmp_directory):
    Path('setup.py').touch()
    Path('pipeline.yaml').touch()

    Path('src', 'package').mkdir(parents=True)
    Path('src', 'package', 'pipeline.yaml').touch()

    dir_ = Path('path', 'to', 'dir')
    dir_.mkdir(parents=True)
    os.chdir(dir_)

    with pytest.raises(DAGSpecInvalidError):
        default.find_root_recursively()


@pytest.mark.parametrize('filenames', [
    ['path/pipeline.yaml'],
    ['path/to/pipeline.yaml'],
    ['path/to/pipeline.train.yaml'],
    ['path/pipeline.yaml', 'path/pipeline.train.yaml'],
    ['path/pipeline.yaml', 'another/pipeline.train.yaml'],
])
def test_warns_if_other_pipeline_yaml_as_children_of_root_path(
        tmp_directory, filenames):
    pip = Path('pipeline.yaml').resolve()
    pip.touch()

    for filename in filenames:
        filename = Path(filename)
        filename.parent.mkdir(parents=True, exist_ok=True)
        filename.touch()

    # try switching this off
    dir_ = Path('some', 'path')
    dir_.mkdir(parents=True)
    os.chdir(dir_)

    # try in the same location and check not warn
    # do not warn if setup.py and src/*/pipeline.yaml

    with pytest.warns(UserWarning) as record:
        default.find_root_recursively()

    assert len(record) == 1
    assert 'Found other pipeline files' in record[0].message.args[0]
    assert all([str(Path(f)) in record[0].message.args[0] for f in filenames])


@pytest.mark.parametrize('to_create, to_move', [
    [
        ['setup.py', 'src/my_package/pipeline.yaml'],
        '.',
    ],
])
def test_find_package_name(tmp_directory, to_create, to_move):
    for f in to_create:

        Path(f).parent.mkdir(exist_ok=True, parents=True)

        if f.endswith('/'):
            Path(f).mkdir()
        else:
            Path(f).touch()

    os.chdir(to_move)

    assert default.find_package_name() == 'my_package'


def test_error_if_no_package(tmp_directory):
    Path('setup.py').touch()

    with pytest.raises(DAGSpecInvalidError) as excinfo:
        default.find_package_name()

    expected = "Failed to determine project root"
    assert expected in str(excinfo.value)


@pytest.mark.parametrize('filename, name', [
    ['pipeline.yaml', None],
    ['pipeline.serve.yaml', 'serve'],
    [Path('src', 'my_pkg', 'pipeline.yaml'), None],
    [Path('src', 'my_pkg', 'pipeline.serve.yaml'), 'serve'],
])
def test_entry_point_relative(tmp_directory, filename, name):
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    Path(filename).touch()

    assert default.entry_point_relative(name=name) == str(filename)


def test_entry_point_relative_error_if_both_exist(tmp_directory):
    Path('pipeline.yaml').touch()
    dir_ = Path('src', 'some_pkg')
    dir_.mkdir(parents=True)
    (dir_ / 'pipeline.yaml').touch()

    with pytest.raises(DAGSpecInvalidError):
        default.entry_point_relative()


def test_entry_point_relative_error_if_doesnt_exist(tmp_directory):
    with pytest.raises(DAGSpecInvalidError):
        default.entry_point_relative()


@pytest.mark.parametrize('arg, expected', [
    ['env.x.yaml', 'x'],
    ['env.x.y.yaml', 'x'],
    ['env.yaml', None],
    ['env', None],
])
def test_extract_name(arg, expected):
    assert default.extract_name(arg) == expected
