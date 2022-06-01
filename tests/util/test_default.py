import os
import warnings
from pathlib import Path
from unittest.mock import Mock

import pytest

from ploomber.exceptions import DAGSpecInvalidError
from ploomber.util import util
from ploomber.util import default


def create_package_with_name(name, base='.'):
    Path('setup.py').touch()
    parent = Path(base, 'src', name)
    parent.mkdir(parents=True)
    pkg_location = (parent / 'pipeline.yaml')
    pkg_location.touch()
    return pkg_location


@pytest.fixture
def pkg_location():
    location = create_package_with_name('package_a')
    return str(location)


def test_package_location(tmp_directory, pkg_location):
    assert default._package_location(root_path='.') == str(
        Path('src', 'package_a', 'pipeline.yaml'))


def test_package_location_with_root_path(tmp_directory):
    create_package_with_name('package_a', 'some-dir')
    assert default._package_location(root_path='some-dir') == str(
        Path('some-dir', 'src', 'package_a', 'pipeline.yaml'))


def test_package_location_with_custom_name(tmp_directory):
    create_package_with_name('package_b')
    assert default._package_location(root_path='.') == str(
        Path('src', 'package_b', 'pipeline.yaml'))


def test_no_package_location(tmp_directory):
    assert default._package_location(root_path='.') is None


def test_package_location_warns_if_more_than_one(tmp_directory):
    create_package_with_name('package_a')
    create_package_with_name('package_b')

    with pytest.warns(UserWarning) as record:
        out = default._package_location(root_path='.')

    assert len(record) == 1
    assert 'Found more than one package' in record[0].message.args[0]
    # most return the first one in alphabetical order
    assert out == str(Path('src', 'package_a', 'pipeline.yaml'))


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


@pytest.mark.parametrize('method', ['entry_point', 'entry_point_with_name'])
def test_entry_point_pkg_location(tmp_directory, pkg_location, method):
    assert getattr(default, method)() == str(pkg_location)


@pytest.mark.parametrize('method', ['entry_point', 'entry_point_with_name'])
def test_entry_point_pkg_location_ignore_egg_info(tmp_directory, method):
    Path('setup.py').touch()

    for pkg in ['package_a.egg-info', 'package_b']:
        parent = Path('src', pkg)
        parent.mkdir(parents=True)
        pkg_location = (parent / 'pipeline.yaml')
        pkg_location.touch()

    assert getattr(default,
                   method)() == str(Path('src', 'package_b', 'pipeline.yaml'))


@pytest.mark.parametrize('method', ['entry_point', 'entry_point_with_name'])
def test_entry_point_pkg_location_multiple_pkgs(tmp_directory, method):
    Path('setup.py').touch()

    for pkg in ['package_a', 'package_b']:
        parent = Path('src', pkg)
        parent.mkdir(parents=True)
        pkg_location = (parent / 'pipeline.yaml')
        pkg_location.touch()

    assert getattr(default,
                   method)() == str(Path('src', 'package_a', 'pipeline.yaml'))


@pytest.mark.parametrize('method', ['entry_point', 'entry_point_with_name'])
def test_entry_point_error_if_not_exists(method):
    with pytest.raises(DAGSpecInvalidError):
        getattr(default, method)()


@pytest.mark.parametrize('method', ['entry_point', 'entry_point_with_name'])
def test_entry_point_in_parent_folder(tmp_directory, method):
    Path('dir').mkdir()
    Path('pipeline.yaml').touch()
    os.chdir('dir')
    assert getattr(default, method)() == str(Path('..', 'pipeline.yaml'))


@pytest.mark.parametrize('method', ['entry_point', 'entry_point_with_name'])
def test_entry_point_in_src_while_in_sibling_folder(tmp_directory, method):
    Path('setup.py').touch()
    pkg = Path('src', 'package')
    pkg.mkdir(parents=True)
    (pkg / 'pipeline.yaml').touch()
    Path('tests').mkdir()
    os.chdir('tests')
    assert getattr(default, method)() == str(
        Path('..', 'src', 'package', 'pipeline.yaml'))


@pytest.mark.parametrize('method', ['entry_point', 'entry_point_with_name'])
def test_entry_point_from_root_path(tmp_directory, method):
    Path('dir').mkdir()
    Path('dir', 'pipeline.yaml').touch()

    assert getattr(default, method)(root_path='dir')


def test_entry_point_with_name(tmp_directory):
    Path('pipeline.yaml').touch()
    Path('pipeline.train.yaml').touch()

    assert default.entry_point_with_name(
        name='pipeline.train.yaml') == 'pipeline.train.yaml'


def test_entry_point_with_nameuses_pipeline_yaml_if_src_one_is_missing(
        tmp_directory):
    Path('setup.py').touch()
    Path('pipeline.yaml').touch()

    assert default.entry_point_with_name() == 'pipeline.yaml'


@pytest.mark.parametrize('spec_name, env_name', [
    ['pipeline.yaml', 'env.yaml'],
    ['pipeline.train.yaml', 'env.yaml'],
])
def test_path_to_env_local(tmp_directory, spec_name, env_name):
    Path(env_name).touch()

    Path('dir').mkdir()
    Path('dir', spec_name).touch()

    assert default.path_to_env_from_spec(Path('dir', spec_name)) == str(
        Path(env_name).resolve())


def test_path_to_env_loads_file_with_same_name(tmp_directory):
    Path('env.train.yaml').touch()

    Path('dir').mkdir()
    Path('dir', 'pipeline.train.yaml').touch()

    assert default.path_to_env_from_spec(
        Path('dir',
             'pipeline.train.yaml')) == str(Path('env.train.yaml').resolve())


def test_path_to_env_prefers_file_wih_name_over_plain_env_yaml(tmp_directory):
    Path('env.train.yaml').touch()
    Path('env.yaml').touch()

    Path('dir').mkdir()
    Path('dir', 'pipeline.train.yaml').touch()

    assert default.path_to_env_from_spec(
        Path('dir',
             'pipeline.train.yaml')) == str(Path('env.train.yaml').resolve())


def test_path_to_env_prefers_env_variable(tmp_directory, monkeypatch):
    monkeypatch.setenv('PLOOMBER_ENV_FILENAME', 'env.local.yaml')

    Path('env.local.yaml').touch()
    Path('env.train.yaml').touch()
    Path('env.yaml').touch()

    Path('dir').mkdir()
    Path('dir', 'pipeline.train.yaml').touch()

    assert default.path_to_env_from_spec(
        Path('dir',
             'pipeline.train.yaml')) == str(Path('env.local.yaml').resolve())


def test_error_if_env_var_has_directories(monkeypatch):
    monkeypatch.setenv('PLOOMBER_ENV_FILENAME', 'path/to/env.local.yaml')

    with pytest.raises(ValueError):
        default.path_to_env_from_spec('pipeline.yaml')


def test_error_if_env_var_file_missing(monkeypatch):
    monkeypatch.setenv('PLOOMBER_ENV_FILENAME', 'env.local.yaml')

    with pytest.raises(FileNotFoundError):
        default.path_to_env_from_spec('pipeline.yaml')


def test_path_to_parent_sibling(tmp_directory):
    Path('dir').mkdir()
    Path('dir', 'env.yaml').touch()

    assert default.path_to_env_from_spec('dir/pipeline.yaml') == str(
        Path('dir', 'env.yaml').resolve())


@pytest.mark.parametrize('arg', ['dir/pipeline.yaml', None])
def test_path_to_env_none(tmp_directory, arg):
    Path('dir').mkdir()

    assert default.path_to_env_from_spec(arg) is None


def test_path_to_env_error_if_no_extension():
    with pytest.raises(ValueError) as excinfo:
        default.path_to_env_from_spec('pipeline')

    expected = ("Expected path to spec to have a file extension "
                "but got: 'pipeline'")
    assert str(excinfo.value) == expected


def test_path_to_env_error_if_dir(tmp_directory):
    Path('pipeline.yaml').mkdir()

    with pytest.raises(ValueError) as excinfo:
        default.path_to_env_from_spec('pipeline.yaml')

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


def test_finds_pipeline_with_name(tmp_directory):
    expected = Path(tmp_directory).resolve()
    pip = Path('pipeline.serve.yaml').resolve()
    pip.touch()

    dir_ = Path('path', 'to', 'dir')
    dir_.mkdir(parents=True)
    os.chdir(dir_)

    assert expected == default.find_root_recursively(
        filename='pipeline.serve.yaml').resolve()


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


@pytest.mark.parametrize('return_data', [[(None, None),
                                          (default._filesystem_root(), None)],
                                         [(default._filesystem_root(), 3),
                                          (default._filesystem_root(), 1)]])
def test_pipeline_yaml_in_root_directory(return_data, monkeypatch):
    mock = Mock(side_effect=return_data)
    monkeypatch.setattr(default, 'find_parent_of_file_recursively', mock)
    with pytest.raises(DAGSpecInvalidError) as excinfo:
        default.find_root_recursively()

    assert ('pipeline.yaml cannot be in the filesystem root. '
            'Please add it inside a directory like '
            'project-name/pipeline.yaml. ') in str(excinfo.value)
    assert mock.call_count == len(return_data)


@pytest.mark.parametrize('return_data', [[(None, None),
                                          (default._filesystem_root(), None)],
                                         [(default._filesystem_root(), 3),
                                          (default._filesystem_root(), 1)]])
def test_pipeline_yaml_with_name_in_root_directory(return_data, monkeypatch):
    mock = Mock(side_effect=return_data)
    monkeypatch.setattr(default, 'find_parent_of_file_recursively', mock)
    with pytest.raises(DAGSpecInvalidError) as excinfo:
        default.find_root_recursively(filename='pipeline.serve.yaml')

    assert ('pipeline.serve.yaml cannot be in the filesystem root. '
            'Please add it inside a directory like '
            'project-name/pipeline.serve.yaml. ') in str(excinfo.value)
    assert mock.call_count == len(return_data)


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


def test_find_root_recursively_error_pipeline_yaml_under_src_but_no_setup_py(
        tmp_directory):
    parent = Path('src', 'package')
    parent.mkdir(parents=True)

    (parent / 'pipeline.yaml').touch()

    os.chdir(parent)

    with pytest.raises(DAGSpecInvalidError) as excinfo:
        default.find_root_recursively()

    assert 'Invalid project layout' in str(excinfo.value)
    assert str(Path('src', 'package')) in str(excinfo.value)


@pytest.mark.parametrize('root_location, working_dir, to_create, filename', [
    ['some/dir/pipeline.yaml', 'some/dir', 'some/pipeline.yaml', None],
    [
        'some/dir/pipeline.x.yaml', 'some/dir', 'some/pipeline.x.yaml',
        'pipeline.x.yaml'
    ],
    [
        'some/dir/another/dir/pipeline.yaml', 'some/dir/another/dir',
        'some/pipeline.yaml', None
    ],
])
def test_warns_if_other_pipeline_yaml_as_parents_of_root_path(
        tmp_directory, root_location, working_dir, to_create, filename):
    Path(root_location).parent.mkdir(parents=True, exist_ok=True)
    pip = Path(root_location).resolve()
    pip.touch()

    to_create = Path(to_create)
    to_create.parent.mkdir(parents=True, exist_ok=True)
    to_create.touch()

    os.chdir(working_dir)

    with pytest.warns(UserWarning) as record:
        default.find_root_recursively(filename=filename)

    assert len(record) == 1
    assert 'Found project root with filename' in record[0].message.args[0]


def test_doesnt_warn_if_pipeline_yaml_in_the_same_directory(tmp_directory):
    Path('pipeline.yaml').touch()
    Path('pipeline.serve.yaml').touch()

    with pytest.warns(None) as record:
        default.find_root_recursively()

    assert not len(record)


def test_doesnt_warn_if_there_arent_nested_pipeline_yaml(tmp_directory):
    p = Path('path', 'to', 'dir', 'pipeline.yaml')
    p.parent.mkdir(parents=True)
    p.touch()

    os.chdir(p.parent)

    with pytest.warns(None) as record:
        default.find_root_recursively()

    assert not len(record)


def test_returns_spec_location_if_setup_py_is_in_a_parent_folder(
        tmp_directory):
    p = Path('path', 'to', 'dir', 'pipeline.yaml')
    p.parent.mkdir(parents=True)
    p = p.resolve()
    p.touch()

    Path('setup.py').touch()

    os.chdir(p.parent)

    assert Path(
        default.find_root_recursively()).resolve() == p.parent.resolve()


def test_error_if_filename_contains_directories():
    with pytest.raises(ValueError) as excinfo:
        default.find_root_recursively(filename='a/b')

    assert "'a/b' should be a filename" in str(excinfo.value)


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


def test_error_if_no_project_root(tmp_directory):
    Path('setup.py').touch()

    with pytest.raises(DAGSpecInvalidError) as excinfo:
        default.find_package_name()

    expected = "Failed to determine project root"
    assert expected in str(excinfo.value)


def test_error_if_no_package(tmp_directory):
    Path('pipeline.yaml').touch()

    with pytest.raises(ValueError) as excinfo:
        default.find_package_name()

    expected = "Could not find a valid package."
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


# Check no warning when an empty string is passed
def test_empty_reqs_mixed_envs():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        util.check_mixed_envs("")
        util.check_mixed_envs("nlnlyo2h3fnoun29hf2nu39ub")  # No \n in str


def test_config_ignored_if_missing_ploomber_section(tmp_directory):
    Path('setup.cfg').write_text("""
[some-tool]
x = 1
""")

    Path('pipeline.yaml').touch()

    assert default.entry_point() == 'pipeline.yaml'


@pytest.mark.parametrize('entry_point', [
    'pipeline.another.yaml',
    'dir/pipeline.yaml',
    'dir\\pipeline.yaml',
])
def test_entry_point_from_config_file(tmp_directory, entry_point):
    Path('setup.cfg').write_text(f"""
[ploomber]
entry-point = {entry_point}
""")

    Path(entry_point).parent.mkdir(exist_ok=True)
    Path(entry_point).touch()
    Path('pipeline.yaml').touch()

    assert Path(default.entry_point()).resolve() == Path(entry_point).resolve()


def test_entry_point_from_config_file_nested(tmp_directory):
    Path('dir').mkdir()
    Path('dir', 'setup.cfg').write_text("""
[ploomber]
entry-point = pipeline.another.yaml
""")

    Path('dir', 'pipeline.another.yaml').touch()
    Path('pipeline.yaml').touch()

    assert Path(default.entry_point(root_path='dir')).resolve() == Path(
        'dir', 'pipeline.another.yaml').resolve()
