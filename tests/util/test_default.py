import os
from pathlib import Path

import pytest

from ploomber.util import default


@pytest.fixture
def pkg_location():
    parent = Path('src', 'package_a')
    parent.mkdir(parents=True)
    pkg_location = (parent / 'pipeline.yaml')
    pkg_location.touch()
    return str(pkg_location)


def test_entry_point_env_var(monkeypatch, tmp_directory, pkg_location):
    monkeypatch.setenv('ENTRY_POINT', 'some.entry.point')
    assert default.entry_point() == 'some.entry.point'


def test_entry_point_pkg_location(tmp_directory, pkg_location):
    assert default.entry_point() == str(pkg_location)


def test_entry_point_pkg_location_and_yaml(tmp_directory, pkg_location):
    Path('pipeline.yaml').touch()
    assert default.entry_point() == 'pipeline.yaml'


def test_entry_point_pkg_location_ignore_egg_info(tmp_directory):
    for pkg in ['package_a.egg-info', 'package_b']:
        parent = Path('src', pkg)
        parent.mkdir(parents=True)
        pkg_location = (parent / 'pipeline.yaml')
        pkg_location.touch()

    assert default.entry_point() == str(
        Path('src', 'package_b', 'pipeline.yaml'))


def test_entry_point_pkg_location_multiple_pkgs(tmp_directory):
    for pkg in ['package_a', 'package_b']:
        parent = Path('src', pkg)
        parent.mkdir(parents=True)
        pkg_location = (parent / 'pipeline.yaml')
        pkg_location.touch()

    assert default.entry_point() == str(
        Path('src', 'package_a', 'pipeline.yaml'))


def test_entry_point():
    assert default.entry_point() == 'pipeline.yaml'


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


def test_path_to_env_local(tmp_directory):
    Path('env.yaml').touch()

    Path('dir').mkdir()
    Path('dir', 'env.yaml').touch()

    assert default.path_to_env('dir') == str(Path('env.yaml').resolve())


def test_path_to_parent_sibling(tmp_directory):
    Path('dir').mkdir()
    Path('dir', 'env.yaml').touch()

    assert default.path_to_env('dir') == str(Path('dir', 'env.yaml').resolve())


def test_path_to_env_none(tmp_directory):
    Path('dir').mkdir()

    assert default.path_to_env('dir') is None
