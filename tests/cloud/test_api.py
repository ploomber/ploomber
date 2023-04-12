import os
from unittest.mock import Mock
import zipfile
from pathlib import Path

import click
import pytest
from pydantic import ValidationError

from ploomber.cloud.api import PloomberCloudAPI
from ploomber.cloud.api import zip_project
from ploomber.cloud import api
from ploomber_core.exceptions import BaseException


@pytest.fixture
def sample_project():
    Path("a").touch()
    Path("b").mkdir()
    Path("b", "b1").touch()
    Path("c", "c1").mkdir(parents=True)
    Path("c", "c1", "c2").touch()


@pytest.fixture
def tmp_project(monkeypatch, tmp_nbs):
    monkeypatch.setenv("PLOOMBER_CLOUD_KEY", "some-key")


def test_zip_project(tmp_directory, sample_project):
    zip_project(force=False, runid="runid", github_number="number", verbose=False)

    with zipfile.ZipFile("project.zip") as zip:
        files = zip.namelist()

    assert set(files) == {
        "a",
        "c/",
        "b/",
        "c/c1/",
        "c/c1/c2",
        "b/b1",
        ".ploomber-cloud",
    }


def test_zip_project_with_base_dir(tmp_directory, sample_project):
    Path("c", "c1", "nested").mkdir()
    Path("c", "c1", "nested", "file").touch()

    zip_project(
        force=False,
        runid="runid",
        github_number="number",
        verbose=False,
        base_dir="c/c1",
    )

    with zipfile.ZipFile("c/c1/project.zip") as zip:
        files = zip.namelist()

    assert set(files) == {"c2", ".ploomber-cloud", "nested/", "nested/file"}


@pytest.mark.parametrize(
    "ignore_prefixes, base_dir, expected",
    [
        [
            ["a"],
            "",
            {
                "b/",
                "b/b1",
                "c/",
                "c/c1/",
                "c/c1/c2",
                ".ploomber-cloud",
            },
        ],
        [
            ["a", "b"],
            "",
            {
                "c/",
                "c/c1/",
                "c/c1/c2",
                ".ploomber-cloud",
            },
        ],
        [
            None,
            "c",
            {
                "c1/",
                "c1/c2",
                ".ploomber-cloud",
            },
        ],
        [
            ["c1"],
            "c",
            {
                ".ploomber-cloud",
            },
        ],
    ],
)
def test_zip_project_ignore_prefixes(
    tmp_directory, sample_project, ignore_prefixes, base_dir, expected
):
    zip_project(
        force=False,
        runid="runid",
        github_number="number",
        verbose=False,
        ignore_prefixes=ignore_prefixes,
        base_dir=base_dir,
    )

    with zipfile.ZipFile(Path(base_dir, "project.zip")) as zip:
        files = zip.namelist()

    assert set(files) == expected


@pytest.mark.skip(reason="no way of currently testing this")
def test_runs_new():
    PloomberCloudAPI().runs_new(metadata=dict(a=1))


def test_upload_project_errors_if_missing_reqs_lock_txt(tmp_project):
    with pytest.raises(BaseException) as excinfo:
        PloomberCloudAPI().build()

    assert "requirements.lock.txt" in str(excinfo.value)
    assert "environment.lock.yml" in str(excinfo.value)


def test_upload_project_errors_if_invalid_cloud_yaml(tmp_project):
    Path("requirements.lock.txt").touch()
    Path("cloud.yaml").write_text(
        """
key: value
"""
    )

    with pytest.raises(ValidationError):
        PloomberCloudAPI().build()


def test_upload_project_ignores_product_prefixes(monkeypatch, tmp_nbs):
    monkeypatch.setenv("PLOOMBER_CLOUD_KEY", "some-key")
    monkeypatch.setattr(PloomberCloudAPI, "runs_new", Mock(return_value="runid"))
    monkeypatch.setattr(PloomberCloudAPI, "get_presigned_link", Mock())
    monkeypatch.setattr(api, "upload_zipped_project", Mock())
    monkeypatch.setattr(PloomberCloudAPI, "trigger", Mock())
    Path("requirements.lock.txt").touch()

    Path("output").mkdir()
    Path("output", "should-not-appear").touch()

    PloomberCloudAPI().build()

    with zipfile.ZipFile("project.zip") as zip:
        files = zip.namelist()

    assert "output/should-not-appear" not in files


def test_run_detailed_print_finish_no_task(monkeypatch, capsys):
    def mock_return(self, runid):
        return {"run": {"status": "finished"}, "tasks": []}

    monkeypatch.setattr(PloomberCloudAPI, "run_detail", mock_return)
    PloomberCloudAPI().run_detail_print("some-key")
    captured = capsys.readouterr()

    assert "Pipeline finished. Check outputs:" in captured.out
    assert (
        captured.out.splitlines()[0] == "Pipeline finished due "
        "to no newly triggered tasks, try running ploomber cloud build --force"
    )


def test_run_detailed_print_finish_with_tasks(monkeypatch, capsys):
    def mock_return(self, runid):
        return {
            "run": {"status": "finished"},
            "tasks": [
                {
                    "taskid": "mock-id",
                    "name": "mock",
                    "runid": "some-key",
                    "status": "finished",
                }
            ],
        }

    monkeypatch.setattr(PloomberCloudAPI, "run_detail", mock_return)
    PloomberCloudAPI().run_detail_print("some-key")
    captured = capsys.readouterr()

    assert "Pipeline finished. Check outputs:" in captured.out
    assert "taskid" in captured.out.splitlines()[0]
    assert "name" in captured.out.splitlines()[0]
    assert "runid" in captured.out.splitlines()[0]
    assert "status" in captured.out.splitlines()[0]


def test_run_detailed_print_abort(monkeypatch, capsys):
    def mock_return(self, runid):
        return {
            "run": {"status": "aborted"},
            "tasks": [
                {
                    "taskid": "mock-id",
                    "name": "mock",
                    "runid": "some-key",
                    "status": "aborted",
                }
            ],
        }

    monkeypatch.setattr(PloomberCloudAPI, "run_detail", mock_return)
    PloomberCloudAPI().run_detail_print("some-key")
    captured = capsys.readouterr()

    assert captured.out.splitlines()[0] == "Pipeline aborted..."
    assert "taskid" in captured.out.splitlines()[1]
    assert "name" in captured.out.splitlines()[1]
    assert "runid" in captured.out.splitlines()[1]
    assert "status" in captured.out.splitlines()[1]


def test_run_detailed_print_fail(monkeypatch, capsys):
    def mock_return(self, runid):
        return {
            "run": {"status": "failed"},
            "tasks": [
                {
                    "taskid": "mock-id",
                    "name": "mock",
                    "runid": "some-key",
                    "status": "failed",
                }
            ],
        }

    monkeypatch.setattr(PloomberCloudAPI, "run_detail", mock_return)

    with pytest.raises(click.ClickException) as excinfo:
        PloomberCloudAPI().run_detail_print("some-key")

    captured = capsys.readouterr()

    assert "Pipeline failed." == str(excinfo.value)
    assert "taskid" in captured.out.splitlines()[0]
    assert "name" in captured.out.splitlines()[0]
    assert "runid" in captured.out.splitlines()[0]
    assert "status" in captured.out.splitlines()[0]


@pytest.fixture
def tmp_directory_with_name(tmp_directory):
    path = Path(tmp_directory, "files")
    path.mkdir()
    os.chdir("files")
    yield path
    os.chdir("..")


def create_all(paths):
    for path in paths:
        path = Path(path)
        path.parent.mkdir(exist_ok=True, parents=True)
        path.touch()


@pytest.mark.parametrize(
    "base_dir, paths, expected",
    [
        ["", ["something.txt", "another.txt"], ["something.txt", "another.txt"]],
        [".", ["something.txt", "another.txt"], ["something.txt", "another.txt"]],
        ["dir", [str(Path("dir/something.txt"))], [str(Path("dir/something.txt"))]],
        [
            "dir",
            [str(Path("dir/nested/something.txt"))],
            [str(Path("dir/nested/something.txt"))],
        ],
        [
            "dir",
            [str(Path("../files/dir/something.txt"))],
            [str(Path("dir/something.txt"))],
        ],
        [
            "dir",
            [str(Path("../files/dir/../dir/something.txt"))],
            [str(Path("dir/something.txt"))],
        ],
    ],
)
def test_get_files_from_include_section(
    tmp_directory_with_name, base_dir, paths, expected
):
    create_all(paths)
    assert api.get_files_from_include_section(base_dir, paths) == expected


def test_get_files_from_include_section_absolute(tmp_directory_with_name):
    Path("dir").mkdir()
    Path("dir", "something.txt").touch()
    assert api.get_files_from_include_section(
        "dir", [str(Path("dir", "something.txt").resolve())]
    ) == [str(Path("dir/something.txt"))]


def test_get_files_from_include_section_directory(tmp_directory_with_name):
    for f in ("x", "y", "z"):
        for base in ("a", "b", "a/b"):
            Path(base).mkdir(exist_ok=True)
            Path(base, f).touch()

    expected = {
        str(Path("a/x")),
        str(Path("a/y")),
        str(Path("a/z")),
        str(Path("b/x")),
        str(Path("b/y")),
        str(Path("b/z")),
        str(Path("a/b")),
        str(Path("a/b/x")),
        str(Path("a/b/y")),
        str(Path("a/b/z")),
    }
    assert set(api.get_files_from_include_section(".", ["a", "b"])) == expected


@pytest.mark.parametrize(
    "base_dir, paths, expected_paths",
    [
        ["dir", ["../another/something.txt", "a.txt"], ["../another/something.txt"]],
        [
            "dir",
            ["../../stuff/something.txt", "../dir/x.txt"],
            ["../../stuff/something.txt"],
        ],
        ["dir", ["/path/to/things.txt"], ["/path/to/things.txt"]],
    ],
    ids=[
        "parent",
        "grand-parent",
        "absolute",
    ],
)
def test_get_files_from_include_section_error(
    tmp_directory, base_dir, paths, expected_paths
):
    with pytest.raises(BaseException) as excinfo:
        api.get_files_from_include_section(base_dir, paths)

    for expected in expected_paths:
        assert expected in str(excinfo.value)


def test_get_files_from_include_section_error_if_missing(tmp_directory):
    Path("a").touch()

    with pytest.raises(BaseException) as excinfo:
        api.get_files_from_include_section(".", ["b", "c"])

    assert "- b" in str(excinfo.value)
    assert "- c" in str(excinfo.value)


@pytest.mark.parametrize(
    "paths, expected",
    [
        [["a"], {"a"}],
        [
            ["a", "dir"],
            {
                "a",
                str(Path("dir/a")),
                str(Path("dir/b")),
                str(Path("dir/c")),
            },
        ],
        [
            [str(Path("some/nested/dir"))],
            {
                str(Path("some/nested/dir/a")),
                str(Path("some/nested/dir/b")),
                str(Path("some/nested/dir/c")),
            },
        ],
        [
            ["some"],
            {
                str(Path("some/nested/dir/a")),
                str(Path("some/nested/dir/b")),
                str(Path("some/nested/dir/c")),
                str(Path("some/nested/a")),
                str(Path("some/nested/b")),
                str(Path("some/nested/c")),
                str(Path("some/a")),
                str(Path("some/b")),
                str(Path("some/c")),
                str(Path("some/nested")),
                str(Path("some/nested/dir")),
            },
        ],
    ],
    ids=[
        "single-file",
        "directory",
        "nested",
        "nested-multiple",
    ],
)
def test_glob_from_paths(tmp_directory, paths, expected):
    Path("a").touch()

    for f in ("a", "b", "c"):
        for base in ("dir", "some", "some/nested", "some/nested/dir"):
            Path(base).mkdir(parents=True, exist_ok=True)
            Path(base, f).touch()

    assert set(api.glob_from_paths(paths)) == expected


@pytest.mark.parametrize(
    "name, expected",
    [
        ["notebook-12345678-", "notebook-12345678"],
        ["nb-abcdfegf-", "nb-abcdfegf"],
    ],
)
def test_is_notebook_project(name, expected):
    assert api.is_notebook_project(name) == expected
