import pytest

from ploomber.entrypoint import (
    EntryPoint,
    try_to_find_entry_point_type,
    find_entry_point_type,
)


@pytest.mark.parametrize(
    "value, type_",
    [
        ["*.py", EntryPoint.Pattern],
        ["*", EntryPoint.Pattern],
        ["pkg::pipeline.yaml", EntryPoint.ModulePath],
        ["pkg::pipeline.train.yaml", EntryPoint.ModulePath],
        ["dotted.path", EntryPoint.DottedPath],
        ["another.dotted.path", EntryPoint.DottedPath],
    ],
)
def test_entry_point_type(value, type_):
    assert EntryPoint(value).type == type_


def test_entry_point_module_path(monkeypatch):
    e = EntryPoint("test_pkg::pipeline.yaml")

    assert e.type == "module-path"
    assert not e.is_dir()
    assert e.suffix == ".yaml"


def test_dotted_path_that_ends_with_yaml():
    with pytest.raises(ValueError) as excinfo:
        EntryPoint("some.dotted.path.yaml").type

    assert "Could not determine the entry point type" in str(excinfo.value)


def test_try_to_find_entry_point_type_with_none():
    assert try_to_find_entry_point_type(None) is None


@pytest.mark.parametrize(
    "filename",
    [
        "some/path/file.yaml",
        "another.yaml",
        "more.yml",
    ],
)
def test_find_entry_point_type_with_yaml(filename):
    with pytest.raises(ValueError) as excinfo:
        find_entry_point_type(filename)

    expected = (
        "Could not determine the entry point type from value: "
        f"{filename!r}. The file does not exist."
    )
    assert str(excinfo.value) == expected


@pytest.mark.parametrize(
    "arg",
    [
        "some-directory",
        "some/other/directory",
    ],
)
def test_find_entry_point_type_not_a_yaml(arg):
    with pytest.raises(ValueError) as excinfo:
        find_entry_point_type(arg)

    expected = (
        "Could not determine the entry point type from value: "
        f"{arg!r}. Expected an existing file"
    )

    assert expected in str(excinfo.value)
