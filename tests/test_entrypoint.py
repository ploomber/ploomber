import pytest

from ploomber.entrypoint import EntryPoint


@pytest.mark.parametrize('value, type_', [
    ['*.py', EntryPoint.Pattern],
    ['*', EntryPoint.Pattern],
    ['pkg::pipeline.yaml', EntryPoint.ModulePath],
    ['pkg::pipeline.train.yaml', EntryPoint.ModulePath],
    ['dotted.path', EntryPoint.DottedPath],
    ['another.dotted.path', EntryPoint.DottedPath],
])
def test_entry_point_type(value, type_):
    assert EntryPoint(value).type == type_


def test_entry_point_module_path(monkeypatch):

    e = EntryPoint('test_pkg::pipeline.yaml')

    assert e.type == 'module-path'
    assert not e.is_dir()
    assert e.suffix == '.yaml'


def test_dotted_path_that_ends_with_yaml():
    with pytest.raises(ValueError):
        EntryPoint('some.dotted.path.yaml').type
