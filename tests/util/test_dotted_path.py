import os
import inspect
import importlib
from pathlib import Path
from unittest.mock import Mock

import parso
from test_pkg import functions
import pytest

from ploomber.util import dotted_path
from ploomber.exceptions import SpecValidationError
from ploomber.sources.inspect import getfile
from ploomber.util.dotted_path import (create_intermediate_modules,
                                       dotted_path_exists)


@pytest.mark.parametrize('spec', [
    'test_pkg.functions.some_function',
    {
        'dotted_path': 'test_pkg.functions.some_function'
    },
])
def test_call_dotted_path_calls_function(monkeypatch, spec):
    mock = Mock()
    monkeypatch.setattr(functions, 'some_function', mock)

    dotted_path.DottedPath(spec)()

    mock.assert_called_once_with()


def test_call_spec_with_kwargs(monkeypatch):
    mock = Mock()
    monkeypatch.setattr(functions, 'some_function', mock)

    spec = {
        'dotted_path': 'test_pkg.functions.some_function',
        'a': 1,
        'b': 2,
    }

    dotted_path.DottedPath(spec)()

    mock.assert_called_once_with(a=1, b=2)


def test_call_spec_without_dotted_path_key():
    spec = {'a': 1}

    with pytest.raises(SpecValidationError) as excinfo:
        dotted_path.DottedPath(spec)()

    assert excinfo.value.errors == [{
        'loc': ('dotted_path', ),
        'msg': 'field required',
        'type': 'value_error.missing'
    }]


@pytest.mark.parametrize('kwargs, expected', [
    [None, 42],
    [dict(a=1), 1],
])
def test_call_dotted_path(tmp_directory, add_current_to_sys_path,
                          no_sys_modules_cache, kwargs, expected):

    Path('my_module.py').write_text("""
def function(a=42):
    return a
""")

    assert dotted_path.call_dotted_path('my_module.function',
                                        kwargs=kwargs) == expected


def test_call_dotted_path_unexpected_kwargs(tmp_directory,
                                            add_current_to_sys_path,
                                            no_sys_modules_cache):

    Path('my_module.py').write_text("""
def function():
    pass
""")

    with pytest.raises(TypeError) as excinfo:
        dotted_path.call_dotted_path('my_module.function', kwargs=dict(a=1))

    expected = ("function() got an unexpected keyword argument 'a' "
                "(Loaded from:")
    assert expected in str(excinfo.value)


_two = """
def some_name():
    pass

def some_name():
    pass
"""

_nested_before = """
def something():
    def some_name():
        pass

def some_name():
    pass
"""

_nested_after = """
def some_name():
    pass

def something():
    def some_name():
        pass
"""

_decorated = """
@some_dectorator
def some_name():
    pass
"""

_decorated_many = """
@some_decorator
@another_dectorator
def some_name():
    pass
"""

_test_many_names = """
def another():
    some_name = 1

some_name = 1

def some_name():
    some_name = pd.read_csv('aa')
    x['some_name']
    fn(some_name)
"""


@pytest.mark.parametrize('source, loc_expected', [
    [_test_many_names, 'function.py:7'],
    [_two, 'function.py:5'],
    [_nested_before, 'function.py:6'],
    [_decorated, 'function.py:2'],
    [_decorated_many, 'function.py:2'],
    [_nested_after, 'function.py:2'],
],
                         ids=[
                             'test-many-name',
                             'two',
                             'nested-before',
                             'decorated',
                             'decorated-many',
                             'nested-after',
                         ])
def test_check_defines_function_with_name(tmp_directory,
                                          add_current_to_sys_path,
                                          no_sys_modules_cache, source,
                                          loc_expected):
    Path('function.py').write_text(source)

    loc, source = dotted_path._check_defines_function_with_name(
        'function.py', 'some_name', None)

    assert loc == loc_expected


_overwritten_int = """
def name():
    pass

name = 1
"""

_overwritten_multi = """
def name():
    pass

name, x = 1, 2
"""

_overwritten_import = """
def name():
    pass

import name
"""

_overwritten_from_import = """
def name():
    pass

from something import name
"""

_overwritten_class = """
def name():
    pass

class name:
    pass
"""


@pytest.mark.parametrize('source', [
    _overwritten_int,
    _overwritten_import,
    _overwritten_from_import,
    _overwritten_multi,
    _overwritten_class,
])
def test_check_last_definition_is_function(source):

    module = parso.parse(source)

    with pytest.raises(TypeError) as excinfo:
        dotted_path._check_last_definition_is_function(module, 'name',
                                                       'x.name')

    assert ("Failed to load dotted path 'x.name'. "
            "Expected last defined 'name' to be a function. Got:"
            in str(excinfo.value))


# TODO: test many names but last one is correct
# -sub test case: with decorator

# TODO nested alias, should be skipped>?""
# TODO: test ignores other imports that do not alias
# try more than one alias


@pytest.mark.parametrize('import_', [
    'from pkg import some_name',
    'from pkg.sub import some_name',
    'from . import some_name',
    'from .pkg import some_name',
    'from .pkg.sub import some_name',
    'from .pkg.sub import some_name, another_name',
    'from pkg import some_name, another_name',
    'from pkg.sub import some_name, another_name',
])
def test_check_defines_function_with_name_detects_aliasing(
        tmp_directory, add_current_to_sys_path, no_sys_modules_cache, import_):
    Path('function.py').write_text(import_)

    with pytest.raises(NotImplementedError):
        dotted_path._check_defines_function_with_name('function.py',
                                                      'some_name', None)


@pytest.mark.parametrize('dotted_path_str', [
    'test_pkg.decorated.functions.function',
    'test_pkg.decorated.functions.decorated_function',
    'test_pkg.decorated.functions.double_decorated_function',
    'test_pkg.callables.root',
],
                         ids=[
                             'regular',
                             'decorated-function',
                             'double-decorated-function',
                             'defined-in-init-file',
                         ])
def test_lazily_located_dotted_path(dotted_path_str, tmp_imports):
    loc, source = dotted_path.lazily_locate_dotted_path(dotted_path_str)

    obj = dotted_path.load_dotted_path(dotted_path_str)

    loc_real = getfile(obj)
    lines, line = inspect.getsourcelines(obj)
    source_expected = ''.join(lines)
    loc_expected = f'{loc_real}:{line}'

    assert loc == loc_expected
    assert source == source_expected


@pytest.mark.parametrize('dotted_path_str', ['a.b', 'a.b.c'])
def test_lazily_locate_dotted_path_error_if_no_package_spec(dotted_path_str):

    with pytest.raises(ModuleNotFoundError) as excinfo:
        dotted_path.lazily_locate_dotted_path(dotted_path_str)

    assert (f"Error processing dotted path '{dotted_path_str}', no "
            "module named 'a'" in str(excinfo.value))


@pytest.mark.parametrize('dotted_path_str', ['a', 'a..b.c'])
def test_lazily_locate_dotted_path_error_if_invalid_dotted_path(
        dotted_path_str):
    with pytest.raises(ValueError) as excinfo:
        dotted_path.lazily_locate_dotted_path(dotted_path_str)

    expected = (f"Invalid dotted path '{dotted_path_str}'. "
                "Value must be a dot "
                "separated string, with at least two parts: "
                "[module_name].[function_name]")
    assert str(excinfo.value) == expected


def test_lazily_locate_dotted_path_missing_module(tmp_directory,
                                                  add_current_to_sys_path,
                                                  no_sys_modules_cache):
    Path('a').mkdir()
    Path('a', '__init__.py').touch()

    with pytest.raises(ModuleNotFoundError) as excinfo:
        dotted_path.lazily_locate_dotted_path('a.b.c')

    assert "No module named 'a.b'. Expected to find one of" in str(
        excinfo.value)


def test_error_if_doesnt_define_name(tmp_directory, add_current_to_sys_path,
                                     no_sys_modules_cache):

    Path('a.py').touch()

    with pytest.raises(AttributeError) as excinfo:
        dotted_path.lazily_locate_dotted_path('a.unknown_name')

    assert "Failed to locate dotted path 'a.unknown_name'" in str(
        excinfo.value)
    assert "a.py" in str(excinfo.value)
    assert "a function named 'unknown_name'" in str(excinfo.value)


def test_lazy_load_missing_function():
    dp = dotted_path.DottedPath('not_a_module.not_a_function', lazy_load=True)

    with pytest.raises(ModuleNotFoundError):
        dp()


def test_eager_load_missing_function():
    with pytest.raises(ModuleNotFoundError):
        dotted_path.DottedPath('not_a_module.not_a_function', lazy_load=False)


def test_init_and_call_dotted_path(tmp_directory, tmp_imports):
    Path('some_module.py').write_text("""
def fn(some_arg):
    return some_arg
""")

    dp = dotted_path.DottedPath('some_module.fn', lazy_load=False)
    assert dp(42) == 42


@pytest.mark.parametrize('primitive', [
    'some_module.fn',
    {
        'dotted_path': 'some_module.fn'
    },
    {
        'dotted_path': 'some_module.fn',
        'some_arg': 42,
    },
])
def test_dotted_path_repr(tmp_directory, tmp_imports, primitive):
    Path('some_module.py').write_text("""
def fn(some_arg):
    return some_arg
""")

    dp = dotted_path.DottedPath(primitive, lazy_load=True)
    assert repr(dp) == "DottedPath('some_module.fn')"

    dp._load_callable()

    assert 'loaded:' in repr(dp)


def test_dotted_path_from_dict(tmp_directory, tmp_imports):
    Path('some_module.py').write_text("""
def fn(some_arg):
    return some_arg
""")

    dp = dotted_path.DottedPath(dict(dotted_path='some_module.fn',
                                     some_arg=10),
                                lazy_load=False)

    assert dp() == 10


def test_dotted_path_if_overriding_args(tmp_directory, tmp_imports):
    Path('some_module.py').write_text("""
def fn(some_arg):
    return some_arg
""")

    dp = dotted_path.DottedPath(dict(dotted_path='some_module.fn',
                                     some_arg=10),
                                lazy_load=False)

    with pytest.warns(UserWarning) as record:
        dp(some_arg=20)

    expected = ("Got duplicated arguments ('some_arg') when calling "
                "dotted path 'some_module.fn'. Overriding values...")
    assert record[0].message.args[0] == expected


@pytest.mark.parametrize('path, err_msg', [
    [
        'ast.another.fn',
        ("An error occured when trying to import "
         "dotted path 'ast.another.fn': No module named "
         "'ast.another'; 'ast' is not a package"),
    ],
    [
        'something.another',
        ("An error occured when trying to import "
         "dotted path 'something.another': No module named 'something'"),
    ],
    [
        'my_module.my_function',
        ("An error occured when trying to import "
         "dotted path 'my_module.my_function': No module named 'something'"),
    ],
],
                         ids=[
                             'module-sub-not-found',
                             'module-root-not-found',
                             'indirect-module-due-import-not-found',
                         ])
def test_load_dotted_path_if_import_fails(path, err_msg, tmp_directory,
                                          tmp_imports):

    Path('my_module.py').write_text('import something')

    mod_name = path.split('.')[0]
    spec = importlib.util.find_spec(mod_name)

    if spec:
        err_msg = err_msg + f' (loaded {mod_name!r} from {spec.origin!r})'

    with pytest.raises(ModuleNotFoundError) as excinfo:
        dotted_path.load_dotted_path(path)

    assert str(excinfo.value) == err_msg


@pytest.mark.parametrize('path, err_msg, root', [
    [
        'my_module.func',
        ("Could not get 'func' from module 'my_module' "
         "(loaded 'my_module' from {}). Ensure it is defined in such module"),
        'my_module.py',
    ],
    [
        'another.sub.func',
        ("Could not get 'func' from module 'another.sub' "
         "(loaded 'another.sub' from {}). Ensure it is defined in such module"
         ),
        Path('another', 'sub.py'),
    ],
],
                         ids=[
                             'simple',
                             'with-submodule',
                         ])
def test_load_dotted_path_if_attribute_not_found(path, err_msg, root,
                                                 tmp_directory, tmp_imports):
    Path('my_module.py').write_text('')

    Path('another').mkdir()
    Path('another', 'sub.py').touch()

    with pytest.raises(AttributeError) as excinfo:
        dotted_path.load_dotted_path(path)

    expected = err_msg.format(repr(os.path.abspath(root)))
    assert str(excinfo.value) == expected


def test_create_intermediate_modules_error_if_exists(tmp_directory,
                                                     tmp_imports):
    Path('my_functions.py').write_text("""
def my_function():
    pass
""")

    with pytest.raises(ValueError) as excinfo:
        create_intermediate_modules(['my_functions', 'my_function'])

    expected = "Module 'my_functions.my_function' already exists"
    assert str(excinfo.value) == expected


def test_create_intermediate_modules(tmp_directory):

    modules_and_function = ["sweet", "home", "alabama"]

    create_intermediate_modules(modules_and_function)

    assert Path(tmp_directory, "sweet").exists()
    assert Path(tmp_directory, "sweet", "__init__.py").exists()
    assert Path(tmp_directory, "sweet", "home").exists()
    assert Path(tmp_directory, "sweet", "home", "__init__.py").exists()
    assert Path(tmp_directory, "sweet", "home", "alabama.py").exists()


def test_test_create_intermediate_modules_existing_package(
        backup_test_pkg, tmp_directory):
    modules_and_function = [
        'test_pkg',
        'new_module',
        'another',
    ]
    create_intermediate_modules(modules_and_function)

    assert Path(backup_test_pkg, 'new_module', 'another.py').is_file()


def test_create_intermediate_modules_single_namespace(tmp_directory):
    Path('namespace_pkg').mkdir()

    create_intermediate_modules(['namespace_pkg', 'another'])

    assert Path('namespace_pkg', 'another.py').is_file()


def test_create_intermediate_modules_single(tmp_directory):
    create_intermediate_modules(['something'])

    assert Path('something.py').exists()


@pytest.mark.parametrize('dotted_path, expected', [
    ['some.package.stuff.function', False],
    ['some.things.non_existent', False],
    ['some.package.a_function', True],
])
def test_dotted_path_exists(tmp_directory, tmp_imports, dotted_path, expected):
    dir_ = Path('some', 'package')
    dir_.mkdir(parents=True)
    (dir_ / 'stuff.py').touch()
    Path('some', '__init__.py').touch()
    Path('some', 'package', '__init__.py').write_text("""
def a_function():
    pass
""")

    assert bool(dotted_path_exists(dotted_path)) is expected
