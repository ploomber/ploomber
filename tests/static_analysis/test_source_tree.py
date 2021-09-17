import os
from pathlib import Path
import importlib

import pytest

from ploomber.static_analysis import source_tree

# TODO: add a test with a namespace module. e.g., the same module
# as two different locations and we must look up in both to find a symbol

# TODO: warn on import shadowing
# import x
# x = 1

# TODO: add test with an import of am module that doesn't exist


def write_recursively(*args, text=None):
    path = Path(*args)
    path.parent.mkdir(parents=True)
    if text:
        path.write_text(text)
    else:
        path.touch()


@pytest.fixture
def sample_files(tmp_directory, tmp_imports):
    Path('package').mkdir()
    Path('package', '__init__.py').touch()
    Path('package', 'sub').mkdir()
    Path('package', 'sub', '__init__.py').write_text("""
def x():
    pass

def z():
    pass
""")
    Path('package', 'sub_other').mkdir()
    Path('package', 'sub_other', '__init__.py').write_text("""
def a():
    pass
""")

    Path('module.py').write_text("""
def a():
    pass

def b():
    pass
""")

    Path('another_module.py').write_text("""
def a():
    pass

def b():
    pass
""")


@pytest.mark.parametrize('origin, expected', [
    [importlib.util.find_spec('math').origin, False],
    [importlib.util.find_spec('jupyter').origin, False],
    [importlib.util.find_spec('test_pkg').origin, True],
    ['built-in', False],
],
                         ids=[
                             'built-in',
                             'site-package',
                             'editable-package',
                             'windows-built-in',
                         ])
def test_should_track_origin(origin, expected):
    assert source_tree.should_track_origin(origin) == expected


@pytest.mark.parametrize('origin, expected', [
    [
        Path('env', 'lib', 'python', 'site-packages', 'pkg', '__init__.py'),
        False
    ],
    [Path('proj', 'pkg', '__init__.py'), True],
],
                         ids=[
                             'venv-site-packages',
                             'not-site-packages',
                         ])
def test_should_track_origin_in_virtual_env(origin, expected, monkeypatch):
    monkeypatch.setattr(source_tree.sys, 'prefix', 'something')
    monkeypatch.setattr(source_tree.sys, 'base_prefix', 'another')
    origin = str(origin)
    assert source_tree.should_track_origin(origin) == expected


@pytest.mark.parametrize('origin, expected', [
    [
        Path('env', 'lib', 'python', 'site-packages', 'pkg', '__init__.py'),
        False
    ],
    [Path('proj', 'pkg', '__init__.py'), True],
],
                         ids=[
                             'venv-site-packages',
                             'not-site-packages',
                         ])
def test_should_track_origin_in_virtual_env_ipython(origin, expected,
                                                    monkeypatch):
    monkeypatch.setitem(source_tree.os.environ, 'VIRTUAL_ENV', 'path/to/env')
    origin = str(origin)
    assert source_tree.should_track_origin(origin) == expected


@pytest.mark.parametrize(
    'script, expected',
    [
        ["""
from math import *
""", {}],
        ["""
# built-in module
import math

math.square(1)
""", {}],
        ["""
# local module imported but unused
import another_module
""", {}],
        [
            """
# local module
import another_module

another_module.a
""", {
                'another_module.a': 'def a():\n    pass'
            }
        ],
        # NOTE: this is only possible if "import package" triggers an
        # "import package.sub" or "from package import sub". In such case
        # "package" is modified and gets a new "sub" attribute. Here's an
        # explanation:
        # http://python-notes.curiousefficiency.org/en/latest/python_concepts/import_traps.html#the-submodules-are-added-to-the-package-namespace-trap # noqa
        # FIXME: the next two cases are duplicated
        [
            """
import package.sub

package.sub.x()
""", {
                'package.sub.x': 'def x():\n    pass'
            }
        ],
        [
            """
# submodule
import package.sub

package.sub.x()
""", {
                'package.sub.x': 'def x():\n    pass'
            }
        ],
        ["""
# submodule
import package.sub
""", {}],
        [
            """
# from .. import {sub-module}
from package import sub_other

sub_other.a()
""", {
                'package.sub_other.a': 'def a():\n    pass'
            }
        ],
        [
            """
# from .. import {sub-module}
from package import sub_other

""", {}
        ],
        # FIXME: the output reference here is wrong. a, b arent used so they
        # should be ignored
        [
            """
# from .. import attribute1, attrbute2
from module import a, b
""", {
                'module.a': 'def a():\n    pass',
                'module.b': 'def b():\n    pass'
            }
        ],
        [
            """
# module
import another_module as some_alias

some_alias.a
""", {
                'another_module.a': 'def a():\n    pass'
            }
        ],
        [
            """
# submodule with alias
import package.sub as some_alias

some_alias.x()
""", {
                'package.sub.x': 'def x():\n    pass'
            }
        ],
        [
            """
# built-in module
import math
# module
import another_module
# submodule
import package.sub
# from {some-pkg} import {sub-module}
from package import sub_other
# from {some-pkg} import {attribute}
from module import a, b

another_module.a()
another_module.b()
package.sub.x()
sub_other.a()
""", {
                'another_module.a': 'def a():\n    pass',
                'another_module.b': 'def b():\n    pass',
                'package.sub.x': 'def x():\n    pass',
                'package.sub_other.a': 'def a():\n    pass',
                'module.a': 'def a():\n    pass',
                'module.b': 'def b():\n    pass'
            }
        ],
    ],
    ids=[
        'import-star',
        'built-in',
        'local-unused',
        'local',
        'local-nested',
        'submodule',
        'submodule-empty',
        'from-import',
        'from-import-empty',
        # FIXME: only retrieve source code if attributes are actually
        # used in the code
        'from-import-multiple',
        'import-as',
        'submodule-import-as',
        'complete',
    ])
def test_extract_from_script(sample_files, script, expected):
    Path('script.py').write_text(script)

    # TODO: try accessing a constant like dictionary defined in a module
    # e.g. module.sub['a'], should we also look for changes there?
    specs = source_tree.extract_from_script('script.py')

    assert specs == expected


@pytest.mark.parametrize('code', [
    """
from some_module import *
""",
    """
from some_module import *
from another_module import *
""",
    """
import test_pkg

def x():
    pass

from some_module import *
""",
],
                         ids=[
                             'one',
                             'two',
                             'one-middle',
                         ])
# FIXME: should_track_dotted_path in extract_from_script is breaking this
# FIXME: remove add_current_to_sys_path once we stop using find_spec
def test_warns_if_star_import(tmp_directory, add_current_to_sys_path, code):
    Path('some_module.py').touch()
    Path('another_module.py').touch()
    Path('script.py').write_text(code)

    with pytest.warns(UserWarning) as record:
        specs = source_tree.extract_from_script('script.py')

    assert specs == {}
    assert len(record) == 1
    assert 'contains star imports' in record[0].message.args[0]


@pytest.mark.parametrize('code', [
    """
from math import *
""",
    """
from math import *

class A:
    pas

from string import *
""",
    """
from jupyter import *
""",
    """
from jupyter import *
from math import *
""",
],
                         ids=[
                             'built-in',
                             'built-in-many',
                             'external-module',
                             'mixed',
                         ])
def test_no_warning_if_built_in_or_external_module(tmp_directory, code):
    Path('script.py').write_text(code)

    with pytest.warns(None) as record:
        specs = source_tree.extract_from_script('script.py')

    assert specs == {}
    assert not len(record)


@pytest.mark.parametrize('script, expected', [
    [
        """
from . import module

module.a()
    """, {
            'module.a': 'def a():\n    pass'
        }
    ],
    [
        """
from . import module as some_alias

some_alias.a()
    """, {
            'module.a': 'def a():\n    pass'
        }
    ],
    [
        """
from . import module

module.a()

class A:
    def __init__(self):
        module.b()
    """, {
            'module.a': 'def a():\n    pass',
            'module.b': 'def b():\n    pass',
        }
    ],
    [
        """
from .package import sub

sub.x()
    """, {
            'package.sub.x': 'def x():\n    pass'
        }
    ],
    [
        """
from .package import sub as some_alias

some_alias.x()
    """, {
            'package.sub.x': 'def x():\n    pass'
        }
    ],
    [
        """
from .package import sub

sub.x()

def fn():
    df = sub.z()
    """, {
            'package.sub.x': 'def x():\n    pass',
            'package.sub.z': 'def z():\n    pass',
        }
    ],
],
                         ids=[
                             'sibling-one-import-one-accessed',
                             'sibling-one-import-one-accessed-aliased',
                             'sibling-one-import-many-accessed',
                             'nested-one-import-one-accessed',
                             'nested-one-import-one-accessed-aliased',
                             'nested-one-import-many-accessed',
                         ])
def test_extract_from_script_with_relative_imports(
    sample_files,
    script,
    expected,
):
    Path('script.py').write_text(script)
    specs = source_tree.extract_from_script('script.py')
    assert specs == expected


@pytest.mark.parametrize('script, expected', [
    [
        """
from .. import module

module.a()
    """, {
            'module.a': 'def a():\n    pass'
        }
    ],
    [
        """
from .. import module as some_alias

some_alias.a()
    """, {
            'module.a': 'def a():\n    pass'
        }
    ],
    [
        """
from .. import module

module.a()

class A:
    def __init__(self):
        module.b()
    """, {
            'module.a': 'def a():\n    pass',
            'module.b': 'def b():\n    pass',
        }
    ],
],
                         ids=[
                             'grandparent-one-import-one-accessed',
                             'grandparent-one-import-one-accessed-aliased',
                             'grandparent-one-import-many-accessed',
                         ])
# NOTE: these tests are passing because of the parent_or_child function
# e.g. one is /path/to/package
# another is /path/to/module.py
# I think we should remove that function and change it for something that
# ignores anything inside site-packages
# TODO: add from ..something import x
def test_extract_from_script_with_relative_imports_nested(
    sample_files,
    script,
    expected,
):
    os.chdir('package')
    Path('script.py').write_text(script)
    specs = source_tree.extract_from_script('script.py')
    assert specs == expected


def test_extract_attribute_access():
    code = """
import my_module

result = my_module.some_fn(1)

def do_something(x):
    return my_module.another_fn(x) + x


def do_more_stuff(x):
    my_module = dict(something=1)
    return my_module['something']
"""

    assert source_tree.extract_attribute_access(
        code, 'my_module') == ['some_fn', 'another_fn']


def test_extract_attribute_access_2():
    code = """
import functions

functions.a()
"""
    assert source_tree.extract_attribute_access(code, 'functions') == ['a']


def test_extract_attribute_access_3():
    code = """
import functions

# some comment
functions.a()
functions.b()
"""
    # TODO: parametrize with and without comments
    assert source_tree.extract_attribute_access(code,
                                                'functions') == ['a', 'b']


@pytest.mark.parametrize('source, expected', [
    [
        """
mod.sub.some_fn(1)
""",
        ['some_fn'],
    ],
    [
        """
result = mod.sub.some_fn(1)
""",
        ['some_fn'],
    ],
    [
        """
def do_something(x):
    return mod.sub.another_fn(x) + x
""",
        ['another_fn'],
    ],
    [
        """
mod.sub.some_dict[1]
""",
        ['some_dict'],
    ],
    [
        """
mod.sub.nested.attribute
""",
        ['nested.attribute'],
    ],
    [
        """
import mod.sub

result = mod.sub.some_fn(1)

def do_something(x):
    return mod.sub.another_fn(x) + x

mod = something_else()
mod.sub['something']

mod.sub.some_dict[1]

mod.sub.nested.attribute
""",
        ['some_fn', 'another_fn', 'some_dict', 'nested.attribute'],
    ],
],
                         ids=[
                             'function-call',
                             'function-call-assignment',
                             'function-call-nested',
                             'getitem',
                             'nested-attribute',
                             'complete',
                         ])
def test_extract_nested_attribute_access(source, expected):
    assert source_tree.extract_attribute_access(source, 'mod.sub') == expected


@pytest.mark.parametrize('symbol, source', [
    ['a', 'def a():\n    pass'],
    ['B', 'class B:\n    pass'],
])
def test_extract_symbol(symbol, source):
    code = """
def a():
    pass

class B:
    pass
"""

    assert source_tree.extract_symbol(code, symbol) == source


# TODO: try with a[1], a.something
def test_get_source_from_function_import(tmp_directory, tmp_imports):
    Path('functions.py').write_text("""
def a():
    pass
""")

    code = """
a()
"""

    assert source_tree.get_source_from_import('functions.a', code, 'a') == {
        'functions.a': 'def a():\n    pass'
    }


def test_get_source_with_nested_access(sample_files, tmp_imports,
                                       add_current_to_sys_path):
    code = """
package.sub.x()
"""
    assert source_tree.get_source_from_import('package.sub', code,
                                              'package.sub') == {
                                                  'package.sub.x':
                                                  'def x():\n    pass'
                                              }


def test_get_source_from_module_import(tmp_directory, tmp_imports):
    Path('functions.py').write_text("""
def a():
    pass
""")

    code = """
import functions

functions.a()
"""

    # TODO: what if accessing attributes that do not exist e.g., functions.b()
    assert source_tree.get_source_from_import('functions', code,
                                              'functions') == {
                                                  'functions.a':
                                                  'def a():\n    pass'
                                              }


def test_missing_init_in_submodule(tmp_directory, tmp_imports):
    write_recursively('package', '__init__.py')
    # note that we are missing package/sub/__init__.py
    # this works since Python 3.3
    # http://python-notes.curiousefficiency.org/en/latest/python_concepts/import_traps.html#the-missing-init-py-trap
    write_recursively('package',
                      'sub',
                      'functions.py',
                      text="""
def a():
    pass
""")

    code = """
from package.sub import functions

functions.a()
"""

    assert source_tree.get_source_from_import('package.sub.functions.a', code,
                                              'functions') == {
                                                  'package.sub.functions.a':
                                                  'def a():\n    pass'
                                              }


def test_missing_init_in_module(tmp_directory, tmp_imports):
    # note that we are missing package/__init__.py
    # this works since Python 3.3
    # http://python-notes.curiousefficiency.org/en/latest/python_concepts/import_traps.html#the-missing-init-py-trap
    write_recursively('package',
                      'functions.py',
                      text="""
def a():
    pass
""")

    code = """
from package.sub import functions

functions.a()
"""

    assert source_tree.get_source_from_import('package.functions.a', code,
                                              'functions') == {
                                                  'package.functions.a':
                                                  'def a():\n    pass'
                                              }


def test_missing_spec(tmp_directory, tmp_imports):
    # this will causes the spec.origin to be None
    Path('package').mkdir()
    # FIXME: init can be missing, cover that case
    Path('package', '__init__.py').touch()

    code = """
import package

package.a()
"""
    assert source_tree.get_source_from_import('package', code, 'package') == {}


def test_get_source_from_function_source(tmp_directory, tmp_imports):
    Path('utils.py').write_text("""
def do_more():
    pass
""")
    assert source_tree.get_source_from_import('utils.do_more',
                                              'def call_do():\n    do()\n',
                                              'do_more') == {}


# TODO: same test cases as when extracting from script
@pytest.mark.parametrize('attr_name, expected', [
    ['call_do', {
        'utils.do': 'def do():\n    pass'
    }],
    ['call_do_more', {
        'utils.do_more': 'def do_more():\n    pass'
    }],
    [
        'call_both', {
            'utils.do': 'def do():\n    pass',
            'utils.do_more': 'def do_more():\n    pass',
        }
    ],
    [
        'call_local', {
            'objects.call_nothing': 'def call_nothing():\n    pass',
        }
    ],
    [
        'call_local_class', {
            'objects.LocalClass': 'class LocalClass:\n    pass',
        }
    ],
    [
        'call_external_class', {
            'utils.ExternalClass': 'class ExternalClass:\n    pass',
        }
    ],
    [
        'call_nested', {
            'utils.nested': 'def nested():\n    do()',
            'utils.do': 'def do():\n    pass',
        }
    ],
    [
        'SomeClass', {
            'objects.LocalClass': 'class LocalClass:\n    pass',
            'utils.do': 'def do():\n    pass'
        }
    ],
],
                         ids=[
                             'call_do',
                             'call_do_more',
                             'call_both',
                             'call_local',
                             'call_local_class',
                             'call_external_class',
                             'call_nested',
                             'SomeClass',
                         ])
def test_extract_from_object(sample_files, tmp_imports, attr_name, expected):
    Path('objects.py').write_text("""
from utils import do, do_more
import utils

class SomeClass:
    def some_method(self):
        do()

    @classmethod
    def another_method(cls):
        LocalClass()

class LocalClass:
    pass

def call_do():
    do()

def call_do_more():
    for x in range(10):
        do_more()

def call_both():
    for x in range(10):
        do_more()

    do()

def call_nothing():
    pass

def call_local():
    return call_nothing()

def call_local_class():
    obj = LocalClass()

def call_external_class():
    obj = utils.ExternalClass()

def call_nested():
    utils.nested()
""")

    Path('utils.py').write_text("""
class ExternalClass:
    pass

def do():
    pass

def do_more():
    pass

def nested():
    do()
""")

    import objects

    assert (source_tree.extract_from_object(getattr(objects,
                                                    attr_name)) == expected)


@pytest.mark.parametrize('attr_name, expected', [
    ['uses_constant', {}],
    ['uses_literal', {}],
    ['uses_lambda', {}],
    ['uses_nested_imported_object', {}],
])
def test_extract_from_object_ignores_non_objects(tmp_directory, tmp_imports,
                                                 attr_name, expected):
    Path('objects.py').write_text("""
from utils import CONSTANT, LITERAL, some_lambda, some_function

def uses_constant():
    CONSTANT

def uses_literal():
    LITERAL


def uses_lambda():
    some_lambda()


def uses_nested_imported_object():
    some_function()
""")

    Path('utils.py').write_text("""
from more import some_function

CONSTANT = 1

LITERAL = [1, 2, 3]

some_lambda = lambda: 1
""")

    Path('more.py').write_text("""
def some_function():
    pass
""")

    import objects

    assert (source_tree.extract_from_object(getattr(objects,
                                                    attr_name)) == expected)


@pytest.mark.parametrize('code', [
    """
def fn():
    do_more()
""", """
def fn():
    do_more['something']
""", """
def fn():
    do_more.some_attribute
""", """
def fn():
    do_more.some_static_method()
""", """
def fn():
    new_name = do_more
""", """
def fn():
    do_more
"""
])
def test_did_access_name(code):
    assert source_tree.did_access_name(code, 'do_more')


def test_did_access_name_false():
    code = """
def call_do_more():
    for x in range(10):
        do_more()
"""
    assert not source_tree.did_access_name(code, 'do')


@pytest.mark.parametrize('code, names, expected', [
    [
        """
a()
b()
""",
        ['a', 'b', 'c'],
        ['a', 'b'],
    ],
    [
        """
a.something()
z()
""",
        ['a', 'b', 'x'],
        ['a'],
    ],
])
def test_accessed_names(code, names, expected):
    assert source_tree.get_accessed_names(code, names) == expected


@pytest.mark.parametrize('code, expected', [
    [
        """
def a():
    pass

def b():
    pass

def c():
    pass

def fn():
    a()
""",
        {
            'functions.a': 'def a():\n    pass'
        },
    ],
    [
        """
def a():
    pass


def b():
    pass

def fn(something):
    a()

    numbers = [b(n) for n in something]
""",
        {
            'functions.a': 'def a():\n    pass',
            'functions.b': 'def b():\n    pass'
        },
    ],
])
def test_get_source_from_accessed_symbol_in_callable(tmp_directory,
                                                     tmp_imports, code,
                                                     expected):
    Path('functions.py').write_text(code)

    import functions

    assert source_tree._extract_accessed_objects_from_callable(
        functions.fn) == expected
