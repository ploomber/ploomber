import os
from pathlib import Path

import pytest

from ploomber.static_analysis import imports

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

    # TODO: finding imports recursively

    # TODO: try with nested imports (i.e. inside a function)

    # TODO: try accessing an attribute that's imported in __init__
    # hence the source isn't there...

    # TODO: try accessing a constant like dictionary defined in a module
    # e.g. module.sub['a'], should we also look for changes there?
    specs = imports.extract_from_script('script.py')

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
        specs = imports.extract_from_script('script.py')

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
        specs = imports.extract_from_script('script.py')

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
    specs = imports.extract_from_script('script.py')
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
    specs = imports.extract_from_script('script.py')
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

    assert imports.extract_attribute_access(
        code, 'my_module') == ['some_fn', 'another_fn']


def test_extract_attribute_access_2():
    code = """
import functions

functions.a()
"""
    assert imports.extract_attribute_access(code, 'functions') == ['a']


def test_extract_attribute_access_3():
    code = """
import functions

# some comment
functions.a()
functions.b()
"""
    # TODO: parametrize with and without comments
    assert imports.extract_attribute_access(code, 'functions') == ['a', 'b']


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
    assert imports.extract_attribute_access(source, 'mod.sub') == expected


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

    assert imports.extract_symbol(code, symbol) == source


# TODO: should this check that the function is called?
def test_get_source_from_function_import(tmp_directory, tmp_imports):
    Path('functions.py').write_text("""
def a():
    pass
""")

    assert imports.get_source_from_import('functions.a', '', 'functions',
                                          Path('.').resolve()) == {
                                              'functions.a':
                                              'def a():\n    pass'
                                          }


def test_get_source_with_nested_access(sample_files, tmp_imports,
                                       add_current_to_sys_path):
    code = """
package.sub.x()
"""
    assert imports.get_source_from_import('package.sub', code, 'package.sub',
                                          Path('.').resolve()) == {
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
    assert imports.get_source_from_import('functions', code, 'functions',
                                          Path('.').resolve()) == {
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

    assert imports.get_source_from_import('package.sub.functions.a', code,
                                          'functions',
                                          Path('.').resolve()) == {
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

    assert imports.get_source_from_import('package.functions.a', code,
                                          'functions',
                                          Path('.').resolve()) == {
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
    assert imports.get_source_from_import('package', code, 'package',
                                          Path('.').resolve()) == {}
