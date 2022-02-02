# TODO: these tests need clean up, is a merge from two files since
# StringPlaceholder was removed and its interface was implemented directly
# in Placeholder
import yaml
import json
import tempfile
from copy import copy, deepcopy
from pathlib import Path

import pytest
from ploomber.placeholders.placeholder import (Placeholder,
                                               SQLRelationPlaceholder,
                                               _get_key)
from ploomber.placeholders.util import get_tags_in_str, get_defined_variables
from ploomber.tasks._upstream import Upstream
from ploomber import SourceLoader
from jinja2 import (Template, Environment, PackageLoader, FileSystemLoader,
                    StrictUndefined)
from jinja2.exceptions import TemplateRuntimeError
from ploomber.exceptions import UpstreamKeyError


@pytest.mark.parametrize(
    "s,expected",
    [("{% set x = 1 %} {{hello}}", {
        'x': 1
    }),
     ("{% set x = ('a', 'b') %} {% for n in numbers %} {%endfor%}", {
         'x': ('a', 'b')
     }), ("{% set x = ['a', 'b'] %}", {
         'x': ['a', 'b']
     }), ("{% set x = {'y': 1} %}", {
         'x': {
             'y': 1
         }
     })])
def test_get_defined_variables(s, expected):
    assert get_defined_variables(s) == expected


def test_get_tags_in_str():
    assert get_tags_in_str('{{a}} {{b}}') == {'a', 'b'}


def test_get_tags_in_none():
    assert get_tags_in_str(None) == set()


def test_verify_if_strict_template_is_literal():
    assert not Placeholder('no need for rendering').needs_render


def test_verify_if_strict_template_needs_render():
    assert Placeholder('I need {{params}}').needs_render


def test_raises_error_if_missing_parameter():
    with pytest.raises(TypeError):
        Placeholder('SELECT * FROM {{table}}').render()


def test_raises_error_if_extra_parameter():
    with pytest.raises(TypeError):
        (Placeholder('SELECT * FROM {{table}}').render(table=1, not_a_param=1))


def test_error_if_raw_source_cant_be_retrieved():
    with pytest.raises(ValueError) as excinfo:
        Placeholder(Template('some template', undefined=StrictUndefined))

    assert 'Could not load raw source from jinja2.Template' in str(
        excinfo.value)


def test_error_if_initialized_with_unsupported_type():
    with pytest.raises(TypeError) as excinfo:
        Placeholder(None)

    assert ('Placeholder must be initialized with a Template, '
            'Placeholder, pathlib.Path or str, got NoneType instead') == str(
                excinfo.value)


def test_error_on_read_before_render():
    placeholder = Placeholder('some template {{variable}}')

    with pytest.raises(RuntimeError) as excinfo:
        str(placeholder)

    assert (
        'Tried to read Placeholder '
        'Placeholder(\'some template {{variable}}\') without rendering first'
    ) == str(excinfo.value)


def test_strict_templates_initialized_from_jinja_template(path_to_assets):
    path = str(path_to_assets / 'templates')
    env = Environment(loader=FileSystemLoader(path), undefined=StrictUndefined)
    st = Placeholder(env.get_template('template.sql'))
    assert st.render({'file': 1})


def test_strict_templates_raises_error_if_not_strictundefined(path_to_assets):
    path = str(path_to_assets / 'templates')
    env = Environment(loader=FileSystemLoader(path))

    with pytest.raises(ValueError):
        Placeholder(env.get_template('template.sql'))


def test_strict_templates_initialized_from_strict_template(path_to_assets):
    path = str(path_to_assets / 'templates')
    env = Environment(loader=FileSystemLoader(path), undefined=StrictUndefined)
    st = Placeholder(env.get_template('template.sql'))
    assert Placeholder(st).render({'file': 1})


def test_can_copy_placeholders(path_to_assets):
    path = str(path_to_assets / 'templates')
    env = Environment(loader=FileSystemLoader(path), undefined=StrictUndefined)
    st = Placeholder(env.get_template('template.sql'))
    cst = copy(st)
    dpst = deepcopy(st)

    assert cst.render({'file': 'a_file'}) == '\n\na_file'
    assert str(cst) == '\n\na_file'
    assert dpst.render({'file': 'a_file2'}) == '\n\na_file2'
    assert str(dpst) == '\n\na_file2'


def test_string_identifier_initialized_with_str():

    si = Placeholder('things').render({})

    # assert repr(si) == "StringPlaceholder('things')"
    assert str(si) == 'things'


def test_string_identifier_initialized_with_str_with_tags():

    si = Placeholder('{{key}}').render(params=dict(key='things'))

    # assert repr(si) == "StringPlaceholder('things')"
    assert str(si) == 'things'


def test_string_identifier_initialized_with_template_raises_error():

    with pytest.raises(ValueError):
        Placeholder(Template('{{key}}')).render(params=dict(key='things'))


def test_string_identifier_initialized_with_template_from_env():

    tmp = tempfile.mkdtemp()

    Path(tmp, 'template.sql').write_text('{{key}}')

    env = Environment(loader=FileSystemLoader(tmp), undefined=StrictUndefined)

    template = env.get_template('template.sql')

    si = Placeholder(template).render(params=dict(key='things'))

    assert str(si) == 'things'


def test_init_placeholder_with_placeholder():
    t = Placeholder('{{file}}')
    tt = Placeholder(t)

    assert tt.render({'file': 'some file'})


def test_repr_shows_tags_if_unrendered():
    assert repr(Placeholder('{{tag}}')) == "Placeholder('{{tag}}')"


def test_sql_placeholder_repr_shows_tags_if_unrendered_sql():
    expected = "SQLRelationPlaceholder(('{{schema}}', '{{name}}', 'table'))"
    assert (repr(SQLRelationPlaceholder(
        ('{{schema}}', '{{name}}', 'table'))) == expected)


def _filesystem_loader(path_to_test_pkg):
    return Environment(loader=FileSystemLoader(
        str(Path(path_to_test_pkg, 'templates'))),
                       undefined=StrictUndefined)


def _package_loader(path_to_test_pkg):
    return Environment(loader=PackageLoader('test_pkg', 'templates'),
                       undefined=StrictUndefined)


def _source_loader_path(path_to_test_pkg):
    path = str(Path(path_to_test_pkg, 'templates'))
    return SourceLoader(path=path)


def _source_loader_module(path_to_test_pkg):
    return SourceLoader(path='templates', module='test_pkg')


_env_initializers = [
    _filesystem_loader, _package_loader, _source_loader_path,
    _source_loader_module
]


@pytest.mark.parametrize('env_init', _env_initializers)
def test_macros_with_template_environment(env_init, path_to_test_pkg):
    env = env_init(path_to_test_pkg)

    # this template contains a macro
    placeholder = Placeholder(env.get_template('query.sql'))
    placeholder.render({})

    assert str(placeholder) == 'SELECT * FROM table'


@pytest.mark.parametrize('env_init', _env_initializers)
def test_hot_reload_with_template_env(env_init, path_to_test_pkg):
    query_path = Path(path_to_test_pkg, 'templates', 'query.sql')
    query_original = query_path.read_text()

    env = env_init(path_to_test_pkg)

    placeholder = Placeholder(env.get_template('query.sql'), hot_reload=True)
    placeholder.render({})

    assert str(placeholder) == 'SELECT * FROM table'

    # use a macro to make sure the template loader is correctly initialized
    query = ('{% import "macros.sql" as m %}SELECT * FROM {{m.my_macro()}}'
             ' WHERE x = 10')
    query_path.write_text(query)
    placeholder.render({})

    assert str(placeholder) == 'SELECT * FROM table WHERE x = 10'

    # revert query to their original value
    query_path.write_text(query_original)


def test_hot_reload_with_with_path(tmp_directory):
    query_path = Path(tmp_directory, 'simple_query.sql')
    query_path.write_text('SELECT * FROM {{tag}}')

    placeholder = Placeholder(Path(query_path), hot_reload=True)
    placeholder.render({'tag': 'table'})

    assert str(placeholder) == 'SELECT * FROM table'

    query_path.write_text('SELECT * FROM {{tag}} WHERE x = 10')
    placeholder.render({'tag': 'table'})

    assert str(placeholder) == 'SELECT * FROM table WHERE x = 10'


def test_error_when_init_from_string_and_hot_reload():
    with pytest.raises(ValueError) as excinfo:
        Placeholder('SELECT * FROM table', hot_reload=True)

    m = 'hot_reload only works when Placeholder is initialized from a file'
    assert str(excinfo.value) == m


@pytest.mark.parametrize('env_init', _env_initializers)
def test_placeholder_initialized_with_placeholder(env_init, path_to_test_pkg):
    env = env_init(path_to_test_pkg)
    placeholder = Placeholder(env.get_template('query.sql'))
    placeholder_new = Placeholder(placeholder)

    assert placeholder_new._raw == placeholder._raw
    assert placeholder_new.path == placeholder.path

    assert placeholder_new is not placeholder
    assert placeholder_new._loader_init is not None
    assert placeholder_new._loader_init == placeholder._loader_init
    assert placeholder_new._loader_init is not placeholder._loader_init


def test_error_if_missing_upstream():
    p = Placeholder('SELECT * FROM {{upstream["name"]}}')
    upstream = Upstream({'a': 1}, name='task')

    with pytest.raises(UpstreamKeyError) as excinfo:
        p.render({'upstream': upstream})

    assert ('Cannot obtain upstream dependency "name" for task "task"'
            in str(excinfo.value))


def test_error_if_no_upstream():
    p = Placeholder('SELECT * FROM {{upstream["name"]}}')
    upstream = Upstream({}, name='task')

    with pytest.raises(UpstreamKeyError) as excinfo:
        p.render({'upstream': upstream})

    msg = ('Cannot obtain upstream dependency "name". '
           'Task "task" has no upstream dependencies')
    assert msg == str(excinfo.value)


def test_raise(tmp_directory):
    p = Placeholder("{% raise 'some error message' %}")

    with pytest.raises(TemplateRuntimeError) as excinfo:
        p.render({})

    assert str(excinfo.value) == 'some error message'


def test_globals():
    p = Placeholder('something')

    assert {'get_key'} <= set(p._template.environment.globals)


def test_source_loader_globals(tmp_directory):
    Path('template').touch()

    sl = SourceLoader(path='.')

    assert {'get_key'} <= set(sl['template']._template.environment.globals)


@pytest.mark.parametrize('filename, fn', [
    ['file.json', json.dumps],
    ['file.yaml', yaml.dump],
])
def test_get_key(tmp_directory, filename, fn):
    Path(filename).write_text(fn(dict(key='value')))

    assert _get_key(filename, 'key') == 'value'


def test_get_key_error_if_unsupported_extension():
    with pytest.raises(ValueError) as excinfo:
        _get_key('something.csv', 'key')

    expected = "get_key must be used with .json or .yaml files. Got: '.csv'"
    assert str(excinfo.value) == expected
