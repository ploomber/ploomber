# TODO: these tests need clean up, is a merge from two files since
# StringPlaceholder was removed and its interface was implemented directly
# in Placeholder
import sys
import tempfile
from copy import copy, deepcopy
from pathlib import Path

import pytest
from ploomber.templates.Placeholder import Placeholder, SQLRelationPlaceholder
from ploomber import SourceLoader
from jinja2 import Template, Environment, PackageLoader, FileSystemLoader, StrictUndefined


@pytest.fixture()
def sys_path():
    original = sys.path
    yield sys.path
    sys.path = original


def test_get_name_property():
    p = Path(tempfile.mktemp())
    p.write_text('This is some text in a file used as Placeholder {{tag}}')
    assert p.name == Placeholder(p).name


def test_verify_if_strict_template_is_literal():
    assert not Placeholder('no need for rendering').needs_render


def test_verify_if_strict_template_needs_render():
    assert Placeholder('I need {{params}}').needs_render


def test_raises_error_if_missing_parameter():
    with pytest.raises(TypeError):
        Placeholder('SELECT * FROM {{table}}').render()


def test_raises_error_if_extra_parameter():
    with pytest.raises(TypeError):
        (Placeholder('SELECT * FROM {{table}}')
         .render(table=1, not_a_param=1))


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
    assert repr(Placeholder('{{tag}}')) == 'Placeholder("{{tag}}")'


def test_sql_placeholder_repr_shows_tags_if_unrendered_sql():
    expected = 'SQLRelationPlaceholder({{schema}}.{{name}})'
    assert (repr(SQLRelationPlaceholder(('{{schema}}', '{{name}}', 'table')))
            == expected)


def _setup_sample_templates_structure(tmp_directory):
    templates_dir = Path(tmp_directory, 'placeholder_package', 'templates')
    templates_dir.mkdir(parents=True)

    macros = '{% macro my_macro() -%} table {%- endmacro %}'
    (templates_dir / 'macros.sql').write_text(macros)

    query = '{% import "macros.sql" as m %}SELECT * FROM {{m.my_macro()}}'
    (templates_dir / 'query.sql').write_text(query)


def _filesystem_loader(tmp_directory):
    _setup_sample_templates_structure(tmp_directory)
    return Environment(loader=FileSystemLoader(str(Path('placeholder_package',
                                                        'templates'))),
                       undefined=StrictUndefined)


def _package_loader(tmp_directory):
    _setup_sample_templates_structure(tmp_directory)
    # pytest does not add the current directory to sys.path, add it manually
    sys.path.append(tmp_directory)
    tmp_directory = Path(tmp_directory)
    (tmp_directory / '..' / '__init__.py').touch()
    (tmp_directory / '__init__.py').touch()
    return Environment(loader=PackageLoader('placeholder_package',
                                            'templates'),
                       undefined=StrictUndefined)


def _source_loader(tmp_directory):
    _setup_sample_templates_structure(tmp_directory)
    return SourceLoader((str(Path('placeholder_package', 'templates'))))


_env_initializers = [_filesystem_loader, _package_loader, _source_loader]


@pytest.mark.parametrize('env_init', _env_initializers)
def test_macros_with_template_environment(env_init, sys_path, tmp_directory):
    env = env_init(tmp_directory)

    # this template contains a macro
    placeholder = Placeholder(env.get_template('query.sql'))
    placeholder.render({})

    assert str(placeholder) == 'SELECT * FROM table'


@pytest.mark.parametrize('env_init', _env_initializers)
def test_hot_reload_with_template_env(env_init, tmp_directory):
    env = env_init(tmp_directory)

    placeholder = Placeholder(env.get_template('query.sql'), hot_reload=True)
    placeholder.render({})

    assert str(placeholder) == 'SELECT * FROM table'

    # use a macro to make sure the template loader is correctly initialized
    query = ('{% import "macros.sql" as m %}SELECT * FROM {{m.my_macro()}}'
             ' WHERE x = 10')

    query_path = Path('placeholder_package', 'templates', 'query.sql')
    query_path.write_text(query)
    placeholder.render({})

    assert str(placeholder) == 'SELECT * FROM table WHERE x = 10'


def test_hot_reload_with_with_path(tmp_directory):
    _setup_sample_templates_structure(tmp_directory)

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
def test_placeholder_initialized_with_placeholder(env_init, tmp_directory):
    env = env_init(tmp_directory)
    placeholder = Placeholder(env.get_template('query.sql'))
    placeholder_new = Placeholder(placeholder)

    assert placeholder_new.raw == placeholder.raw
    assert placeholder_new.path == placeholder.path

    assert placeholder_new is not placeholder
    assert placeholder_new._loader_init is not None
    assert placeholder_new._loader_init == placeholder._loader_init
    assert placeholder_new._loader_init is not placeholder._loader_init
