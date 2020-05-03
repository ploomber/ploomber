# TODO: these tests need clean up, is a merge from two files since
# StringPlaceholder was removed and its interface was implemented directly
# in Placeholder
import tempfile
from copy import copy, deepcopy
from pathlib import Path

import pytest
from ploomber.templates.Placeholder import Placeholder, SQLRelationPlaceholder
from jinja2 import Template, Environment, FileSystemLoader, StrictUndefined


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
