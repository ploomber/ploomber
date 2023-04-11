import pytest

from ploomber.placeholders.placeholder import SQLRelationPlaceholder
from ploomber.products import (
    SQLiteRelation,
    PostgresRelation,
    GenericSQLRelation,
    SQLRelation,
)

classes = [
    SQLRelationPlaceholder,
    SQLiteRelation,
    PostgresRelation,
    GenericSQLRelation,
    SQLRelation,
]


@pytest.mark.parametrize("class_", classes)
def test_literal(class_):
    p = class_(("schema", "name", "table"))
    assert repr(p) == class_.__name__ + "(('schema', 'name', 'table'))"
    assert str(p) == "schema.name"


@pytest.mark.parametrize("class_", classes)
def test_with_placeholder(class_):
    p = class_(("schema", "{{placeholder}}", "table"))
    assert repr(p) == (class_.__name__ + "(('schema', '{{placeholder}}', 'table'))")
    p.render({"placeholder": "name"})
    assert repr(p) == class_.__name__ + "(('schema', 'name', 'table'))"
    assert str(p) == "schema.name"


@pytest.mark.parametrize("class_", classes)
@pytest.mark.parametrize("schema", [None, ""])
def test_empty_schema_is_not_rendered(class_, schema):
    p = class_((schema, "name", "table"))
    assert repr(p) == class_.__name__ + "(('name', 'table'))"
    assert str(p) == "name"


@pytest.mark.parametrize("class_", classes)
def test_two_elements(class_):
    p = class_(("name", "table"))
    assert repr(p) == class_.__name__ + "(('name', 'table'))"
    assert str(p) == "name"


@pytest.mark.parametrize("class_", classes)
def test_can_convert_to_str(class_):
    p = class_(("schema", "name", "table"))
    assert str(p) == "schema.name"


@pytest.mark.parametrize("class_", classes)
def test_keeps_double_quotes(class_):
    p = class_(('"schema"', '"name"', "table"))
    assert str(p) == '"schema"."name"'


@pytest.mark.parametrize("class_", classes)
def test_get_name(class_):
    p = class_(("schema", "name", "table"))
    assert p.name == "name"


@pytest.mark.parametrize("class_", classes)
def test_get_schema(class_):
    p = class_(("schema", '"name"', "table"))
    assert p.schema == "schema"


@pytest.mark.parametrize("class_", classes)
def test_get_kind(class_):
    p = class_(("schema", "name", "table"))
    assert p.kind == "table"
