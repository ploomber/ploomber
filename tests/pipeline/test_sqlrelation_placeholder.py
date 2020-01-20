from ploomber.templates.Placeholder import SQLRelationPlaceholder


def test_can_convert_to_str():
    p = SQLRelationPlaceholder(('schema', 'name', 'table'))
    assert str(p) == '"schema"."name"'


def test_ignores_double_quotes():
    p = SQLRelationPlaceholder(('"schema"', '"name"', 'table'))
    assert str(p) == '"schema"."name"'


def test_get_name():
    p = SQLRelationPlaceholder(('schema', '"name"', 'table'))
    assert p.name == 'name'


def test_get_schema():
    p = SQLRelationPlaceholder(('"schema"', '"name"', 'table'))
    assert p.schema == 'schema'


def test_get_kind():
    p = SQLRelationPlaceholder(('"schema"', '"name"', 'table'))
    assert p.kind == 'table'
