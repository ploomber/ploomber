import pytest
from pathlib import Path
from ploomber.env.frozenjson import FrozenJSON


def test_init_from_dict():
    d_raw = {"a": {"b": 1}}
    d = FrozenJSON(d_raw)

    assert isinstance(d.a, FrozenJSON)
    assert d.a.b == 1
    assert d["a"]["b"] == 1
    assert str(d) == str(d_raw)


def test_init_from_yaml(tmp_directory):
    Path("some.yaml").write_text("a:\n  b: 1")

    d = FrozenJSON.from_yaml("some.yaml")

    assert isinstance(d.a, FrozenJSON)
    assert d.a.b == 1
    assert d["a"]["b"] == 1
    assert str(d) == "some.yaml"


def test_raises_key_error():
    d = FrozenJSON({"a": 1})

    with pytest.raises(KeyError):
        d.b

    with pytest.raises(KeyError):
        d["b"]
