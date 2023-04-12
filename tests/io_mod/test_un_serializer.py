import os
import json
import pickle
from pathlib import Path
from unittest.mock import Mock

import pytest
import joblib
import cloudpickle
import pandas as pd

from ploomber.io import serialize
from ploomber.io import unserialize
from ploomber.products import File, MetaProduct


def write_text(obj, product):
    Path(product).write_text(obj)


def write_json(obj, product):
    Path(product).write_text(json.dumps(obj))


def read_text(product):
    return Path(product).read_text()


def read_json(product):
    return json.loads(Path(product).read_text())


def serializer_undecorated(obj, product):
    raise NotImplementedError


# SERIALIZER


@serialize.serializer({".txt": write_text})
def serializer_txt(obj, product):
    raise NotImplementedError


@serialize.serializer({".txt": write_text, ".json": write_json})
def serializer_multi(obj, product):
    raise NotImplementedError


@serialize.serializer({".txt": write_text, ".json": write_json}, unpack=True)
def serializer_unpack(obj, product):
    raise NotImplementedError


def test_serialize_txt(tmp_directory):
    serializer_txt("something", File("a.txt"))
    assert Path("a.txt").read_text() == "something"


def test_serialize_fallback_invalid():
    with pytest.raises(ValueError) as excinfo:

        @serialize.serializer(fallback="invalid")
        def serializer_fallback(obj, product):
            raise NotImplementedError

    expected = (
        "Invalid fallback argument 'invalid' in "
        "function 'serializer_fallback'. Must be one of "
        "True, 'joblib', or 'cloudpickle'"
    )

    assert str(excinfo.value) == expected


def test_serialize_fallback(tmp_directory, monkeypatch):
    mock = Mock(wraps=pickle.dump)
    monkeypatch.setitem(serialize._EXTERNAL, True, mock)

    @serialize.serializer(fallback=True)
    def serializer_fallback(obj, product):
        raise NotImplementedError

    serializer_fallback(42, File("a.pkl"))

    mock.assert_called_once()
    assert pickle.loads(Path("a.pkl").read_bytes()) == 42


def test_serialize_fallback_joblib(tmp_directory, monkeypatch):
    mock = Mock(wraps=joblib.dump)
    monkeypatch.setitem(serialize._EXTERNAL, "joblib", mock)

    @serialize.serializer(fallback="joblib")
    def serializer_fallback_joblib(obj, product):
        raise NotImplementedError

    serializer_fallback_joblib(42, File("a.joblib"))

    mock.assert_called_once()
    assert joblib.load("a.joblib") == 42


def test_serialize_fallback_cloudpickle(tmp_directory, monkeypatch):
    mock = Mock(wraps=cloudpickle.dump)
    monkeypatch.setitem(serialize._EXTERNAL, "cloudpickle", mock)

    @serialize.serializer(fallback="cloudpickle")
    def serializer_fallback_cloudpickle(obj, product):
        raise NotImplementedError

    serializer_fallback_cloudpickle(42, File("a.cloudpickle"))

    mock.assert_called_once()
    assert cloudpickle.loads(Path("a.cloudpickle").read_bytes()) == 42


@pytest.mark.parametrize("provider", ["joblib", "cloudpickle"])
def test_serialize_missing_provider(monkeypatch, provider):
    monkeypatch.setitem(serialize._EXTERNAL, provider, None)

    with pytest.raises(ModuleNotFoundError) as excinfo:

        @serialize.serializer(fallback=provider)
        def serializer_fallback(obj, product):
            raise NotImplementedError

    expected = (
        f"Error serializing with function 'serializer_fallback'."
        f" {provider} is not installed"
    )
    assert str(excinfo.value) == expected


def test_serialize_multi(tmp_directory):
    serializer_multi("something", File("a.txt"))
    serializer_multi(dict(a=1, b=2), File("b.json"))

    assert Path("a.txt").read_text() == "something"
    assert json.loads(Path("b.json").read_text()) == dict(a=1, b=2)


def test_serialize_unpack(tmp_directory):
    serializer_unpack("something", File("something.txt"))
    serializer_unpack({"a.txt": "a", "b.txt": "b"}, File("directory"))

    assert Path("something.txt").read_text() == "something"
    assert Path("directory", "a.txt").read_text() == "a"
    assert Path("directory", "b.txt").read_text() == "b"


def test_serialize_unpack_deletes_file_if_exists(tmp_directory):
    Path("directory").touch()

    serializer_unpack({"a.txt": "a", "b.txt": "b"}, File("directory"))

    assert set(os.listdir("directory")) == {"a.txt", "b.txt"}


def test_serialize_unpack_deletes_previous_files(tmp_directory):
    Path("directory").mkdir()
    Path("directory", "c.txt").touch()

    serializer_unpack({"a.txt": "a", "b.txt": "b"}, File("directory"))

    assert set(os.listdir("directory")) == {"a.txt", "b.txt"}


def test_serialize_unpack_error_if_invalid_keys(tmp_directory):
    with pytest.raises(TypeError) as excinfo:
        serializer_unpack({1: "a"}, File("directory"))

    expected = (
        "Error creating output path from key with value 1: "
        "expected str, bytes or os.PathLike object, not int"
    )
    assert str(excinfo.value) == expected


def test_serialize_executes_function_if_no_suffix_match():
    with pytest.raises(NotImplementedError):
        serializer_txt("something", File("a.unknown"))


def test_signature_serializer():
    with pytest.raises(TypeError) as excinfo:

        @serialize.serializer(fallback=True)
        def serializer_wrong_signature(a, b, c):
            pass

    expected = (
        "Expected serializer 'serializer_wrong_signature' to "
        "take 2 arguments, but it takes 3"
    )
    assert str(excinfo.value) == expected


def test_serialize_multipe_products(tmp_directory):
    serializer_txt(
        {"a": "contents of a", "b": "contents of b"},
        MetaProduct({"a": "a.txt", "b": "b.txt"}),
    )

    assert Path("a.txt").read_text() == "contents of a"
    assert Path("b.txt").read_text() == "contents of b"


@pytest.mark.parametrize(
    "obj",
    [
        {
            "a": "a",
        },
        {
            "a": "a",
            "b": "b",
            "c": "c",
        },
        {
            "a": "a",
            "c": "c",
        },
    ],
    ids=["missing", "extra", "both"],
)
def test_serialize_multiple_products_validates_obj_keys(tmp_directory, obj):
    with pytest.raises(ValueError) as excinfo:
        serializer_txt(obj, MetaProduct({"a": "a.txt", "b": "b.txt"}))

    assert "Error serializing task" in str(excinfo.value)
    assert "with valid keys" in str(excinfo.value)


def test_serialize_multiple_products_validates_obj(tmp_directory):
    with pytest.raises(TypeError) as excinfo:
        serializer_txt(["a", "b"], MetaProduct({"a": "a.txt", "b": "b.txt"}))

    assert "Error serializing task: if task generates multiple" in str(excinfo.value)


def test_serialize_with_txt_default(tmp_directory):
    @serialize.serializer(defaults=[".txt"])
    def serializer(obj, product):
        raise NotImplementedError

    serializer("something", File("a.txt"))

    assert Path("a.txt").read_text() == "something"


def test_serialize_with_json_default(tmp_directory):
    @serialize.serializer(defaults=[".json"])
    def serializer(obj, product):
        raise NotImplementedError

    serializer(dict(a=1, b=2), File("a.json"))

    assert json.loads(Path("a.json").read_text()) == dict(a=1, b=2)


@pytest.mark.parametrize(
    "extension, reading_fn, kwargs",
    [
        [".csv", pd.read_csv, dict(index_col=0)],
        [".parquet", pd.read_parquet, dict()],
    ],
)
def test_serialize_with_df_default(tmp_directory, extension, reading_fn, kwargs):
    @serialize.serializer(defaults=[extension])
    def serializer(obj, product):
        raise NotImplementedError

    df = pd.DataFrame({"x": range(10)})

    serializer(df, File(f"a{extension}"))

    assert df.equals(reading_fn(f"a{extension}", **kwargs))


def test_serialize_with_txt_and_json_default(tmp_directory):
    @serialize.serializer(defaults=[".txt", ".json"])
    def serializer(obj, product):
        raise NotImplementedError

    serializer("something", File("a.txt"))
    serializer(dict(a=1, b=2), File("b.json"))

    assert Path("a.txt").read_text() == "something"
    assert json.loads(Path("b.json").read_text()) == dict(a=1, b=2)


@pytest.mark.parametrize("extension", [".txt", ".json", ".csv", ".parquet"])
def test_serializer_error_on_invalid_types(tmp_directory, extension):
    @serialize.serializer(defaults=[".txt", ".json", ".csv", ".parquet"])
    def serializer(obj, product):
        raise NotImplementedError

    with pytest.raises(TypeError) as excinfo:
        serializer(object(), File(f"a{extension}"))

    assert f"Error serializing product File('a{extension}') to {extension}" in str(
        excinfo.value
    )


def test_serializer_pickle(tmp_directory):
    serialize.serializer_pickle(1, File("number"))
    serialize.serializer_pickle(dict(a=1, b=2), File("dict"))

    assert pickle.loads(Path("number").read_bytes()) == 1
    assert pickle.loads(Path("dict").read_bytes()) == dict(a=1, b=2)


# UNSERIALIZER


@unserialize.unserializer({".txt": read_text})
def unserializer_txt(obj):
    raise NotImplementedError


@unserialize.unserializer({".txt": read_text, ".json": read_json})
def unserializer_multi(obj):
    raise NotImplementedError


@unserialize.unserializer({".txt": read_text, ".json": read_json}, unpack=True)
def unserializer_unpack(obj):
    raise NotImplementedError


def unserializer_undecorated(obj):
    raise NotImplementedError


def test_unserialize_txt(tmp_directory):
    Path("a.txt").write_text("something")

    obj = unserializer_txt(File("a.txt"))
    assert obj == "something"


def test_unserialize_fallback_invalid():
    with pytest.raises(ValueError) as excinfo:

        @unserialize.unserializer(fallback="invalid")
        def unserializer_fallback(product):
            raise NotImplementedError

    expected = (
        "Invalid fallback argument 'invalid' in "
        "function 'unserializer_fallback'. Must be one of "
        "True, 'joblib', or 'cloudpickle'"
    )

    assert str(excinfo.value) == expected


def test_unserialize_fallback(tmp_directory, monkeypatch):
    mock = Mock(wraps=pickle.load)
    monkeypatch.setitem(unserialize._EXTERNAL, True, mock)
    Path("a.pkl").write_bytes(pickle.dumps(42))

    @unserialize.unserializer(fallback=True)
    def unserializer_fallback(product):
        raise NotImplementedError

    obj = unserializer_fallback(File("a.pkl"))
    assert obj == 42
    mock.assert_called_once()


def test_unserialize_fallback_joblib(tmp_directory, monkeypatch):
    mock = Mock(wraps=joblib.load)
    monkeypatch.setitem(unserialize._EXTERNAL, "joblib", mock)
    with open("a.joblib", "wb") as f:
        joblib.dump(42, f)

    @unserialize.unserializer(fallback="joblib")
    def unserializer_fallback(product):
        raise NotImplementedError

    obj = unserializer_fallback(File("a.joblib"))
    assert obj == 42
    mock.assert_called_once()


def test_unserialize_fallback_cloudpickle(tmp_directory, monkeypatch):
    mock = Mock(wraps=cloudpickle.load)
    monkeypatch.setitem(unserialize._EXTERNAL, True, mock)
    Path("a.pkl").write_bytes(cloudpickle.dumps(42))

    @unserialize.unserializer(fallback=True)
    def unserializer_fallback(product):
        raise NotImplementedError

    obj = unserializer_fallback(File("a.pkl"))
    assert obj == 42
    mock.assert_called_once()


@pytest.mark.parametrize("provider", ["joblib", "cloudpickle"])
def test_unserialize_missing_provider(monkeypatch, provider):
    monkeypatch.setitem(unserialize._EXTERNAL, provider, None)

    with pytest.raises(ModuleNotFoundError) as excinfo:

        @unserialize.unserializer(fallback=provider)
        def unserializer_fallback(product):
            raise NotImplementedError

    expected = (
        f"Error unserializing with function 'unserializer_fallback'."
        f" {provider} is not installed"
    )
    assert str(excinfo.value) == expected


def test_unserialize_multi(tmp_directory):
    Path("a.txt").write_text("something")
    Path("b.json").write_text(json.dumps(dict(a=1, b=2)))

    obj_txt = unserializer_multi(File("a.txt"))
    obj_json = unserializer_multi(File("b.json"))

    assert obj_txt == "something"
    assert obj_json == dict(a=1, b=2)


def test_unserialize_unpack(tmp_directory):
    Path("directory").mkdir()
    Path("directory", "a.txt").write_text("something")
    Path("directory", ".b.json").write_text(json.dumps(dict(a=1, b=2)))

    obj = unserializer_unpack(File("directory"))

    assert obj == {"a.txt": "something", ".b.json": dict(a=1, b=2)}


def test_unserialize_executes_function_if_no_suffix_match():
    with pytest.raises(NotImplementedError):
        unserializer_txt(File("a.unknown"))


def test_signature_unserializer():
    with pytest.raises(TypeError) as excinfo:

        @unserialize.unserializer(fallback=True)
        def unserializer_wrong_signature(a, b, c):
            pass

    expected = (
        "Expected unserializer 'unserializer_wrong_signature' to "
        "take 1 argument, but it takes 3"
    )
    assert str(excinfo.value) == expected


def test_unserialize_multipe_products(tmp_directory):
    Path("a.txt").write_text("contents of a")
    Path("b.txt").write_text("contents of b")

    obj = unserializer_txt(MetaProduct({"a": "a.txt", "b": "b.txt"}))

    assert obj == {"a": "contents of a", "b": "contents of b"}


def test_unserialize_with_txt_default(tmp_directory):
    @unserialize.unserializer(defaults=[".txt"])
    def unserializer(product):
        raise NotImplementedError

    Path("a.txt").write_text("something")

    assert unserializer(File("a.txt")) == "something"


def test_unserialize_with_json_default(tmp_directory):
    @unserialize.unserializer(defaults=[".json"])
    def unserializer(product):
        raise NotImplementedError

    Path("a.json").write_text(json.dumps(dict(a=1, b=2)))

    assert unserializer(File("a.json")) == dict(a=1, b=2)


@pytest.mark.parametrize(
    "extension, serializing_fn",
    [
        [".csv", "to_csv"],
        [".parquet", "to_parquet"],
    ],
)
def test_unserialize_with_df_default(tmp_directory, extension, serializing_fn):
    @unserialize.unserializer(defaults=[extension])
    def unserializer(product):
        raise NotImplementedError

    df = pd.DataFrame({"x": range(10)})
    getattr(df, serializing_fn)(f"a{extension}", index=False)

    assert df.equals(unserializer(File(f"a{extension}")))


@pytest.mark.parametrize(
    "extension, serializing_fn",
    [
        [".csv", "to_csv"],
        [".parquet", "to_parquet"],
    ],
)
def test_unserialize_error_if_pandas_missing(
    tmp_directory, extension, serializing_fn, monkeypatch
):
    monkeypatch.setattr(unserialize, "pd", None)

    @unserialize.unserializer(defaults=[extension])
    def unserializer(product):
        raise NotImplementedError

    with pytest.raises(ModuleNotFoundError) as excinfo:
        unserializer(File(f"a{extension}"))

    expected = (
        f"Error using {extension} default unserializer: " "pandas is not installed"
    )
    assert str(excinfo.value) == expected


def test_unserialize_with_txt_and_json_default(tmp_directory):
    @unserialize.unserializer(defaults=[".txt", ".json"])
    def unserializer(product):
        raise NotImplementedError

    Path("a.txt").write_text("something")
    Path("a.json").write_text(json.dumps(dict(a=1, b=2)))

    assert unserializer(File("a.txt")) == "something"
    assert unserializer(File("a.json")) == dict(a=1, b=2)


def test_unserializer_pickle(tmp_directory):
    Path("number").write_bytes(pickle.dumps(1))
    Path("dict").write_bytes(pickle.dumps(dict(a=1, b=2)))

    assert unserialize.unserializer_pickle(File("number")) == 1
    assert unserialize.unserializer_pickle(File("dict")) == dict(a=1, b=2)


# BOTH


@pytest.mark.parametrize(
    "decorator_factory, name",
    [
        [unserialize.unserializer, "unserializer"],
        [serialize.serializer, "serializer"],
    ],
)
def test_un_serializer_error_on_default_invalid_value(decorator_factory, name):
    decorator = decorator_factory(defaults=[".invalid", "another"])

    with pytest.raises(ValueError) as excinfo:

        @decorator
        def my_fn():
            pass

    assert f"Error when adding @{name}" in str(excinfo.value)
    assert "my_fn" in str(excinfo.value)
    assert str({".invalid", "another"}) in str(excinfo.value) or str(
        {"another", ".invalid"}
    ) in str(excinfo.value)


@pytest.mark.parametrize(
    "decorator_factory, name",
    [
        [unserialize.unserializer, "unserializer"],
        [serialize.serializer, "serializer"],
    ],
)
def test_un_serializer_error_on_extension_mapping_and_defaults_overlap(
    decorator_factory, name
):
    decorator = decorator_factory(
        {
            ".txt": None,
            ".csv": None,
            ".pkl": None,
        },
        defaults=[".txt", ".csv"],
    )

    with pytest.raises(ValueError) as excinfo:

        @decorator
        def my_fn():
            pass

    assert f"Error when adding @{name}" in str(excinfo.value)
    assert "my_fn" in str(excinfo.value)
    assert str({".txt", ".csv"}) in str(excinfo.value) or str({".csv", ".txt"}) in str(
        excinfo.value
    )


@pytest.mark.parametrize(
    "decorator_factory, fn",
    [
        [serialize.serializer, serializer_undecorated],
        [unserialize.unserializer, unserializer_undecorated],
    ],
)
def test_validates_extension_mapping_type(decorator_factory, fn):
    decorator = decorator_factory([".csv"])

    with pytest.raises(TypeError) as excinfo:
        decorator(fn)

    assert "Invalid extension_mapping" in str(excinfo.value)


@pytest.mark.parametrize(
    "decorator_factory, fn",
    [
        [serialize.serializer, serializer_undecorated],
        [unserialize.unserializer, unserializer_undecorated],
    ],
)
def test_validates_extension_mapping_keys(decorator_factory, fn):
    decorator = decorator_factory({"csv": None})

    with pytest.raises(ValueError) as excinfo:
        decorator(fn)

    assert "Invalid extension_mapping" in str(excinfo.value)


@pytest.mark.parametrize(
    "decorator_factory, fn",
    [
        [serialize.serializer, serializer_undecorated],
        [unserialize.unserializer, unserializer_undecorated],
    ],
)
def test_validates_defaults_type(decorator_factory, fn):
    decorator = decorator_factory(defaults=".csv")

    with pytest.raises(TypeError) as excinfo:
        decorator(fn)

    assert "Invalid defaults" in str(excinfo.value)
