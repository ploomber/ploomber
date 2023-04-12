from unittest.mock import Mock

import pytest
import pandas as pd

from ploomber import DAG
from ploomber.tasks import SQLDump, NotebookRunner, PythonCallable
from ploomber.products import File


def test_unsupported_extension():
    task = SQLDump(
        "SELECT * FROM table", File("my_file.json"), DAG(), name="task", client=Mock()
    )

    with pytest.raises(NotImplementedError):
        task.load()


@pytest.mark.parametrize(
    "product, kwargs",
    [
        [File("my_file.csv"), dict()],
        [File("my_file.csv"), dict(sep=",")],
    ],
    ids=["simple", "with-kwargs"],
)
def test_sqldump(product, kwargs, tmp_directory):
    df = pd.DataFrame({"a": [1, 2, 3]})
    df.to_csv("my_file.csv", index=False)
    task = SQLDump("SELECT * FROM table", product, DAG(), name="task", client=Mock())

    loaded = task.load(**kwargs)
    assert df.equals(loaded)


def test_notebookrunner(tmp_directory):
    df = pd.DataFrame({"a": [1, 2, 3]})
    df.to_csv("my_file.csv", index=False)

    task = NotebookRunner(
        '# +tags=["parameters"]',
        {"nb": File("nb.ipynb"), "data": File("my_file.csv")},
        DAG(),
        name="task",
        ext_in="py",
    )

    loaded = task.load("data")
    assert df.equals(loaded)


# PythonCallable does not use FileLoaderMixin directly because it gives
# preference to task.unserializer, but if it doesn't exist, it uses
# FileLoaderMixin internal API


@pytest.mark.parametrize(
    "product, kwargs",
    [
        [File("my_file.csv"), dict()],
        [File("my_file.csv"), dict(sep=",")],
        [{"a": File("my_file.csv"), "b": File("another.csv")}, dict(key="a")],
    ],
    ids=["simple", "with-kwargs", "with-multiple-products"],
)
def test_pythoncallable(tmp_directory, product, kwargs):
    df = pd.DataFrame({"a": [1, 2, 3]})
    df.to_csv("my_file.csv", index=False)

    def callable_(product):
        pass

    task = PythonCallable(callable_, product, DAG(), name="task")

    loaded = task.load(**kwargs)
    assert df.equals(loaded)
