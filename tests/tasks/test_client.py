from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.tasks import (
    SQLDump,
    SQLTransfer,
    SQLUpload,
    PostgresCopyFrom,
    ShellScript,
    PythonCallable,
    SQLScript,
)
from ploomber.products import (
    File,
    SQLiteRelation,
    PostgresRelation,
    GenericSQLRelation,
    GenericProduct,
    SQLRelation,
)
from ploomber.util.dotted_path import DottedPath
from ploomber.exceptions import MissingClientError

# TODO: test error if no task and no dag level client duting init
# we'd have to check {Product, Task}.client during initfor this to work,
# the implementation used to do that but now that we have a client property
# client checking is delayed until accessing .client for the first time,
# perhaps we should check it after running init?
# TODO: maybe list all classes automatically to prevent listing one by one
# and having new ones tested


@pytest.mark.parametrize(
    "product_class, arg",
    [
        [SQLiteRelation, ["name", "schema", "table"]],
        [PostgresRelation, ["name", "schema", "table"]],
        [GenericSQLRelation, ["name", "schema", "table"]],
        [GenericProduct, "something"],
    ],
)
def test_exception_if_missing_product_client(product_class, arg):
    prod = product_class(arg)

    def fn():
        pass

    PythonCallable(fn, prod, dag=DAG())

    with pytest.raises(MissingClientError):
        prod.client


@pytest.mark.parametrize(
    "task_class, task_arg, product",
    [
        [SQLDump, "SELECT * FROM my_table", File("data.csv")],
        [
            SQLScript,
            "CREATE TABLE {{product}} AS SELECT * FROM my_table",
            SQLRelation(["schema", "name", "table"]),
        ],
        [
            SQLTransfer,
            "SELECT * FROM my_table",
            SQLiteRelation(["schema", "name", "table"]),
        ],
        [
            SQLUpload,
            "SELECT * FROM my_table",
            SQLiteRelation(["schema", "name", "table"]),
        ],
        [
            PostgresCopyFrom,
            "SELECT * FROM my_table",
            PostgresRelation(["schema", "name", "table"]),
        ],
    ],
)
def test_exception_if_missing_task_client(task_class, task_arg, product):
    task = task_class(task_arg, product, dag=DAG(), name="task")

    with pytest.raises(MissingClientError):
        task.client


@pytest.mark.parametrize(
    "product_class, arg",
    [
        [SQLiteRelation, ["name", "schema", "table"]],
        [PostgresRelation, ["name", "schema", "table"]],
        [GenericSQLRelation, ["name", "schema", "table"]],
        [GenericProduct, "something"],
        [File, "something"],
    ],
)
def test_resolve_client(tmp_directory, tmp_imports, product_class, arg):
    """
    Test tries to use task-level client, then dag-level client
    """
    Path("my_testing_client.py").write_text(
        """
def get():
    return 1
"""
    )

    task = product_class(arg, client=DottedPath("my_testing_client.get"))

    assert task.client == 1


@pytest.mark.parametrize(
    "task_class, task_arg, product",
    [
        [SQLDump, "SELECT * FROM my_table", File("data.csv")],
        [
            SQLScript,
            "CREATE TABLE {{product}} AS SELECT * FROM my_table",
            SQLRelation(["schema", "name", "table"]),
        ],
        [
            SQLTransfer,
            "SELECT * FROM my_table",
            SQLiteRelation(["schema", "name", "table"]),
        ],
        [
            SQLUpload,
            "SELECT * FROM my_table",
            SQLiteRelation(["schema", "name", "table"]),
        ],
        [
            PostgresCopyFrom,
            "SELECT * FROM my_table",
            PostgresRelation(["schema", "name", "table"]),
        ],
        [ShellScript, "touch {{product}}", File("data.csv")],
    ],
)
def test_initialize_task_level_client_with_dotted_spec_path(
    tmp_directory, tmp_imports, task_class, task_arg, product
):
    Path("my_testing_client.py").write_text(
        """
def get():
    return 1
"""
    )

    task = task_class(
        task_arg,
        product,
        DAG(),
        name="task",
        client=DottedPath("my_testing_client.get"),
    )

    assert task.client == 1
