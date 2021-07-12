from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.tasks import (SQLDump, SQLTransfer, SQLUpload, PostgresCopyFrom)
from ploomber.products import (File, SQLiteRelation, PostgresRelation)
from ploomber.util.dotted_path import DottedPathSpec

# TODO: test error if no task and no dag level client duting init
# TODO: if client is a dotted_path spec, check the output type


def test_resolve_client():
    """
    Test tries to use task-level client, then dag-level client
    """
    pass


@pytest.mark.parametrize(
    'task_class, product',
    [
        [SQLDump, File('data.csv')],
        # [SQLScript, SQLRelation(['schema', 'name', 'table'])],
        [SQLTransfer, SQLiteRelation(['schema', 'name', 'table'])],
        [SQLUpload, SQLiteRelation(['schema', 'name', 'table'])],
        [PostgresCopyFrom,
         PostgresRelation(['schema', 'name', 'table'])]
    ])
def test_initialize_task_level_client_with_dotted_spec_path(
        tmp_directory, tmp_imports, task_class, product):
    Path('my_testing_client.py').write_text("""
def get():
    return 1
""")

    task = task_class('SELECT * FROM my_table',
                      product,
                      DAG(),
                      name='task',
                      client=DottedPathSpec('my_testing_client.get'))

    assert task.client == 1
