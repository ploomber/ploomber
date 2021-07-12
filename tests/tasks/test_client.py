from pathlib import Path

import pytest

from ploomber import DAG
from ploomber.tasks import (SQLDump, SQLTransfer, SQLUpload, PostgresCopyFrom,
                            ShellScript)
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
    'task_class, task_arg, product',
    [
        [SQLDump, 'SELECT * FROM my_table',
         File('data.csv')],
        # [SQLScript, SQLRelation(['schema', 'name', 'table'])],
        [
            SQLTransfer, 'SELECT * FROM my_table',
            SQLiteRelation(['schema', 'name', 'table'])
        ],
        [
            SQLUpload, 'SELECT * FROM my_table',
            SQLiteRelation(['schema', 'name', 'table'])
        ],
        [
            PostgresCopyFrom, 'SELECT * FROM my_table',
            PostgresRelation(['schema', 'name', 'table'])
        ],
        [ShellScript, 'touch {{product}}',
         File('data.csv')]
    ])
def test_initialize_task_level_client_with_dotted_spec_path(
        tmp_directory, tmp_imports, task_class, task_arg, product):
    Path('my_testing_client.py').write_text("""
def get():
    return 1
""")

    task = task_class(task_arg,
                      product,
                      DAG(),
                      name='task',
                      client=DottedPathSpec('my_testing_client.get'))

    assert task.client == 1
