from datetime import datetime
from pathlib import Path

from ploomber.products import SQLiteRelation
from ploomber.clients import SQLAlchemyClient

import pandas as pd
import numpy as np


def test_sqlite_product_exists(tmp_directory):
    """

    >>> import tempfile
    >>> tmp_directory = tempfile.mkdtemp()
    """
    tmp = Path(tmp_directory)

    # create a db
    conn = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database.db"))

    numbers = SQLiteRelation((None, 'numbers', 'table'), conn)
    numbers.render({})

    assert not numbers.exists()

    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn.engine)

    assert numbers.exists()


def test_sqlite_product_delete(tmp_directory):
    """
    >>> import tempfile
    >>> tmp_directory = tempfile.mkdtemp()
    """
    tmp = Path(tmp_directory)
    conn = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database.db"))

    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn.engine)

    numbers = SQLiteRelation((None, 'numbers', 'table'), conn)
    numbers.render({})
    numbers.delete()

    assert not numbers.exists()


def test_sqlite_product_fetch_metadata_none_if_not_exists(tmp_directory):
    tmp = Path(tmp_directory)
    conn = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database.db"))

    numbers = SQLiteRelation((None, 'numbers', 'table'), conn)
    numbers.render({})

    assert numbers.fetch_metadata() is None


def test_sqlite_product_fetch_metadata_none_if_empty_metadata(tmp_directory):
    tmp = Path(tmp_directory)
    conn = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database.db"))

    df = pd.DataFrame({'a': np.arange(0, 100), 'b': np.arange(100, 200)})
    df.to_sql('numbers', conn.engine)

    numbers = SQLiteRelation((None, 'numbers', 'table'), conn)
    numbers.render({})

    assert numbers.fetch_metadata() is None


def test_sqlite_product_save_metadata(tmp_directory):
    tmp = Path(tmp_directory)
    conn = SQLAlchemyClient('sqlite:///{}'.format(tmp / "database.db"))

    numbers = SQLiteRelation((None, 'numbers', 'table'), conn)
    numbers.render({})

    metadata_new = {'timestamp': datetime.now().timestamp(),
                    'stored_source_code': 'some code'}

    numbers.save_metadata(metadata_new)

    fetched = numbers.fetch_metadata()

    assert fetched == metadata_new
