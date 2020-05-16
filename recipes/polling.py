"""
A pipeline that polls a database for new observations, process them and
inserts them to a table
"""

# +
import shutil
from pathlib import Path
import tempfile
import pandas as pd

from sqlalchemy import create_engine

from ploomber import DAG
from ploomber.tasks import SQLDump, PythonCallable, SQLUpload
from ploomber.products import File, SQLiteRelation
from ploomber.clients import SQLAlchemyClient
from ploomber.exceptions import DAGBuildEarlyStop


def make(tmp):
    """Make the dag
    """
    tmp = Path(tmp)
    dag = DAG()

    # db with the source data
    client_source = SQLAlchemyClient('sqlite:///' + str(tmp / 'source.db'))
    # db where we'll insert processed data (can be the same as the
    # source db)
    client_target = SQLAlchemyClient('sqlite:///' + str(tmp / 'target.db'))

    dag.clients[SQLDump] = client_source
    dag.clients[SQLUpload] = client_target
    dag.clients[SQLiteRelation] = client_target

    # we dump new observations to this file
    dumped_data = File(tmp / 'x.csv')
    # we add a hook that allows us to save info on the latest seen value
    dumped_data.prepare_metadata = add_last_value

    # the actual task that dumps data
    dump = SQLDump("""
    -- if the product (x.csv file) already has metadata (it has run before)
    -- use the value in the metadata to only get row above such value
    {% if product.metadata.timestamp %}
        SELECT * FROM data
        WHERE x > {{product.metadata['last_value']}}
    -- if there is no metadata, just get everything
    {% else %}
        SELECT * FROM  data
    {% endif %}
    """, dumped_data, dag=dag, name='dump', chunksize=None)

    # on finish hook, will stop DAG execution if there aren't new observations
    dump.on_finish = dump_on_finish

    # a dummy task to modify the data
    plus_one = PythonCallable(_plus_one, File(tmp / 'plus_one.csv'),
                              dag=dag, name='plus_one')

    # upload the data to the target database
    upload = SQLUpload('{{upstream["plus_one"]}}',
                       product=SQLiteRelation((None, 'plus_one', 'table')),
                       dag=dag,
                       name='upload',
                       # append observations if the table already exists
                       to_sql_kwargs={'if_exists': 'append', 'index': False})

    dump >> plus_one >> upload

    return dag


def add_last_value(metadata, product):
    """Hook to save last value downloaded before saving metadata
    """
    df = pd.read_csv(str(product))

    # data has not been changed (the query triggered downloading 0 rows),
    # do not alter metadata
    if not df.shape[0]:
        return metadata

    # got some data...

    new_max = int(df.x.max())

    # if running this for the first time or last value it's bigger than the
    # one we saved, save a new last_value
    if ('last_value' not in metadata
            or new_max > metadata['last_value']):
        metadata['last_value'] = new_max
        return metadata


def _plus_one(product, upstream):
    """A function that adds 1 to column x
    """
    df = pd.read_csv(str(upstream['dump']))
    df['x'] = df.x + 1
    df.to_csv(str(product), index=False)


def dump_on_finish(product):
    df = pd.read_csv(str(product))

    # if we dumped data but got no new observations, stop execution gracefully
    if not df.shape[0]:
        raise DAGBuildEarlyStop('No new observations')


# create dag
tmp = tempfile.mkdtemp()
dag = make(tmp)


# add some sample data to the database
engine = create_engine('sqlite:///' + str(Path(tmp, 'source.db')))
df = pd.DataFrame({'x': range(10)})
df.to_sql('data', engine)

target = create_engine('sqlite:///' + str(Path(tmp, 'target.db')))

# run dag, should pull this first 10 observations
dag.build(force=True)

# checking downloaded data with the plus one added
df = pd.read_sql('SELECT * FROM plus_one', target)
assert df.x.max() == 10 and df.shape[0] == 10
df

# run the dag again, this time plus one should be the same
dag.build(force=True)

df = pd.read_sql('SELECT * FROM plus_one', target)
assert df.x.max() == 10 and df.shape[0] == 10
df


# simulate new data arrival
df = pd.DataFrame({'x': range(10)})
df['x'] = df.x + 10
df.to_sql('data', engine, if_exists='append')

# +
# should only get new rows
dag.build(force=True)

df = pd.read_sql('SELECT * FROM plus_one', target)
assert df.x.max() == 20 and df.shape[0] == 20
df
# -


shutil.rmtree(tmp)
