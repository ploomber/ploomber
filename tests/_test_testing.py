import psycopg2
from ploomber import postgres as pg
from ploomber.dag import DAG
from ploomber import Env
from ploomber import testing

env = Env()

dag = DAG()

pg.CLIENT = psycopg2.connect(dbname=env.db.dbname, host=env.db.host,
                             user=env.db.user, password=env.db.password)

# script does not create anything
p = pg.PostgresRelation(('public', 'wine', 'table'))


no_nas = testing.Postgres.no_nas_in_column('alcohol')
colors = testing.Postgres.distinct_values_in_column('color', {'red', 'white'})

assert no_nas(p)
assert colors(p)

pg.CLIENT.close()
