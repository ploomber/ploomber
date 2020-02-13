"""
Communicating pipelines
=======================

Being able to explain how a pipeline works is critical so any stakeholder
involved is aware of the pipeline's logic and required resources. While the
pipeline declaration provides a blueprint for developers, it is not well
suited for communication with non-technical partners (or even technical ones
that are not familiar with ploomber). For that reason, ploomber provides ways
of easily communicating important pipeline details to a wider audience.
"""

###############################################################################
# Setup
# -----
from pathlib import Path
import tempfile

import pandas as pd
from sklearn import datasets
from IPython.display import HTML

from ploomber import DAG
from ploomber.tasks import PythonCallable, SQLUpload, SQLScript
from ploomber.products import SQLiteRelation, File
from ploomber.clients import SQLAlchemyClient


tmp_dir = Path(tempfile.mkdtemp())
db_uri = 'sqlite:///' + str(tmp_dir / 'my_db.db')
client = SQLAlchemyClient(db_uri)


###############################################################################
# Pipeline declaration
# --------------------

def _get_data(product):
    wine = datasets.load_wine()
    df = pd.DataFrame(wine.data)
    df.columns = wine.feature_names
    df['label'] = wine.target
    df.to_parquet(str(product))


dag = DAG()
dag.clients[SQLUpload] = client
dag.clients[SQLiteRelation] = client
dag.clients[SQLScript] = client

get_data = PythonCallable(_get_data,
                          product=File(tmp_dir / 'wine.parquet'),
                          dag=dag,
                          name='get')

upload = SQLUpload('{{upstream["get"]}}',
                   product=SQLiteRelation((None, 'wine', 'table')),
                   dag=dag,
                   name='upload')


###############################################################################
# In a real project, your SQL scripts should be separate files, we include
# this here to make this example standalone. SQL is a language that people
# from a lot of backgrounds understand, you could easily communicate your
# analysis with business analysts to make your your data assumptions are
# correct
_clean = """
/*
Cleaning dataset, we decided to ignore rows where magnesium is over 100
since we believe the data is corrupted
*/
CREATE TABLE {{product}} AS
SELECT * FROM {{upstream['upload']}}
WHERE magnesium < 100.0
"""

clean = SQLScript(_clean,
                  product=SQLiteRelation((None, 'wine_clean', 'table')),
                  dag=dag,
                  name='clean')


get_data >> upload >> clean

###############################################################################
# Pipeline build
# --------------

dag.build()


###############################################################################
# Pipeline plot
# -------------

dag.plot(output='matplotlib')

###############################################################################
# Generate report
# ---------------
# Extract all information in the pipeline declaration and generate a summary
# in HTML format. Since the report contains the pipeline's plot, summary
# and code, it is ot only useful to communicate with stakeholders but also
# an good tool for new developers joining the project to quickly have an
# pipeline visual overview.

html = dag.to_markup()


###############################################################################
# Report
# ------
# `Click here to see the generated report.  <../communicate-report.html>`_

# do not run these lines. they just move the report to make the above link work
out = Path('../doc/_build/html/communicate-report.html')

if out.parent.exists():
    out.write_text(html)
