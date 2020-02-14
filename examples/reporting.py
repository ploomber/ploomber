"""
Reporting using Jupyter notebooks
=================================

ploomber is great for reporting pipelines, instead of having a big script or
notebook that contains the entire pipeline, you can break down logic in small
tasks, whenever you apply a change to any of the steps, you simply have to
build the pipeline again and you will get a new report.
"""

###############################################################################
# Pipeline tasks
# --------------

from pathlib import Path
import tempfile

import pandas as pd
from sklearn import datasets

from ploomber import DAG
from ploomber.tasks import PythonCallable, NotebookRunner
from ploomber.products import File


def _get_data(product):
    data = datasets.load_boston()
    df = pd.DataFrame(data.data)
    df.columns = data.feature_names
    df['price'] = data.target
    df.to_parquet(str(product))


def _clean_data(upstream, product):
    df = pd.read_parquet(str(upstream['raw']))
    clean = df[df.AGE > 50]
    clean.to_parquet(str(product))

###############################################################################
# In a real project, you want so separate your code in files and import them
# to declare your pipeline. For this example we declare it here.
# NotebookRunner supports any input format that jupytext supports and any
# output format that nbconverter does.


report = """
# +
# This file is in jupytext light format
import seaborn as sns
import pandas as pd
# -

# + tags=['parameters']
# papermill will add the parameters below this cell
# upstream = None
# product = None
# -

# +
path = upstream['clean']
df = pd.read_parquet(path)
# -

# ## AGE distribution

# +
_ = sns.distplot(df.AGE)
# -

# ## Price distribution

# +
_ = sns.distplot(df.price)
# -
"""

###############################################################################
# Pipeline declaration
# --------------------

tmp_dir = Path(tempfile.mkdtemp())

dag = DAG()

get_data = PythonCallable(_get_data,
                          product=File(tmp_dir / 'raw.parquet'),
                          dag=dag,
                          name='raw')

clean_data = PythonCallable(_clean_data,
                            product=File(tmp_dir / 'clean.parquet'),
                            dag=dag,
                            name='clean')


report = NotebookRunner(report,
                        product=File(tmp_dir / 'report.html'),
                        dag=dag,
                        name='report',
                        kernelspec_name='python3',
                        ext_in='py')


get_data >> clean_data >> report


dag.build()

###############################################################################
# Pipeline plot
# -------------

dag.plot(output='matplotlib')

###############################################################################
# Output
# ------
# `Click here to see the generated report.  <../_static/reporting-nb.html>`_

# do not run these lines. they just move the report to make the above link work
doc = Path('../doc')

if doc.exists():
    out = doc / '_static/reporting-nb.html'
    html = Path(str(report.product)).read_text()
    out.write_text(html)
