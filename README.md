<p align="center" width="100%">
  <img src="https://ploomber.io/ploomber-logo.png" height="250">
</p>


[![CI Linux](https://github.com/ploomber/ploomber/workflows/CI%20Linux/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20Linux/badge.svg)
[![CI macOS](https://github.com/ploomber/ploomber/workflows/CI%20macOS/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20macOS/badge.svg)
[![CI Windows](https://github.com/ploomber/ploomber/workflows/CI%20Windows/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20Windows/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/ploomber/badge/?version=latest)](https://ploomber.readthedocs.io/en/latest/?badge=latest)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fspec-api-python%252FREADME.ipynb%26branch%3Dmaster)
[![Deepnote](https://deepnote.com/buttons/launch-in-deepnote-small.svg)](https://deepnote.com/launch?template=deepnote&url=https://github.com/ploomber/projects/blob/master/spec-api-python/README.ipynb)
[![PyPI](https://badge.fury.io/py/ploomber.svg)](https://badge.fury.io/py/ploomber)
[![Coverage](https://coveralls.io/repos/github/ploomber/ploomber/badge.svg?branch=master)](https://coveralls.io/github/ploomber/ploomber?branch=master)


![Diagram](https://ploomber.io/main-diagram.png)

Ploomber is the simplest way to build reliable data pipelines for Data
Science and Machine Learning. Provide your source code in a standard
form and Ploomber will automatically construct the pipeline for you.
Tasks can be anything from Python functions, Jupyter notebooks,
Python/R/shell scripts, and SQL scripts.

Once your pipeline is constructed, you'll be equipped with lots of development features to experiment faster. When you're ready, deploy to Airflow or
Kubernetes (using Argo) without code changes.

Here's how a pipeline task looks like:

<table>

<tr>
<th>Function</th>
<th>Jupyter notebook or Python script</th>
<th>SQL script</th>
<th>Pipeline declaration</th>
</tr>

<tr>

<td valign="top">

```python
def clean_users(product, upstream):
    # runs 'get_users' before this task and
    # passes the output location
    df = pd.read_csv(upstream['get_users'])

    # your code here...

    # save output using the provided
    # product variable
    df.to_csv(product)
```
</td>

<td valign="top">

```python
# + tags=["parameters"]
# run 'clean users' and 'clean_activity'
# before this task
upstream = ['clean_users', 'clean_activity']
# -

# a new code cell is injected here with
# the output location of this task
# (product) and dependencies: 'clean_users,
# 'clean_activity'

# your code here...

# save output using the provided product variable
Path(product).write_bytes(pickle.dumps(model))
```
</td>

<td valign="top">

```sql
-- {{product}} is replaced by the table name
CREATE TABLE AS {{product}}
/*
runs 'raw_data' before this task and replace
{{upstream['raw_data']}} with table name
*/
SELECT * FROM {{upstream['raw_data']}}
```
</td>


<td valign="top">

```yaml
tasks:
  # script
  - source: scripts/get_users.py
    product: output/users-raw.csv

  # function
  - source: functions.clean_users
    product: output/users-clean.csv

  # notebook
  - source: notebooks/model-template.ipynb
    product:
      model: output/model.pickle
      nb: output/model-evaluation.html
```

</td>

</tr>

</table>

To run your pipeline, just call `ploomber build`. For full flexibility, you can directly use the Python API. [Click here to see an
example](https://github.com/ploomber/projects/blob/master/ml-advanced/src/ml_advanced/pipeline.py).

[Watch JupyterCon 2020
talk](https://www.youtube.com/watch?v=M6mtgPfsA3M)

Main features
-------------

1.  **Jupyter integration**. When you open your notebooks, Ploomber will
    automatically inject a new cell with the location of your input
    files, as inferred from your `upstream` variable. If you open a
    Python or R script, it will be converted to a notebook on the fly.
2.  **Incremental builds**. Speed up execution by skipping tasks whose
    source code hasn't changed.
3.  **Parallelization**. Run tasks in parallel to speed up computations.
4.  **Pipeline testing**. Run tests upon task execution to verify that
    the output data has the right properties (e.g. values within
    expected range).
5.  **Pipeline inspection**. Start an interactive session with
    `ploomber interact` to debug your pipeline. Call
    `dag['task_name'].debug()` to start a debugging session.
6.  **[Beta] Deployment to Kubernetes and Airflow**. You can develop
    and execute locally. But if you want to scale things up, deploy to
    [Kubernetes or Airflow](https://github.com/ploomber/soopervisor)

Try it out!
-----------

You can choose from one of the hosted options (no installation
required):

[![image](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fspec-api-python%252FREADME.ipynb%26branch%3Dmaster)
[![image](https://deepnote.com/buttons/launch-in-deepnote-small.svg)](https://deepnote.com/launch?template=deepnote&url=https://github.com/ploomber/projects/blob/master/spec-api-python/README.ipynb)

Or run an example locally:

```sh
# clone the sample projects
git clone https://github.com/ploomber/projects

# move to the machine learning pipeline example
cd projects/spec-api-python

# install dependencies
# 1) if you have conda installed
conda env create -f environment.yml
conda activate spec-api-python
# 2) if you don't have conda
pip install ploomber pandas scikit-learn pyarrow sklearn-evaluation

# open README.ipynb or execute the following commands in the terminal...

# create output folder
mkdir output

# run the pipeline
ploomber build    
```

When execution finishes, you\'ll see the output in the `output/` folder.

More examples available [here](https://github.com/ploomber/projects).

Installation
------------

```sh
pip install ploomber
```

Compatible with Python 3.6 and higher.

How does Ploomber compare to X?
-------------------------------

Ploomber has two goals:

1. Provide a great development experience for
Data Science/Machine learning projects, which require a lot of
experimentation/iteration: incremental builds and Jupyter integration are
a fundamental part of this.
2. Integrate with deployment tools (Airflow and Argo) to streamline deployment.

For a complete comparison, read our
[survey on workflow management tools](https://ploomber.io/posts/survey/).

Resources
---------

-   [Sample projects (Machine Learning pipeline, ETL, among
    others)](https://github.com/ploomber/projects)
-   [Documentation](https://ploomber.readthedocs.io/)
