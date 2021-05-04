<p align="center" width="100%">
  <img src="https://ploomber.io/ploomber-logo.png" height="250">
</p>

[![CI Linux](https://github.com/ploomber/ploomber/workflows/CI%20Linux/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20Linux/badge.svg)
[![CI macOS](https://github.com/ploomber/ploomber/workflows/CI%20macOS/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20macOS/badge.svg)
[![CI Windows](https://github.com/ploomber/ploomber/workflows/CI%20Windows/badge.svg)](https://github.com/ploomber/ploomber/workflows/CI%20Windows/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/ploomber/badge/?version=latest)](https://ploomber.readthedocs.io/en/latest/?badge=latest)
[![PyPI](https://badge.fury.io/py/ploomber.svg)](https://badge.fury.io/py/ploomber)
[![Coverage](https://coveralls.io/repos/github/ploomber/ploomber/badge.svg?branch=master)](https://coveralls.io/github/ploomber/ploomber?branch=master)
[![Twitter](https://img.shields.io/twitter/follow/edublancas?label=Follow&style=social)](https://twitter.com/intent/user?screen_name=edublancas)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fspec-api-python%252FREADME.ipynb%26branch%3Dmaster)
[![Deepnote](https://deepnote.com/buttons/launch-in-deepnote-small.svg)](https://deepnote.com/launch?template=deepnote&url=https://github.com/ploomber/projects/blob/master/spec-api-python/README.ipynb)


![Diagram](https://ploomber.io/main-diagram.png)

Ploomber is the simplest way to build reliable data pipelines for Data
Science and Machine Learning. Provide your source code in a standard
form, and Ploomber automatically constructs the pipeline for you.
Tasks can be anything from Python functions, Jupyter notebooks,
Python/R/shell scripts, and SQL scripts.

When you're ready, deploy to Airflow or Kubernetes (using Argo) without code changes.

Here's how pipeline tasks look like:

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
    # run 'get_users' before this function.
    # upstream['get_users'] returns the output
    # of such task, used as input here
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
# before this script/notebook
upstream = ['clean_users', 'clean_activity']
# -

# a new cell is injected here with
# the product variable
# e.g., product = '/path/output.csv'
# and a new upstream variable:
# e.g., upstream = {'clean_users': '/path/...'
#                   'clean_activity': '/another/...'}

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
run 'raw_data' before this task. Replace
{{upstream['raw_data']}} with table name
at runtime
*/
SELECT * FROM {{upstream['raw_data']}}
```
</td>


<td valign="top">

```yaml
tasks:
  # function
  - source: functions.clean_users
    product: output/users-clean.csv

  # python script (or notebook)
  - source: notebooks/model-template.py
    product:
      model: output/model.pickle
      nb: output/model-evaluation.html
  
  # sql script
  - source: scripts/some_script.sql
    product: [schema, name, table]
    client: db.get_client
```

</td>

</tr>

</table>

## Resources

* [Documentation](https://ploomber.readthedocs.io/)
* [Sample projects (Machine Learning pipeline, ETL, among others)](https://github.com/ploomber/projects)
* [Watch JupyterCon 2020 talk](https://www.youtube.com/watch?v=M6mtgPfsA3M)
* [Argo Community Meeting talk](https://youtu.be/FnpXyg-5W_c)

## Installation

```sh
pip install ploomber
```

Compatible with Python 3.6 and higher.

## Try it out!

You can choose from one of the hosted options:

[![image](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Fspec-api-python%252FREADME.ipynb%26branch%3Dmaster)
[![image](https://deepnote.com/buttons/launch-in-deepnote-small.svg)](https://deepnote.com/launch?template=deepnote&url=https://github.com/ploomber/projects/blob/master/spec-api-python/README.ipynb)

Or run locally:

```sh
# ML pipeline example
ploomber examples --name ml-basic
cd ml-basic

# if using pip
pip install -r requirements.txt

# if using conda
conda env create --file environment.yml
conda activate ml-basic

# run pipeline
ploomber build
```

Pipeline output saved in the `output/` folder. Check out the pipeline definition
in the `pipeline.yaml` file.

To get a list of examples, run `ploomber examples`.

## Main features

1.  **Jupyter integration**. When you open your notebooks, Ploomber will
automatically inject a new cell with the location of your input
    files, as inferred from your `upstream` variable. If you open a
    Python or R script, it's converted to a notebook on the fly.
2.  **Incremental builds**. Speed up execution by skipping tasks whose
    source code hasn't changed.
3.  **Parallelization**. Run tasks in parallel to speed up computations.
4.  **Pipeline testing**. Run tests upon task execution to verify that
    the output data has the right properties (e.g., values within
    expected range).
5.  **Pipeline inspection**. Start an interactive session with
    `ploomber interact` to debug your pipeline. Call
    `dag['task_name'].debug()` to start a debugging session.
6.  **Deployment to Kubernetes and Airflow**. You can develop
    and execute locally. Once you are ready to deploy, export to
    [Kubernetes](https://soopervisor.readthedocs.io/en/stable/kubernetes.html) or [Airflow](https://soopervisor.readthedocs.io/en/stable/airflow.html).


## How does Ploomber compare to X?

Ploomber has two goals:

1. Provide an excellent development experience for
Data Science/Machine learning projects, which require a lot of
experimentation/iteration: incremental builds and Jupyter integration are
a fundamental part of this.
2. Integrate with deployment tools (Airflow and Argo) to streamline deployment.

For a complete comparison, read our
[survey on workflow management tools](https://ploomber.io/posts/survey/).
