<!-- start header -->
To run this example locally, [install Ploomber](https://docs.ploomber.io/en/latest/get-started/install.html) and execute: `ploomber examples -n templates/ml-basic`

To start a free, hosted JupyterLab: [![binder-logo](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/ploomber/binder-env/main?urlpath=git-pull%3Frepo%3Dhttps%253A%252F%252Fgithub.com%252Fploomber%252Fprojects%26urlpath%3Dlab%252Ftree%252Fprojects%252Ftemplates/ml-basic%252FREADME.ipynb%26branch%3Dmaster)

Found an issue? [Let us know.](https://github.com/ploomber/projects/issues/new?title=templates/ml-basic%20issue)

Have questions? [Ask us anything on Slack.](https://ploomber.io/community/)

For a notebook version (with outputs) of this file, [click here](https://github.com/ploomber/projects/blob/master/templates/ml-basic/README.ipynb)
<!-- end header -->



# Basic ML project

<!-- start description -->
Download data, clean it, generate features and train a model.
<!-- end description -->

## Description

Let's take a look at the `pipeline.yaml`:

<!-- #md -->
```yaml
# Content of pipeline.yaml
tasks:
    # tasks.get, features and join are python functions
  - source: tasks.get
    product: output/get.parquet

  - source: tasks.features
    product: output/features.parquet

  - source: tasks.join
    product: output/join.parquet

    # fit.py is a script (that you can open as notebook in Jupyter)
  - source: fit.py
    name: fit
    product:
        # this output notebook is the model's evaluation report
        nb: output/nb.html
        model: output/model.pickle

    # optional config to hide the code in the output/nb.html report
    nbconvert_export_kwargs:
      exclude_input: True
```
<!-- #endmd -->

Note that the first three tasks as Python functions, while the last one is a
script.

Generate the plot:

```bash
ploomber plot
```

```python
# If using jupyter, you can show the plot with this code:
from IPython.display import Image
Image(filename='pipeline.png')

# otherwise open the pipeline.png file directly
```

## Build pipeline

```bash
ploomber build
```

Since Ploomber executes scripts as notebooks by default, `fit.py` will
generate a model report at [output/nb.ipynb](output/nb.ipynb) with evaluation
charts.

## Interacting with the pipeline

The command-line interface is a convenient way to interact with your
pipeline. Try this in a terminal:

~~~bash
ploomber interact
~~~