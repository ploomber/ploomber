---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.13.0
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

# Basic ML project

<!-- start description -->
Download data, clean it, generate features and train a model.
<!-- end description -->

## Description

Let's take a look at the `pipeline.yaml`:

<% expand('pipeline.yaml') %>

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
