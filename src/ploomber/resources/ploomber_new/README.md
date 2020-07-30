# [[name]]

## File layout

* `pipeline.yaml` - Contains the pipeline's configuration and list of tasks
[% if conda %]
* `environment.yml` - Project dependencies
[% endif %]
* `raw.py`, `clean.py`, `plot.py` - Pipeline tasks
* `output/` - Executed notebooks generate from pipeline tasks and other generated files
[% if db %]
* `db.py` - Function to create a connection to the database
[% endif %]


[% if conda %]
## Setup environment

To create your environment with the following command:

```sh
conda env create --file environment.yml
```
Then activate it:

```sh
  conda activate my-project
```
[% endif %]

## Execute pipeline

```sh
ploomber build
```

Make sure you execute this command inside your project's root folder (the one that contains the `pipeline.yaml` file).

All output is saved in `output/`.
