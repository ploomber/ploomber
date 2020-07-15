# [[name]]

Make sure you execute these commands inside your project's root folder (the one that contains the `pipeline.yaml` file)

## File layout

* `pipeline.yaml` - Contains the pipeline's configuration and list of tasks
* `environment.yml` - Project dependencies
* `clean.py`, `plot.py` - Pipeline tasks
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
ploomber entry pipeline.yaml
```

All output is saved in `output/`

## Development

The best way to use Ploomber is via `jupyter notebook` or `jupyter lab`

Add links to docs explaining the workflow

how to add tasks, declared dependencies, products, etc


