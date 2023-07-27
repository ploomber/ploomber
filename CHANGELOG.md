# CHANGELOG

## 0.22.5 (2023-07-27)

* [Feature] Deprecates `ploomber cloud`
* [Fix] Compatibility with mistune 3
* [Fix] Compatibility with pydantic V2

## 0.22.4 (2023-06-01)

* [Feature] Add `executor` option to NotebookRunner to choose ploomber-engine or papermill
* [Fix] Fix error in `ScriptRunner` that didn't allow to import modules in script's directory ([#1072](https://github.com/ploomber/ploomber/issues/1072))

## 0.22.3 (2023-04-13)

* [Fix] Clearer error messages when generating pipeline plots
* [Fix] Fix error when choosing which backend plot to use (when user didn't expicitly requested one)
* [Fix] Fixed local execution error when using Scriptrunner

## 0.22.2 (2023-01-30)

* [Fix] Fixes error that printed a git error message when failing to retrieve current git hash ([#1067](https://github.com/ploomber/ploomber/issues/1067))

## 0.22.1 (2023-01-28)

* [Fix] Pinning `jupyter_client<8` due to breaking change

## 0.22.0 (2023-01-13)

* [API Change] Deprecates `.develop()` method in Notebook Function tasks
* [API Change] Deprecates the following `ploomber cloud` commands: `get-pipelines`, `write-pipeline`, and `delete-pipeline`

## 0.21.9 (2023-01-04)

* Fixes error that caused `ploomber nb --inject` to fail in pipelines that had function tasks ([#1056](https://github.com/ploomber/ploomber/issues/1056))

## 0.21.8 (2022-12-27)

* Adds environment variable expansion via `{{env.ENV_VAR_NAME}}` ([#1042](https://github.com/ploomber/ploomber/issues/1042))
* Adds support for including extra files when using `ploomber cloud nb` via the `include` section

## 0.21.7 (2022-11-09)

* Option to prioritize cell injection via `setup.cfg` when the same notebook appears more than once in the pipeline ([#1019](https://github.com/ploomber/ploomber/issues/1019))

## 0.21.6 (2022-11-06)

* Adds `--summary` to `ploomber cloud status`
* Adds `--summary` to `ploomber cloud download`
* Clearer output when deleting cloud products
* Improved error message when missing product key ([#1027](https://github.com/ploomber/ploomber/issues/1027))

## 0.21.5 (2022-10-31)

* `ploomber build --partially` doesn't create a `DAG` deep copy
* Deep copying DAG does not copy the clients
* Fixes error in `ploomber cloud download` when the metadata for a product is missing

## 0.21.4 (2022-10-27)

* General `ploomber cloud` CLI improvements
* General `ploomber cloud nb` accepts URL as argument

## 0.21.3 (2022-10-24)

* Fix to internal Cloud CLI

## 0.21.2 (2022-10-21)

* Adds `ploomber cloud nb` command
* Allows setting `null` in `pipeline.yaml` clients ([#1025](https://github.com/ploomber/ploomber/issues/1025))
* Wrapping text in D3 plot for tasks with long names ([#968](https://github.com/ploomber/ploomber/issues/968))
* Merge `env.yaml` nested keys when using `import_from` ([#1034](https://github.com/ploomber/ploomber/issues/1034))

## 0.21.1 (2022-10-02)

* Adds `ploomber cloud task` command
* `ploomber cloud` can take the `[@latest](https://github.com/latest)` argument in the `abort`, `log` and `status` commands
* Adds conda support to `ploomber cloud build` via `environment.lock.yml`
* Adds feature to select which parameters to install if the same source appears more than once ([#985](https://github.com/ploomber/ploomber/issues/985))
* Fixes an error when pairing notebooks ([#979](https://github.com/ploomber/ploomber/issues/979))
* Fixes error in `ploomber cloud download` command ([#1010](https://github.com/ploomber/ploomber/issues/1010))
* Remove `parameters` cell missing exception and add `parameters` cell in `NotebookRunner` and `ScriptRunner` when it is missing ([#971](https://github.com/ploomber/ploomber/issues/971))
* `setup.cfg` allows to set which task params to inject using `inject-priority` ([#902](https://github.com/ploomber/ploomber/issues/902))

## 0.21 (2022-08-22)

* Adds `ploomber.micro` module for writing micro pipelines
* Allow dotted paths in params ([#477](https://github.com/ploomber/ploomber/issues/477))
* Adds progress bar to `Parallel` executor
* Allows to switch method when using `Parallel` executor ([#957](https://github.com/ploomber/ploomber/issues/957))
* Clean traceback when using ploomber task ([#889](https://github.com/ploomber/ploomber/issues/889))
* `grid` requires strict dotted path format (`a.b::c`)
* Raise errors on wrong type or empty returned value for dotted paths in `params` and `grid`
* Compatibility with papermill `2.4.0`
* Compatibility with IPython `8` ([#978](https://github.com/ploomber/ploomber/issues/978))
* Python API allows to execute notebooks without a parameters cell ([#971](https://github.com/ploomber/ploomber/issues/971))
* Compatible with nbconvert `7`
* Compatibility with mistune `0.8.x` and `2.x`
* Adds deprecation warning to `NotebookRunner.develop()` and `PythonCallable.develop()`

## 0.20 (2022-08-04)

*Note: Ploomber 0.20 dropped support for Python 3.6 ([#876](https://github.com/ploomber/ploomber/issues/876))*

* Adds support to serialize the traceback for later post-mortem debugging: `ploomber build --debuglater` and `ploomber task {name} --debuglater`
* Support for notebook post-mortem debugging: `ploomber build --debug` `ploomber task {name} --debug` ([#823](https://github.com/ploomber/ploomber/issues/823))
* `env.yaml` allows to use existing keys in subsequent values
* Add `start_method` to Parallel executor to set the method which should be used to start child processes ([#942](https://github.com/ploomber/ploomber/issues/942))
* Clearing screen before updating execution status when using the `Parallel` executor
* Fixes missing plot when generating `ploomber report` with `--backend d3` ([#946](https://github.com/ploomber/ploomber/issues/946))
* Fixes error when using dotted paths in `grid` ([#951](https://github.com/ploomber/ploomber/issues/951))

## 0.19.9 (2022-07-26)

*Note: this is the latest version compatible with Python 3.6*

* Adds warning notice when running on Python 3.6
* Add `--backend` to `ploomber report` to choose plotting backend ([#904](https://github.com/ploomber/ploomber/issues/904))
* Add *did you mean?* feature to `ploomber examples` ([#805](https://github.com/ploomber/ploomber/issues/805))

## 0.19.8 (2022-07-16)

* Allow dotted paths in `pipeline.yaml` `grid` parameters ([#804](https://github.com/ploomber/ploomber/issues/804))
* Templating task names when using `grid` via `[[placeholders]]` ([#698](https://github.com/ploomber/ploomber/issues/698))
* Improved performance when loading the DAG in the Jupyter plugin ([#894](https://github.com/ploomber/ploomber/issues/894))

## 0.19.7 (2022-07-04)

* Suppressing "black is not installed" message ([#831](https://github.com/ploomber/ploomber/issues/831))

* Better error message in `ploomber cloud build` when pipeline is up-to-date ([#815](https://github.com/ploomber/ploomber/issues/815))

* Error message when `env.yml` found instead of `env.yaml` ([#829](https://github.com/ploomber/ploomber/issues/829))

* Fixes jinja extractor when upstream had nested getitem ([#859](https://github.com/ploomber/ploomber/issues/859))

* Fixes notebook loading on Windows when UTF-8 is not the default encoding ([#870](https://github.com/ploomber/ploomber/issues/870))

* Remove extraneous output in `ploomber task` tracebacks ([#828]https://github.com/ploomber/ploomber/issues/828)

## 0.19.6 (2022-06-02)

* `setup.cfg` allows to switch default entry point
* Generate multiple notebook products from a single task ([#708](https://github.com/ploomber/ploomber/issues/708))
* `NotebookRunner` uploads partially executed notebook if it fails and a client is configured

## 0.19.5 (2022-05-30)

* Adds support for choosing environment in `cloud.yaml`

## 0.19.4 (2022-05-21)

* Fixes error when running `python -m ploomber.onboard` on Linux
* Moving email prompt to onboarding tutorial ([#800](https://github.com/ploomber/ploomber/issues/800))

## 0.19.3 (2022-05-20)

* Adds onboarding command: `python -m ploomber.onboard`
* Updating `pipeline.yaml` if `ploomnber nb --format {fmt}` command changes extensions ([#755](https://github.com/ploomber/ploomber/issues/755))

## 0.19.2 (2022-05-17)

* Adds documentation for `pipeline.yaml` `meta` section
* Adds many inline examples
* Improved docs for `pipeline.yaml` `grid`
* `ploomber task` prints a confirmation message upon successful execution
* `DAG.close_clients()` only calls `.close()` on each client once
* Fixes `dag.plot()` error when dag needs rendering

## 0.19.1 (2022-05-14)

* Fixes incompatibility with nbconvert 5 ([#741](https://github.com/ploomber/ploomber/issues/741))
* Improved error messages when the network fails while hitting the cloud build API
* Hiding posthog error logs ([#744](https://github.com/ploomber/ploomber/issues/744))

## 0.19 (2022-05-07)

* `ploomber plot` uses D3 backend if `pygraphviz` is not installed
* Request email (optional) after running `ploomber examples` for the first time
* Changes to `ploomber cloud`

## 0.18.1 (2022-04-22)

* Compatibility with click `7.x` and `8.x` ([#719](https://github.com/ploomber/ploomber/issues/719))
* Deprecates casting for boolean `static_analysis` flag ([#586](https://github.com/ploomber/ploomber/issues/586))

## 0.18 (2022-04-16)

* Support for `env.yaml` composition via `meta.import_from` ([#679](https://github.com/ploomber/ploomber/issues/679))
* Support for `webpdf` for notebook conversion ([#675](https://github.com/ploomber/ploomber/issues/675))
* SQLAlchemyClient accepts URL object in the constructor ([#699](https://github.com/ploomber/ploomber/issues/699))
* Better error message when product has an incorrect extension ([#472](https://github.com/ploomber/ploomber/issues/472))
* Better error when `pipeline.yaml` in root directory `/` ([#497](https://github.com/ploomber/ploomber/issues/497))
* Better error message when `NotebookRunner` initialized with a `str` ([#705](https://github.com/ploomber/ploomber/issues/705))
* Error message when missing placeholder in `env.yaml` includes path to offending file
* Fixes error when expanding complex args in `env.yaml` ([#709](https://github.com/ploomber/ploomber/issues/709))
* Validating object returned by `import_tasks_from` ([#686](https://github.com/ploomber/ploomber/issues/686))

## 0.17.2 (2022-03-31)

* [FEATURE] Custom name for products generated by `grid` tasks ([#647](https://github.com/ploomber/ploomber/issues/647))
* [FEATURE] Execute notebooks/scripts without generating and output notebook via `ScriptRunner` ([#614](https://github.com/ploomber/ploomber/issues/614))
* [FEATURE] more robust "Did you mean?" suggestions for product and task classes typos in `pipeline.yaml`
* [BUGFIX] `ploomber nb --remove` works with `.ipynb` files ([#692](https://github.com/ploomber/ploomber/issues/692))
* [BUGFIX]  Use `grid` and `params` in `pipeline.yaml` ([#522](https://github.com/ploomber/ploomber/issues/522))
* [DOC] Adds versioning user guide
* [DOC] Adds cloud user guide

## 0.17.1 (2022-03-26)

* Better error message when failing to deepcopy a DAG ([#670](https://github.com/ploomber/ploomber/issues/670))
* Improvements to the `{{git}}` placeholder feature ([#667](https://github.com/ploomber/ploomber/issues/667))
* Replaces DAG colors in `ploomber plot` with their RGB values for better compatibility
* Pinning `jinja2` to prevent `nbconvert` from failing

## 0.17 (2022-03-19)

* Style improvements to DAG plot ([#650](https://github.com/ploomber/ploomber/issues/650))
* DAG plot only includes task names by default ([#393](https://github.com/ploomber/ploomber/issues/393))
* `ploomber plot --include-products/-p` generates plots with task names and products
* `DAG.plot(include_products=True)` generates plots with task names and products
* Fixes error when replacing file on Windows ([#333](https://github.com/ploomber/ploomber/issues/333))
* Fixes error message when config file does not exist ([#652](https://github.com/ploomber/ploomber/issues/652))
* Fixes typo in nb command ([#665](https://github.com/ploomber/ploomber/issues/665))

## 0.16.4 (2022-03-11)

* Using UTF-8 for reading and writing in notebook tasks ([#334](https://github.com/ploomber/ploomber/issues/334))

## 0.16.3 (2022-03-06)

* Clearer error message when DAG deepcopy fails
* Beta release of cloud pipeline monitoring
* More robust suggestions when invoking a non-existing command
* CLI loading performance improvements
* Prints message before starting to load the pipeline for better user feedback
* Displaying community link when DAG fails to render or build

## 0.16.2 (2022-03-03)

* Improved documentation in "ploomber nb --help" ([#623](https://github.com/ploomber/ploomber/issues/623))
* Fixed a few errors in the basic concepts tutorial
* More informative error when task does not generate some products
* Better error when all the code is in the parameters cell

## 0.16.1 (2022-02-27)

* Improves error message when `source` in a task spec is a string without an extension ([#619](https://github.com/ploomber/ploomber/issues/619))
* Fixes error that caused `dag.render(force=True)` to download remote metadata
* Simplify traceback when calling Ploomber task ([#605](https://github.com/ploomber/ploomber/issues/605))
* Emitting warning when `resources_` points to large files ([#609](https://github.com/ploomber/ploomber/issues/609))
* Adds auto-completion steps to documentation ([#612](https://github.com/ploomber/ploomber/issues/612))
* Updates documentation to reflect new default format (`py:percent`) ([#564](https://github.com/ploomber/ploomber/issues/564))
* Showing a mesage when a new version of Ploomber is available ([#558](https://github.com/ploomber/ploomber/issues/558))

## 0.16 (2022-02-17)

* Cleaner tracebacks when DAG fails to build or render
* Automatically adding a parameters cell to scripts and notebooks if it's missing
* `NotebookRunner` `static_analysis` behaves differently: it's less picky now, the old behavior default behavior can be turned on if passing `strict` , and can be turned off if passing `disable` ([#566](https://github.com/ploomber/ploomber/issues/566))
* Improves many error messages for clarity
* `ploomber install` installs dependencies in the current virtual environment by default
* `ploomber install` works in systems where `python` links to Python 2.7 ([#435](https://github.com/ploomber/ploomber/issues/435))
* `ploomber install` uses lock files by default if they exist
* `ploomber install` has options to customize its behavior
* `ploomber scaffold` accepts one positional argument ([#484](https://github.com/ploomber/ploomber/issues/484))
* Fixes an issue that caused `ploomber nb` to hide traceback when failed to load pipeline ([#468](https://github.com/ploomber/ploomber/issues/468))

## 0.15.3 (2022-02-13)

* Fixed error when parsing cell magics with inline python

## 0.15.2 (2022-02-11)

* Fixed misspelling in `pygraphviz` error message ([#575](https://github.com/ploomber/ploomber/issues/575))

## 0.15.1 (2022-02-08)

* Sets minimum networkx version ([#536](https://github.com/ploomber/ploomber/issues/536))
* Updates documentation links to the new domain ([#549](https://github.com/ploomber/ploomber/issues/549))
* Suggests adding the appropriate `pygraphviz` version depending on the Python version ([#539](https://github.com/ploomber/ploomber/issues/539))
* Improved error message when `pipeline.yaml` does not exist ([#517](https://github.com/ploomber/ploomber/issues/517))
* Fixes error when scaffolding functions

## 0.15 (2022-02-03)

* Adds SQL runtime parameters
* `SQLScript` and `SQLDump` display source code when `client.execute` fails
* Clearer error message when `NotebookRunner` fails to initialize
* `cli_endpoint` decorator hides traceback when raising `BaseException` errors
* `DAGSpec` and `TaskSpec` errors raised as `DAGSpecInitializationError`
* Less verbose `ploomber examples` output

## 0.14.8 (2022-01-29)

* Better user feedback after running `ploomber nb --inject`
* Fixed `ploomber nb --inject` when `pipeline.yaml` has `.ipynb` files

## 0.14.7 (2022-01-25)

* Adds `ploomber nb --single-click/--single-click-disable` to enable/disable opening `.py` as notebooks with a click on Jupyter
* `ploomber nb` no longer requires a valid entry point if the selected option doesn't need one
* Better error message when `Pool` in the `Serial` executor raises `RuntimeError`
* Notebook static analysis: Better support for IPython magics, support for inline shell (`! echo hi`). closes [#478](https://github.com/ploomber/ploomber/issues/478)

## 0.14.6 (2022-01-20)

* Documents `S3Client` and `GCloudStorageClient`
* Updates to the telemetry module

## 0.14.5 (2022-01-15)

* Fixes error message when failing to load dotted paths
* `ploomber scaffold` now supports `.R and .Rmd` files ([#476](https://github.com/ploomber/ploomber/issues/476))
* Fixes an error that caused `ploomber scaffold` to ignore the location of existing packages ([#459](https://github.com/ploomber/ploomber/issues/459))
* Better error message when running `ploomber execute/run` (suggests `ploomber build`)
* Better error message when passing positional arguments to `ploomber build` (suggests `ploomber task`)

## 0.14.4 (2022-01-07)

* Fixes an error in the telemetry module

## 0.14.3 (2022-01-06)

* Improved [anonymous user statistics](https://docs.ploomber.io/en/latest/community/user-stats.html)

## 0.14.2 (2022-01-03)

* `PLOOMBER_STATS_ENABLED` environment variable can be used to disable stats
* Improved error message when a dotted path fails to load ([#410](https://github.com/ploomber/ploomber/issues/410))

## 0.14.1 (2022-01-02)

* `ploomber scaffold` creates missing modules when adding functions ([#332](https://github.com/ploomber/ploomber/issues/332), [@fferegrino](https://github.com/fferegrino))
* `NotebookRunner` creates product's parent directories before running ([#460](https://github.com/ploomber/ploomber/issues/460))

## 0.14 (2021-12-25)

* Adds `ploomber nb` command for integration with VSCode, PyCharm, Spyder, etc.
* Adds methods for saving and removing injected cells to `NotebookSource`
* Adds methods for pairing and syncing to `NotebookSource`
* Fixes [#448](https://github.com/ploomber/ploomber/issues/448): `SQLUpload` ignoring `io_handler`
* Fixes [#447](https://github.com/ploomber/ploomber/issues/447): `pipeline.yaml` supports passing custom init parameters to `executor`
* Adds optional [anonymous user statistics](https://docs.ploomber.io/en/latest/community/user-stats.html)

## 0.13.7 (2021-12-18)

* Fixes `{{root}}` expansion when path_to_here is different than the current working directory
* Better error message when initializing `MetaProduct` with non-products
* Adds refactoring section (`soorgeon`) to the user guide
* Adds shell scripts user guide
* `Commander` allows `jinja2.Environment` customization

## 0.13.6 (2021-11-17)

* `GenericSource` supports extracting upstream

## 0.13.5 (2021-10-27)

* Fixes an error that caused `copy.deepcopy` to fail on `SourceLoader`

## 0.13.4 (2021-10-25)

* Adds `{{now}}` (current timestamp in ISO 8601 format) to default placeholders
* Adds `--output/-o` to `ploomber examples` to change output directory

## 0.13.3 (2021-10-15)

* Adds `--log-file/-F` option to CLI to log to a file
* Clearer error message when a task in a `pipeline.yaml` has `grid` and `params`
* Right bar highlighting fixed
* `normalize_python` returns input if passed non-Python code
* Better error message when requesting an unknown example in `ploomber examples`
* Better error message when `ploomber examples` fails due to an unexpected error
* Fixes an error in `ploomber examples` that caused the `--branch/-b` argument to be ignored

## 0.13.2 (2021-10-09)

* Adds support for using `grid` and task-level hooks in spec API

## 0.13.1 (2021-10-08)

* Allow serialization of a subset of params ([#338](https://github.com/ploomber/ploomber/issues/338))
* NotebookRunner `static_analysis` turned on by default
* NotebookRunner `static_analysis` ignores IPython magics
* Improved error message when NotebookRunner `static_analysis` fails
* Support for collections in `env.yaml`
* Adds `unpack` argument to `serializer`/`unserializer` decorators to allow a variable number of outputs
* General CSS documentation improvements
* Mobile-friendly docs
* Add table explaining each documentation section
* Adds hooks, serialization, debugging, logging, and parametrization cookbook
* Adds FAQ on tasks with a variable number of outputs
* Auto-documenting methods/attributes for classes in the Python API section
* Documents `io` module

## 0.13 (2021-09-22)

* Refactors scripts/notebooks `static_analysis` feature
* Shows warning if using default value in scripts/notebooks `static_analysis` parameter
* Better error message when `DAG` has duplicated task names
* Adds more info to the files generated by ploomber scaffold
* Better error when trying to initialize a task from a path with an unknown extension

## 0.12.8 (2021-09-08)

* Support for dag-level hooks in Spec API
* Better error message when invalid extension in `NotebookRunner` product
* Fixes an error when loading nested templates on Windows

## 0.12.7 (2021-09-03)

* Task hooks (e.g., `on_finish`) accept custom args

## 0.12.6 (2021-09-02)

* Fixes look up of conda root when running `ploomber install` when conda binary is inside the `Library` directory (Windows)
* No longer looking up pip inside conda when running `ploomber install` and `setup.py` does not exist
* Adds `--use-lock/-l` option to `ploomber install` to install using lock files

## 0.12.5 (2021-08-16)

* Simplifies serializer and unserializer creation with new decorators
* Adds guide on serializer/unserializer decorators to the documentation

## 0.12.4 (2021-08-12)

* Clearer error message when failing to import function
* Better error message when `tasks` in YAML spec is not a list
* Fixes an issue that caused dag plot to fail when using `.svg`
* Fixes duplicated log entries when viewing a file in Jupyter

## 0.12.3 (2021-08-03)

* Fixes cell injection when using the `--notebook-dir` during Jupyter initialization
* Reduces verbosity in Jupyter logs ([#314](https://github.com/ploomber/ploomber/issues/314))
* Adds `tasks[*].params.resources_` to track changes in external files
* Minor bug fixes

## 0.12.2 (2021-07-26)

* Lazy load for `serializer`, `unserialize`, DAG clients, Task clients, Product clients, and task hooks, which allows the Jupyter plugin to work even if the Jupyter process does not have the dependencies required to import such dotted paths
* CLI `--help` message shows if `ENTRY_POINT` environment variable is defined
* `ploomber scaffold` now takes a `-e/--entry-point` optional argument
* Fixes error that caused the `{{here}}` placeholder not to work if an `env.yaml` exists
* Adds `--empty` option to `ploomber scaffold` to create a `pipeline.yaml` with no tasks

## 0.12.1 (2021-07-09)

- Allowing `pipeline.yaml` at project root if setup.py but `src/*/pipeline.yaml` is missing
- Fixes bug in `EnvDict.find` that caused the `{{here}}` placeholder to point to the `env.yaml` file instead of its parent
- `DAGSpec._find_relative` returns relative path to spec
- Fixes error that missed `env.yaml` loading when initializing DAGSpecPartial

## 0.12 (2021-07-08)

- Changes the logic that determines project root: only considers `pipeline.yaml` and `setup.py` (instead of `environment.yml` or `requirements.txt`)
- Adds configuration and scaffold user guides
- Updates Jupyter user guide
- Deletes conda user guide
- Renames internal modules for consistency (this should not impact end-users)
- Fixes error that caused Files generated from TaskGroups in the spec API not to resolve to their absolute values
- Fixes error that caused metadata not to delete on when saving files in Jupyter if using a source in more than one task
- `DAGSpec` loads an `env.{name}.yaml` file when loading a `pipeline.{name}.yaml` if one exists
- `ploomber plot` saves to `pipeline.{name}.png`
- Override `env.yaml` to load using `PLOOMBER_ENV_FILENAME` environment variable
- `EnvDict` init no longer searches recursively, moved that logic to `EnvDict.find`. `with_env` decorator now uses the latter to prevent breaking the API
- `PostgresCopyFrom` compatible with `psycopg>=2.9`
- `jupyter_hot_reload=True` by default
- `PythonCallableSource` finds the location of a dotted path without importing any of the submodules
- Jupyter integration lazily loads DAGs (no need to import callable tasks)
- CLI no longer showing `env.yaml` parameters when initializing from directory or pattern

## 0.11.1 (2021-06-08)

- Task's `metadata.params` stores `null` if any parameter isn't serializable
- Task status ignores `metadata.params` if they are `null`
- Fixes unserialization when an upstream task produces a `MetaProduct`

## 0.11 (2021-05-31)

- Adds `remote` parameter to `DAG.render` to check status against remote storage
- `NotebookSource` no longer includes the injected cell in its `str` representation
- `Metadata` uses task params to determine task status
- Support for wildcards when building dag partially
- Support to skip upstream dependencies when building partially
- Faster `File` remote metadata downloads using multi-threading during `DAG.render`
- Faster upstream dependencies parallel download using multi-threading during `Task.build`
- Suppresses papermill `FutureWarning` due to importing a deprecated `pyarrow` module
- Fixes error that caused a warning due to unused env params when using `import_tasks_from`
- Other bug fixes

## 0.10.4 (2021-05-22)

- `DAGSpec.find` exposes `starting_dir` parameter
- `ploomber install` supports `pip`'s `requirements.txt` files
- `ploomber install` supports non-packages (i.e., no `setup.py`)
- `ploomber scaffold` flags to use conda (`--conda`) and create package (`--package`)

## 0.10.3 (2021-05-17)

- `ParamGrid` supports initialization from a list
- Adds `tasks[*].grid` to generate multiple tasks at once
- Support for using wildcards to declare dependencies (e.g., `task-*`)
- Fixes to `ploomber scaffold` and `ploomber install`
- `PythonCallable` creates parent directories before execution
- Support for the parallel executor in Spec API
- `DagSpec.find` exposes `lazy_import` argument
- `TaskGroup` internal API changes

## 0.10.2 (2021-05-05)

- `GCloudStorageClient` loads credentials relative to the project root
- Adds `ploomber install`
- Adds `S3Client`

## 0.10.1 (2021-04-17)

- `DAGSpec` warns if parameter declared in env but unused
- Implements `{SQLDump, NotebookRunner, PythonCallable}.load()`
- `File.client` downloads during task execution instead of render
- Adds `ploomber.OnlineModel`, which provides a simpler API than `OnlineDAG` for models that implement a `.predict()` method
- Adds function to find package name if using standard layout

## 0.10 (2021-03-13)

- Changes `extract_product` default in spec API to False
- Tasks get a default name equal to the filename without extension (e.g., plot.py -> plot)
- `File` saves metadata in a `.{filename}.metadata` file instead of `{filename}.source`
- Adds `ploomber examples` command
- Adds Deployment guide to documentation
- `EnvDict` loads `env.yaml` and uses values as defaults when passing a custom dict
- Simpler repr for SQL products
- Improved Spec API docs
- Adds `ploomber.tasks.TaskGroup.from_params` to create multiple tasks at once

## 0.9.5 (2021-03-07)

- Changes a lot of error messages for clarity
- Clearer `__repr__` for `Placeholder`, `File`, and `MetaProduct`
- Default placeholders can be used in `pipeline.yaml` without defining `env.yaml`
- Better formatting for displaying DAG build and render errors
- Spec API initializes task spec as `SQLDump` if product has suffix `.csv` or `.parquet`
- Coloring CLI error traceback
- Spec API skips `SourceLoader` if passing an absolute path
- `DAG.clients` validates keys (using `DAGClients`)
- `params` available as hook argument
- Rewritten Spec API documentation

## 0.9.4 (2021-02-15)

- Better display of errors when building or rendering a DAG (layout and colors)
- `File` implements the `os.PathLike` interface (this works now: `pandas.read_parquet(File('file.parquet'))`)
- Several error messages refactored for clarity
- Adds `DAGSpec.find()` to automatically find `pipeline.yaml`

## 0.9.3 (2021-02-13)

- Adds `OnlineDAG` to convert `DAG` objects for in-memory inference
- Spec API (`pipeline.yaml`) supports DAG-level and Task-level `serializer` and `serializer`
- CLI looks for `src/{pkg}/pipeline.yaml` if `pipeline.yaml` doesn't exist
- Adds `{{cwd}}` placeholder for `env.yaml` that expands to current working directory

## 0.9.2 (2021-02-11)

- Support for Python 3.9
- `SQLAlchemyClient` now accepts an argument to pass custom parameters to `sqlalchemy.create_engine`
- Temporarily pins papermill version due to an incompatibility with jupytext and nbformat (jupytext does not support cell ids yet)
- Adds `--on-finish/-of` to `ploomber task` to execute the `on_finish` hook
- DAGs with R notebooks can render even if the ir kernel is not installed

## 0.9.1 (2021-02-01)

- `File` now supports a `client` argument to upload products to cloud
  storage
- Adds `GCloudStorageClient`
- Fixes error that caused jupyter to fail to initialize the dag when
  adding a function to a module already included in the YAML spec
- Fixes IPython namespace errors when using `ploomber interact`
- Adds `ploomber.testing.sql.assert_no_duplicates_in_column` to check
  for record duplicates and optionally show duplicates statistics
- Deprecates a few internal methods: `Table.save`, `DAG.to_dict()`,
  `Task.to_dict()`
- Improvements to SQL static analyzer to warn when relations created
  by a SQL script do not match `Product`
- A few changes to `Metadata` (internal API) to cover some edge cases
- Warning when `Product` metadata is corrupted
- Adds new `meta.import_tasks_from` option in YAML specs to import
  tasks from another file

## 0.9 (2021-01-18)

- Deprecates `ploomber new` and `ploomber add`
- Adds `ploomber scaffold`
- Jupyter plugin now exports functions as notebooks using
  `jupyter_functions_as_notebooks` in `pipeline.yaml`

## 0.8.6 (2021-01-08)

- `ploomber add` generates template tasks and functions if they don't exist
- Jupyter plugin now shows PythonCallable tasks as notebooks

## 0.8.5 (2020-12-14)

- Documentation tutorials re-organization and CSS fixes
- Improvements to the `InMemoryDAG` API
- Minor bug fixes
- `File.__repr__` shows a relative path whenever possible

## 0.8.4 (2020-11-21)

- Adds support for passing glob-like patterns in `ploomber build` (via
  `DAGSpec.from_directory`)

## 0.8.3 (2020-11-15)

- Full Windows compatibility
- Adds documentation to show how to customize notebook output using
  `nbconvert`
- Improvements to introductory tutorials
- Adds `--debug/-d` option to `ploomber build` to drop a debugger if
  an exception happens
- Ensuring all dag-level, task-level and product-level clients are
  closed after `dag.build()` is done
- Minor bug fixes

## 0.8.2 (2020-10-31)

- Removes `matplotlib` from dependencies, now using `IPython.display`
  for inline plotting
- Fixes bug that caused custom args to
  `{PythonCallable, NotebookRunner}.develop(args"--arg=value")` not
  to be sent correctly to the subprocess
- `NotebookRunner` (initialized from ipynb) only considers the actual
  code as its source, ignores the rest of the JSON contents
- Fixes bug when `EnvDict` was initialized from another `EnvDict`
- `PythonCallableSource` can be initialized with dotted paths
- `DAGSpec` loads `env.yaml` when initialized with a YAML spec and
  there is a `env.yaml` file in the spec parent folder
- `DAGSpec` converts relative paths in sources to be so to the
  project's root folder
- Adds `lazy_import` to `DAGspec`, to avoid importing `PythonCallable`
  sources (passes the dotted paths as strings instead)

## 0.8.1 (2020-10-18)

- `ploomber interact` allows to switch DAG parameters, just like
  `ploomber build`
- Adds `PythonCallable.develop()` to develop Python functions
  interactively
- `NotebookRunner.develop()` to develop now also works with Jupyter
  lab

## 0.8 (2020-10-15)

- Dropping support for Python 3.5
- Removes `DAGSpec.from_file`, loading from a file is now handled
  directly by the `DAGSpec` constructor
- Performance improvements, DAG does not fetch metadata when it doesn't need to
- Factory functions: Bool parameters with default values are now
  represented as flags when called from the CLI
- CLI arguments to replace values from `env.yaml` are now
  built with double hyphens instead of double underscores
- `NotebookRunner` creates parent folders for output file if they don't exist
- Bug fixes

## 0.7.5 (2020-10-02)

- NotebookRunner.develop accepts passing arguments to jupyter notebook
- Spec API now supports PythonCallable (by passing a dotted path)
- Upstream dependencies of PythonCallables can be inferred via the
  `extract_upstream` option in the Spec API
- Faster `DAG.render(force=True)` (avoid checking metadata when
  possible)
- Faster notebook rendering when using the extension thanks to the
  improvement above
- `data_frame_validator` improvement: `validate_schema` can now
  validate optional columns dtypes
- Bug fixes

## 0.7.4 (2020-09-14)

- Improved `__repr__` methods in PythonCallableSource and
  NotebookSource
- Improved output layout for tables
- Support for nbconvert>=6
- Docstrings are parsed from notebooks and displayed in DAG status table ([#242](https://github.com/ploomber/ploomber/issues/242))
- Jupyter extension now works for DAGs defined via directories (via
  `ENTRY_POINT` env variable)
- Adds Jupyter integration guide to documentation
- Several bug fixes

## 0.7.3 (2020-08-19)

- Improved support for R notebooks (`.Rmd`)
- New section for `testing.sql` module in the documentation

## 0.7.2 (2020-08-17)

- New guides: parametrized pipelines, SQL templating, pipeline testing
  and debugging
- `NotebookRunner.debug(kind='pm')` for post-mortem debugging
- Fixes bug in Jupyter extension when the pipeline has a task whose
  source is not a file (e.g. SQLDump)
- Fixes a bug in the CLI custom arg parser that caused dynamic params
  not to show up
- `DAGspec` now supports `SourceLoader`
- Docstring (from dotted path entry point) is shown in the CLI summary
- Customized sphinx build to execute guides from notebooks

## 0.7.1 (2020-08-06)

- Support for R
- Adding section on R pipeline to the documentation
- Construct pipeline from a directory (no need to write a
  `pipeline.yaml` file)
- Improved error messages when DAG fails to initialize (jupyter
  notebook app)
- Bug fixes
- CLI accepts factory function parameters as positional arguments,
  types are inferred using type hints, displayed when calling `--help`
- CLI accepts env variables (if any), displayed when calling `--help`

## 0.7 (2020-07-30)

- Simplified CLI (breaking changes)
- Refactors internal API for notebook conversion, adds tests for
  common formats
- Metadata is deleted when saving a script from the Jupyter notebook
  app to make sure the task runs in the next pipeline build
- SQLAlchemyClient now supports custom tokens to split source

## 0.6.3 (2020-07-24)

- Adding `--log` option to CLI commands
- Fixes a bug that caused the `dag` variable not to be
  exposed during interactive sessions
- Fixes `ploomber task` forced run
- Adds SQL pipeline tutorial to get started docs
- Minor CSS changes to docs

## 0.6.2 (2020-07-22)

- Support for `env.yaml` in `pipeline.yaml`
- Improved CLI. Adds `plot`, `report` and `task` commands`

## 0.6.1 (2020-07-20)

- Changes `pipeline.yaml` default (extract_product: True)
- Documentation re-design
- Simplified `ploomber new` generated files
- Ability to define `product` in SQL scripts
- Products are resolved to absolute paths to avoid ambiguity
- Bug fixes

## 0.6 (2020-07-08)

- Adds Jupyter notebook extension to inject parameters when opening a
  task
- Improved CLI `ploomber new`, `ploomber add` and `ploomber entry`
- Spec API documentation additions
- Support for `on_finish`, `on_failure` and `on_render` hooks in spec API
- Improved validation for DAG specs
- Several bug fixes

## 0.5.1 (2020-06-30)

- Reduces the number of required dependencies
- A new option in DBAPIClient to split source with a custom separator

## 0.5 (2020-06-27)

- Adds CLI
- New spec API to instantiate DAGs using YAML files
- NotebookRunner.debug() for debugging and .develop() for interacive
  development
- Bug fixes

## 0.4.1 (2020-05-19)

- PythonCallable.debug() now works in Jupyter notebooks

## 0.4.0 (2020-05-18)

- PythonCallable.debug() now uses IPython debugger by default
- Improvements to Task.build() public API
- Moves hook triggering logic to Task to simplify executors
  implementation
- Adds DAGBuildEarlyStop exception to signal DAG execution stop
- New option in Serial executor to turn warnings and exceptions
  capture off
- Adds Product.prepare_metadata hook
- Implements hot reload for notebooks and python callables
- General clean ups for old `__str__` and `__repr__` in several modules
- Refactored ploomber.sources module and ploomber.placeholders
  (previously ploomber.templates)
- Adds NotebookRunner.debug() and NotebookRunner.develop()
- NotebookRunner: now has an option to run static analysis on render
- Adds documentation for DAG-level hooks
- Bug fixes

## 0.3.5 (2020-05-03)

- Bug fixes [#88](https://github.com/ploomber/ploomber/issues/88), [#89](https://github.com/ploomber/ploomber/issues/89), [#90](https://github.com/ploomber/ploomber/issues/90), [#84](https://github.com/ploomber/ploomber/issues/84), [#91](https://github.com/ploomber/ploomber/issues/91)
- Modifies Env API: Env() is now Env.load(), Env.start() is now Env()
- New advanced Env guide added to docs
- Env can now be used with a context manager
- Improved DAGConfigurator API
- Deletes logger configuration in executors constructors, logging is
  available via DAGConfigurator

## 0.3.4 (2020-04-25)

- Dependencies cleanup
- Removed (numpydoc) as dependency, now optional
- A few bug fixes: [#79](https://github.com/ploomber/ploomber/issues/79), [#71](https://github.com/ploomber/ploomber/issues/71)
- All warnings are captured and shown at the end (Serial executor)
- Moves differ parameter from DAG constructor to DAGConfigurator

## 0.3.3 (2020-04-23)

- Cleaned up some modules, deprecated some rarely used functionality
- Improves documentation aimed to developers looking to extend
  ploomber
- Introduces DAGConfigurator for advanced DAG configuration
  [Experimental API]
- Adds task to upload files to S3 (ploomber.tasks.UploadToS3),
  requires boto3
- Adds DAG-level on_finish and on_failure hooks
- Support for enabling logging in entry points (via `--logging`)
- Support for starting an interactive session using entry points (via
  python -i -m)
- Improved support for database drivers that can only send one query
  at a time
- Improved repr for SQLAlchemyClient, shows URI (but hides password)
- PythonCallable now validates signature against params at render time
- Bug fixes

## 0.3.2 (2020-04-07)

- Faster Product status checking, now performed at rendering time
- New products: GenericProduct and GenericSQLRelation for Products
  that do not have a specific implementation (e.g. you can use Hive
  with the DBAPI client + GenericSQLRelation)
- Improved DAG build reports, subselect columns, transform to
  pandas.DataFrame and dict
- Parallel executor now returns build reports, just like the Serial
  executor

## 0.3.1 (2020-04-01)

- DAG parallel executor
- Interact with pipelines from the command line (entry module)
- Bug fixes
- Refactored access to Product.metadata

## 0.3 (2020-03-20)

- New Quickstart and User Guide section in documentation
- DAG rendering and build now continue until no more tasks can
  render/build (instead of failing at the first exception)
- New `with_env` and `load_env` decorators for managing environments
- Env expansion ({{user}} expands to the current, also {{git}} and
  {{version}} available)
- `Task.name` is now optional when Task is initialized with a source
  that has `__name__` attribute (Python functions) or a name
  attribute (like Placeholders returned from SourceLoader)
- New Task.on_render hook
- Bug fixes
- A lot of new tests
- Now compatible with Python 3.5 and higher

## 0.2.1 (2020-02-20)

- Adds integration with pdb via PythonCallable.debug
- Env.start now accepts a filename to look for
- Improvements to data_frame_validator

## 0.2 (2020-02-13)

- Simplifies installation
- Deletes BashCommand, use ShellScript
- More examples added
- Refactored env module
- Renames SQLStore to SourceLoader
- Improvements to SQLStore
- Improved documentation
- Renamed PostgresCopy to PostgresCopyFrom
- SQLUpload and PostgresCopy have now the same API
- A few fixes to PostgresCopy ([#1](https://github.com/ploomber/ploomber/issues/1), [#2](https://github.com/ploomber/ploomber/issues/2))

## 0.1

- First release
