# CHANGELOG

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
    `{PythonCallable, NotebookRunner}.develop(args="--arg=value")` not
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
- Docstrings are parsed from notebooks and displayed in DAG status table (#242)
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

- Bug fixes #88, #89, #90, #84, #91
- Modifies Env API: Env() is now Env.load(), Env.start() is now Env()
- New advanced Env guide added to docs
- Env can now be used with a context manager
- Improved DAGConfigurator API
- Deletes logger configuration in executors constructors, logging is
    available via DAGConfigurator

## 0.3.4 (2020-04-25)

- Dependencies cleanup
- Removed (numpydoc) as dependency, now optional
- A few bug fixes: #79, #71
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
- New `@with_env` and `@load_env` decorators for managing environments
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
- A few fixes to PostgresCopy (#1, #2)

## 0.1

- First release
