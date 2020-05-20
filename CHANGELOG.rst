CHANGELOG
=========

0.4.2dev
--------
* Experimental PythonCallable.develop() and NotebookRunner.develop()
* Experimental NotebookRunner.debug()

0.4.1 (2020-05-19)
-------------------
* PythonCallable.debug() now works in Jupyter notebooks


0.4.0 (2020-05-18)
-------------------
* PythonCallable.debug() now uses IPython debugger by default
* Improvements to Task.build() public API
* Moves hook triggering logic to Task to simplify executors implementation
* Adds DAGBuildEarlyStop exception to signal DAG execution stop
* New option in Serial executor to turn warnings and exceptions capture off
* Adds Product.prepare_metadata hook
* Implements hot reload for notebooks and python callables
* General clean ups for old `__str__` and `__repr__` in several modules
* Refactored ploomber.sources module and ploomber.placeholders (previously ploomber.templates)
* Adds NotebookRunner.debug() and NotebookRunner.develop()
* NotebookRunner: now has an option to run static analysis on render
* Adds documentation for DAG-level hooks
* Bug fixes

0.3.5 (2020-05-03)
-------------------
* Bug fixes #88, #89, #90, #84, #91
* Modifies Env API: Env() is now Env.load(), Env.start() is now Env()
* New advanced Env guide added to docs
* Env can now be used with a context manager
* Improved DAGConfigurator API
* Deletes logger configuration in executors constructors, logging is available via DAGConfigurator


0.3.4 (2020-04-25)
-------------------
* Dependencies cleanup
* Removed (numpydoc) as dependency, now optional
* A few bug fixes: #79, #71
* All warnings are captured and shown at the end (Serial executor)
* Moves differ parameter from DAG constructor to DAGConfigurator


0.3.3 (2020-04-23)
-------------------
* Cleaned up some modules, deprecated some rarely used functionality
* Improves documentation aimed to developers looking to extend ploomber
* Introduces DAGConfigurator for advanced DAG configuration [Experimental API]
* Adds task to upload files to S3 (ploomber.tasks.UploadToS3), requires boto3
* Adds DAG-level on_finish and on_failure hooks
* Support for enabling logging in entry points (via --logging)
* Support for starting an interactive session using entry points (via python -i -m)
* Improved support for database drivers that can only send one query at a time
* Improved repr for SQLAlchemyClient, shows URI (but hides password)
* PythonCallable now validates signature against params at render time
* Bug fixes


0.3.2 (2020-04-07)
------------------

* Faster Product status checking, now performed at rendering time
* New products: GenericProduct and GenericSQLRelation for Products that do not have a specific implementation (e.g. you can use Hive with the DBAPI client + GenericSQLRelation)
* Improved DAG build reports, subselect columns, transform to pandas.DataFrame and dict
* Parallel executor now returns build reports, just like the Serial executor



0.3.1 (2020-04-01)
------------------

* DAG parallel executor
* Interact with pipelines from the command line (entry module)
* Bug fixes
* Refactored access to Product.metadata


0.3 (2020-03-20)
----------------
* New Quickstart and User Guide section in documentation
* DAG rendering and build now continue until no more tasks can render/build (instead of failing at the first exception)
* New @with_env and @load_env decorators for managing environments
* Env expansion ({{user}} expands to the current, also {{git}} and {{version}} available)
* Task.name is now optional when Task is initialized with a source that has __name__ attribute (Python functions) or a name attribute (like Placeholders returned from SourceLoader)
* New Task.on_render hook
* Bug fixes
* A lot of new tests
* Now compatible with Python 3.5 and higher

0.2.1 (2020-02-20)
------------------

* Adds integration with pdb via PythonCallable.debug
* Env.start now accepts a filename to look for
* Improvements to data_frame_validator

0.2 (2020-02-13)
----------------

* Simplifies installation
* Deletes BashCommand, use ShellScript
* More examples added
* Refactored env module
* Renames SQLStore to SourceLoader
* Improvements to SQLStore
* Improved documentation
* Renamed PostgresCopy to PostgresCopyFrom
* SQLUpload and PostgresCopy have now the same API
* A few fixes to PostgresCopy (#1, #2)

0.1
---

* First release