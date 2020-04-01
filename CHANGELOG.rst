CHANGELOG
=========

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