# Architecture

This document contains a high-level description of Ploomber's codebase.

The three core elements in Ploomber are `DAG`, `Task`, and `Product`.
`DAGs` are a collection of `Tasks`, and `Tasks` produce `Products`.

# DAG

The `DAG` impementation is in `src/ploomber/dag/dag.py`. A `DAG` is a
mapping-like object that contains tasks and relationships among them.

DAG is used to manage the status of tasks; for example, if a tasks's source
code hasn't changed since the last execution, `DAG` marks such task as `skipped` and is not executed
again if the source doesn't change. Status resolution happens during a rendering
step (`DAG.render()`), which runs before building the DAG.

It also manages hooks (e.g., `on_finish`, `on_failure`), which are functions
that execute after running a task.

# Task

A task is a unit of work. The abstract class is in `src/ploomber/tasks/abc.py`,
all concrete classes (e.g., `PythonCallable`, `NotebookRunner`) are subclasses
of the abstract class.

Tasks implement a `Task.render` step, which `DAG.render` calls. Implementation
details vary depending on each case, but this is where the task prepares for
execution. For example, in SQL tasks, this is where jinja placeholders resolve
to their actual values.

When calling `DAG.build`, `Task.build` executes, and the task status determines
whether to run the task or not

# Product

Products are persistent objects (i.e., files or SQL tables/views) generated
from task execution. The abstract class is in
`src/ploomber/products/product.py`, all concrete classes
(e.g., `File`, and `SQLRelation`) are subclasses of the abstract class.

A Product is an object initialized with an identifier (e.g., `File` initializes
with a path to a file, `SQLRelation` with a `(name, schema, kind)` tuple), such
the identifier is passed to the `Task` when executing it so the user calls
the appropriate logic to save any files or SQL tables/views.

To determine `Task` status, `Products` store `Metadata` (a JSON string) with
the task's source code and execution time. The `Metadata` needs
a storage backend, which depends on the `Product` implementation. `File` stores
metadata in a file (e.g., `/path/data.csv` stores metadata in
`/path/.data.csv.metadata`). However, SQL tasks need another backend.
`PostgresRelation` uses table comments but to avoid implementing one metadata
backend for each database engine, there's a generic backend that stores
metadata in a `SQLite` database. Alternatively, SQL Products can skip saving
metadata, but they lose incremental builds.

Tasks may generate more than one task. `MetaProuct`
(`src/ploomber/products/metaproduct.py`) implements a Product-like interface
that can hold more than one Product. This way, `Task` can interact with it
as if it was a single task.

# Source

Sources are objects that represent source code. Users do not interact with
the directly. They expose a common API defined via an abstract class
(`src/ploombers/sources/abc.py`) that allows Tasks to show useful information
to users such as source code file location, and the source code string.
Optionally, they implement verification logic to detect errors during
`DAG.render`, that otherwise would be raised until `DAG.build`. There are two
notable cases to highlight: `PythonCallableSource`, and `NotebookSource`.

`PythonCallable` uses `PythonCallableSource` to hold Python functions. It
allows to extract information from the function *without loading it*. This is
necessary in cases where we want to extract some information from a DAG but
the running process cannot should not load certain functions because doing
so would yield `ModuleNotFoundError`, this scenario happens when loading
DAGs in the Jupyter plug-in for cell injection. The Jupyter process does not
necessarily have all packages installed to run a DAG but it should still
extract the location of the source code.

On the other hand, `NotebookRunner` uses `NotebookSource`, which implements
the cell injection process, notebook validation (ensure there is cell
tagged `parameters` with an `upstream` variable), verifying kernelspec data,
and exporting the notebook as a string with Python code, which allows us to
use pdb in a terminal to debug a notebook.

# Metadata

Products define the Metadata storage backend, but the in-memory representation
is the same and described in `src/ploomber/products/metadata.py`. `Metadata`
takes care of managing metadata's life cycle, ensuring that all tasks have
the most recent metadata version at all times.

# Executors

`DAG` orchestrate execution, but they *do not* run tasks, `Executors` do. An
`Executor` is an object that receives a rendered `DAG` and calls `Task.build`
on each task. `Executors` may run tasks in child processes, so they must
ensure that status of the original `Task` object is updated; they also
provide other features such as capturing tracebacks and displaying them at the
end. There are currently two executors, `Serial`, and `Parallel`.

# Clients

Clients communicate `Tasks` and `Products` with external systems. However,
depending on the client, the purpose is different. For example, SQL clients
communicate a SQL Task with a database to execute a script. But another
SQL client communicates a SQL Product with a database to store the metadata.
The SQL client in a Task and Product may or may not be the same. File clients
work differently; they allow files to upload to remote storage such
as Amazon S3 or Google Cloud Storage. The abstract class is defined in
`src/ploomber/clients/client.py`.

# DAGSpec / TaskSpec

`DAGSpec` and `TaskSpec` convert dictionaries into `DAG` and `Task` objects
respectively, and are used to parse a `pipeline.yaml` file. Both are
implemented in the `src/ploomber/spec/` module. They implement the
heavy lifting to make the *Spec API* work with as little code as
possible: infer which Task and Product class to use depending on the user's
values, initialize `env.yaml`, resolve upstream dependencies by analyzing
task's source code, among others.
