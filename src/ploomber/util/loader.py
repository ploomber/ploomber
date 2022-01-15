from pathlib import Path
import os

from ploomber.spec import DAGSpec
from ploomber.util import default
from ploomber.util.dotted_path import load_callable_dotted_path
from ploomber.exceptions import DAGSpecInitializationError
from ploomber.entrypoint import try_to_find_entry_point_type, EntryPoint


# NOTE: this function is importable in the top-level package to allow
# users debug errors when using the Jupyter integration
def lazily_load_entry_point(starting_dir=None, reload=False):
    """
    Lazily loads entry point by recursively looking in starting_dir directory
    and parent directories.
    """

    starting_dir = starting_dir or '.'

    entry_point = os.environ.get('ENTRY_POINT')

    type_ = try_to_find_entry_point_type(entry_point)

    if type_ == EntryPoint.Directory:
        spec = DAGSpec.from_directory(entry_point)
        path = Path(entry_point)
    elif type_ == EntryPoint.DottedPath:
        entry = load_callable_dotted_path(str(entry_point), raise_=True)
        dag = entry()
        spec = dict(meta=dict(jupyter_hot_reload=False,
                              jupyter_functions_as_notebooks=False))
        # potential issue: dag defines sources as relative paths
        path = Path().resolve()
        return spec, dag, path
    else:
        spec, path, _ = _default_spec_load(starting_dir=starting_dir,
                                           reload=reload,
                                           lazy_import=True)

    # chain exception to provide more context
    dag = spec.to_dag()

    # we remove the on_render hook because this is a lazy load, if we don't do
    # it, calling the hook will cause an error since the function never loads
    dag.on_render = None

    # same with task-level hooks
    # also disable static_analysis since we don't want to break cell injection
    # because of some issues in te code
    for name in dag._iter():
        task = dag[name]
        task._on_render = None

        if hasattr(task, 'static_analysis'):
            task.static_analysis = False

    return spec, dag, path


def _default_spec_load(starting_dir=None, lazy_import=False, reload=False):
    """
    NOTE: this is a private API. Use DAGSpec.find() instead

    Looks for a pipeline.yaml, generates a DAGSpec and returns a DAG.
    Currently, this is only used by the PloomberContentsManager, this is
    not intended to be a public API since initializing specs from paths
    where we have to recursively look for a pipeline.yaml has some
    considerations regarding relative paths that make this confusing,
    inside the contents manager, all those things are all handled for that
    use case.

    The pipeline.yaml parent folder is temporarily added to sys.path when
    calling DAGSpec.to_dag() to make sure imports work as expected

    Returns DAG and the directory where the pipeline.yaml file is located.
    """
    root_path = starting_dir or os.getcwd()
    path_to_entry_point = default.entry_point(root_path=root_path)

    try:
        spec = DAGSpec(path_to_entry_point,
                       env=None,
                       lazy_import=lazy_import,
                       reload=reload)

        path_to_spec = Path(path_to_entry_point)
        return spec, path_to_spec.parent, path_to_spec

    except Exception as e:
        exc = DAGSpecInitializationError('Error initializing DAG from '
                                         f'{path_to_entry_point!s}')
        raise exc from e
