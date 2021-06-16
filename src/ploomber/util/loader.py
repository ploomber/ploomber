from pathlib import Path
import os

from ploomber.spec import DAGSpec
from ploomber.util import default
from ploomber.exceptions import DAGSpecInitializationError


def entry_point_load(starting_dir, reload):
    entry_point = os.environ.get('ENTRY_POINT')

    # TODO: validate that entry_point is a valid .yaml value
    # any other thing should raise an exception

    if entry_point and Path(entry_point).is_dir():
        spec = DAGSpec.from_directory(entry_point)
        path = Path(entry_point)
    else:
        spec, path = _default_spec_load(starting_dir=starting_dir,
                                        reload=reload)

    # chain exception to provide more context
    dag = spec.to_dag()

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

        return spec, Path(path_to_entry_point).parent

    except Exception as e:
        exc = DAGSpecInitializationError('Error initializing DAG from '
                                         f'{path_to_entry_point!s}')
        raise exc from e
