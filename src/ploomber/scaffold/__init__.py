from pathlib import Path

import click

from ploomber.scaffold.scaffoldloader import ScaffoldLoader
from ploomber.util.util import add_to_sys_path
from ploomber.util import loader
from ploomber.exceptions import DAGSpecInvalidError


def load_dag():
    # setting lazy_import to true causes sources to be returned as paths,
    # instead of placeholders
    try:
        return loader._default_spec_load(lazy_import="skip")
    except DAGSpecInvalidError:
        return None


def add(spec, path_to_spec):
    """Add scaffold templates for tasks whose source does not exist

    Parameters
    ----------
    spec : DAGSpec
        The spec to inspect to create missing task.source

    path_to_spec : str
        Path to the spec, only used to emit messages to the console
    """
    loader = ScaffoldLoader()

    # TODO: when the dag has a source loader, the argument passed to
    # ploomber_add should take that into account to place the new file
    # in the appropriate location (instead of doing it relative to
    # pipeline.yaml)

    # TODO: raise an error if the location is inside the site-packages folder

    # NOTE: lazy loading freom source loader will giev errors because
    # initializing a source with a path only, loses the information from the
    # jinja environment to make macros workj. I have to test this. the best
    # solution is to add a lazy_load param to Placeholder, so it can be
    # initialized with a path for a file that does not exist

    if path_to_spec:
        click.echo(f"Found spec at {str(path_to_spec)!r}")

        n = 0

        # make sure current working dir is in the path, otherwise we might not
        # be able to import the PythonCallable functions, which we need to do
        # to locate the modules
        path_to_parent = str(Path(path_to_spec).resolve().parent)

        with add_to_sys_path(path_to_parent, chdir=False):
            for task in spec["tasks"]:
                did_create = loader.create(
                    source=task["source"], params=spec["meta"], class_=task["class"]
                )
                n += int(did_create)

        if not n:
            click.echo(
                f"All tasks sources declared in {str(path_to_spec)!r} "
                "exist, nothing was created."
            )
        else:
            click.echo(f"Created {n} new task sources.")

    else:
        click.echo("Error: No pipeline.yaml spec found...")
