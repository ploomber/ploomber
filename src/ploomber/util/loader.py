from pathlib import Path
import os

from ploomber.spec import DAGSpec

# raise an error if name is not None and ENTRY_POINT set when calling fn?
# maybe create a separate function that works at the env var level and leave
# the other one at the name arg level?

# TODO:
# refactor DAGSpec.find - it should receive the name arg and ignore the
# env var. the reasoning is that find is called by users, who can simply
# pass another name arg. auto_load should not have the name arg because
# that's called automatically (e.g. by jupyter) so any name customizations
# should be done via the env var


def entry_point_load(starting_dir, reload):
    entry_point = os.environ.get('ENTRY_POINT')

    # TODO: validate that entry_point is a valid .yaml value
    # any other thing should raise an exception

    if entry_point and Path(entry_point).is_dir():
        spec = DAGSpec.from_directory(entry_point)
        path = Path(entry_point)
        return spec, spec.to_dag(), path
    else:
        return DAGSpec._auto_load(starting_dir=starting_dir, reload=reload)
