from ploomber.spec.dagspec import DAGSpec
from ploomber.scaffold.ScaffoldLoader import ScaffoldLoader
from ploomber.util.util import add_to_sys_path
from ploomber.exceptions import DAGSpecInitializationError


def load_dag():
    # setting lazy_import to true causes sources to be returned as paths,
    # instead of placeholders
    try:
        return DAGSpec._auto_load(to_dag=False, lazy_import=True)
    except DAGSpecInitializationError:
        return None


def add(spec, path_to_spec):
    """Add scaffold templates for tasks whose source does not exist
    """
    loader = ScaffoldLoader('ploomber_add')

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
        print('Found spec at {}'.format(path_to_spec))

        # make sure current working dir is in the path, otherwise we might not
        # be able to import the PythonCallable functions, which we need to do
        # to locate the modules
        with add_to_sys_path(path_to_spec, chdir=False):
            for task in spec['tasks']:
                loader.create(source=task['source'],
                              params=spec['meta'],
                              class_=task['class'])
    else:
        print('Error: No pipeline.yaml spec found...')
