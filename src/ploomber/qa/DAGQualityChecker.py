# NOTE: should this be part of each Source?
import warnings

from ploomber.util import requires


class DAGQualityChecker:

    def __call__(self, dag):
        self.empty_docstrings(dag)

    def empty_docstrings(self, dag):
        """Evaluate code quality
        """
        for name, task in dag.items():

            doc = task.source.doc

            if doc is None or doc == '':
                warnings.warn('Task "{}" has no docstring'.format(name))


@requires(['numpydoc'])
def diagnose(source):
    """Prints some diagnostics
    """
    from numpydoc.docscrape import NumpyDocString

    # [WIP] function to validate docstrings in sources that
    # have placeholders
    found = source.value.variables
    docstring_np = NumpyDocString(source.value.docstring())
    documented = set([p[0] for p in docstring_np['Parameters']])

    print('The following variables were found in the template but are '
          'not documented: {}'.format(found - documented))

    print('The following variables are documented but were not found in '
          'the template: {}'.format(documented - found))

    return documented, found
