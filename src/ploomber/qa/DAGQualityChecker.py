import warnings


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
