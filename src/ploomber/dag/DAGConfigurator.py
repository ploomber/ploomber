from ploomber.dag.DAG import DAG


class DAGConfigurator:
    """
    A helper class to build dags based on the essence pattern by Andy Carlson
    http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.81.1898&rep=rep1&type=pdf
    """
    def __init__(self):
        self.outdated_by_code = None

    @classmethod
    def default(cls):
        """
        Returns a configurator object with default values, these values are
        used when a DAG is initialized using the its constructor (DAG()) as
        opposed to calling the DAGConfigurator.create() method
        """
        configurator = cls()
        configurator.outdated_by_code = True
        return configurator

    @classmethod
    def from_dict(cls, d):
        configurator = cls()

        for key, value in d.items():
            setattr(configurator, key, value)

        return configurator

    def create(self):
        """Return a DAG with the given parameters
        """
        dag = DAG()
        dag._cfg = self
        return dag
