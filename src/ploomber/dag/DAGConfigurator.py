from ploomber.dag.DAG import DAG
from ploomber.dag.DAGConfiguration import DAGConfiguration


class DAGConfigurator:
    """
    A helper class to build dags based on the essence pattern by Andy Carlson
    http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.81.1898&rep=rep1&type=pdf
    """
    def __init__(self, cfg=None):
        self.cfg = cfg or DAGConfiguration()

    @classmethod
    def default(cls):
        cfg = DAGConfiguration.default()
        return cls(cfg=cfg)

    @classmethod
    def from_dict(cls, d):
        cfg = DAGConfiguration.from_dict(d)
        return cls(cfg=cfg)

    def create(self):
        """Return a DAG with the given parameters
        """
        dag = DAG()
        dag._cfg = self.cfg

        # TODO: validate configuration

        return dag

    def set_param(self, key, value):
        return setattr(self.cfg, key, value)
