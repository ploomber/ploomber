"""
As we started adding more features to DAG, I added a few more parameters to
the constructor, default values cover a lot of cases and most of the time
only a few parameters are actually modified. To prevent making the DAG API
unnecessarily complex, custom behavior will be provided via this object.

This is based in the essence pattern by by Andy Carlson
http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.81.1898&rep=rep1&type=pdf
"""

from ploomber.dag.DAG import DAG
from ploomber.dag.DAGConfiguration import DAGConfiguration


class DAGConfigurator:
    """An object to customize DAG behavior

    Note: this API is experimental an subject to change

    To keep the DAG API clean, only the most important parameters are included
    in the constructor, the rest are accesible via a DAGConfigurator object

    Available parameters:

    outdated_by_code: whether source code differences make a task outdated
    cache_rendered_status: keep results from dag.render() whenever are needed
    again (e.g. when calling dag.build()) or compute it again every time

    Examples
    --------
    >>> from ploomber import DAGConfigurator
    >>> configurator = DAGConfigurator()
    >>> configurator.set_param('outdated_by_code', True)
    >>> configurator.set_param('cache_rendered_status', False)
    >>> dag = configurator.create()
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
