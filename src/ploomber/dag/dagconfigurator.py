"""
As we started adding more features to DAG, I added a few more parameters to
the constructor, default values cover a lot of cases and most of the time
only a few parameters are actually modified. To prevent making the DAG API
unnecessarily complex, custom behavior will be provided via this object.

This is based in the essence pattern by by Andy Carlson
http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.81.1898&rep=rep1&type=pdf
"""
from copy import copy
from ploomber.dag.dag import DAG
from ploomber.dag.dagconfiguration import DAGConfiguration


class DAGConfigurator:
    """An object to customize DAG behavior

    Note: this API is experimental an subject to change

    To keep the DAG API clean, only the most important parameters are included
    in the constructor, the rest are accesible via a DAGConfigurator object

    Available parameters:

    outdated_by_code: whether source code differences make a task outdated
    cache_rendered_status: keep results from dag.render() whenever are needed
    again (e.g. when calling dag.build()) or compute it again every time.

    cache_rendered_status: If True, once the DAG is rendered, subsequent calls
    to render will not do anything (rendering is implicitely called in build,
    plot, status), otherwise it will always render again.

    hot_reload: Reload sources whenever they are updated

    Examples
    --------
    >>> from ploomber import DAGConfigurator
    >>> configurator = DAGConfigurator()
    >>> configurator.params.outdated_by_code = True
    >>> configurator.params.cache_rendered_status = False
    >>> configurator.params.hot_reload = True
    >>> dag = configurator.create()
    """

    def __init__(self, d=None):
        if d:
            self._params = DAGConfiguration.from_dict(d)
        else:
            self._params = DAGConfiguration()

    @property
    def params(self):
        return self._params

    def create(self, *args, **kwargs):
        """Return a DAG with the given parameters

        *args, **kwargs
            Parameters to pass to the DAG constructor
        """
        dag = DAG(*args, **kwargs)
        dag._params = copy(self.params)
        return dag

    def __setattr__(self, key, value):
        if key != "_params":
            raise AttributeError(
                "Cannot assign attributes to DAGConfigurator,"
                " use configurator.params.param_name = value"
            )
        super().__setattr__(key, value)
