class DAGConfiguration:
    """

    Attributes
    ----------
    cache_rendered_status : bool
        If True, once the DAG is rendered, subsequent calls to render will
        not do anything (rendering is implicitely called in build, plot,
        status), otherwise it will always render again
    """
    @classmethod
    def default(cls):
        """
        Returns a configuration object with default values, these values are
        used when a DAG is initialized using the its constructor (DAG()) as
        opposed to calling the DAGConfigurator.create() method
        """
        cfg = cls()
        cfg.outdated_by_code = True
        cfg.cache_rendered_status = False
        return cfg

    @classmethod
    def from_dict(cls, d):
        cfg = cls()

        for key, value in d.items():
            setattr(cfg, key, value)

        return cfg

    def __init__(self):
        self.outdated_by_code = None
        self.cache_rendered_status = None
