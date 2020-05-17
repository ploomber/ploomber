from ploomber.CodeDiffer import CodeDiffer


def _logging_factory():
    return None


class DAGConfiguration:
    """
    DAGConfiguration() initializes a configuration object with default values.
    """
    __attrs = {'outdated_by_code', 'cache_rendered_status',
               'logging_factory', 'differ', 'hot_reload'}

    @classmethod
    def from_dict(cls, d):
        cfg = cls()

        for key, value in d.items():
            setattr(cfg, key, value)

        return cfg

    def __init__(self):
        self.outdated_by_code = True
        self.cache_rendered_status = False
        self.hot_reload = False
        self.logging_factory = _logging_factory
        self.differ = CodeDiffer()

    @property
    def outdated_by_code(self):
        return self._outdated_by_code

    @outdated_by_code.setter
    def outdated_by_code(self, value):
        if value not in {True, False}:
            raise ValueError('outdated_by_code must be True or False')

        self._outdated_by_code = value

    @property
    def hot_reload(self):
        return self._hot_reload

    @hot_reload.setter
    def hot_reload(self, value):
        if value not in {True, False}:
            raise ValueError('hot_reload must be True or False')

        self._hot_reload = value

    @property
    def cache_rendered_status(self):
        return self._cache_rendered_status

    @cache_rendered_status.setter
    def cache_rendered_status(self, value):
        if value not in {True, False}:
            raise ValueError('cache_rendered_status must be True or False')

        self._cache_rendered_status = value

    @property
    def differ(self):
        return self._differ

    @differ.setter
    def differ(self, value):
        if value == 'default':
            value = CodeDiffer()

        self._differ = value

    @property
    def logging_factory(self):
        return self._logging_factory

    @logging_factory.setter
    def logging_factory(self, value):
        self._logging_factory = value

    def __setattr__(self, key, value):
        # prevent non-existing parameters from being created
        attr_name = key[1:] if key.startswith('_') else key
        if attr_name not in self.__attrs:
            raise AttributeError('"{}" is not a valid parameter, must be '
                                 'one of: {}'.format(key, self.__attrs))
        super().__setattr__(key, value)
