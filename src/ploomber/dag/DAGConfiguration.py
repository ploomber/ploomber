from ploomber.CodeDiffer import CodeDiffer


class DAGConfiguration:
    """
    DAGConfiguration() initializes a configuration object with default values.
    """

    @classmethod
    def from_dict(cls, d):
        cfg = cls()

        for key, value in d.items():
            setattr(cfg, key, value)

        return cfg

    def __init__(self):
        self._outdated_by_code = True
        self._cache_rendered_status = False
        self._differ = CodeDiffer()

    @property
    def outdated_by_code(self):
        return self._outdated_by_code

    @outdated_by_code.setter
    def outdated_by_code(self, value):
        if value not in {True, False}:
            raise ValueError('outdated_by_code must be True or False')

        self._outdated_by_code = value

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
    def logging_handler(self):
        return self._logging_handler

    @logging_handler.setter
    def logging_handler(self, value):
        self._logging_handler = value
