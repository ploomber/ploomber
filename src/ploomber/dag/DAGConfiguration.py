class DAGConfiguration:
    @classmethod
    def default(cls):
        cfg = cls()
        cfg.outdated_by_code = True
        return cfg

    def __init__(self):
        self.outdated_by_code = None

    @property
    def outdated_by_code(self):
        """
        Callable to be executed if task fails (passes Task as first parameter
        and the exception as second parameter)
        """
        return self._outdated_by_code

    @outdated_by_code.setter
    def outdated_by_code(self, value):
        self._outdated_by_code = value
