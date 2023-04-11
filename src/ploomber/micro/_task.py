import inspect
from ploomber.sources import PythonCallableSource
from ploomber.tasks import PythonCallable


class Source(PythonCallableSource):
    """A subclass of PythonCallableSource that performs no validation"""

    def _post_render_validation(self, rendered_value, params):
        parameters = inspect.signature(self.primitive).parameters

        if "input_data" in parameters and "input_data" not in params:
            raise ValueError(f"{self.name!r} function missing 'input_data'")


class _PythonCallableNoValidation(PythonCallable):
    """A subclass of PythonCallable that uses Source as source"""

    @staticmethod
    def _init_source(source, kwargs):
        return Source(source, **kwargs)
