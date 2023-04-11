from ploomber.exceptions import MissingClientError
from ploomber.util.dotted_path import DottedPath


class ClientMixin:
    """
    A mixin that exposes a client property. Looks at the _client attribute,
    then looks at self.dag. If _client returns a DottedSpecPath, it is called,
    replaced, and returned

    Raises
    ------
    ValueError
        If there is no valid client to use
    """

    @property
    def client(self):
        if self._client is None:
            dag_client = self.dag.clients.get(type(self))

            if dag_client is None:
                raise MissingClientError(
                    f"{type(self).__name__} must be initialized with a "
                    "client. Pass a client directly or set a DAG-level one"
                )

            return dag_client

        if isinstance(self._client, DottedPath):
            # NOTE: should we test the output ot self._client()?
            # maybe check if it's a subclass of the Client abstract class
            self._client = self._client()

        return self._client
