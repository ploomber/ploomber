from ploomber.util.dotted_path import DottedPathSpec


class ProductWithClientMixin:
    """
    Adds the client property to a Product with the hierarchical resolution
    logic: Product -> Task -> DAG.clients. Product.client is only used
    for storing metadata
    """
    @property
    def client(self):
        if self._client is None:
            if self._task is None:
                raise ValueError('Cannot obtain client for this product, '
                                 'the constructor did not receive a client '
                                 'and this product has not been assigned '
                                 'to a DAG yet (cannot look up for clients in'
                                 'dag.clients)')

            dag_client = self.task.dag.clients.get(type(self))

            # TODO: raise a different error to be able to catch it
            if dag_client is None:
                raise ValueError(
                    f'{type(self).__name__} must be initialized with a client.'
                    ' Pass a client directly or set a DAG-level one')

            return dag_client

        if isinstance(self._client, DottedPathSpec):
            self._client = self._client()

        return self._client


class SQLProductMixin:
    """
    Concrete SQL product classes must inherit from this one to signal
    they create SQL tables/views
    """
    pass
