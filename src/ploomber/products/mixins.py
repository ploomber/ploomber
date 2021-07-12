class ProductWithClientMixin:
    """
    Adds the client property to a Product with the hierarchical resolution
    logic: Product -> Task -> DAG.clients. Product.client is only used
    for storing metadata
    """
    @property
    def client(self):
        # FIXME: this nested reference looks ugly, how can we improve this?
        if self._client is None:
            # TODO: raise a different error to be able to catch it
            if self._task is None:
                raise ValueError('Cannot obtain client for this product, '
                                 'the constructor did not receive a client '
                                 'and this product has not been assigned '
                                 'to a DAG yet (cannot look up for clients in'
                                 'dag.clients)')

            default = self.task.dag.clients.get(type(self))

            if default is None:
                raise ValueError(
                    f'{type(self).__name__} must be initialized with a client.'
                    ' Pass a client directly or set a DAG-level one')
            else:
                self._client = default

            # TODO: process dotted path spec

        return self._client


class SQLProductMixin:
    """
    Concrete SQL product classes must inherit from this one to signal
    they create SQL tables/views
    """
    pass
