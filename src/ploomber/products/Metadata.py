import logging
import warnings
import abc
from datetime import datetime
from ploomber.util.util import callback_check


class AbstractMetadata(abc.ABC):
    """Abstract class to represent Product's metadata

    If product does not exist, initialize empty metadata, otherwise use
    product.fetch_metadata, and accept it after doing some validations
    """
    def __init__(self, product):
        # NOTE: cannot fetch initial metadata here, since product is not
        # rendered on Metadata initialization, we have to do lazy loading
        self._data = None
        self._product = product

        self._logger = logging.getLogger('{}.{}'.format(
            __name__,
            type(self).__name__))

    @property
    @abc.abstractmethod
    def timestamp(self):
        """When the product was originally created
        """
        pass

    @property
    @abc.abstractmethod
    def stored_source_code(self):
        """Source code that generated the product
        """
        pass

    @abc.abstractmethod
    def update(self, source_code):
        """
        """
        pass

    @abc.abstractmethod
    def delete(self):
        """Delete metadata
        """
        pass

    @abc.abstractmethod
    def load(self):
        """Load metadata
        """
        pass

    def __eq__(self, other):
        return self.data == other

    def __getstate__(self):
        state = self.__dict__.copy()

        if '_logger' in state:
            del state['_logger']

        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._logger = logging.getLogger('{}.{}'.format(
            __name__,
            type(self).__name__))


class Metadata(AbstractMetadata):
    """
    Internal class to standardize access to Product's metadata
    """
    # TODO: this should be _data, since it is an internal API
    @property
    def data(self):
        if self._data is None:
            self.load()
        return self._data

    @property
    def timestamp(self):
        return self.data.get('timestamp')

    @property
    def stored_source_code(self):
        return self.data.get('stored_source_code')

    def load(self):
        if not self._product.exists():
            metadata = dict(timestamp=None, stored_source_code=None)
        else:
            metadata_fetched = self._product.fetch_metadata()

            if metadata_fetched is None:
                self._logger.debug(
                    'fetch_metadata for product %s returned '
                    'None', self._product)
                metadata = dict(timestamp=None, stored_source_code=None)
            else:
                # FIXME: we need to further validate this, need to check
                # that this is an instance of mapping, if yes, then
                # check keys [timestamp, stored_source_code], check
                # types and fill with None if any of the keys is missing
                metadata = metadata_fetched

        self._data = metadata

    def update(self, source_code):
        """
        """
        self.data['timestamp'] = datetime.now().timestamp()
        self.data['stored_source_code'] = source_code

        kwargs = callback_check(self._product.prepare_metadata,
                                available={
                                    'metadata': self.data,
                                    'product': self._product
                                })

        data = self._product.prepare_metadata(**kwargs)

        self._product.save_metadata(data)

    def delete(self):
        self._product.delete_metadata()

    def __getitem__(self, key):
        return self._data[key]


class MetadataCollection(AbstractMetadata):
    """Metadata class used for MetaProduct
    """
    def __init__(self, products):
        self._products = products

    @property
    def timestamp(self):
        """When the product was originally created
        """
        # TODO: refactor, all products should have the same metadata
        timestamps = [
            p.metadata.timestamp for p in self._products
            if p.metadata.timestamp is not None
        ]
        if timestamps:
            return max(timestamps)
        else:
            return None

    @property
    def stored_source_code(self):
        """Source code that generated the product
        """
        stored_source_code = set([
            p.metadata.stored_source_code for p in self._products
            if p.metadata.stored_source_code is not None
        ])
        if len(stored_source_code):
            warnings.warn(
                'Stored source codes for products {} '
                'are different, but they are part of the same '
                'MetaProduct, returning stored_source_code as None'.format(
                    self._products))
            return None
        else:
            return list(stored_source_code)[0]

    def update(self, source_code):
        """
        """
        for p in self._products:
            p.metadata.update(source_code)

    def delete(self):
        for p in self._products:
            p.delete_metadata()

    def load(self):
        for p in self._products:
            p.metadata.load()

    # TODO: add getitem


class MetadataAlwaysUpToDate(AbstractMetadata):
    """
    Metadata for Link tasks (always up-to-date)
    """
    def __init__(self):
        pass

    @property
    def timestamp(self):
        return 0

    @property
    def stored_source_code(self):
        return None

    def load(self):
        pass

    def update(self, source_code):
        pass

    def delete(self):
        pass

    # TODO: add getitem
