import logging
import warnings
import abc
from datetime import datetime
from copy import deepcopy
from ploomber.util.util import callback_check


class AbstractMetadata(abc.ABC):
    """Abstract class to represent Product's metadata

    If product does not exist, initialize empty metadata, otherwise use
    product.fetch_metadata, and accept it after doing some validations
    """
    def __init__(self, product):
        self._data = None
        self._product = product

        self._logger = logging.getLogger('{}.{}'.format(
            __name__,
            type(self).__name__))

    @property
    @abc.abstractmethod
    def _data(self):
        """
        Private API, returns the dictionary representation of the metadata
        """
        pass

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
    def _get(self):
        """Load metadata
        """
        pass

    @abc.abstractmethod
    def clear(self):
        """
        Clear tne in-memory copy, if the metadata is accessed again, it should
        trigger another call to load()
        """
        pass

    @abc.abstractmethod
    def update_locally(self, data):
        """
        Updates metadata locally. Called then tasks are successfully
        executed in a subproces, to make the local copy synced again (because
        the call to .update() happens in the subprocess as well)
        """
        pass

    def to_dict(self):
        """Returns a dict copy of ._data
        """
        return deepcopy(self._data)

    def __eq__(self, other):
        return self._data == other

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

    This implementation tries to avoid fetching metadata when it can, because
    it might be a slow process. Since this class also performs metadata
    validation, there are cases when the metadata here and the one in the
    storage backend don't match, for example when the product does not exist,
    metadata in the storage backend is simply ignored. The values returned
    by this class should be considered the "true metadata".

    Attributes
    ----------
    timestamp
        Last updated product timestamp
    stored_source_code
        Last updates product source code
    """
    def __init__(self, product):
        self.__data = None
        self._product = product

        self._logger = logging.getLogger('{}.{}'.format(
            __name__,
            type(self).__name__))
        self._did_fetch = False

    @property
    def _data(self):
        return self.__data

    @_data.setter
    def _data(self, value):
        self.__data = value
        # whenever metadata changes, we have to reset these
        self._product._outdated_data_dependencies_status = None
        self._product._outdated_code_dependency_status = None

    @property
    def timestamp(self):
        if not self._did_fetch:
            self._get()

        return self._data.get('timestamp')

    @property
    def stored_source_code(self):
        """
        Public attribute for getting metadata source code
        """
        if not self._did_fetch:
            self._get()

        return self._data.get('stored_source_code')

    def _get(self):
        """
        Get the "true metadata", fetches only if it needs to. Should not
        be called directly, it is used bu the timestamp and stored_source_code
        attributes
        """
        # if the product does not exist, ignore metadata in backend storage

        # FIXME: cache the output of this command, we are using it in several
        # places, sometimes we have to re-fetch but sometimes we can cache,
        # look for product.exists() references and .exists() references
        # in the Product definition
        if not self._product.exists():
            metadata = dict(timestamp=None, stored_source_code=None)
        else:
            # FIXME: if anything goes wrong when fetching metadata, warn
            # and set it to a valid dictionary with None values, validation
            # should happen here, not in the fetch_metadata method
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

        self._did_fetch = True
        self._data = metadata

    def update(self, source_code):
        """
        Update metadata in the storage backend, this should be called by
        Task objects when running successfully to update metadata in the
        backend storage. If saving in the backend storage succeeds the local
        copy is updated as well
        """
        if self._data is None:
            self._data = dict(timestamp=None, stored_source_code=None)

        new_data = dict(timestamp=datetime.now().timestamp(),
                        stored_source_code=source_code)

        kwargs = callback_check(self._product.prepare_metadata,
                                available={
                                    'metadata': new_data,
                                    'product': self._product
                                })

        data = self._product.prepare_metadata(**kwargs)

        self._product.save_metadata(data)

        # if saving went good, we can update the local copy
        self._data = new_data

    def update_locally(self, data):
        # NOTE: do we have to copy here? is it a problem if all products
        # in a metadproduct have the same obj in metadata?
        self._data = data

    # NOTE: I don't think I'm using this anywhere
    def delete(self):
        self._product._delete_metadata()
        self._data = dict(timestamp=None, stored_source_code=None)

    def clear(self):
        """
        Clears up metadata local copy, next time the timestamp or
        stored_source_code are needed, this will trigger another call to
        ._get(). Should be called only when the local copy might be outdated
        due external execution. Currently, we are only using this when running
        DAG.build_partially, because that triggers a deep copy of the original
        DAG. hence our local copy in the original DAG is not valid anymore
        """
        self._did_fetch = False
        self._data = dict(timestamp=None, stored_source_code=None)


class MetadataCollection(AbstractMetadata):
    """Metadata class used for MetaProduct
    """

    # FIXME: this can be optimized. instead of keeping separate copies for each
    # the metadata objects can share the underlying dictionary, since they
    # must have the same values anyway, this allows to remove the looping logic
    def __init__(self, products):
        self._products = products

    @property
    def timestamp(self):
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
        for p in self._products:
            p.metadata.update(source_code)

    def update_locally(self, data):
        for p in self._products:
            p.metadata.update_locally(data)

    def delete(self):
        for p in self._products:
            p._delete_metadata()

    def _get(self):
        for p in self._products:
            p.metadata._get()

    def clear(self):
        for p in self._products:
            p.metadata.clear()

    def to_dict(self):
        return list(self._products)[0].metadata.to_dict()

    @property
    def _data(self):
        return list(self._products)[0].metadata._data


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

    def _get(self):
        pass

    def update(self, source_code):
        pass

    def update_locally(self, data):
        pass

    @property
    def _data(self):
        return {'timestamp': 0, 'stored_source_code': None}

    def delete(self):
        pass

    def clear(self):
        pass
