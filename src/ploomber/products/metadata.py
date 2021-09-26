"""
Metadata represents the information we need to save in order to support
incremental builds: source code and build timestmp
"""
import logging
import warnings
import abc
from datetime import datetime
from copy import deepcopy

from ploomber.util.util import callback_check
from ploomber.products._resources import process_resources
from ploomber.products.serializeparams import remove_non_serializable_top_keys


class AbstractMetadata(abc.ABC):
    """Abstract class to represent Product's metadata

    If product does not exist, initialize empty metadata, otherwise use
    ``product.fetch_metadata`` to load it
    """
    def __init__(self, product):
        self.__data = None
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
        pass  # pragma: no cover

    @property
    @abc.abstractmethod
    def timestamp(self):
        """When the product was originally created
        """
        pass  # pragma: no cover

    @property
    @abc.abstractmethod
    def stored_source_code(self):
        """Source code that generated the product
        """
        pass  # pragma: no cover

    @property
    @abc.abstractclassmethod
    def params(self):
        """Task params
        """
        pass

    @abc.abstractmethod
    def update(self, source_code, params):
        """
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def delete(self):
        """Delete metadata
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def _get(self):
        """Load metadata
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def clear(self):
        """
        Clear tne in-memory copy, if the metadata is accessed again, it should
        trigger another call to load()
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def update_locally(self, data):
        """
        Updates metadata locally. Called when tasks are successfully
        executed in a subproces, to sync the values in the main
        process (because the call to .update() happens in the subprocess as
        well)
        """
        pass  # pragma: no cover

    def to_dict(self):
        """Returns a dict copy of ._data
        """
        return deepcopy(self._data)

    def __eq__(self, other):
        self._fetch()

        if isinstance(other, type(self)):
            # metadata is lazily loaded, ensure you have a local copy
            other._fetch()
            return self._data == other._data
        else:
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

    def _fetch(self):
        """Fetches metadata if needed. If already fetched, does nothing
        """
        if not self._did_fetch:
            self._get()


class Metadata(AbstractMetadata):
    """
    Internal class to standardize access to Product's metadata

    This implementation tries to avoid fetching metadata when it can, because
    it might be a slow process. Since this class also performs metadata
    validation, there are cases when the metadata here and the one in the
    storage backend won't match, for example when the product does not exist,
    metadata in the storage backend is simply ignored. The values returned
    by this class should be considered the "true metadata".

    Note that Metadata relies the actual Product implementation for
    most operations. The objective of this class is to encapsuate logic
    that requires calling several of Products's methods and do validation
    upon metadata fetching.

    Attributes
    ----------
    timestamp
        Last updated product timestamp
    stored_source_code
        Last updates product source code
    """
    def _default_metadata(self):
        return dict(timestamp=None, stored_source_code=None, params=None)

    def __init__(self, product):
        self.__data = None
        self._product = product

        self._logger = logging.getLogger('{}.{}'.format(
            __name__,
            type(self).__name__))
        self._did_fetch = False

    @property
    def params(self):
        return self._data.get('params')

    @property
    def timestamp(self):
        return self._data.get('timestamp')

    @property
    def stored_source_code(self):
        return self._data.get('stored_source_code')

    @property
    def _data(self):
        if not self._did_fetch:
            self._get()

        return self.__data

    @_data.setter
    def _data(self, value):
        # if data changes, cached status in product is no longer valid
        self._product._reset_cached_outdated_status()
        self.__data = value

    def _get(self):
        """
        Get the "true metadata", ignores actual metadata if the product does
        not exist. It is lazily called by the timestamp and stored_source_code
        attributes. Ignores fetched metadata and replaces it with the stored
        metadata
        """
        # if the product does not exist, ignore metadata in backend storage

        # FIXME: cache the output of this command, we are using it in several
        # places, sometimes we have to re-fetch but sometimes we can cache,
        # look for product.exists() references and .exists() references
        # in the Product definition
        if not self._product.exists():
            metadata = self._default_metadata()
        else:
            # FIXME: if anything goes wrong when fetching metadata, warn
            # and set it to a valid dictionary with None values, validation
            # should happen here, not in the fetch_metadata method, but we
            # shouldn't catch all exceptions. Create a new one in
            # ploomber.exceptions and raise it on each fetch_metadata
            # implementation when failing to unserialize
            metadata_fetched = self._product.fetch_metadata()

            if metadata_fetched is None:
                self._logger.debug(
                    'fetch_metadata for product %s returned '
                    'None', self._product)
                metadata = self._default_metadata()
            else:
                # FIXME: we need to further validate this, need to check
                # that this is an instance of mapping, if yes, then
                # check keys [timestamp, stored_source_code], check
                # types and fill with None if any of the keys is missing
                metadata = metadata_fetched

        self._did_fetch = True
        self._data = metadata

    def update(self, source_code, params):
        """
        Update metadata in the storage backend, this should be called by
        Task objects when running successfully to update metadata in the
        backend storage. If saving in the backend storage succeeds the local
        copy is updated as well

        Parameters
        ----------
        source_code : str
            Task's source code

        params : dict
            Task's params
        """
        # remove any unserializable parameters
        params = remove_non_serializable_top_keys(params)

        new_data = dict(
            timestamp=datetime.now().timestamp(),
            stored_source_code=source_code,
            # process params to store hashes in case they're
            # declared as resources
            params=process_resources(params))

        kwargs = callback_check(self._product.prepare_metadata,
                                available={
                                    'metadata': new_data,
                                    'product': self._product
                                })

        data = self._product.prepare_metadata(**kwargs)

        self._product.save_metadata(data)

        # if saving worked, we can update the local in-memory copy
        self.update_locally(new_data)

    def update_locally(self, data):
        """Updates the in-memory copy, does not update persistent copy
        """
        # could be the case that we haven't fetched metadata yet. since this
        # overwrites existing metadata. we no longer have to fetch
        self._did_fetch = True
        self._data = deepcopy(data)

    def delete(self):
        """Calls ._product._delete_metadata()
        """
        self._product._delete_metadata()
        self._data = self._default_metadata()

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
        self._data = self._default_metadata()

    def __repr__(self):
        return f'{type(self).__name__}({self._data!r})'


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
        timestamps = [
            p.metadata.timestamp for p in self._products
            if p.metadata.timestamp is not None
        ]

        any_none = any(p.metadata.timestamp is None for p in self._products)

        # edge cases: 1) any of the timestamps is none, this is mostly
        # due to metadata corruption, we can no longer compute the timestamp
        # reliable. 2) no timestamps at all happens when the task hasn't been
        # executed
        if any_none or not timestamps:
            # warn on corrupted data
            if any_none and timestamps:
                warnings.warn(f'Corrupted product metadata ({self!r}): '
                              'at least one product had a null timestamp, '
                              'but others had non-null timestamp')

            return None
        else:
            # timestamps should usually be very close, but there can be
            # differences for some products whose metadata takes some time
            # to save (e.g. remote db) in such case, we use the
            # minimum to cover the edge case where another process runs an
            # upstream dependency in between saving metadata
            return min(timestamps)

    @property
    def stored_source_code(self):
        stored_source_code = [
            p.metadata.stored_source_code for p in self._products
        ]
        # if source code differs (i.e. more than one element)
        if len(set(stored_source_code)) > 1:
            warnings.warn(
                'Stored source codes for products {} '
                'are different, but they are part of the same '
                'MetaProduct, returning stored_source_code as None'.format(
                    self._products))
            return None
        else:
            return stored_source_code[0]

    @property
    def params(self):
        return self._products.first.params

    def update(self, source_code, params):
        for p in self._products:
            p.metadata.update(source_code, params)

    def update_locally(self, data):
        for p in self._products:
            p.metadata.update_locally(data)

    def delete(self):
        for p in self._products:
            p.metadata.delete()

    def _get(self):
        for p in self._products:
            p.metadata._get()

    def clear(self):
        for p in self._products:
            p.metadata.clear()

    def to_dict(self):
        products = list(self._products)
        source = set(p.metadata.stored_source_code for p in products)
        large_diff = large_timestamp_difference(p.metadata.timestamp
                                                for p in products)

        # warn if metadata does not match, give a little tolerance (5 seconds)
        # for timestamps since they are expected to have slight differences
        if len(source) > 1 or large_diff:
            warnings.warn(f'Metadata acros products ({self!r}) differs, '
                          'this could be due to metadata corruption or '
                          'slow metadata storage backend, returning the '
                          'metadata from the first product')

        return products[0].metadata.to_dict()

    @property
    def _data(self):
        products = list(self._products)
        source = set(p.metadata.stored_source_code for p in products)
        large_diff = large_timestamp_difference(p.metadata.timestamp
                                                for p in products)

        # warn if metadata does not match, give a little tolerance (5 seconds)
        # for timestamps since they are expected to have slight differences
        if len(source) > 1 or large_diff:
            warnings.warn(f'Metadata acros products ({self!r}) differs, '
                          'this could be due to metadata corruption or '
                          'slow metadata storage backend, returning the '
                          'metadata from the first product')

        return products[0].metadata._data


def large_timestamp_difference(timestamps):
    """Returns True if there is at least one timestamp difference > 5 seconds
    """
    dts = [datetime.fromtimestamp(ts) for ts in timestamps]

    for i in range(len(dts)):
        for j in range(len(dts)):
            if i != j:
                diff = (dts[i] - dts[j]).total_seconds()

                if abs(diff) > 5:
                    return True

    return False


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

    @property
    def params(self):
        return None

    def _get(self):
        pass  # pragma: no cover

    def update(self, source_code, params):
        pass  # pragma: no cover

    def update_locally(self, data):
        pass  # pragma: no cover

    @property
    def _data(self):
        return {'timestamp': 0, 'stored_source_code': None, 'params': {}}

    def delete(self):
        pass  # pragma: no cover

    def clear(self):
        pass  # pragma: no cover
