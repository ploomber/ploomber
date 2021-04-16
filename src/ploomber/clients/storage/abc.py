import abc


class AbstractStorageClient(abc.ABC):
    @abc.abstractmethod
    def __init__(self):
        pass

    @abc.abstractmethod
    def download(self, local, destination=None):
        """
        Download remote copy of file in local path

        Parameters
        ----------
        local
            Path to local file whose remote copy will be downloaded
        destination
            Download location. If None, overwrites local copy
        """
        pass

    @abc.abstractmethod
    def upload(self, local):
        """
        Upload file in local path
        """
        pass

    @abc.abstractmethod
    def close(self):
        """
        Close client
        """
        pass
