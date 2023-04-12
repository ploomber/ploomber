from ploomber.cloud.api import PloomberCloudAPI, _download_file


def download_data(key, *, path=None):
    """

    Examples
    --------
    >>> from ploomber.cloud import download_data
    >>> download_data('raw.csv') # doctest: +SKIP
    """
    # TODO: this should find the root directory and download relative to it
    # then return the absolute path
    url = PloomberCloudAPI().download_data(key).text
    return _download_file(url, skip_if_exists=True, raise_on_missing=True, path=path)
