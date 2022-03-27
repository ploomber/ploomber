from ploomber.cloud import api


def download_data(key):
    """

    Examples
    --------
    >>> from ploomber.cloud import download_data
    >>> download_data('raw.csv')
    """
    # TODO: this should find the root directory and download relative to it
    # then return the absolute path
    url = api.download_data(key).text
    return api._download_file(url, skip_if_exists=True)
