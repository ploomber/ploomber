"""Efficiently upload/download data
"""

import boto3
import requests

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


def yield_index(file_size, max_size):
    parts = int(file_size / max_size)

    for i in range(parts):
        idx = max_size * i

        yield idx, idx + max_size

    if file_size % max_size:
        idx = parts * max_size
        yield idx, file_size


def read_from_index(path, i, j):
    with Path(path).open('rb') as f:
        f.seek(i)
        return f.read(j - i)


def yield_parts(path, max_size):
    file_size = Path(path).stat().st_size

    for i, j in yield_index(file_size, max_size):
        yield read_from_index(path, i, j)


def n_parts(path, max_size=None):
    max_size = max_size or (10 * 1024 * 1024)
    file_size = Path(path).stat().st_size
    return int(file_size / max_size) + int(file_size % max_size > 0)


def generate_links(bucket_name, key, upload_id, n_parts):
    s3 = boto3.client('s3')
    return [
        s3.generate_presigned_url(ClientMethod='upload_part',
                                  Params={
                                      'Bucket': bucket_name,
                                      'Key': key,
                                      'UploadId': upload_id,
                                      'PartNumber': part_no
                                  }) for part_no in range(1, n_parts + 1)
    ]


class UploadJobGenerator:
    """

    Examples
    --------
    >>> from ploomber.cloud import io
    >>> io.upload('v2.mov', 5 * 1024 * 1024, 'ploomber-bucket', 'raw/v2.mov')

    Notes
    -----
    https://github.com/boto/boto3/issues/2305
    """
    def __init__(self,
                 path,
                 key,
                 upload_id,
                 links,
                 bucket=None,
                 max_size=None):
        # only reuired if calling complete
        self._bucket = bucket
        self._upload_id = upload_id
        self._max_size = max_size or (10 * 1024 * 1024)
        self._path = path
        self._key = key
        self._links = links
        self._n_parts = n_parts(path, max_size)

    @property
    def n_parts(self):
        return self._n_parts

    @classmethod
    def from_scratch(cls, path, max_size, bucket, key):
        s3 = boto3.client('s3')
        res = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = res['UploadId']
        links = generate_links(bucket, key, upload_id, n_parts(path, max_size))
        return cls(path, max_size, bucket, key, upload_id, links)

    def __iter__(self):
        file_size = Path(self._path).stat().st_size

        for num, (link, (i, j)) in enumerate(zip(
                self._links, yield_index(file_size, self._max_size)),
                                             start=1):
            yield UploadJob(self._path, link, i, j, num)

    def upload(self, complete=False):

        with ThreadPoolExecutor(max_workers=4) as executor:
            future2job = {
                executor.submit(upload_job): upload_job
                for upload_job in self
            }

            for future in as_completed(future2job):
                exception = future.exception()

                if exception:
                    job = future2job[future]
                    raise RuntimeError(
                        'An error occurred when downloading product from '
                        f'job: {job!r}') from exception

        parts = [job._res for job in future2job.values()]

        if complete:
            self.complete(parts)

        return parts

    def complete(self, parts):
        s3 = boto3.client('s3')
        return s3.complete_multipart_upload(Bucket=self._bucket,
                                            Key=self._key,
                                            MultipartUpload={'Parts': parts},
                                            UploadId=self._upload_id)


class UploadJob:
    def __init__(self, path, link, i, j, num):
        self._path = path
        self._link = link
        self._i = i
        self._j = j
        self._num = num
        self._res = None

    def __call__(self):
        res = requests.put(self._link,
                           data=read_from_index(self._path, self._i, self._j))
        etag = res.headers['ETag']
        self._res = {'ETag': etag, 'PartNumber': self._num}
