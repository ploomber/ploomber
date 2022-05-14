"""
Internal module for making requests to external APIs. It wraps the requests
module to provide friendlier error messages and defaults
"""
import requests


def _request_factory(method):
    def _request(*args, **kwargs):
        response = method(*args, **kwargs)

        if response.status_code >= 300:
            json_ = response.json()
            message = json_.get("Message")

            if message:
                raise BaseException(
                    f'{message} (status: {response.status_code})')
            else:
                raise BaseException(f'Error: {json_}')

        return response

    return _request


get = _request_factory(requests.get)
post = _request_factory(requests.post)
put = _request_factory(requests.put)
delete = _request_factory(requests.delete)
