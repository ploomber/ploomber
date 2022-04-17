from pathlib import PurePosixPath
from urllib import parse
import cgi
import sys
from pathlib import Path
from urllib.request import urlretrieve, urlopen
from urllib.error import HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from glob import glob
import zipfile
from functools import wraps
from datetime import datetime
import json

import click
import requests
import humanize

from ploomber.table import Table
from ploomber.cloud import io, config
from ploomber.exceptions import BaseException
from ploomber.spec import DAGSpec
from ploomber.dag import util
from ploomber.cli.cloud import get_key

HOST = os.environ.get('PLOOMBER_CLOUD_HOST', 'https://api.ploomber.io')


def _remove_prefix(path):
    parts = PurePosixPath(path).parts[2:]
    return str(PurePosixPath(*parts))


def _download_file(url, skip_if_exists=False, raise_on_missing=False):
    try:
        remotefile = urlopen(url)
    except HTTPError as e:
        if e.code == 404:
            path = _remove_prefix(parse.urlparse(url).path[1:])

            if raise_on_missing:
                raise FileNotFoundError(
                    'The requested file does not exist.'
                    ' Upload it to cloud storage and try again.')
            else:
                click.echo(f'File not found: {path}')
                return

        else:
            raise

    content_disposition = remotefile.info()['Content-Disposition']

    if content_disposition:
        _, params = cgi.parse_header(content_disposition)
        path = params["filename"]
    else:
        path = Path(parse.urlparse(url).path[1:])

    if skip_if_exists and Path(path).exists():
        print(f'{path} exists, skipping...')
    else:
        Path(path).parent.mkdir(exist_ok=True, parents=True)
        print(f'Downloading {path}')
        urlretrieve(url, path)

    return path


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


_get = _request_factory(requests.get)
_post = _request_factory(requests.post)
_put = _request_factory(requests.put)
_delete = _request_factory(requests.delete)


def download_from_presigned(presigned):
    if not presigned:
        click.echo('No files matched the criteria.\n'
                   'To list files: ploomber cloud products')
        return

    with ThreadPoolExecutor(max_workers=64) as executor:
        future2url = {
            executor.submit(_download_file, url=url): url
            for url in presigned
        }

        for future in as_completed(future2url):
            exception = future.exception()

            if exception:
                task = future2url[future]
                raise RuntimeError(
                    'An error occurred when downloading product from '
                    f'url: {task!r}') from exception


def auth_header(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        api_key = get_key()

        if api_key:
            headers = {
                "Authorization": api_key,
                "Content-Type": "application/json"
            }

            return func(headers, *args, **kwargs)
        else:
            click.secho('Error: Missing api key', fg='red')
            sys.exit(1)

    return wrapper


@auth_header
def runs_new(headers, metadata):
    """Register a new run in the database
    """
    response = _post(f"{HOST}/runs", headers=headers, json=metadata)
    return response.json()['runid']


@auth_header
def runs_update(headers, runid, graph):
    """Update run status, store graph
    """
    return _put(f"{HOST}/runs/{runid}", headers=headers, json=graph).json()


@auth_header
def runs_register_ids(headers, runid, ids):
    """Update run status, store ids
    """
    return _put(f"{HOST}/runs/{runid}/ids", headers=headers, json=ids).json()


@auth_header
def runs(headers):
    res = _get(f"{HOST}/runs", headers=headers).json()

    for run in res:
        run['created_at'] = humanize.naturaltime(
            datetime.fromisoformat(run['created_at']),
            when=datetime.utcnow(),
        )

    print(Table.from_dicts(res))


# NOTE: this doesn't need authentication (add unit test)
def tasks_update(task_id, status):
    return _get(f"{HOST}/tasks/{task_id}/{status}").json()


@auth_header
def run_detail(headers, run_id):
    res = _get(f"{HOST}/runs/{run_id}", headers=headers).json()
    return res


def run_detail_print(run_id):
    out = run_detail(run_id)
    tasks = out['tasks']
    run = out['run']

    if run['status'] == 'created':
        click.echo('Run created...')
    elif tasks:
        click.echo(Table.from_dicts(tasks))
    else:
        click.echo('Pipeline up-to-date, no tasks scheduled for this run.')

    return out


@auth_header
def run_logs(headers, run_id):
    res = _get(f"{HOST}/runs/{run_id}/logs", headers=headers).json()

    for name, log in res.items():
        click.echo(f'\n\n***** START OF LOGS FOR TASK: {name} *****')
        click.echo(log)
        click.echo(f'***** END OF LOGS FOR TASK: {name} *****')


@auth_header
def run_logs_image(headers, run_id, tail=None):
    res = _get(f"{HOST}/runs/{run_id}/logs/image", headers=headers)

    if not len(res.text):
        out = "Image build hasn't started yet..."
    elif tail:
        out = '\n'.join(res.text.splitlines()[-tail:])
    else:
        out = res.text

    click.echo(out)


@auth_header
def run_abort(headers, run_id):
    _get(f"{HOST}/runs/{run_id}/abort", headers=headers).json()
    print("Aborted.")


@auth_header
def run_finished(headers, runid):
    response = _get(f"{HOST}/runs/{runid}/finished", headers=headers)
    return response


@auth_header
def products_list(headers):
    res = _get(f"{HOST}/products", headers=headers).json()

    if res:
        print(Table.from_dicts([{'path': r} for r in res]))
    else:
        print("No products found.")


@auth_header
def data_list(headers):
    res = _get(f"{HOST}/data", headers=headers).json()

    if res:
        print(Table.from_dicts([{'path': r} for r in res]))
    else:
        print("No data found.")


@auth_header
def products_download(headers, pattern):
    res = _post(f"{HOST}/products",
                headers=headers,
                json=dict(pattern=pattern)).json()
    download_from_presigned(res)


def _has_prefix(path, prefixes):
    if not prefixes:
        return False

    return any(str(path).startswith(prefix) for prefix in prefixes)


def zip_project(force, runid, github_number, verbose, ignore_prefixes=None):
    ignore_prefixes = ignore_prefixes or []

    if Path("project.zip").exists():
        if verbose:
            click.secho("Deleting existing project.zip...", fg="yellow")
        Path("project.zip").unlink()

    files = glob("**/*", recursive=True)

    # TODO: ignore __pycache__, ignore .git directory
    with zipfile.ZipFile("project.zip", "w", zipfile.ZIP_DEFLATED) as zip:
        for path in files:
            if not _has_prefix(path, ignore_prefixes):
                zip.write(path, arcname=path)
            else:
                click.echo(f'Ignoring: {path}')

        # NOTE: it's weird that force is loaded from the buildspec but the
        # other two parameters are actually loaded from dynamodb
        zip.writestr(
            '.ploomber-cloud',
            json.dumps({
                'force': force,
                'runid': runid,
                'github_number': github_number
            }))

    MAX = 5 * 1024 * 1024

    if Path('project.zip').stat().st_size > MAX:
        raise BaseException("Error: Your project's source code is over "
                            "5MB, which isn't supported. Tip: Ensure there "
                            "aren't any large data files and try again")


@auth_header
def get_presigned_link(headers):
    return _get(f"{HOST}/upload", headers=headers).json()


def upload_zipped_project(response, verbose):
    with open("project.zip", "rb") as f:
        files = {"file": f}
        http_response = _post(response["url"],
                              data=response["fields"],
                              files=files)

    if http_response.status_code != 204:
        raise ValueError(f"An error happened: {http_response}")

    if verbose:
        click.secho("Uploaded project, starting execution...", fg="green")


@auth_header
def trigger(headers):
    res = _get(f"{HOST}/trigger", headers=headers).json()
    return res


def upload_project(force=False,
                   github_number=None,
                   github_owner=None,
                   github_repo=None,
                   verbose=False):
    dag = DAGSpec('pipeline.yaml').to_dag().render(show_progress=False)

    # TODO: test
    if not Path("requirements.lock.txt").exists():
        raise BaseException("Missing requirements.lock.txt file, add one "
                            "with the dependencies to install")

    config.validate()

    runid = runs_new(
        dict(force=force,
             github_number=github_number,
             github_owner=github_owner,
             github_repo=github_repo))

    # TODO: ignore relative paths in products
    if verbose:
        click.echo("Zipping project -> project.zip")

    # TODO: test
    zip_project(force,
                runid,
                github_number,
                verbose,
                ignore_prefixes=util.extract_product_prefixes(dag))

    if verbose:
        click.echo("Uploading project...")

    response = get_presigned_link()

    upload_zipped_project(response, verbose)

    if verbose:
        click.echo("Starting build...")

    trigger()

    # TODO: if anything fails after runs_new, update the status to error
    # convert runs_new into a context manager

    return runid


@auth_header
def upload_data(headers, path):
    key = Path(path).name

    create = _post(f"{HOST}/upload/data/create",
                   headers=headers,
                   json=dict(key=key, n_parts=io.n_parts(path))).json()

    gen = io.UploadJobGenerator(path,
                                key=key,
                                upload_id=create['upload_id'],
                                links=create['urls'])

    click.echo('Uploading...')
    parts = gen.upload()

    _post(f"{HOST}/upload/data/complete",
          headers=headers,
          json=dict(key=key, parts=parts,
                    upload_id=create['upload_id'])).json()

    # print snippet showing how to download it in the pipeline


@auth_header
def download_data(headers, key):
    response = _post(f"{HOST}/download/data",
                     headers=headers,
                     json=dict(key=key))
    return response


@auth_header
def delete_data(headers, pattern):
    response = _delete(f"{HOST}/data",
                       headers=headers,
                       json=dict(pattern=pattern))
    print(response.json())


@auth_header
def delete_products(headers, pattern):
    response = _delete(f"{HOST}/products",
                       headers=headers,
                       json=dict(pattern=pattern))
    print(response.json())
