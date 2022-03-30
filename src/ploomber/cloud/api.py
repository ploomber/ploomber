from urllib import parse
import cgi
import sys
from pathlib import Path
from urllib.request import urlretrieve, urlopen
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
from ploomber.cloud import io

HOST = os.environ.get(
    'PLOOMBER_CLOUD_HOST',
    'https://8lfxtyhlx2.execute-api.us-east-1.amazonaws.com/api/')


def _download_file(url, skip_if_exists=False):
    remotefile = urlopen(url)
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
            raise ValueError(
                f'Error (status: {response.status_code}): {response.json()}')

        return response

    return _request


_get = _request_factory(requests.get)
_post = _request_factory(requests.post)
_put = _request_factory(requests.put)


def download_from_presigned(presigned):
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
        api_key = os.environ.get("PLOOMBER_CLOUD_KEY")

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
        print('Run created...')
    else:
        print(Table.from_dicts(tasks))

    return out


@auth_header
def run_logs(headers, run_id):
    res = _get(f"{HOST}/runs/{run_id}/logs", headers=headers).json()

    for name, log in res.items():
        click.echo(f'\n\n***** START OF LOGS FOR TASK: {name} *****')
        click.echo(log)
        click.echo(f'***** END OF LOGS FOR TASK: {name} *****')


@auth_header
def run_abort(headers, run_id):
    _get(f"{HOST}/runs/{run_id}/abort", headers=headers).json()
    print("Aborted.")


@auth_header
def products_list(headers):
    res = _get(f"{HOST}/products", headers=headers).json()

    if res:
        print(Table.from_dicts([{'path': r} for r in res]))
    else:
        print("No products found.")


@auth_header
def products_download(headers, pattern):
    res = _get(f"{HOST}/products/{pattern}", headers=headers).json()
    download_from_presigned(res)


def zip_project(force, runid, github_number, verbose):
    if Path("project.zip").exists():
        if verbose:
            click.secho("Deleting existing project.zip...", fg="yellow")
        Path("project.zip").unlink()

    files = glob("**/*", recursive=True)

    # TODO: ignore __pycache__, ignore .git directory
    with zipfile.ZipFile("project.zip", "w", zipfile.ZIP_DEFLATED) as zip:
        for path in files:
            zip.write(path, arcname=path)

        # NOTE: it's weird that force is loaded from the buildspec but the
        # other two parameters are actually loaded from dynamodb
        zip.writestr(
            '.ploomber-cloud',
            json.dumps({
                'force': force,
                'runid': runid,
                'github_number': github_number
            }))


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


def upload_project(force, github_number, github_owner, github_repo, verbose):
    # TODO: use soopervisor's logic to auto find the pipeline
    # check pipeline is working before submitting
    # DAGSpec('pipeline.yaml').to_dag().render(show_progress=verbose)

    if not Path("requirements.lock.txt").exists():
        raise ValueError("missing requirements.lock.txt")

    runid = runs_new(
        dict(force=force,
             github_number=github_number,
             github_owner=github_owner,
             github_repo=github_repo))

    # TODO: ignore relative paths in products
    if verbose:
        click.echo("Zipping project...")

    # TODO: raise an error if zip file is too large
    zip_project(force, runid, github_number, verbose)

    if verbose:
        click.echo("Uploading project...")

    response = get_presigned_link()

    upload_zipped_project(response, verbose)

    if verbose:
        click.echo("Starting build...")

    trigger()

    # TODO: if anything fails after runs_new, update the status to error

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
