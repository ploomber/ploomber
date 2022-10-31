from pathlib import PurePosixPath
from urllib import parse
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from urllib.error import HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from glob import glob
import zipfile
from functools import wraps
from datetime import datetime
import json

import click
import humanize

from ploomber.table import Table
from ploomber.cloud import io, config
from ploomber.exceptions import BaseException, NetworkException
from ploomber.spec import DAGSpec
from ploomber.dag import util
from ploomber.cloud.key import get_key
from ploomber import _requests


def _is_s3_metadata(parsed):
    is_s3 = '.s3.amazonaws.com' in parsed.netloc
    name = PurePosixPath(parsed.path).name
    return is_s3 and name and name[0] == '.' and name.endswith('.metadata')


def _remove_prefix(path):
    parts = PurePosixPath(path).parts[2:]
    return str(PurePosixPath(*parts))


def _download_file(url,
                   skip_if_exists=False,
                   raise_on_missing=False,
                   path=None):
    try:
        response = _requests.get(url)

        if path is None:
            parsed_url = urlparse(url)
            path = parse_qs(
                parsed_url.query)['response-content-disposition'][0].split(
                    "filename = ")[1]

    except (HTTPError, NetworkException) as e:
        if e.code == 404:
            parsed = parse.urlparse(url)
            path = _remove_prefix(parsed.path[1:])

            if _is_s3_metadata(parsed) and e.code == 404:
                click.secho(f'Missing metadata file: {path}', fg='yellow')
                return
            elif raise_on_missing:
                raise FileNotFoundError(
                    'The requested file does not exist.'
                    ' Upload it to cloud storage and try again.')
            else:
                click.echo(f'File not found: {path}')
                return

        else:
            raise

    except Exception as e:
        print(f"There was an issue downloading the file: {e}")
        raise

    if skip_if_exists and Path(path).exists():
        print(f'{path} exists, skipping...')
    else:
        Path(path).parent.mkdir(exist_ok=True, parents=True)
        print(f'Writing file into path {path}')
        try:
            with open(path, "wb") as file:
                file.write(response.content)
        except Exception as e:
            print(f"Error reading the file from URL: {e}")

    return path


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


def _parse_datetime(timestamp):
    return humanize.naturaltime(
        datetime.fromisoformat(timestamp),
        when=datetime.utcnow(),
    )


def formatter(out, json_):
    if json_:
        s = json.dumps(out)
    else:
        s = Table.from_dicts(out)

    click.echo(s)


class Echo:

    def __init__(self, enable):
        self.enable = enable

    def __call__(self, s, **kwargs):
        if self.enable:
            click.secho(s, **kwargs)


def _has_prefix(path, prefixes, base_dir):
    if not prefixes:
        return False

    # remove leading ./ if needed
    path = str(Path(path))

    return any(
        path.startswith(str(Path(base_dir, prefix))) for prefix in prefixes)


def auth_header(func):

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        api_key = self._key

        if api_key:
            headers = {
                "Authorization": api_key,
                "Content-Type": "application/json"
            }

            return func(self, headers, *args, **kwargs)
        else:
            click.secho(
                'Error: Missing API key. '
                'Get one: https://www.cloud.ploomber.io/signin.html',
                fg='red')
            sys.exit(1)

    return wrapper


class PloomberCloudAPI:

    def __init__(self, key=None, host=None):
        self._key = key or get_key()
        self._host = host or os.environ.get('PLOOMBER_CLOUD_HOST',
                                            'https://api.ploomber.io')

    # NOTE: this doesn't need authentication (add unit test)
    def tasks_update(self, task_id, status):
        return _requests.get(f"{self._host}/tasks/{task_id}/{status}").json()

    @auth_header
    def runs_new(self, headers, metadata):
        """Register a new run in the database
        """
        response = _requests.post(f"{self._host}/runs",
                                  headers=headers,
                                  json=metadata)
        return response.json()['runid']

    @auth_header
    def runs_update(self, headers, runid, graph):
        """Update run status, store graph
        """
        return _requests.put(f"{self._host}/runs/{runid}",
                             headers=headers,
                             json=graph).json()

    @auth_header
    def runs_register_ids(self, headers, runid, ids):
        """Update run status, store ids
        """
        return _requests.put(f"{self._host}/runs/{runid}/ids",
                             headers=headers,
                             json=ids).json()

    @auth_header
    def runs(self, headers, json=False):
        res = _requests.get(f"{self._host}/runs", headers=headers).json()

        for run in res:
            run['created_at'] = _parse_datetime(run['created_at'])

        formatter(res, json_=json)

    @auth_header
    def run_detail(self, headers, run_id):
        res = _requests.get(f"{self._host}/runs/{run_id}",
                            headers=headers).json()
        return res

    @auth_header
    def run_logs(self, headers, run_id, name=None):
        run_id = self.process_run_id(run_id)
        res = _requests.get(f"{self._host}/runs/{run_id}/logs",
                            headers=headers).json()

        if not name:
            for name, log in res.items():
                click.echo(f'\n\n***** START OF LOGS FOR TASK: {name} *****')
                click.echo(log)
                click.echo(f'***** END OF LOGS FOR TASK: {name} *****')
        elif name not in res:
            keys = ','.join(res.keys())
            click.secho(f'Run has no task with name {name!r}. '
                        f'Available tasks are: {keys}')
        else:
            click.echo(f'\n\n***** START OF LOGS FOR TASK: {name} *****')
            click.echo(res[name])
            click.echo(f'***** END OF LOGS FOR TASK: {name} *****')

    @auth_header
    def run_logs_image(self, headers, run_id, tail=None):
        done = False
        run_id = self.process_run_id(run_id)
        res = _requests.get(f"{self._host}/runs/{run_id}/logs/image",
                            headers=headers)

        if not len(res.text):
            out = "Image build hasn't started yet. Wait a moment..."
        elif tail:
            out = '\n'.join(res.text.splitlines()[-tail:])
        else:
            out = res.text

        click.echo(out)

        if "POST_BUILD State: SUCCEEDED" in out:
            done = True
            click.secho(
                '\nSuccessful Docker build! Monitor task status:\n  '
                f'$ ploomber cloud status {run_id} --watch',
                fg='green')

        return done

    @auth_header
    def run_abort(self, headers, run_id):
        run_id = self.process_run_id(run_id)
        _requests.get(f"{self._host}/runs/{run_id}/abort",
                      headers=headers).json()
        print("Aborted.")

    @auth_header
    def run_finished(self, headers, runid):
        response = _requests.get(f"{self._host}/runs/{runid}/finished",
                                 headers=headers)
        return response

    @auth_header
    def run_failed(self, headers, runid, reason):
        if reason != "none":
            _requests.get(f"{self._host}/runs/{runid}/failed", headers=headers)
            click.echo(f'Marking run {runid} as failed...')

    @auth_header
    def run_latest_id(self, headers):
        res = _requests.get(f"{self._host}/runs/latest",
                            headers=headers).json()
        return res['runid']

    @auth_header
    def products_list(self, headers, json=False):
        res = _requests.get(f"{self._host}/products", headers=headers).json()

        if res:
            paths = [{'path': r} for r in res]
            formatter(paths, json_=json)
        else:
            print("No products found.")

    @auth_header
    def data_list(self, headers):
        res = _requests.get(f"{self._host}/data", headers=headers).json()

        if res:
            print(Table.from_dicts([{'path': r} for r in res]))
        else:
            print("No data found.")

    @auth_header
    def products_download(self, headers, pattern):
        res = _requests.post(f"{self._host}/products",
                             headers=headers,
                             json=dict(pattern=pattern)).json()
        download_from_presigned(res)

    @auth_header
    def get_presigned_link(self, headers, runid):
        return _requests.get(f"{self._host}/upload/{runid}",
                             headers=headers).json()

    @auth_header
    def trigger(self, headers, runid):
        res = _requests.get(f"{self._host}/trigger/{runid}",
                            headers=headers).json()

        return res

    @auth_header
    def upload_data(self,
                    headers,
                    path,
                    prefix,
                    key,
                    version=False,
                    verbose=True):
        """Upload a file to the user's workspace

        Parameters
        ----------
        version : bool, default=False
            If True, it adds a unique suffix to the `key`. e.g.,
            `path/to/nb.ipynb` becomes `path/to/nb-{uuid}.ipynb`, this causes
            the uploaded file to be unique. The function returns teh generated
            key.

        verbose : bool, default=True
            If True, it prints response information. Otherwise, it just returns
            the server's response.
        """
        key = key or Path(path).name

        response_create = _requests.post(f"{self._host}/upload/data/create",
                                         headers=headers,
                                         json=dict(key=key,
                                                   n_parts=io.n_parts(path),
                                                   prefix=prefix,
                                                   version=version)).json()

        if version:
            key = response_create['key']
            response_upload = response_create['upload']
        else:
            response_upload = response_create

        gen = io.UploadJobGenerator(path,
                                    key=f'{prefix}/{key}' if prefix else key,
                                    upload_id=response_upload['upload_id'],
                                    links=response_upload['urls'])

        if verbose:
            click.echo(f'Uploading {key}...')

        parts = gen.upload()

        _requests.post(f"{self._host}/upload/data/complete",
                       headers=headers,
                       json=dict(
                           key=key,
                           parts=parts,
                           prefix=prefix,
                           upload_id=response_upload['upload_id'])).json()

        return response_create

    @auth_header
    def download_data(self, headers, key):
        response = _requests.post(f"{self._host}/download/data",
                                  headers=headers,
                                  json=dict(key=key))
        return response

    @auth_header
    def delete_data(self, headers, pattern):
        response = _requests.delete(f"{self._host}/data",
                                    headers=headers,
                                    json=dict(pattern=pattern))
        print(response.json())

    @auth_header
    def delete_products(self, headers, pattern):
        response = _requests.delete(f"{self._host}/products",
                                    headers=headers,
                                    json=dict(pattern=pattern))
        print(response.json())

    @auth_header
    def notebooks_interface(self, headers, nbid):
        res = _requests.get(f"{self._host}/notebooks/{nbid}/interface",
                            headers=headers).json()
        return res

    @auth_header
    def notebooks_execute(self, headers, nbid, json_):
        """
        Parameters
        ----------
        json_ : bool
            If True, errors in the request are returned in JSON format
        """
        res = _requests.post(f"{self._host}/notebooks/{nbid}/execute",
                             headers=headers,
                             json=dict(),
                             json_error=json_).json()

        return res

    def process_run_id(self, run_id):
        if run_id in {'@latest', 'latest', 'last', '@last'}:
            run_id = self.run_latest_id()
        return run_id

    def run_detail_print(self, run_id, json=False):
        run_id = self.process_run_id(run_id)
        out = self.run_detail(run_id)
        tasks = out['tasks']
        run = out['run']
        echo = Echo(enable=not json)

        if run['status'] == 'created':
            echo('Run created...')

        elif run['status'] == 'finished':
            if tasks:
                formatter(tasks, json_=json)
            else:
                echo('Pipeline finished due to no newly triggered tasks,'
                     ' try running ploomber cloud build --force')

            echo(
                '\nPipeline finished. Check outputs:'
                '\n  $ ploomber cloud products',
                fg='green')

        elif tasks:
            tasks_created = all([t['status'] == 'created' for t in tasks])
            if tasks_created:
                echo('Tasks created. Execution started...\n')
            elif run['status'] == 'aborted':
                echo('Pipeline aborted...')

            formatter(tasks, json_=json)

            if run['status'] == 'failed':
                echo(
                    '\nPipeline failed. Check the logs.\n\nAll tasks:'
                    f'\n  $ ploomber cloud logs {run_id}\n'
                    '\nSpecific task:'
                    f'\n  $ ploomber cloud logs {run_id} '
                    '--task {task-name}\n',
                    fg='red')
                raise click.exceptions.ClickException('Pipeline failed.')

        else:
            echo('Unknown status: ' + run['status'] + ', no tasks triggered.')

        return out

    def build(self,
              force=False,
              github_number=None,
              github_owner=None,
              github_repo=None,
              verbose=False,
              task=None,
              base_dir=None):
        """Upload project and execute it
        """
        base_dir = Path(base_dir or '')

        # TODO: this should use the function in the default.py module to load
        # the default entry-point
        dag = DAGSpec(str(
            base_dir / 'pipeline.yaml')).to_dag().render(show_progress=False)

        if not ((base_dir / "requirements.lock.txt").exists() or
                (base_dir / "environment.lock.yml").exists()):
            raise BaseException(
                "A pip requirements.lock.txt file or "
                "conda environment.lock.yml file is required. Add one "
                "and try again.")

        config.validate()

        runid = self.runs_new(
            dict(force=force,
                 github_number=github_number,
                 github_owner=github_owner,
                 github_repo=github_repo))

        # TODO: ignore relative paths in products
        if verbose:
            click.echo("Zipping project -> project.zip")

        zip_project(force,
                    runid,
                    github_number,
                    verbose,
                    ignore_prefixes=util.extract_product_prefixes(
                        dag, base_dir=base_dir),
                    task=task,
                    base_dir=base_dir)

        if verbose:
            click.echo("Uploading project...")

        response = self.get_presigned_link(runid=runid)

        upload_zipped_project(response, verbose, base_dir=base_dir)

        if verbose:
            click.echo(f"Starting build with ID: {runid}")
            click.secho(
                "Monitor Docker build process with:\n  "
                f"$ ploomber cloud logs {runid} --image --watch",
                fg='green')

        self.trigger(runid=runid)

        # TODO: if anything fails after runs_new, update the status to error
        # convert runs_new into a context manager

        return runid


def zip_project(force,
                runid,
                github_number,
                verbose,
                ignore_prefixes=None,
                task=None,
                base_dir=None):
    """Compress project in a zip file

    Parameters
    ----------
    force
        Force flag (execute all tasks)

    runid
        ID identifying this run
    """
    base_dir = Path(base_dir or '')
    ignore_prefixes = ignore_prefixes or []

    path_to_zip = base_dir / "project.zip"

    if path_to_zip.exists():
        if verbose:
            click.echo("Deleting existing project.zip...")
        path_to_zip.unlink()

    files = glob(f"{base_dir}/**/*", recursive=True)

    # TODO: ignore __pycache__, ignore .git directory
    with zipfile.ZipFile(path_to_zip, "w", zipfile.ZIP_DEFLATED) as zip:
        for path in files:
            if not _has_prefix(path, ignore_prefixes, base_dir=base_dir):
                arcname = Path(path).relative_to(base_dir)
                zip.write(path, arcname=arcname)
            else:
                click.echo(f'Ignoring: {path}')

        # NOTE: it's weird that force is loaded from the buildspec but the
        # other two parameters are actually loaded from dynamodb
        zip.writestr(
            '.ploomber-cloud',
            json.dumps({
                'force': force,
                'runid': runid,
                'github_number': github_number,
                'task': task,
            }))

    MAX = 5 * 1024 * 1024

    if path_to_zip.stat().st_size > MAX:
        raise BaseException("Error: Your project's source code is over "
                            "5MB, which isn't supported. Tip: Ensure there "
                            "aren't any large data files and try again")


def upload_zipped_project(response, verbose, *, base_dir):
    base_dir = Path(base_dir or '')

    with open(base_dir / "project.zip", "rb") as f:
        files = {"file": f}
        http_response = _requests.post(response["url"],
                                       data=response["fields"],
                                       files=files)

    if http_response.status_code != 204:
        raise ValueError(f"An error happened: {http_response}")

    if verbose:
        click.echo("Uploaded project, starting execution...")
