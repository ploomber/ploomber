"""
Clients that communicate with shell processes
"""
import tempfile
from pathlib import Path
import random
import string
import logging
import shlex
import subprocess
from subprocess import CalledProcessError

from ploomber.clients.Client import Client
from ploomber.templates.Placeholder import Placeholder

import paramiko


class ShellClient(Client):
    """Client to run command in the local shell
    """

    def __init__(self,
                 subprocess_run_kwargs={'stderr': subprocess.PIPE,
                                        'stdout': subprocess.PIPE,
                                        'shell': False}):
        """
        """
        self.subprocess_run_kwargs = subprocess_run_kwargs
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))

    @property
    def connection(self):
        raise NotImplementedError('ShellClient does not need a connection')

    def execute(self, code, run_template='bash {{path_to_code}}'):
        """Run code
        """
        _, path_to_tmp = tempfile.mkstemp()
        Path(path_to_tmp).write_text(code)

        run_template = Placeholder(run_template)
        source = run_template.render(dict(path_to_code=path_to_tmp))

        res = subprocess.run(shlex.split(source), **self.subprocess_run_kwargs)

        if res.returncode != 0:
            # log source code without expanded params
            self._logger.info(f'{code} returned stdout: '
                              f'{res.stdout} and stderr: {res.stderr} '
                              f'and exit status {res.returncode}')
            raise CalledProcessError(res.returncode, code)
        else:
            self._logger.info(f'Finished running {self}. stdout: {res.stdout},'
                              f' stderr: {res.stderr}')

    def close(self):
        pass


class RemoteShellClient(Client):
    """Client to run commands in a remote shell
    """

    def __init__(self, connect_kwargs, path_to_directory):
        """

        path_to_directory: str
            A path to save temporary files

        connect_kwargs: dict
            Parameters to send to the paramiko.SSHClient.connect constructor
        """
        self.path_to_directory = path_to_directory
        self.connect_kwargs = connect_kwargs
        self._raw_client = None
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))
        # CLIENTS.append(self)

    @property
    def connection(self):
        # client has not been created
        if self._raw_client is None:
            self._raw_client = paramiko.SSHClient()
            self._raw_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy())
            self._raw_client.connect(**self.connect_kwargs)

        # client has been created but still have to check if it's active:
        else:

            is_active = False

            # this might not always work: https://stackoverflow.com/a/28288598
            if self._raw_client.get_transport() is not None:
                is_active = self._raw_client.get_transport().is_active()

            if not is_active:
                self._raw_client.connect(**self.connect_kwargs)

        return self._raw_client

    def _random_name(self):
        filename = (''.join(random.choice(string.ascii_letters)
                            for i in range(16)))
        return filename

    def read_file(self, path):
        ftp = self.connection.open_sftp()

        _, path_to_tmp = tempfile.mkstemp()
        ftp.get(path, path_to_tmp)

        path_to_tmp = Path(path_to_tmp)

        content = path_to_tmp.read_text()
        path_to_tmp.unlink()

        ftp.close()

        return content

    def write_to_file(self, content, path):
        ftp = self.connection.open_sftp()

        _, path_to_tmp = tempfile.mkstemp()
        path_to_tmp = Path(path_to_tmp)
        path_to_tmp.write_text(content)
        ftp.put(path_to_tmp, path)

        ftp.close()
        path_to_tmp.unlink()

    def execute(self, code, run_template='bash {{path_to_code}}'):
        """Run code
        """
        ftp = self.connection.open_sftp()
        path_remote = self.path_to_directory + self._random_name()

        _, path_to_tmp = tempfile.mkstemp()
        Path(path_to_tmp).write_text(code)

        ftp.put(path_to_tmp, path_remote)
        ftp.close()

        run_template = Placeholder(run_template)
        source = run_template.render(dict(path_to_code=path_remote))

        # stream stdout. related: https://stackoverflow.com/q/31834743
        # using pty is not ideal, fabric has a clean implementation for this
        # worth checking out
        stdin, stdout, stderr = self.connection.exec_command(source,
                                                             get_pty=True)

        for line in iter(stdout.readline, ""):
            self._logger.info('(STDOUT): {}'.format(line))

        returncode = stdout.channel.recv_exit_status()

        stdout = ''.join(stdout)
        stderr = ''.join(stderr)

        if returncode != 0:
            # log source code without expanded params
            self._logger.info(f'{code} returned stdout: '
                              f'{stdout} and stderr: {stderr} '
                              f'and exit status {returncode}')
            raise CalledProcessError(returncode, code)
        else:
            self._logger.info(f'Finished running {self}. stdout: {stdout},'
                              f' stderr: {stderr}')

        return {'returncode': returncode, 'stdout': stdout, 'stderr': stderr}

    def close(self):
        if self._raw_client is not None:
            self._logger.info(f'Closing client {self._raw_client}')
            self._raw_client.close()
            self._raw_client = None
