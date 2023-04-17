"""
Clients that communicate with shell processes
"""
import os
import tempfile
from pathlib import Path
import random
import string
import logging
import shlex
import subprocess

from ploomber.clients.client import Client
from ploomber.placeholders.placeholder import Placeholder
from ploomber_core.dependencies import requires


class ShellClient(Client):
    """Client to run command in the local shell

    Parameters
    ----------
    run_template : str
        Template for executing commands, must include the {{path_to_code}}
        placeholder which will point to the rendered code. Defaults to
        'bash {{path_to_code}}'
    subprocess_run_kwargs : dict
        Keyword arguments to pass to the subprocess.run when running
        run_template
    """

    def __init__(
        self,
        run_template="bash {{path_to_code}}",
        subprocess_run_kwargs={
            "stderr": subprocess.PIPE,
            "stdout": subprocess.PIPE,
            "shell": False,
        },
    ):
        self.subprocess_run_kwargs = subprocess_run_kwargs
        self.run_template = run_template
        self._logger = logging.getLogger("{}.{}".format(__name__, type(self).__name__))

    @property
    def connection(self):
        raise NotImplementedError("ShellClient does not need a connection")

    def execute(self, code):
        """Run code"""
        fd, path_to_tmp = tempfile.mkstemp()
        os.close(fd)
        Path(path_to_tmp).write_text(code)

        run_template = Placeholder(self.run_template)
        # we need quoting to make windows paths keep their \\ separators when
        # running shlex.split
        source = run_template.render(dict(path_to_code=shlex.quote(path_to_tmp)))

        res = subprocess.run(shlex.split(source), **self.subprocess_run_kwargs)
        Path(path_to_tmp).unlink()

        stdout = res.stdout.decode("utf-8")
        stderr = res.stderr.decode("utf-8")

        if res.returncode != 0:
            # log source code without expanded params
            self._logger.info(
                ("{} returned stdout: " "{}\nstderr: {}\n" "exit status {}").format(
                    code, stdout, stderr, res.returncode
                )
            )
            raise RuntimeError(
                (
                    "Error executing code.\nReturned stdout: "
                    "{}\nstderr: {}\n"
                    "exit status {}"
                ).format(stdout, stderr, res.returncode)
            )
        else:
            self._logger.info(
                ("Finished running {}. stdout: {}," " stderr: {}").format(
                    self, stdout, stderr
                )
            )

    def close(self):
        pass


class RemoteShellClient(Client):
    """EXPERIMENTAL: Client to run commands in a remote shell"""

    @requires(["paramiko"], "RemoteShellClient")
    def __init__(
        self, connect_kwargs, path_to_directory, run_template="bash {{path_to_code}}"
    ):
        """

        path_to_directory: str
            A path to save temporary files

        connect_kwargs: dict
            Parameters to send to the paramiko.SSHClient.connect constructor
        """
        self.path_to_directory = path_to_directory
        self.connect_kwargs = connect_kwargs
        self.run_template = run_template
        self._raw_client = None
        self._logger = logging.getLogger("{}.{}".format(__name__, type(self).__name__))

    @property
    def connection(self):
        # client has not been created
        if self._raw_client is None:
            self._raw_client, policy = ssh_client_and_policy()
            self._raw_client.set_missing_host_key_policy(policy)
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
        filename = "".join(random.choice(string.ascii_letters) for i in range(16))
        return filename

    def read_file(self, path):
        """Read file in remote path"""
        ftp = self.connection.open_sftp()

        # tmp location for file
        fd, path_to_tmp = tempfile.mkstemp()
        os.close(fd)
        ftp.get(path, path_to_tmp)

        path_to_tmp = Path(path_to_tmp)
        content = path_to_tmp.read_text()
        path_to_tmp.unlink()

        ftp.close()

        return content

    def write_to_file(self, content, path):
        """Write file to remote path"""
        ftp = self.connection.open_sftp()

        fd, path_to_tmp = tempfile.mkstemp()
        os.close(fd)
        path_to_tmp = Path(path_to_tmp)
        path_to_tmp.write_text(content)
        ftp.put(path_to_tmp, path)

        ftp.close()
        path_to_tmp.unlink()

    def execute(self, code):
        """Run code"""
        ftp = self.connection.open_sftp()
        path_remote = self.path_to_directory + self._random_name()

        fd, path_to_tmp = tempfile.mkstemp()
        os.close(fd)
        Path(path_to_tmp).write_text(code)

        ftp.put(path_to_tmp, path_remote)
        ftp.close()

        run_template = Placeholder(self.run_template)
        source = run_template.render(dict(path_to_code=path_remote))

        # stream stdout. related: https://stackoverflow.com/q/31834743
        # using pty is not ideal, fabric has a clean implementation for this
        # worth checking out
        stdin, stdout, stderr = self.connection.exec_command(source, get_pty=True)

        for line in iter(stdout.readline, ""):
            self._logger.info("(STDOUT): {}".format(line))

        returncode = stdout.channel.recv_exit_status()

        stdout = "".join(stdout)
        stderr = "".join(stderr)

        if returncode != 0:
            # log source code without expanded params
            self._logger.info(
                "%s returned stdout: " "%s and stderr: %s " "and exit status %s",
                code,
                stdout,
                stderr,
                returncode,
            )
            raise subprocess.CalledProcessError(returncode, code)
        else:
            self._logger.info(
                "Finished running %s. stdout: %s," " stderr: %s", self, stdout, stderr
            )

        return {"returncode": returncode, "stdout": stdout, "stderr": stderr}

    def close(self):
        if self._raw_client is not None:
            self._logger.info("Closing client %s", self._raw_client)
            self._raw_client.close()
            self._raw_client = None


def ssh_client_and_policy():
    # lazily loading paramiko, importing it takes a bit and we don't want
    # to slow down importing ploomber.clients. Instead of having this code
    # in the client's constructor, we add this function to helps us mock the
    # for testing
    import paramiko

    return paramiko.SSHClient(), paramiko.AutoAddPolicy()
