"""
Implementation of:

$ plomber install

This command runs a bunch of pip/conda commands (depending on what's available)
and it does the *right thing*: creating a new environment if needed, and
locking dependencies.
"""
import sys
import json
import os
import shutil
from pathlib import Path
from contextlib import contextmanager

import click
import yaml

from ploomber.io._commander import Commander
from ploomber_core.exceptions import BaseException
from ploomber.util.util import check_mixed_envs
from ploomber.cli.io import command_endpoint
from ploomber_core.telemetry import telemetry as _telemetry
from ploomber.util._sys import _python_bin
from ploomber.telemetry import telemetry

_SETUP_PY = "setup.py"

_REQS_LOCK_TXT = "requirements.lock.txt"
_REQS_TXT = "requirements.txt"

_ENV_YML = "environment.yml"
_ENV_LOCK_YML = "environment.lock.yml"

_PYTHON_BIN_NAME = _python_bin()


@command_endpoint
@telemetry.log_call("install")
def main(use_lock, create_env=None, use_venv=False):
    """
    Install project, automatically detecting if it's a conda-based or pip-based
    project.

    Parameters
    ---------
    use_lock : bool
        If True Uses requirements.lock.txt/environment.lock.yml and
        requirements.dev.lock.txt/environment.dev.lock.yml files. If False
        uses regular files and creates the lock ones after installing
        dependencies. If None, it uses lock files if they exist, if they don't
        it uses regular files

    create_env : bool, default=None
        If True, creates a new environment, if False, it installs in the
        current environment. If None, it creates a new environment if there
        isn't one already active

    use_venv : bool, default=False
        Force to use Python's venv module, ignoring conda if installed
    """
    USE_CONDA = shutil.which("conda") and not use_venv
    ENV_YML_EXISTS = Path(_ENV_YML).exists()
    ENV_LOCK_YML_EXISTS = Path(_ENV_LOCK_YML).exists()
    REQS_TXT_EXISTS = Path(_REQS_TXT).exists()
    REQS_LOCK_TXT_EXISTS = Path(_REQS_LOCK_TXT).exists()

    if use_lock is None:
        if USE_CONDA:
            use_lock = ENV_LOCK_YML_EXISTS
        else:
            use_lock = REQS_LOCK_TXT_EXISTS

    if use_lock and not ENV_LOCK_YML_EXISTS and not REQS_LOCK_TXT_EXISTS:
        raise BaseException(
            "Expected an environment.lock.yaml "
            "(conda) or requirements.lock.txt (pip) in the current "
            "directory. Add one of them and try again.",
            type_="no_lock",
        )
    elif not use_lock and not ENV_YML_EXISTS and not REQS_TXT_EXISTS:
        raise BaseException(
            "Expected an environment.yaml (conda)"
            " or requirements.txt (pip) in the current directory."
            " Add one of them and try again.",
            type_="no_env_requirements",
        )
    elif (
        not USE_CONDA and use_lock and ENV_LOCK_YML_EXISTS and not REQS_LOCK_TXT_EXISTS
    ):
        raise BaseException(
            "Found env environment.lock.yaml "
            "but conda is not installed. Install conda or add a "
            "requirements.lock.txt to use pip instead",
            type_="no_conda",
        )
    elif not USE_CONDA and not use_lock and ENV_YML_EXISTS and not REQS_TXT_EXISTS:
        raise BaseException(
            "Found environment.yaml but conda is not installed."
            " Install conda or add a requirements.txt to use pip instead",
            type_="no_conda2",
        )
    elif USE_CONDA and use_lock and ENV_LOCK_YML_EXISTS:
        # TODO: emit warnings if unused requirements.txt?
        main_conda(
            use_lock=True,
            create_env=create_env
            if create_env is not None
            else _should_create_conda_env(),
        )
    elif USE_CONDA and not use_lock and ENV_YML_EXISTS:
        # TODO: emit warnings if unused requirements.txt?
        main_conda(
            use_lock=False,
            create_env=create_env
            if create_env is not None
            else _should_create_conda_env(),
        )
    else:
        # TODO: emit warnings if unused environment.yml?
        main_pip(
            use_lock=use_lock,
            create_env=create_env
            if create_env is not None
            else not _telemetry.in_virtualenv(),
        )


def main_pip(use_lock, create_env=True):
    """
    Install pip-based project (uses venv), looks for requirements.txt files

    Parameters
    ----------
    start_time : datetime
        The initial runtime of the function.

    use_lock : bool
        If True Uses requirements.txt and requirements.dev.lock.txt files

    create_env : bool
        If True, it uses the venv module to create a new virtual environment,
        then installs the dependencies, otherwise it installs the dependencies
        in the current environment
    """
    reqs_txt = _REQS_LOCK_TXT if use_lock else _REQS_TXT
    reqs_dev_txt = "requirements.dev.lock.txt" if use_lock else "requirements.dev.txt"

    cmdr = Commander()

    # TODO: modify readme to add how to activate env? probably also in conda
    name = Path(".").resolve().name

    try:
        _run_pip_commands(cmdr, create_env, name, reqs_dev_txt, reqs_txt, use_lock)
    except Exception as e:
        cmd = f"pip install --requirement {reqs_txt}"
        raise BaseException(
            "Failed to setup your environment. " f"Invoke pip manually.\n{cmd}\n\n"
        ) from e


def _run_pip_commands(cmdr, create_env, name, reqs_dev_txt, reqs_txt, use_lock):
    if create_env:
        venv_dir = f"venv-{name}"
        cmdr.print("Creating venv...")
        cmdr.run(_PYTHON_BIN_NAME, "-m", "venv", venv_dir, description="Creating venv")

        # add venv_dir to .gitignore if it doesn't exist
        if Path(".gitignore").exists():
            with open(".gitignore") as f:
                if venv_dir not in f.read():
                    cmdr.append_inline(venv_dir, ".gitignore")
        else:
            cmdr.append_inline(venv_dir, ".gitignore")

        folder, bin_name = _get_pip_folder_and_bin_name()
        pip = str(Path(venv_dir, folder, bin_name))

        if os.name == "nt":
            cmd_activate = f"{venv_dir}\\Scripts\\Activate.ps1"
        else:
            cmd_activate = f"source {venv_dir}/bin/activate"
    else:
        cmdr.print("Installing in current venv...")
        pip = "pip"
        cmd_activate = None

    # FIXME: using an old version of pip may lead to broken environments, so
    # we need to ensure we upgrade before installing dependencies.

    if Path(_SETUP_PY).exists():
        _pip_install_setup_py_pip(cmdr, pip)

    _pip_install(cmdr, pip, lock=not use_lock, requirements=reqs_txt)

    if Path(reqs_dev_txt).exists():
        _pip_install(cmdr, pip, lock=not use_lock, requirements=reqs_dev_txt)

    _next_steps(cmdr, cmd_activate)


def main_conda(use_lock, create_env=True):
    """
    Install conda-based project, looks for environment.yml files

    Parameters
    ----------
    use_lock : bool
        If True Uses environment.lock.yml and environment.dev.lock.yml files


    create_env : bool
        If True, it uses the venv module to create a new virtual environment,
        then installs the dependencies, otherwise it installs the dependencies
        in the current environment
    """
    env_yml = _ENV_LOCK_YML if use_lock else _ENV_YML

    # TODO: ensure ploomber-scaffold includes dependency file (including
    # lock files in MANIFEST.in
    cmdr = Commander()

    # TODO: provide helpful error messages on each command

    if create_env:
        with open(env_yml) as f:
            env_name = yaml.safe_load(f)["name"]

        current_env = _current_conda_env_name()

        if env_name == current_env:
            err = (
                f"{env_yml} will create an environment "
                f"named {env_name!r}, which is the current active "
                "environment. Activate a different one and try "
                "again: conda activate base"
            )
            telemetry.log_api(
                "install-error",
                metadata={"type": "env_running_conflict", "exception": err},
            )
            raise BaseException(err)
    else:
        env_name = _current_conda_env_name()

    # get current installed envs
    conda = shutil.which("conda")
    mamba = shutil.which("mamba")

    # if already installed and running on windows, ask to delete first,
    # otherwise it might lead to an intermittent error (permission denied
    # on vcruntime140.dll)
    if os.name == "nt" and create_env:
        envs = cmdr.run(conda, "env", "list", "--json", capture_output=True)
        already_installed = any(
            [
                env
                for env in json.loads(envs)["envs"]
                # only check in the envs folder, ignore envs in other locations
                if "envs" in env and env_name in env
            ]
        )

        if already_installed:
            err = (
                f"Environment {env_name!r} already exists, "
                f"delete it and try again "
                f"(conda env remove --name {env_name})"
            )
            telemetry.log_api(
                "install-error", metadata={"type": "duplicate_env", "exception": err}
            )
            raise BaseException(err)

    pkg_manager = mamba if mamba else conda

    try:
        _run_conda_commands(
            cmdr, pkg_manager, create_env, env_yml, env_name, use_lock, conda
        )
    except Exception as e:
        if create_env:
            cmd = f"conda env create --file {env_yml} --force"
        else:
            cmd = f"conda env update --file {env_yml} --name {env_name}"
        raise BaseException(
            "Failed to setup your environment. " f"Invoke conda manually.\n{cmd}\n\n"
        ) from e


def _run_conda_commands(
    cmdr,
    pkg_manager,
    create_env,
    env_yml,
    env_name,
    use_lock,
    conda,
):
    if create_env:
        cmdr.print("Creating conda env...")
        cmdr.run(
            pkg_manager,
            "env",
            "create",
            "--file",
            env_yml,
            "--force",
            description="Creating env",
        )
    else:
        cmdr.print("Installing in current conda env...")

        cmdr.run(
            pkg_manager,
            "env",
            "update",
            "--file",
            env_yml,
            "--name",
            env_name,
            description="Installing dependencies",
        )

    if Path(_SETUP_PY).exists():
        _pip_install_setup_py_conda(cmdr, env_name)

    if not use_lock:
        env_lock = cmdr.run(
            conda,
            "env",
            "export",
            "--no-build",
            "--name",
            env_name,
            description="Locking dependencies",
            capture_output=True,
        )
        Path(_ENV_LOCK_YML).write_text(env_lock)

    _try_conda_install_and_lock_dev(cmdr, pkg_manager, env_name, use_lock=use_lock)

    cmd_activate = f"conda activate {env_name}" if create_env else None
    _next_steps(cmdr, cmd_activate)


def _should_create_conda_env():
    # not in conda env or running in base conda env
    return not _telemetry.is_conda() or (
        _telemetry.is_conda() and _current_conda_env_name() == "base"
    )


def _current_conda_env_name():
    return os.environ.get("CONDA_DEFAULT_ENV") or Path(sys.executable).parents[1].name


def _get_pip_folder_and_bin_name():
    folder = "Scripts" if os.name == "nt" else "bin"
    bin_name = "pip.exe" if os.name == "nt" else "pip"
    return folder, bin_name


def _find_conda_root(conda_bin):
    conda_bin = Path(conda_bin)

    for parent in conda_bin.parents:
        # I've seen variations of this. on windows: Miniconda3 and miniconda3
        # on linux miniconda3, anaconda and miniconda
        if parent.name.lower() in {"miniconda3", "miniconda", "anaconda3"}:
            return parent
    err = (
        "Failed to locate conda root from "
        f"directory: {str(conda_bin)!r}. Please submit an issue: "
        "https://github.com/ploomber/ploomber/issues/new"
    )
    telemetry.log_api(
        "install-error", metadata={"type": "no_conda_root", "exception": err}
    )
    raise BaseException(err)


def _path_to_pip_in_env_with_name(conda_bin, env_name):
    conda_root = _find_conda_root(conda_bin)
    folder, bin_name = _get_pip_folder_and_bin_name()
    return str(conda_root / "envs" / env_name / folder / bin_name)


def _locate_pip_inside_conda(env_name):
    """
    Locates pip inside the conda env with a given name
    """
    pip = _path_to_pip_in_env_with_name(shutil.which("conda"), env_name)

    # this might happen if the environment does not contain python/pip
    if not Path(pip).exists():
        err = (
            f"Could not locate pip in environment {env_name!r}, make sure "
            "it is included in your environment.yml and try again"
        )
        telemetry.log_api(
            "install-error", metadata={"type": "no_pip_env", "exception": err}
        )
        raise BaseException(err)

    return pip


def _pip_install_setup_py_conda(cmdr, env_name):
    """
    Call "pip install --editable ." if setup.py exists. Automatically locates
    the appropriate pip binary inside the conda env given the env name
    """
    pip = _locate_pip_inside_conda(env_name)
    _pip_install_setup_py_pip(cmdr, pip)


def _pip_install_setup_py_pip(cmdr, pip):
    cmdr.run(pip, "install", "--editable", ".", description="Installing project")


def _try_conda_install_and_lock_dev(cmdr, pkg_manager, env_name, use_lock):
    env_yml = "environment.dev.lock.yml" if use_lock else "environment.dev.yml"

    if Path(env_yml).exists():
        cmdr.run(
            pkg_manager,
            "env",
            "update",
            "--file",
            env_yml,
            "--name",
            env_name,
            description="Installing dev dependencies",
        )

        if not use_lock:
            env_lock = cmdr.run(
                shutil.which("conda"),
                "env",
                "export",
                "--no-build",
                "--name",
                env_name,
                description="Locking dev dependencies",
                capture_output=True,
            )
            Path("environment.dev.lock.yml").write_text(env_lock)


def _next_steps(cmdr, cmd_activate):
    cmdr.success("Next steps")
    message = f"$ {cmd_activate}\n" if cmd_activate else ""
    cmdr.print((f"{message}$ ploomber build"))
    cmdr.success()


def _pip_install(cmdr, pip, lock, requirements=_REQS_TXT):
    """Install and freeze requirements

    Parameters
    ----------
    cmdr
        Commander instance

    pip
        Path to pip binary

    lock
        If true, locks dependencies and stores them in a requirements.lock.txt
    """
    cmdr.run(
        pip,
        "install",
        "--requirement",
        requirements,
        description="Installing dependencies",
    )

    if lock:
        pip_lock = cmdr.run(
            pip,
            "freeze",
            "--exclude-editable",
            description="Locking dependencies",
            capture_output=True,
        )
        check_mixed_envs(pip_lock)
        name = Path(requirements).stem
        Path(f"{name}.lock.txt").write_text(pip_lock)


def _environment_yml_has_python(path):
    with open(path) as f:
        env_yml = yaml.safe_load(f)

    deps = env_yml.get("dependencies", [])

    has_python = False
    idx = None

    for i, line in enumerate(deps):
        if isinstance(line, str) and line.startswith("python"):
            has_python = True
            idx = i
            break

    if has_python:
        env_yml["dependencies"].pop(idx)

    return has_python, env_yml


@contextmanager
def check_environment_yaml(path, enable=True):
    has_python, env_yml = _environment_yml_has_python(path)
    TMP_FILENAME = ".ploomber-conda-tmp.yml"

    if has_python and enable:
        path_to_use = Path(TMP_FILENAME)
        path_to_use.write_text(yaml.dump(env_yml))
        click.secho(
            f"WARNING: {path!r} contains Python as "
            "dependency, ignoring it as it may break "
            "the current environment",
            fg="yellow",
        )
    else:
        path_to_use = Path(path)

    try:
        yield str(path_to_use)
    finally:
        if Path(TMP_FILENAME).exists():
            path_to_use.unlink()
