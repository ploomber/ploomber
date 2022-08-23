import sys
import shutil
import subprocess

if sys.version_info < (3, 7):
    print("Python version < 3.7 is not supported. Please upgrade.")
else:
    CONDA_EXISTS = shutil.which('conda')
    ENV_NAME = "ploomber-install-env"

    if CONDA_EXISTS:
        subprocess.run(f"""
        conda create --name {ENV_NAME}
        source activate {ENV_NAME}
        conda install pip
        pip install ploomber
        """,
                       capture_output=False,
                       shell=True,
                       check=True)

        print(f"""
To activate this environment, use
    $ conda activate {ENV_NAME}
To deactivate an active environment, use
    $ conda deactivate\n""")

    else:
        subprocess.run(f"""
        python -m venv {ENV_NAME}
        ./{ENV_NAME}/bin/pip install ploomber
        """,
                       capture_output=False,
                       shell=True,
                       check=True)

        print(f"""
To activate this environment, use
    $ source {ENV_NAME}/bin/activate
To deactivate an active environment, use
    $ deactivate\n""")
