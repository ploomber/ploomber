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
        pip install jupyterlab
        ploomber examples -n guides/intro-to-ploomber
        cd guides/intro-to-ploomber
        ploomber install
        ploomber build
        conda deactivate
        """,
                       capture_output=False,
                       shell=True,
                       check=True)

        completedProcess = subprocess.run("printenv JUPYTER_SERVER_URL",
                                          capture_output=True,
                                          shell=True)

        if (completedProcess.returncode != 0):
            print(f"""
To activate this environment, use
    $ conda activate {ENV_NAME}
To deactivate an active environment, use
    $ conda deactivate\n""")

            subprocess.run(f"""
            source activate {ENV_NAME}
            cd guides/intro-to-ploomber
            jupyter lab
            """,
                           capture_output=False,
                           shell=True,
                           check=True)
        else:
            print(f"""
To activate this environment in JupyterLab, use
    $ CONDA_PATH=$(conda info | grep -i 'base environment' | cut -d ' ' -f 11)
    $ source $CONDA_PATH/etc/profile.d/conda.sh
    $ conda activate {ENV_NAME}
To deactivate an active environment, use
    $ conda deactivate\n""")

    else:
        subprocess.run(f"""
        python -m venv {ENV_NAME}
        ./{ENV_NAME}/bin/pip install ploomber
        ./{ENV_NAME}/bin/pip install jupyterlab
        source {ENV_NAME}/bin/activate
        ploomber examples -n guides/intro-to-ploomber
        cd guides/intro-to-ploomber
        ploomber install
        ploomber build
        deactivate
        """,
                       capture_output=False,
                       shell=True,
                       check=True)

        completedProcess = subprocess.run("printenv JUPYTER_SERVER_URL",
                                          capture_output=True,
                                          shell=True)

        print(f"""
To activate this environment, use
    $ source {ENV_NAME}/bin/activate
To deactivate an active environment, use
    $ deactivate\n""")

        if (completedProcess.returncode != 0):
            subprocess.run(f"""
            source {ENV_NAME}/bin/activate
            cd guides/intro-to-ploomber
            jupyter lab
            """,
                           capture_output=False,
                           shell=True,
                           check=True)
