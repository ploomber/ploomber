#!/bin/sh

which pythona > /dev/null 2>&1
PYTHON_EXIT_STATUS=$?
if [ "$PYTHON_EXIT_STATUS" -ne "0" ] 
then
    echo 'Python not found. Installing Python...'

    which condaa > /dev/null 2>&1
    CONDA_EXIT_STATUS=$?
    if [ "$CONDA_EXIT_STATUS" -ne "0" ] 
    then
        echo 'Conda not found. Installing conda first...'
        OS=$(uname)
        if [ "$OS" == "Darwin" ]
        then 
            wget https://repo.continuum.io/archive/Anaconda3-2022.05-MacOSX-x86_64.sh
            bash Anaconda3-2022.05-MacOSX-x86_64.sh
        elif [ "$OS" == "Linux" ]
        then
            wget https://repo.continuum.io/archive/Anaconda3-2022.05-Linux-x86_64.sh
            bash Anaconda3-2022.05-Linux-x86_64.sh
        elif [ "$OS" == "Windows" ]
        then
            wget https://repo.continuum.io/archive/Anaconda3-2022.05-Windows-x86_64.exe
            bash Anaconda3-2022.05-Windows-x86_64.exe
        fi
    fi
    echo 'Now continuing to install python...'
    conda install -c conda-forge python
fi

python src/ploomber/cli/install_ploomber.py