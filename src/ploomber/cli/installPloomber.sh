#!/bin/sh

which python > /dev/null 2>&1
EXIT_STATUS=$?
if [ "$EXIT_STATUS" -ne "0" ] 
then
    echo 'Python not found. Installing Python...'
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    brew install python@3.9
fi

python src/ploomber/cli/installPloomber.py