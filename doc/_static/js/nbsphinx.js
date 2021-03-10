// This should appear only in nbsphinx
$(document).ready(function () {
    python = document.getElementsByClassName('highlight-python')
    addTerminalStyle(python, "Text editor (Python)")

    postgresql = $('.highlight-postgresql').not(".nboutput .highlight-postgresql")
    addTerminalStyle(postgresql, "Text editor (SQL)")

    yaml = document.getElementsByClassName('highlight-yaml')
    addTerminalStyle(yaml, "Text editor (YAML)")

    python_traceback = document.getElementsByClassName('highlight-pytb')
    addTerminalStyle(python_traceback, "Terminal (Python traceback)")

    python_console = document.getElementsByClassName('highlight-pycon')
    addTerminalStyle(python_console, "Terminal (Python)")
});