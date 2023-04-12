import sys


def _python_bin():
    """
    Get the path to the Python executable, return 'python' if unable to get it
    """
    executable = sys.executable
    return executable if executable else "python"
