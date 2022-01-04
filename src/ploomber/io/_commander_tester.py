import subprocess


class CommanderTester:
    """Object to mock calls to subprocess.check_call

    Parameters
    ----------
    run : list of tuples
        List of tuples, where each tuple represents the argument passed to
        subprocess.check_call. Whenever the mocked object calls, CommandTester
        checks if the passed arg is in the run list, if so, it will
        call subprocess.check_call(arg)

    return_value : dict
        A mapping of tuples -> values. Whenever the mocked object calls,
        CommandTester checks if the passed arg is a key in this dictionary,
        if so, it returns the value corresponding to such key, instead
        of calling subprocess.check_call

    Examples
    --------
    >>> subprocess_mock = Mock()
    >>> tester = CommanderTester(run=[('python', 'script.py')],
    ...                       return_value={('python', '--version'): b'3.10'})
    >>> subprocess_mock.check_call.side_effect = tester
    >>> monkeypatch.setattr(some_module, 'subprocess', subprocess_mock)
    >>> # performs the call to subprocess.check_call
    >>> some_module.subprocess.check_call(['python', 'script.py'])
    >>> # does not perform call to subprocess, it just returns b'3.10'
    >>> some_module.subprocess.check_call(['python', '--version'])
    """
    def __init__(self, run=None, return_value=None):
        self._run = run or []
        self._return_value = return_value or dict()
        self._calls = []

    def __call__(self, cmd):
        self._calls.append(cmd)

        if cmd in self._run:
            return subprocess.check_call(cmd)
        elif cmd in self._return_value:
            return self._return_value[cmd]

    @property
    def calls(self):
        return self._calls
