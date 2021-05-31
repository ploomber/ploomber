import subprocess


class CommanderTester:
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
