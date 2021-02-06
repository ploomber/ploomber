from collections import namedtuple
from _pytest._io.terminalwriter import TerminalWriter
from io import StringIO

_sep = '\n\n' + '-' * 80 + '\n' + '-' * 80 + '\n\n'

Message = namedtuple('Message', ['task_str', 'message', 'obj'])


class MessageCollector:
    """Collect messages and display them as a single str with task names
    Utilities for exception handling

    When a DAG is rendered or built, exceptions/warnings might happen, this
    class helps collect them all to show a meaninful error message where each
    one is shown along with the task name it generated it.
    """
    def __init__(self, messages=None):
        self.messages = messages or []

    def append(self, task_str, message, obj=None):
        self.messages.append(
            Message(task_str=task_str, message=message, obj=obj))

    def __str__(self):
        import os
        os.environ['FORCE_COLOR'] = 'True'
        sio = StringIO()
        self.tw = TerminalWriter(file=sio)

        for exp in self.messages:
            self.tw.sep('-', title=exp.task_str, red=True)
            self.tw._write_source(exp.message.splitlines())
            self.tw.sep('-', red=True)

        self.tw.sep('=',
                    title=f'Failed tasks ({len(self.messages)})',
                    red=True)

        for exp in self.messages:
            # TODO: include error message
            self.tw.write(exp.task_str + '\n')

        self.tw.sep('=', red=True)

        sio.seek(0)
        out = sio.read()
        sio.close()
        return out

    def __bool__(self):
        return bool(self.messages)

    def __iter__(self):
        for message in self.messages:
            yield message
