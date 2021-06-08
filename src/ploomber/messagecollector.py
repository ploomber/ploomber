import abc
from ploomber.io import TerminalWriter
from io import StringIO


class Message:
    def __init__(self, task, message, obj=None):
        self._task = task
        self._message = message
        self._obj = obj

    @property
    def header(self):
        return repr(self._task)

    @property
    def sub_header(self):
        loc = self._task.source.loc
        return None if not loc else str(loc)

    @property
    def message(self):
        return self._message

    @property
    def obj(self):
        return self._obj


class MessageCollector(abc.ABC):
    """Collect messages and display them as a single str with task names
    Utilities for exception handling

    When a DAG is rendered or built, exceptions/warnings might happen, this
    class helps collect them all to show a meaninful error message where each
    one is shown along with the task name it generated it.

    """
    def __init__(self, messages=None):
        self.messages = messages or []

    def append(self, task, message, obj=None):
        self.messages.append(Message(task=task, message=message, obj=obj))

    @abc.abstractmethod
    def __str__(self):
        pass

    def _to_str(self, name=None, file=None, writer_kwargs=None):
        """
        Return the string representation of the collected messages

        Parameters
        ----------
        name
            Title to show at the end
        file
            Text stream to use. If None, uses a temporary StringIO object
        writer_kwargs
            Extra keyword arguments passed to the terminal writer
        """
        writer_kwargs = writer_kwargs or dict()

        if file is None:
            sio = StringIO()
        else:
            sio = file

        sio.write('\n')

        self.tw = TerminalWriter(file=sio)

        if name:
            self.tw.sep('=', title=name, **writer_kwargs)
        else:
            self.tw.sep('=', **writer_kwargs)

        for msg in self.messages:
            self.tw.sep('-', title=msg.header, **writer_kwargs)

            sub_header = msg.sub_header

            if sub_header:
                self.tw.sep('-', title=sub_header, **writer_kwargs)

            self.tw._write_source(msg.message.splitlines(), lexer='pytb')

        n = len(self)
        t = 'task' if n == 1 else 'tasks'
        self.tw.sep('=', title=f'Summary ({n} {t})', **writer_kwargs)

        for msg in self.messages:
            # TODO: include original exception type and error message in
            # summary
            self.tw.write(f'{msg.header}\n')

        if name:
            self.tw.sep('=', title=name, **writer_kwargs)
        else:
            self.tw.sep('=', **writer_kwargs)

        sio.seek(0)
        out = sio.read()

        if file is None:
            sio.close()

        return out

    def __len__(self):
        return len(self.messages)

    def __bool__(self):
        return bool(self.messages)

    def __iter__(self):
        for message in self.messages:
            yield message


class BuildExceptionsCollector(MessageCollector):
    def __str__(self):
        return self._to_str(name='DAG build failed',
                            file=None,
                            writer_kwargs=dict(red=True))


class RenderExceptionsCollector(MessageCollector):
    def __str__(self):
        return self._to_str(name='DAG render failed',
                            file=None,
                            writer_kwargs=dict(red=True))


class BuildWarningsCollector(MessageCollector):
    def __str__(self):
        return self._to_str(name='DAG build with warnings',
                            file=None,
                            writer_kwargs=dict(yellow=True))


class RenderWarningsCollector(MessageCollector):
    def __str__(self):
        return self._to_str(name='DAG render with warnings',
                            file=None,
                            writer_kwargs=dict(yellow=True))
