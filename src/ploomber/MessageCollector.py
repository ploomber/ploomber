from collections import namedtuple

_sep = '\n\n'+'-'*80+'\n'+'-'*80+'\n\n'


Message = namedtuple('Message',
                     ['task_str', 'message', 'obj'])


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
        self.messages.append(Message(task_str=task_str,
                                     message=message,
                                     obj=obj))

    def __str__(self):
        return _sep.join(['* {}: {}'.format(exp.task_str,
                                            exp.message)
                          for exp in self.messages])

    def __bool__(self):
        return bool(self.messages)

    def __iter__(self):
        for message in self.messages:
            yield message
