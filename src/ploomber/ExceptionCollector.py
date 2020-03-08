"""
Utilities for exception handling

When a DAG is rendered or built, multiple errors might happen, this utilities
help collect all errors and shown them at once
"""
from collections import namedtuple

_sep = '\n\n'+'-'*80+'\n'+'-'*80+'\n\n'


ExceptionResult = namedtuple('ExceptionResult',
                             ['task_str', 'traceback_str'])


class ExceptionCollector:

    def __init__(self, exceptions=None):
        self.exceptions = exceptions or []

    def append(self, task_str, traceback_str):
        self.exceptions.append(ExceptionResult(task_str=task_str,
                                               traceback_str=traceback_str))

    def __str__(self):
        return _sep.join(['* {}: {}'.format(exp.task_str,
                                            exp.traceback_str)
                          for exp in self.exceptions])

    def __bool__(self):
        return bool(self.exceptions)
