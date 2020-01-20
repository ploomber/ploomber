"""
Task implementations

A Task is a unit of work that produces a persistent change (Product)
such as a bash or a SQL script
"""
import types
from urllib import request
from multiprocessing import Pool
import shlex
import subprocess
from subprocess import CalledProcessError
import logging
from ploomber.exceptions import SourceInitializationError
from ploomber.tasks.Task import Task
from ploomber.sources import (PythonCallableSource,
                                      GenericSource)


class BashCommand(Task):
    """A task that runs an inline bash command
    """

    def __init__(self, source, product, dag, name, params=None,
                 subprocess_run_kwargs={'stderr': subprocess.PIPE,
                                        'stdout': subprocess.PIPE,
                                        'shell': True},
                 split_source_code=False):
        super().__init__(source, product, dag, name, params)
        self.split_source_code = split_source_code
        self.subprocess_run_kwargs = subprocess_run_kwargs
        self._logger = logging.getLogger(__name__)

    def _init_source(self, source):
        source = GenericSource(str(source))

        if not source.needs_render:
            raise SourceInitializationError('The source for this task "{}"'
                                            ' must be a template since the '
                                            ' product will be passed as '
                                            ' parameter'
                                            .format(source.value.raw))

        return source

    def run(self):
        source_code = (shlex.split(self.source_code) if self.split_source_code
                       else self.source_code)

        res = subprocess.run(source_code,
                             **self.subprocess_run_kwargs)

        if res.returncode != 0:
            # log source code without expanded params
            self._logger.info(f'{self.source_code} returned stdout: '
                              f'{res.stdout} and stderr: {res.stderr} '
                              f'and exit status {res.returncode}')
            raise CalledProcessError(res.returncode, self.source_code)
        else:
            self._logger.info(f'Finished running {self}. stdout: {res.stdout},'
                              f' stderr: {res.stderr}')


class PythonCallable(Task):
    """A task that runs a Python callable (i.e.  a function)
    """
    def __init__(self, source, product, dag, name, params=None):
        super().__init__(source, product, dag, name, params)

    def _init_source(self, source):
        return PythonCallableSource(source)

    def run(self):
        if self.dag._executor.TASKS_CAN_CREATE_CHILD_PROCESSES:
            p = Pool()
            res = p.apply_async(func=self.source._source, kwds=self.params)

            # calling this make sure we catch the exception, from the docs:
            # Return the result when it arrives. If timeout is not None and
            # the result does not arrive within timeout seconds then
            # multiprocessing.TimeoutError is raised. If the remote call
            # raised an exception then that exception will be reraised by
            # get().
            # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.pool.AsyncResult.get
            if self.dag._executor.STOP_ON_EXCEPTION:
                res.get()

            p.close()
            p.join()
        else:
            self.source._source(**self.params)


class ShellScript(Task):
    """A task to run a shell script
    """

    def __init__(self, source, product, dag, name, params=None,
                 client=None):
        super().__init__(source, product, dag, name, params)

        self.client = client or self.dag.clients.get(type(self))

        if self.client is None:
            raise ValueError('{} must be initialized with a client'
                             .format(type(self).__name__))

    def _init_source(self, source):
        source = GenericSource(str(source))

        if not source.needs_render:
            raise SourceInitializationError('The source for this task '
                                            'must be a template since the '
                                            'product will be passed as '
                                            'parameter')

        return source

    def run(self):
        self.client.execute(str(self.source))


class DownloadFromURL(Task):
    def run(self):
        request.urlretrieve(str(self.source), filename=str(self.product))

    def _init_source(self, source):
        return GenericSource(str(source))


# TODO: move this and _Gather to helpers.py (actually create a module
# for partitioned execution)
class _Partition(Task):
    def __init__(self, product, dag, name):
        super().__init__(None, product, dag, name, None)

    def run(self):
        """This Task does not run anything
        """
        # TODO: verify that this task has only one upstream dependencies
        # is this the best place to check?
        pass

    def _init_source(self, source):
        return GenericSource(str(source))

    def _null(self):
        pass

    def _false(self):
        return False


class _Gather(PythonCallable):
    def run(self):
        pass


class Link(Task):
    """A dummy Task used to "plug" an external Product to a pipeline

    The purpose of this Task is to link a pipeline to an external read-only
    dataset, this task does not do anything on the dataset and the product is
    always considered up-to-date. There are two primary use cases:
    when the raw data is automatically uploaded to a file (or table) and the
    pipeline does not have control over data updates, this task can be used
    to link the pipeline to that file, without having to copy it, downstream
    tasks will see this dataset as just another Product. The second use case
    is when developing a prediction pipeline. When making predictions on new
    data, sometimes some of the features are not provided directly by the
    user but have to be looked up, this Task can help linking againts those
    processed datasets so that predictions only provide the minimum input
    to the model
    """

    def __init__(self, product, dag, name):
        # there is no source nor params for this product
        super().__init__(None, product, dag, name, None)

        # do not save metadata (Product's location is read-only)
        self.product.save_metadata = self._null

        # the product will never be considered outdated
        self.product._outdated_data_dependencies = self._false
        self.product._outdated_code_dependency = self._false

    def run(self):
        """This Task does not run anything
        """
        # TODO: verify that this task has no upstream dependencies
        # is this the best place to check?
        pass

    def _init_source(self, source):
        return GenericSource(str(source))

    def _null(self):
        pass

    def _false(self):
        return False


class Input(Task):
    """A dummy task used to represent input provided by the user

    When making new predictions, the user must submit some input data to build
    features and then feed the model, this task can be used for that. It does
    not perform any processing (read-only data) but it is always considered
    outdated, which means it will always trigger execution in downstream
    dependencies. Since at prediction time performance is important, metadata
    saving is skipped.
    """

    def __init__(self, product, dag, name):
        # there is no source nor params for this product
        super().__init__(None, product, dag, name, None)

        # do not save metadata (Product's location is read-only)
        self.product.save_metadata = self._null

        # the product will aleays be considered outdated
        self.product._outdated_data_dependencies = self._true
        self.product._outdated_code_dependency = self._true

    def run(self):
        """This Task does not run anything
        """
        # TODO: verify that this task has no upstream dependencies
        # is this the best place to check?
        pass

    def _init_source(self, source):
        return GenericSource(str(source))

    def _null(self):
        pass

    def _true(self):
        return True
