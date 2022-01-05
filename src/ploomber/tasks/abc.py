"""
Task abstract class

A Task is a unit of work, it has associated source code and a product
(a persistent object such as a table in a database), it has a name and lives
in a DAG.

[WIP] On subclassing Tasks

Implementation details:

* params (dict), upstream (Param object)
* params vs constructor parameters
* params on render vs params on run
* Implementing Task.run (using the source object, product, TaskBuildError)

Optional:

* Validating PRODUCT_CLASSES_ALLOWED
* Using a client parameter

Task execution model:

Upon instantiation, all tasks start with a WaitingRender status. On successful
render, they move to Skipped if they are up-to-date and all upstream
dependencies are up-to-date as well, WaitingExecution if they
are outdated but upstream dependencies are up-to-date or WaitingUpstream, if
any upstream dependency is outdated.

If render fails, they are marked as ErroredRender and downstream dependencies
are marked as AbortedRender.

When a Task is built, and succeeds, the on_finish hook is executed, if
on_finish succeeds as well, the Task is marked as Executed. If either
the task fails or on_finish fails, the Task is marked as Errored.

When a task is marked as Executed, is saves metadata in its Product,
if the product registered a prepare_metadata hook, the metadata to save
can be modified before is saved.

When a Task is marked as Errored, on_failure hook is executed and downstream
dependencies are marked as Aborted.


Beware that sometimes executors call Task._build in a different process, hence
updates won't be visible in Task objects that exist in the main process.
For that reason, executors have to report back status to the original Task
object using Task.exec_status, see executors documentation for details

TODO: describe BrokenProcesssPool status
"""
import abc
import logging
from datetime import datetime
from collections import defaultdict
from pathlib import Path

from ploomber.products import Product, MetaProduct, EmptyProduct, File
from ploomber.exceptions import (TaskBuildError, DAGBuildEarlyStop,
                                 CallbackCheckAborted)
from ploomber.tasks.taskgroup import TaskGroup
from ploomber.constants import TaskStatus
from ploomber.tasks._upstream import Upstream
from ploomber.tasks._params import Params
from ploomber.tasks.util import download_products_in_parallel
from ploomber.table import TaskReport, Row
from ploomber.util import isiterable
from ploomber.util.util import callback_check
from ploomber.dag.abstractdag import AbstractDAG

import humanize


class Task(abc.ABC):
    """
    Abstract class for all Tasks

    Parameters
    ----------
    source: str or pathlib.Path
        Source code for the task, for tasks that do not take source code
        as input (such as PostgresCopyFrom), this can be another thing. The
        source can be a template and can make references to any parameter
        in "params", "upstream" parameters or its own "product", not all
        Tasks have templated source (templating code is mostly used by
        Tasks that take SQL source code as input)
    product: Product
        The product that this task will create upon completion
    dag: DAG
        The DAG holding this task
    name: str
        A name for this task, if None a default will be assigned
    params: dict
        Extra parameters passed to the task on rendering (if templated
        source) or during execution (if not templated source)

    Attributes
    ----------
    params : Params
        A read-only dictionary-like object with params passed, after running
        'product' and 'upstream' are added, if any

    on_render : callable
        Function to execute after rendering. The function can request any of
        the following arguments: task, client, product, and params.

    on_finish : callable
        Function to execute upon execution. Can request the same arguments as
        the on_render hook.

    on_failure : callable
        Function to execute upon failure. Can request the same arguments as
        the on_render hook.

    Notes
    -----
    All subclasses must implement the same constuctor to keep the API
    consistent, optional parameters after "params" are ok
    """
    PRODUCT_CLASSES_ALLOWED = None

    @abc.abstractmethod
    def run(self):
        """This is the only required method Task subclasses must implement
        """
        pass  # pragma: no cover

    @abc.abstractmethod
    def _init_source(self, source, kwargs):
        pass  # pragma: no cover

    def __init__(self, product, dag, name=None, params=None):
        self._params = Params(params)

        if name is None:
            # use name inferred from the source object
            self._name = self._source.name

            if self._name is None:
                raise AttributeError('Task name can only be None if it '
                                     'can be inferred from the source object. '
                                     'For example, when the task receives a '
                                     'pathlib.Path, when using SourceLoader '
                                     'or in PythonCallable. Pass a value '
                                     'explicitly')
        else:
            self._name = name

        if not isinstance(dag, AbstractDAG):
            raise TypeError(
                f"'dag' must be an instance of DAG, got {type(dag)!r}")

        # NOTE: we should get rid of this, maybe just add hooks that are
        # called back on the dag object to avoid having a reference here
        self.dag = dag
        dag._add_task(self)

        if not hasattr(self, '_source'):
            raise RuntimeError(
                'self._source must be initialized before calling '
                '__init__ in Task')

        if self._source is None:
            raise TypeError(
                '_init_source must return a source object, got None')

        if isinstance(product, Product):
            self._product = product

            if self.PRODUCT_CLASSES_ALLOWED is not None:
                if not isinstance(self._product, self.PRODUCT_CLASSES_ALLOWED):
                    raise TypeError('{} only supports the following product '
                                    'classes: {}, got {}'.format(
                                        type(self).__name__,
                                        self.PRODUCT_CLASSES_ALLOWED,
                                        type(self._product).__name__))
        else:
            # if assigned a tuple/list of products, create a MetaProduct
            self._product = MetaProduct(product)

            if self.PRODUCT_CLASSES_ALLOWED is not None:
                if not all(
                        isinstance(p, self.PRODUCT_CLASSES_ALLOWED)
                        for p in self._product):
                    raise TypeError('{} only supports the following product '
                                    'classes: {}, got {}'.format(
                                        type(self).__name__,
                                        self.PRODUCT_CLASSES_ALLOWED,
                                        type(self._product).__name__))

        self._logger = logging.getLogger('{}.{}'.format(
            __name__,
            type(self).__name__))

        self.product.task = self
        self._client = None

        self.exec_status = TaskStatus.WaitingRender

        self._on_finish = None
        self._on_failure = None
        self._on_render = None

    @property
    def _available_callback_kwargs(self):
        # make it a property so we always get the latest value for self.client
        # given that could be None during init
        return {
            'task': self,
            'client': self.client,
            'product': self.product,
            'params': self.params
        }

    @property
    def client(self):
        return self._client

    @property
    def name(self):
        """
        A str that represents the name of the task, you can access tasks
        in a dag using dag['some_name']
        """
        return self._name

    @property
    def source(self):
        """
        Source is used by the task to compute its output, for most cases
        this is source code, for example PythonCallable takes a function
        as source and SQLScript takes a string with SQL code as source.
        But other tasks might take non-code objects as source, for example,
        PostgresCopyFrom takes a path to a file. If source represents code
        doing str(task.source) will return the string representation
        """
        return self._source

    @property
    def product(self):
        """The product this task will create upon execution
        """
        return self._product

    @property
    def upstream(self):
        """
        A mapping for upstream dependencies {task name} -> [task object]
        """
        # this is just syntactic sugar, upstream relations are tracked by the
        # DAG object

        # this always return a copy to prevent global state if contents
        # are modified (e.g. by using pop)
        return self.dag._get_upstream(self.name)

    @property
    def _upstream_product_grouped(self):
        """
        Similar to .upstream but this one nests groups. Output will be the
        same as .upstream if no upstream dependencies are grouped. This is
        only used internally in .render to correctly pass upstream to
        task.params.

        Unlike .upstream, this method returns products instead of task names
        """
        grouped = defaultdict(lambda: {})

        for up_name, up_task in self.upstream.items():
            data = self.dag._G.get_edge_data(up_name, self.name)

            if data and 'group_name' in data:
                group_name = data['group_name']
                grouped[group_name][up_name] = up_task.product
            else:
                grouped[up_name] = up_task.product

        return dict(grouped)

    @property
    def params(self):
        """
        dict that holds the parameter that will be passed to the task upon
        execution. Before rendering, this will only hold parameters passed
        in the Task constructor. After rendering, this will hold new keys:
        "product" contained the rendered product and "upstream" holding
        upstream parameters if there is any
        """
        return self._params

    @property
    def _lineage(self):
        """
        Set with task names of all the dependencies for this task
        (including dependencies of dependencies)
        """
        # if no upstream deps, there is no lineage
        if not len(self.upstream):
            return None
        else:
            # retrieve lineage: upstream tasks + lineage from upstream tasks
            up = list(self.upstream.keys())
            lineage_up = [
                up._lineage for up in self.upstream.values() if up._lineage
            ]
            lineage = up + [task for lineage in lineage_up for task in lineage]
            return set(lineage)

    @property
    def on_finish(self):
        """
        Callable to be executed after this task is built successfully
        (passes Task as first parameter)
        """
        return self._on_finish

    @on_finish.setter
    def on_finish(self, value):
        try:
            callback_check(value, self._available_callback_kwargs)
        # raised when value is a dotted path with lazy loading turned on.
        # cannot check because that requires importing the dotted path
        except CallbackCheckAborted:
            pass

        self._on_finish = value

    def _run_on_finish(self):
        """Call on_finish hook
        """
        if self.on_finish:
            kwargs = callback_check(self.on_finish,
                                    self._available_callback_kwargs)
            self.on_finish(**kwargs)

    def _post_run_actions(self):
        """
        Call on_finish hook, save metadata, verify products exist and upload
        product
        """
        # run on finish first, if this fails, we don't want to save metadata
        try:
            self._run_on_finish()
        except Exception:
            # NOTE: we also set the status in Task._build, which runs during
            # DAG.build() - but setting if here as well to prevent DAG
            # inconsistent state when the user calls Tas.build() directly
            self.exec_status = TaskStatus.Errored
            raise

        if self.exec_status == TaskStatus.WaitingDownload:
            # clear current metadata to force reload
            # and ensure the task uses the downloaded metadata
            self.product.metadata.clear()
        else:
            self.product.metadata.update(
                source_code=str(self.source),
                params=self.params.to_json_serializable(params_only=True))

        # For most Products, it's ok to do this check before
        # saving metadata, but not for GenericProduct, since the way
        # exists() works is by checking metadata, so we have to do it
        # here, after saving metadata
        if not self.product.exists():
            if isinstance(self.product, MetaProduct):
                raise TaskBuildError(
                    'Error building task "{}": '
                    'the task ran successfully but product '
                    '"{}" does not exist yet '
                    '(task.product.exists() returned False). '.format(
                        self.name, self.product))
            else:
                raise TaskBuildError(
                    'Error building task "{}": '
                    'the task ran successfully but at least one of the '
                    'products in "{}" does not exist yet '
                    '(task.product.exists() returned False). '.format(
                        self.name, self.product))

        if self.exec_status != TaskStatus.WaitingDownload:
            self.product.upload()

    @property
    def on_failure(self):
        """
        Callable to be executed if task fails (passes Task as first parameter
        and the exception as second parameter)
        """
        return self._on_failure

    @on_failure.setter
    def on_failure(self, value):
        try:
            callback_check(value, self._available_callback_kwargs)
        # raised when value is a dotted path with lazy loading turned on.
        # cannot check because that requires importing the dotted path
        except CallbackCheckAborted:
            pass
        self._on_failure = value

    def _run_on_failure(self):
        if self.on_failure:
            kwargs = callback_check(self.on_failure,
                                    self._available_callback_kwargs)
            self.on_failure(**kwargs)

    @property
    def on_render(self):
        return self._on_render

    @on_render.setter
    def on_render(self, value):
        try:
            callback_check(value, self._available_callback_kwargs)
        # raised when value is a dotted path with lazy loading turned on.
        # cannot check because that requires importing the dotted path
        except CallbackCheckAborted:
            pass
        self._on_render = value

    def _run_on_render(self):
        if self.on_render:
            self._logger.debug('Calling on_render hook on task %s', self.name)

            kwargs = callback_check(self.on_render,
                                    self._available_callback_kwargs)
            try:
                self.on_render(**kwargs)
            except Exception as e:
                msg = ('Exception when running on_render '
                       'for task "{}": {}'.format(self.name, e))
                self._logger.exception(msg)
                self.exec_status = TaskStatus.ErroredRender
                raise type(e)(msg) from e

    @property
    def exec_status(self):
        return self._exec_status

    @exec_status.setter
    def exec_status(self, value):
        # FIXME: this should only be used for th eexecutor to report back
        # status Executed or Errored, reject all other cases, those are handled
        # internally
        if value not in list(TaskStatus):
            raise ValueError(
                'Setting task.exec_status to an unknown '
                'value: %s', value)

        self._logger.debug('Setting "%s" status to %s', self.name, value)
        self._exec_status = value

        # process might crash, propagate now or changes might not be
        # reflected (e.g. if a Task is marked as Aborted, all downtream
        # tasks should be marked as aborted as well)
        # FIXME: this is inefficient, it is better to traverse
        # the dag in topological order but exclude nodes not affected by
        # this change
        self._update_downstream_status()

    def build(self, force=False, catch_exceptions=True):
        """Build a single task

        Although Tasks are primarily designed to execute via DAG.build(), it
        is possible to do so in isolation. However, this only works if the
        task does not have any unrendered upstream dependencies, if that's the
        case, you should call DAG.render() before calling Task.build()

        Examples
        --------
        >>> from pathlib import Path
        >>> from ploomber import DAG
        >>> from ploomber.tasks import PythonCallable
        >>> from ploomber.products import File
        >>> def fn(product):
        ...     Path(str(product)).touch()
        >>> PythonCallable(fn, File('file.txt'), dag=DAG()).build()

        Returns
        -------
        dict
            A dictionary with keys 'run' and 'elapsed'

        Raises
        ------
        TaskBuildError
            If the error failed to build because it has upstream dependencies,
            the build itself failed or build succeded but on_finish hook failed

        DAGBuildEarlyStop
            If any task or on_finish hook raises a DAGBuildEarlyStop error
        """
        # This is the public API for users who'd to run tasks in isolation,
        # we have to make sure we clear product cache status, otherwise
        # this will interfere with other render calls
        self.render(force=force)

        upstream_exec_status = [t.exec_status for t in self.upstream.values()]

        if any(exec_status == TaskStatus.WaitingRender
               for exec_status in upstream_exec_status):
            raise TaskBuildError('Cannot directly build task "{}" as it '
                                 'has upstream dependencies, call '
                                 'dag.render() first'.format(self.name))

        # we can execute an individual tasks if missing up-to-date upstream
        # dependencies exist in remote storage
        if self.exec_status == TaskStatus.WaitingUpstream:
            ok = {
                t
                for t in self.upstream.values() if t.exec_status in
                {TaskStatus.Skipped, TaskStatus.WaitingDownload}
            }

            not_ok = set(self.upstream.values()) - ok

            if not_ok:
                raise TaskBuildError(
                    f'Cannot build task {self.name!r} because '
                    'the following upstream dependencies are '
                    f'missing: {[t.name for t in not_ok]!r}. Execute upstream '
                    'tasks first. If upstream tasks generate File(s) and you '
                    'configured a File.client, you may also upload '
                    'up-to-date copies to remote storage and they will be '
                    'automatically downloaded')

            download_products_in_parallel(
                t for t in ok if t.exec_status == TaskStatus.WaitingDownload)

        # at this point the task must be WaitingDownload or WaitingExecution
        res, _ = self._build(catch_exceptions=catch_exceptions)

        self.product.metadata.clear()
        return res

    def _build(self, catch_exceptions):
        """
        Private API for building DAGs. This is what executors should call.
        Unlike the public method, this one does not call render, as it
        should happen via a dag.render() call. It takes care of running the
        task and updating status accordingly

        Parameters
        ----------
        catch_exceptions : bool
            If True, catches exceptions during execution and shows a chained
            exception at the end: [original exception] then
            [exception with context info]. Set it to False when debugging
            tasks to drop-in a debugging session at the failing line.
        """
        if not catch_exceptions:
            res = self._run()
            self._post_run_actions()
            return res, self.product.metadata.to_dict()
        else:
            try:
                # TODO: this calls download, if this happens. should
                # hooks be executed when dwnloading? if so, we could
                # change the ran? column from the task report to something
                # like:
                # ran/downloaded/skipped and use that to determine if we should
                # run hooks
                res = self._run()
            except Exception as e:
                msg = 'Error building task "{}"'.format(self.name)
                self._logger.exception(msg)
                self.exec_status = TaskStatus.Errored

                # if there isn't anything left to run, raise exception here
                if self.on_failure is None:
                    if isinstance(e, DAGBuildEarlyStop):
                        raise DAGBuildEarlyStop(
                            'Stopping task {} gracefully'.format(
                                self.name)) from e
                    else:
                        # FIXME: this makes the traceback longer, consider
                        # removing it. The only information this nested
                        # exception provides is the name of the task but we
                        # are still able to provide that if theh executor
                        # has the option to capture exceptions turned on.
                        # An option to consider is to
                        raise TaskBuildError(msg) from e

                build_success = False
                build_exception = e
            else:
                build_success = True
                build_exception = None

            if build_success:
                try:
                    self._post_run_actions()
                except Exception as e:
                    self.exec_status = TaskStatus.Errored
                    msg = ('Exception when running on_finish '
                           'for task "{}": {}'.format(self.name, e))
                    self._logger.exception(msg)

                    if isinstance(e, DAGBuildEarlyStop):
                        raise DAGBuildEarlyStop(
                            'Stopping task {} gracefully'.format(
                                self.name)) from e
                    else:
                        raise TaskBuildError(msg) from e
                else:
                    # sucessful task execution, on_finish hook execution,
                    # metadata saving and upload
                    self.exec_status = TaskStatus.Executed

                return res, self.product.metadata.to_dict()
            # error bulding task
            else:
                try:
                    self._run_on_failure()
                except Exception as e:
                    msg = ('Exception when running on_failure '
                           'for task "{}": {}'.format(self.name, e))
                    self._logger.exception(msg)
                    raise TaskBuildError(msg) from e

                if isinstance(build_exception, DAGBuildEarlyStop):
                    raise DAGBuildEarlyStop(
                        'Stopping task {} gracefully'.format(
                            self.name)) from build_exception
                else:
                    msg = 'Error building task "{}"'.format(self.name)
                    raise TaskBuildError(msg) from build_exception

    def _run(self):
        """
        Run or download task if certain status conditions are met, otherwise
        raise a TaskBuildError exception
        """
        # cannot keep running, we depend on the render step to get all the
        # parameters resolved (params, upstream, product)
        if self.exec_status == TaskStatus.WaitingRender:
            raise TaskBuildError('Error building task "{}". '
                                 'Cannot build task that has not been '
                                 'rendered, call DAG.render() first'.format(
                                     self.name))

        elif self.exec_status == TaskStatus.Aborted:
            raise TaskBuildError('Attempted to run task "{}", whose '
                                 'status is TaskStatus.Aborted'.format(
                                     self.name))
        elif self.exec_status == TaskStatus.Skipped:
            raise TaskBuildError('Attempted to run task "{}", whose '
                                 'status TaskStatus.Skipped. Render again and '
                                 'set force=True if you want to force '
                                 'execution'.format(self.name))

        # NOTE: should i fetch metadata here? I need to make sure I have
        # the latest before building
        self._logger.info('Starting execution: %s', repr(self))

        then = datetime.now()

        _ensure_parents_exist(self.product)

        if self.exec_status == TaskStatus.WaitingDownload:
            try:
                self.product.download()
            except Exception as e:
                raise TaskBuildError(
                    f'Error downloading Product {self.product!r} '
                    f'from task {self!r}. Check the full traceback above for '
                    'details') from e

        # NOTE: should we validate status here?
        # (i.e., check it's WaitingExecution)
        else:
            self.run()

        now = datetime.now()

        elapsed = (now - then).total_seconds()
        self._logger.info(
            'Done. Operation took {:.1f} seconds'.format(elapsed))

        # TODO: also check that the Products were updated:
        # if they did not exist, they must exist now, if they alredy
        # exist, timestamp must be recent equal to the datetime.now()
        # used. maybe run fetch metadata again and validate?

        return TaskReport.with_data(name=self.name, ran=True, elapsed=elapsed)

    def render(self, force=False, outdated_by_code=True, remote=False):
        """
        Renders code and product, all upstream tasks must have been rendered
        first, for that reason, this method will usually not be called
        directly but via DAG.render(), which renders in the right order.

        Render fully determines whether a task should run or not.

        Parameters
        ----------
        force : bool, default=False
            If True, mark status as WaitingExecution/WaitingUpstream even if
            the task is up-to-date (if there are any File(s) with clients, this
            also ignores the status of the remote copy), otherwise, the normal
            process follows and only up-to-date tasks are marked as Skipped.

        outdated_by_code : bool, default=True
            Factors to determine if Task.product is marked outdated when source
            code changes. Otherwise just the upstream timestamps are used.

        remote : bool, default=False
            Use remote metadata to determine status

        Notes
        -----
        This method tries to avoid calls to check for product status whenever
        possible, since checking product's metadata can be a slow operation
        (e.g. if metadata is stored in a remote database)

        When passing force=True, product's status checking is skipped
        altogether, this can be useful when we only want to quickly get
        a rendered DAG object to interact with it
        """
        self._logger.debug('Calling render on task %s', self.name)

        try:
            self._render_product()
        except Exception as e:
            self.exec_status = TaskStatus.ErroredRender
            raise type(e)('Error rendering product from Task "{}", '
                          ' check the full traceback above for details. '
                          'Task params: {}'.format(repr(self),
                                                   self.params)) from e

        # product does not becomes part of the task parameters when passing
        # an EmptyProduct - this special kind of task is used by InMemoryDAG.
        if not isinstance(self.product, EmptyProduct):
            self.params._setitem('product', self.product)

        try:
            self.source.render(self.params)
        except Exception as e:
            self.exec_status = TaskStatus.ErroredRender
            raise e

        # we use outdated status several time, this caches the result
        # for performance reasons. TODO: move this to Product and make it
        # a property
        is_outdated = ProductEvaluator(self.product,
                                       outdated_by_code,
                                       remote=remote)

        # task with no dependencies
        if not self.upstream:
            # nothing to do, just mark it ready for execution
            if force:
                self._exec_status = TaskStatus.WaitingExecution
                self._logger.debug(
                    'Forcing status "%s", outdated conditions'
                    ' ignored...', self.name)

            # task is outdated, check if we need to execute or download
            elif is_outdated.check():
                # This only happens with File
                if is_outdated.check() == TaskStatus.WaitingDownload:
                    self._exec_status = TaskStatus.WaitingDownload
                else:
                    self._exec_status = TaskStatus.WaitingExecution

            # task is up-to-date
            else:
                self._exec_status = TaskStatus.Skipped

        # tasks with dependencies
        else:
            upstream_exec_status = set(t.exec_status
                                       for t in self.upstream.values())

            all_upstream_ready = upstream_exec_status <= {
                TaskStatus.Executed, TaskStatus.Skipped
            }

            # some upstream tasks need execution (or download)
            if not all_upstream_ready:
                if force or is_outdated.check() is True:
                    self._exec_status = TaskStatus.WaitingUpstream
                elif is_outdated.check() == TaskStatus.WaitingDownload:
                    self._exec_status = TaskStatus.WaitingDownload
                else:
                    self._exec_status = TaskStatus.Skipped

            # all upstream ready
            else:
                if force or is_outdated.check() is True:
                    self._exec_status = TaskStatus.WaitingExecution
                elif is_outdated.check() == TaskStatus.WaitingDownload:
                    self._exec_status = TaskStatus.WaitingDownload
                else:
                    self._exec_status = TaskStatus.Skipped

        self._run_on_render()

    def set_upstream(self, other, group_name=None):
        self.dag._add_edge(other, self, group_name=group_name)

    def status(self, return_code_diff=False, sections=None):
        """Prints the current task status

        Parameters
        ----------
        sections : list, optional
            Sections to include. Defaults to "name", "last_run",
            "oudated", "product", "doc", "location"
        """
        sections = sections or [
            'name', 'last_run', 'outdated', 'product', 'doc', 'location'
        ]

        p = self.product

        data = {}

        if 'name' in sections:
            data['name'] = self.name

        if 'type' in sections:
            data['type'] = type(self).__name__

        if 'status' in sections:
            data['status'] = self.exec_status.name

        if 'client' in sections:
            # FIXME: all tasks should have a client property
            data['client'] = (repr(self.client)
                              if hasattr(self, 'client') else None)

        if 'last_run' in sections:
            if p.metadata.timestamp is not None:
                dt = datetime.fromtimestamp(p.metadata.timestamp)
                date_h = dt.strftime('%b %d, %y at %H:%M')
                time_h = humanize.naturaltime(dt)
                data['Last run'] = '{} ({})'.format(time_h, date_h)
            else:
                data['Last run'] = 'Has not been run'

        outd_data = p._outdated_data_dependencies()
        outd_code = p._outdated_code_dependency()

        outd = False

        if outd_code:
            outd = 'Source code'

        if outd_data:
            if not outd:
                outd = 'Upstream'
            else:
                outd += ' & Upstream'

        if 'outdated' in sections:
            data['Outdated?'] = outd

        if 'outdated_dependencies' in sections:
            data['Outdated dependencies'] = outd_data

        if 'outdated_code' in sections:
            data['Outdated code'] = outd_code

        if outd_code and return_code_diff:
            data['Code diff'] = (self.dag.differ.get_diff(
                p.metadata.stored_source_code,
                str(self.source),
                extension=self.source.extension))
        else:
            outd_code = ''

        if 'product_type' in sections:
            data['Product type'] = type(self.product).__name__

        if 'product' in sections:
            data['Product'] = repr(self.product)

        if 'product_client' in sections:
            # FIXME: all products should have a client property
            data['Product client'] = (repr(self.product.client) if hasattr(
                self.product, 'client') else None)

        if 'doc' in sections:
            data['Doc (short)'] = _doc_short(self.source.doc)

        if 'location' in sections:
            data['Location'] = self.source.loc

        return Row(data)

    def debug(self):
        """Debug task, only implemented in certain tasks
        """
        raise NotImplementedError(
            '"debug" is not implemented in "{}" tasks'.format(
                type(self).__name__))

    def develop(self):
        """Develop task, only implemented in certain tasks
        """
        raise NotImplementedError(
            '"develop" is not implemented in "{}" tasks'.format(
                type(self).__name__))

    def load(self):
        """Load task as pandas.DataFrame. Only implemented in certain tasks
        """
        raise NotImplementedError(
            '"load" is not implemented in "{}" tasks'.format(
                type(self).__name__))

    def _render_product(self):
        params_names = list(self.params)

        # add upstream product identifiers to params, if any
        # Params are read-only for users, but we have to add upstream
        # dependencies so we do it directly to the dictionary
        # TODO: process wilcards such as fit-*
        if self.upstream:
            self.params._setitem(
                'upstream',
                Upstream(self._upstream_product_grouped, name=self.name))

        # render the current product
        try:
            # using the upstream products to define the current product
            # is optional, using the parameters passed in params is also
            # optional
            self.product.render(self.params,
                                optional=set(params_names + ['upstream']))
        except Exception as e:
            raise type(e)('Error rendering Product from Task "{}", '
                          ' check the full traceback above for details'.format(
                              repr(self))) from e

    def _get_downstream(self):
        # make the _get_downstream more efficient by
        # using the networkx data structure directly
        downstream = []
        for t in self.dag.values():
            if self in t.upstream.values():
                downstream.append(t)
        return downstream

    def _update_downstream_status(self):
        # FIXME: this is inefficient, it is better to traverse
        # the dag in topological order but exclude nodes not affected by
        # this change
        # TODO: move to DAG
        def update_status(task):
            any_upstream_errored_or_aborted = any([
                t.exec_status in (TaskStatus.Errored, TaskStatus.Aborted)
                for t in task.upstream.values()
            ])
            all_upstream_ready = all([
                t.exec_status in {TaskStatus.Executed, TaskStatus.Skipped}
                for t in task.upstream.values()
            ])

            if any_upstream_errored_or_aborted:
                task.exec_status = TaskStatus.Aborted
            elif any([
                    t.exec_status
                    in (TaskStatus.ErroredRender, TaskStatus.AbortedRender)
                    for t in task.upstream.values()
            ]):
                task.exec_status = TaskStatus.AbortedRender
            # mark as waiting execution if moving from waiting upstream
            # the second situation if when task is waitingdownload, in which
            # case we don't do anything
            elif all_upstream_ready and (task.exec_status
                                         == TaskStatus.WaitingUpstream):
                task.exec_status = TaskStatus.WaitingExecution

        for t in self._get_downstream():
            update_status(t)

    def __rshift__(self, other):
        """ a >> b is the same as b.set_upstream(a)
        """
        other.set_upstream(self)
        # return other so a >> b >> c works
        return other

    def __add__(self, other):
        """ a + b means TaskGroup([a, b])
        """
        if isiterable(other) and not isinstance(other, AbstractDAG):
            return TaskGroup([self] + list(other))
        else:
            return TaskGroup((self, other))

    def __repr__(self):
        return ('{}: {} -> {}'.format(
            type(self).__name__, self.name, repr(self.product)))

    def __str__(self):
        return str(self.product)

    def __getstate__(self):
        state = self.__dict__.copy()
        # _logger is not pickable, so we remove them and build
        # them again in __setstate__
        del state['_logger']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._logger = logging.getLogger('{}.{}'.format(
            __name__,
            type(self).__name__))


def _doc_short(doc):
    if doc is not None:
        return doc.split('\n')[0]
    else:
        return None


class ProductEvaluator:
    """
    A class to temporarily keep the outdated status of a product (when products
    are remote this operation is expensive)
    """
    def __init__(self, product, outdated_by_code, remote):
        self.product = product
        self.outdated_by_code = outdated_by_code
        self._is_outdated = None
        self._remote = remote

    def check(self):
        if self._is_outdated is None:
            callable_ = (self.product._is_remote_outdated
                         if self._remote else self.product._is_outdated)

            self._is_outdated = (callable_(
                outdated_by_code=self.outdated_by_code))

        return self._is_outdated


def _ensure_parents_exist(product):
    if isinstance(product, MetaProduct):
        for prod in product:
            _ensure_file_parents_exist(prod)
    else:
        _ensure_file_parents_exist(product)


def _ensure_file_parents_exist(product):
    if isinstance(product, File):
        Path(product).parent.mkdir(parents=True, exist_ok=True)
