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

Task Status lifecycle:

* They all start on WaitingRender (set in __init__)
* (dag.render() is called)
* Tasks move to:
    * If sucessful render: TaskStatus.Skipped, WaitingExecution or
        WaitingUpstream
    * If failed render: ErroredRender or AbortedRender
    * This happens in Task.render()
    * AbortedRender is not set directly, but by propagating downstream
* (dag.build() is called)
* Check that all TaskStatus are either WaitingExecution, WaitingUpstream
    or Skipped (currently done in Executor)
* Once dag.build() starts, the executor must call task.build() inside a
    try-catch statement to set status to Executed or Errored
* When tasks are set to Errored, Abort is set to downstream tasks

TODO: describe BrokenProcesssPool status
"""
import traceback
import abc
import logging
import warnings
from datetime import datetime
from ploomber.products import Product, MetaProduct
from ploomber.dag.DAG import DAG
from ploomber.exceptions import TaskBuildError
from ploomber.tasks.TaskGroup import TaskGroup
from ploomber.constants import TaskStatus
from ploomber.tasks.Upstream import Upstream
from ploomber.tasks.Params import Params
from ploomber.Table import TaskReport, Row
from ploomber.sources.sources import Source
from ploomber.util import isiterable
from ploomber.util.util import callback_check

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
        the following parmeters: task, client and/or product.

    on_finish : callable
        Function to execute upon execution. Can request the same params as the
        on_render hook.

    on_failure : callable
        Function to execute upon failure. Can request the same params as the
        on_render hook.

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
        pass

    @abc.abstractmethod
    def _init_source(self, source, kwargs):
        pass

    def __init__(self, source, product, dag, name=None, params=None):
        self._params = Params(params)

        kwargs = dict(hot_reload=dag._params.hot_reload)
        self._source = self._init_source(source, kwargs)

        if name is None:
            # use name inferred from the source object
            self._name = self._source.name

            if self._name is None:
                raise AttributeError('Task name can only be None if a name'
                                     'can be inferred from the source object. '
                                     'This only works when the task receives a'
                                     'pathlib.Path, when using SourceLoader '
                                     'or in PythonCallable. Pass a value '
                                     'explicitely.')
        else:
            self._name = name

        if dag is None:
            raise TypeError('DAG cannot be None')

        # NOTE: we should get rid of this, maybe just add hooks that are
        # called back on the dag object to avoid having a reference here
        self.dag = dag
        dag._add_task(self)

        if self._source is None:
            raise TypeError('_init_source must return a value, got None')

        if not isinstance(self._source, Source):
            raise TypeError('_init_source must return a subclass of Source')

        if isinstance(product, Product):
            self._product = product

            if self.PRODUCT_CLASSES_ALLOWED is not None:
                if not isinstance(self._product, self.PRODUCT_CLASSES_ALLOWED):
                    raise TypeError('{} only supports the following product '
                                    'classes: {}, got {}'
                                    .format(type(self).__name__,
                                            self.PRODUCT_CLASSES_ALLOWED,
                                            type(self._product).__name__))
        else:
            # if assigned a tuple/list of products, create a MetaProduct
            self._product = MetaProduct(product)

            if self.PRODUCT_CLASSES_ALLOWED is not None:
                if not all(isinstance(p, self.PRODUCT_CLASSES_ALLOWED)
                           for p in self._product):
                    raise TypeError('{} only supports the following product '
                                    'classes: {}, got {}'
                                    .format(type(self).__name__,
                                            self.PRODUCT_CLASSES_ALLOWED,
                                            type(self._product).__name__))

        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))

        self.product.task = self
        self.client = None

        self.exec_status = TaskStatus.WaitingRender

        self._on_finish = None
        self._on_failure = None
        self._on_render = None

    @property
    def _available_callback_kwargs(self):
        # make it a property so we always get the latest value for self.client
        # given that could be None during init
        return {'task': self,
                'client': self.client,
                'product': self.product}

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
        A mapping for upstream dependencies {task name} -> [task obkect]
        """
        # this is jus syntactic sugar, upstream relations are tracked by the
        # DAG object

        # this always return a copy to prevent global state if contents
        # are modified (e.g. by using pop)
        return self.dag._get_upstream(self.name)

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
            lineage_up = [up._lineage for up in self.upstream.values() if
                          up._lineage]
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
        callback_check(value, self._available_callback_kwargs)
        self._on_finish = value

    def _run_on_finish(self):
        if self.on_finish:
            kwargs = callback_check(self.on_finish,
                                    self._available_callback_kwargs)
            try:
                self.on_finish(**kwargs)
            except Exception as e:
                self.exec_status = TaskStatus.Errored
                raise type(e)('Exception when running on_finish '
                              'for task "{}": {}'
                              .format(self.name, e)) from e

    @property
    def on_failure(self):
        """
        Callable to be executed if task fails (passes Task as first parameter
        and the exception as second parameter)
        """
        return self._on_failure

    @on_failure.setter
    def on_failure(self, value):
        callback_check(value, self._available_callback_kwargs)
        self._on_failure = value

    def _run_on_failure(self):
        if self.on_failure:
            kwargs = callback_check(self.on_failure,
                                    self._available_callback_kwargs)
            try:
                self.on_failure(**kwargs)
            except Exception:
                tr = traceback.format_exc()
                warnings.warn('Exception when running on_failure '
                              'for task "{}". {}'.format(self.name, tr))

    @property
    def on_render(self):
        return self._on_render

    @on_render.setter
    def on_render(self, value):
        callback_check(value, self._available_callback_kwargs)
        self._on_render = value

    def _run_on_render(self):
        if self.on_render:
            self._logger.debug('Calling on_render hook on task %s', self.name)

            kwargs = callback_check(self.on_render,
                                    self._available_callback_kwargs)
            try:
                self.on_render(**kwargs)
            except Exception as e:
                self.exec_status = TaskStatus.ErroredRender
                raise type(e)('Exception when running on_render '
                              'for task "{}": {}'
                              .format(self.name, e)) from e

    @property
    def exec_status(self):
        return self._exec_status

    @exec_status.setter
    def exec_status(self, value):
        # FIXME: this should only be used for th eexecutor to report back
        # status Executed or Errored, reject all other cases, those are handled
        # internally
        if value not in list(TaskStatus):
            raise ValueError('Setting task.exec_status to an unknown '
                             'value: %s', value)

        self._logger.debug('Setting "%s" status to %s', self.name, value)
        self._exec_status = value

        # process might crash, propagate now or changes might not be
        # reflected (e.g. if a Task is marked as Aborted, all downtream
        # tasks should be marked as aborted as well)
        self._update_downstream_status()

        # FIXME: this is inefficient, it is better to traverse
        # the dag in topological order but exclude nodes not affected by
        # this change

        # TODO: move this to build method? do we really need to run this in
        # the main process?
        if value == TaskStatus.Executed:
            # run on finish first, if this fails, we don't want to save
            # metadata
            # Exceptions are *not* silenced here
            self._run_on_finish()

            self.product._save_metadata(str(self.source))

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
                        '(task.product.exists() returned False). '
                        .format(self, self.product))
                else:
                    raise TaskBuildError(
                        'Error building task "{}": '
                        'the task ran successfully but at least one of the '
                        'products in "{}" does not exist yet '
                        '(task.product.exists() returned False). '
                        .format(self, self.product))

        elif value == TaskStatus.Errored:
            # Exceptions here are silenced
            self._run_on_failure()

    def build(self, force=False, within_dag=False):
        """build the task

        Returns
        -------
        dict
            A dictionary with keys 'run' and 'elapsed'
        """
        if within_dag:
            return self._build(force)
        else:
            try:
                res = self._build(force)
            finally:
                self.exec_status = TaskStatus.Errored

            self.exec_status = TaskStatus.Executed
            self.product._clear_cached_status()

            return res

    def _build(self, force):
        # cannot keep running, we depend on the render step to get all the
        # parameters resolved (params, upstream, product)
        if self.exec_status == TaskStatus.WaitingRender:
            raise TaskBuildError('Error building task "{}". '
                                 'Cannot build task that has not been '
                                 'rendered, call DAG.render() first'
                                 .format(self.name))

        elif self.exec_status == TaskStatus.Aborted:
            raise TaskBuildError('Attempted to run task "{}", whose '
                                 'status is TaskStatus.Aborted'
                                 .format(self.name))
        elif self.exec_status == TaskStatus.Skipped and not force:
            raise TaskBuildError('Attempted to run task "{}", whose '
                                 'status TaskStatus.Skipped. Use force=True '
                                 'if you want to execute it anyway'
                                 .format(self.name))

        # NOTE: should i fetch metadata here? I need to make sure I have
        # the latest before building

        if force:
            self._logger.info('Forcing run "%s", status ignored...',
                              self.name)

        self._logger.info('Starting execution: %s', repr(self))

        then = datetime.now()
        self.run()
        now = datetime.now()

        elapsed = (now - then).total_seconds()
        self._logger.info('Done. Operation took {:.1f} seconds'
                          .format(elapsed))

        # TODO: also check that the Products were updated:
        # if they did not exist, they must exist now, if they alredy
        # exist, timestamp must be recent equal to the datetime.now()
        # used. maybe run fetch metadata again and validate?

        return TaskReport.with_data(name=self.name, ran=True, elapsed=elapsed)

    def render(self, force=False, outdated_by_code=True):
        """
        Renders code and product, all upstream tasks must have been rendered
        first, for that reason, this method will usually not be called
        directly but via DAG.render(), which renders in the right order

        Parameters
        ----------
        force : bool
            If True, mark status as WaitingExecution/WaitingUpstream even if
            the task is up to, otherwise up to date tasks are marked as
            Skipped

        outdated_by_code : str
            Factors to determine if Task.product is marked outdated when source
            code changes. Otherwise just the upstream timestamps are used.
        """
        self._logger.debug('Calling render on task %s', self.name)

        try:
            self._render_product()
        except Exception as e:
            self.exec_status = TaskStatus.ErroredRender
            raise type(e)('Error rendering product from Task "{}", '
                          ' check the full traceback above for details'
                          .format(repr(self), self.params)) from e

        # Params are read-only for users, but we have to add the product
        # so we do it directly to the dictionary
        self.params._dict['product'] = self.product

        try:
            self.source.render(self.params)
        except Exception as e:
            self.exec_status = TaskStatus.ErroredRender
            raise type(e)('Error rendering source from Task "{}", '
                          ' check the full traceback above for details'
                          .format(repr(self), self.params)) from e

        # Maybe set ._exec_status directly, since no downstream propagation
        # is needed here.
        is_outdated = (self.product
                       ._is_outdated(outdated_by_code=outdated_by_code))

        if not self.upstream:
            if not is_outdated and not force:
                self._exec_status = TaskStatus.Skipped
            else:
                self._exec_status = TaskStatus.WaitingExecution
        else:
            all_upstream_done = all([t.exec_status
                                     in {TaskStatus.Executed,
                                         TaskStatus.Skipped}
                                     for t in self.upstream.values()])

            if all_upstream_done and is_outdated:
                self._exec_status = TaskStatus.WaitingExecution
            elif all_upstream_done and not is_outdated and not force:
                self._exec_status = TaskStatus.Skipped
            else:
                self._exec_status = TaskStatus.WaitingUpstream

        self._validate_render()
        self._run_on_render()

    # FIXME: delete, this is a special case for Input and Link, should not be
    # part of this abstract class
    def _validate_render(self):
        """
        This hook is executed after rendering, it can be used to perform
        validation on parameters that might cause runtime errors. e.g.
        if is used by PythonCallable to check if the function signature
        matches parameters passed. Note: if is executed before the
        user-supplied on_render hook
        """
        pass

    def set_upstream(self, other):
        self.dag._add_edge(other, self)

    # FIXME: delete
    def plan(self):
        """Shows a text summary of what this task will execute
        """

        plan = """
        Input parameters: {}
        Product: {}

        Source code:
        {}
        """.format(self.params, self.product, str(self.source))

        print(plan)

    def status(self, return_code_diff=False):
        """Prints the current task status
        """
        p = self.product

        data = {}

        data['name'] = self.name
        data['type'] = type(self).__name__
        data['status'] = self.exec_status.name
        # FIXME: all tasks should have a client property
        data['client'] = (repr(self.client)
                          if hasattr(self, 'client')
                          else None)

        if p.metadata.timestamp is not None:
            dt = datetime.fromtimestamp(p.metadata.timestamp)
            date_h = dt.strftime('%b %d, %y at %H:%M')
            time_h = humanize.naturaltime(dt)
            data['Last updated'] = '{} ({})'.format(time_h, date_h)
        else:
            data['Last updated'] = 'Has not been run'

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

        data['Outdated?'] = outd

        data['Outdated dependencies'] = outd_data
        data['Outdated code'] = outd_code

        if outd_code and return_code_diff:
            data['Code diff'] = (self.dag
                                 .differ
                                 .get_diff(p.metadata.stored_source_code,
                                           str(self.source),
                                           extension=self.source.extension))
        else:
            outd_code = ''

        data['Product type'] = type(self.product).__name__
        data['Product'] = str(self.product)
        # FIXME: all products should have a client property
        data['Product client'] = (repr(self.product.client)
                                  if hasattr(self.product, 'client')
                                  else None)
        data['Doc (short)'] = _doc_short(self.source.doc)
        data['Location'] = self.source.loc

        return Row(data)

    def to_dict(self):
        """
        Returns a dict representation of the Task, only includes a few
        attributes
        """
        return dict(name=self.name, product=str(self.product),
                    source_code=str(self.source))

    def _render_product(self):
        params_names = list(self.params)

        # add upstream product identifiers to params, if any
        # Params are read-only for users, but we have to add upstream
        # dependencies so we do it directly to the dictionary
        if self.upstream:
            self.params._dict['upstream'] = Upstream({n: t.product for n, t in
                                                      self.upstream.items()},
                                                     name=self.name)

        # render the current product
        try:
            # using the upstream products to define the current product
            # is optional, using the parameters passed in params is also
            # optional
            self.product.render(self.params,
                                optional=set(params_names + ['upstream']))
        except Exception as e:
            raise type(e)('Error rendering Product from Task "{}", '
                          ' check the full traceback above for details'
                          .format(repr(self), self.params)) from e

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
            any_upstream_errored_or_aborted = any([t.exec_status
                                                   in (TaskStatus.Errored,
                                                       TaskStatus.Aborted)
                                                   for t
                                                   in task.upstream.values()])
            all_upstream_done = all([t.exec_status
                                     in {TaskStatus.Executed,
                                         TaskStatus.Skipped}
                                     for t in task.upstream.values()])

            if any_upstream_errored_or_aborted:
                task.exec_status = TaskStatus.Aborted
            elif any([t.exec_status in (TaskStatus.ErroredRender,
                                        TaskStatus.AbortedRender)
                      for t in task.upstream.values()]):
                task.exec_status = TaskStatus.AbortedRender
            elif all_upstream_done:
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
        if isiterable(other) and not isinstance(other, DAG):
            return TaskGroup([self] + list(other))
        else:
            return TaskGroup((self, other))

    def __repr__(self):
        return ('{}: {} -> {}'
                .format(type(self).__name__, self.name, repr(self.product)))

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
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))


def _doc_short(doc):
    if doc is not None:
        return doc.split('\n')[0]
    else:
        return None
