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
* Validating upstream, product and params in code
* Using a client parameter

NOTE: Params trigger different data output (and should make tasks outdated),
Tasks constructor args (such as chunksize in SQLDump) should not change
the output, hence shoulf not make tasks outdated
"""
import traceback
import abc
import logging
import warnings
from datetime import datetime
from ploomber.products import Product, MetaProduct
from ploomber.dag import DAG
from ploomber.exceptions import TaskBuildError
from ploomber.tasks.TaskGroup import TaskGroup
from ploomber.constants import TaskStatus
from ploomber.tasks.Upstream import Upstream
from ploomber.tasks.Params import Params
from ploomber.Table import Row
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
    def _init_source(self, source):
        pass

    def __init__(self, source, product, dag, name=None, params=None):
        self._params = Params(params)
        self._source = self._init_source(source)

        if name is None:
            # works with pathlib.Path and ploomber.Placeholder
            if hasattr(source, 'name'):
                self._name = source.name
            # works with python functions
            elif hasattr(source, '__name__'):
                self._name = source.__name__
            else:
                raise AttributeError('name can ony be None if the souce '
                                     'has a "name" attribute such as a '
                                     'Placeholder (returned from SourceLoader)'
                                     ' or pathlib.Path objects, or a '
                                     '"__name__" attribute (Python functions)')
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
        self._available_callback_kwargs = {'task': self,
                                           'client': self.client}

    @property
    def name(self):
        """A str that represents the name of the task
        """
        return self._name

    @property
    def source(self):
        """
        A code object which represents what will be run upn task execution,
        for tasks that do not take source code as parameter (such as
        PostgresCopyFrom), the source object will be a different thing
        """
        return self._source

    @property
    def product(self):
        """The product this task will create upon execution
        """
        return self._product

    # FIXME: remove, only keep source
    @property
    def source_code(self):
        """
        A str with the source for that this task will run on execution, if
        templated, it is only available after rendering
        """
        return str(self.source)

    @property
    def upstream(self):
        """{task names} -> [task objects] mapping for upstream dependencies
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
        if value not in list(TaskStatus):
            raise ValueError('Setting task.exec_status to an unknown '
                             'value: %s', value)

        self._logger.debug('Setting "%s" status to %s', self.name, value)
        self._exec_status = value
        self._update_downstream_status()

        # TODO: move this to build method? do we really need to run this in
        # the main process?
        if value == TaskStatus.Executed:
            # run on finish first, if this fails, we don't want to save
            # metadata
            self._run_on_finish()

            self.product._save_metadata(self.source_code)

        elif value == TaskStatus.Errored:
            self._run_on_failure()

    def build(self, force=False, within_dag=False):
        """Run the task if needed by checking its dependencies

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
            except Exception as e:
                self.exec_status = TaskStatus.Errored
                raise type(e)('Error calling build on task {}'
                              .format(self.name)) from e

            self.exec_status = TaskStatus.Executed
            self.product._clear_cached_status()

            return res

    def _build(self, force):
        # cannot keep running, we depend on the render step to get all the
        # parameters resolved (params, upstream, product)
        if self.exec_status == TaskStatus.WaitingRender:
            raise TaskBuildError('Cannot build task that has not been '
                                 'rendered, call DAG.render() first')

        elif self.exec_status == TaskStatus.Aborted:
            raise TaskBuildError('Attempted to run task "{}", which has '
                                 'status TaskStatus.Aborted'
                                 .format(self.name))

        # NOTE: should i fetch metadata here? I need to make sure I have
        # the latest before building

        if force:
            self._logger.info('Forcing run "%s", skipping checks...',
                              self.name)
            run = True
        else:
            run = self.should_execute()

        if run:
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

            if not self.product.exists():
                raise TaskBuildError('Error building task "{}": '
                                     'the task ran successfully but product '
                                     '"{}" does not exist yet '
                                     '(task.product.exist() returned False)'
                                     .format(self, self.product))

        # NOTE: return the TaskReport here?
        return TaskStatus.Executed if run else TaskStatus.Skipped

    def should_execute(self):
        """
        Given current conditions, determine if calling Task.build() should
        actually run the Task.

        Returns
        -------
        bool
            True if the Task should execute, False otherwise
        """
        run = False

        self._logger.info('Checking status for task "%s"', self.name)

        # check product...
        p_exists = self.product.exists()

        # check dependencies only if the product exists and there is metadata
        if p_exists and self.product.metadata is not None:

            outdated_data_deps = self.product._outdated_data_dependencies()
            outdated_code_dep = self.product._outdated_code_dependency()

            if outdated_data_deps:
                run = True
                self._logger.info('Outdated data deps...')
            else:
                self._logger.info('Up-to-date data deps...')

            if outdated_code_dep:
                run = True
                self._logger.info('Outdated code dep...')
            else:
                self._logger.info('Up-to-date code dep...')
        else:
            run = True

            # just log why it will run
            if not p_exists:
                self._logger.info('Product does not exist...')

            if self.product.metadata is None:
                self._logger.info('Product metadata is None...')

        self._logger.info('Should run? %s', run)

        return run

    def render(self):
        """
        Renders code and product, all upstream tasks must have been rendered
        first, for that reason, this method will usually not be called
        directly but via DAG.render(), which renders in the right order
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
            if self.source.needs_render:
                # if this task has upstream dependencies, render using the
                # context manager, which will raise a warning if any of the
                # dependencies is not used, otherwise just render
                if self.params.get('upstream'):
                    with self.params.get('upstream'):
                        self.source.render(self.params)
                else:
                    self.source.render(self.params)
        except Exception as e:
            self.exec_status = TaskStatus.ErroredRender
            raise type(e)('Error rendering source from Task "{}", '
                          ' check the full traceback above for details'
                          .format(repr(self), self.params)) from e

        self.exec_status = (TaskStatus.WaitingExecution
                            if not self.upstream
                            else TaskStatus.WaitingUpstream)
        self._run_on_render()

    def set_upstream(self, other):
        self.dag._add_edge(other, self)

    def plan(self):
        """Shows a text summary of what this task will execute
        """

        plan = """
        Input parameters: {}
        Product: {}

        Source code:
        {}
        """.format(self.params, self.product, self.source_code)

        print(plan)

    def status(self, return_code_diff=False):
        """Prints the current task status
        """
        p = self.product

        data = {}

        data['name'] = self.name

        if p.timestamp is not None:
            dt = datetime.fromtimestamp(p.timestamp)
            date_h = dt.strftime('%b %d, %y at %H:%M')
            time_h = humanize.naturaltime(dt)
            data['Last updated'] = '{} ({})'.format(time_h, date_h)
        else:
            data['Last updated'] = 'Has not been run'

        data['Outdated dependencies'] = p._outdated_data_dependencies()
        outd_code = p._outdated_code_dependency()
        data['Outdated code'] = outd_code

        if outd_code and return_code_diff:
            data['Code diff'] = (self.dag
                                 .differ
                                 .get_diff(p.stored_source_code,
                                           self.source_code,
                                           language=self.source.language))
        else:
            outd_code = ''

        data['Product'] = str(self.product)
        data['Doc (short)'] = self.source.doc_short
        data['Location'] = self.source.loc

        return Row(data)

    def to_dict(self):
        """
        Returns a dict representation of the Task, only includes a few
        attributes
        """
        return dict(name=self.name, product=str(self.product),
                    source_code=self.source_code)

    def _render_product(self):
        params_names = list(self.params)

        # add upstream product identifiers to params, if any
        # Params are read-only for users, but we have to add upstream
        # dependencies so we do it directly to the dictionary
        if self.upstream:
            self.params._dict['upstream'] = Upstream({n: t.product for n, t in
                                                      self.upstream.items()})

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

    def _short_repr(self):
        def short(s):
            max_l = 30
            return s if len(s) <= max_l else s[:max_l - 3] + '...'

        return ('{} -> \n{}'
                .format(short(self.name), self.product._short_repr()))

    # __getstate__ and __setstate__ are needed to make this picklable

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
