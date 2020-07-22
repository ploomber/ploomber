"""
Ploomber execution model:

To orchestrate Task execution, they are organized in a DAG object that keeps
track of dependencies and status.

It all starts with a call to DAG.render(), all placeholders are resolved
and the task status is determined. If any task fails to render, the process
is stopped and such DAG cannot be executed.

Once all tasks render successfully, their product's metadata determined
whether they should run or not based on source code changes, this behavior
can be overridden by passing DAG.render(force=True).

When DAG.build() is called, actual execution happens. Tasks are executed in
order (but up-to-date tasks are skipped). If tasks succeed, their downstream
dependencies will be executed, otherwise they are aborted.

If any tasks fail, DAG executes the on_failure hook, otherwise, it executes
the on_finish hook.

Tasks support hooks as well and building a task is a process by itself, see
the docstring in the Task module for details.

The DAG does not execute tasks, but delegates this to an Executor object,
executors adhere to task status and do not build tasks if they are marked
as Aborted or Skipped.

"""
from collections.abc import Iterable
import traceback
from copy import copy, deepcopy
from pathlib import Path
import warnings
import logging
import collections
import tempfile
from math import ceil

try:
    import importlib.resources as importlib_resources
except ImportError:
    # backported
    import importlib_resources

import networkx as nx
from tqdm.auto import tqdm
from jinja2 import Template
import mistune

from ploomber.Table import Table, TaskReport, BuildReport
from ploomber.products import MetaProduct
from ploomber.util import (image_bytes2html, isiterable, path2fig, requires,
                           markup)
from ploomber import resources
from ploomber import executors
from ploomber.constants import TaskStatus, DAGStatus
from ploomber.exceptions import (DAGBuildError, DAGRenderError,
                                 DAGBuildEarlyStop)
from ploomber.MessageCollector import MessageCollector
from ploomber.util.util import callback_check
from ploomber.dag.DAGConfiguration import DAGConfiguration
from ploomber.dag.DAGLogger import DAGLogger


class DAG(collections.abc.Mapping):
    """A collection of tasks with dependencies

    Parameters
    ----------
    name : str, optional
        A name to identify this DAG
    clients : dict, optional
        A dictionary with classes as keys and clients as values, can be
        later modified using dag.clients[dag] = client
    differ : CodeDiffer
        An object to determine whether two pieces of code are the same and
        to output a diff, defaults to CodeDiffer() (default parameters)
    executor : str or ploomber.executors instance, optional
        The executor to use (ploomber.executors.Serial and
        ploomber.executors.Parallel), is a string is passed ('serial'
        or 'parallel') the corresponding executor is initialized with default
        parameters

    Attributes
    ----------
    name : str
        A name to identify the DAG

    clients : dict
        A class to client mapping

    executor : ploomber.Executor
        Executor object to run tasks

    on_finish : callable
        Function to execute upon execution. Can request a "dag" parameter
        and/or "report", which containes the report object returned by the
        build function.

    on_failure : callable
        Function to execute upon failure. Can request a "dag" parameter and/or
        "traceback" which will contain a dictionary, possible keys are "build"
        which contains the build error traceback and "on_finish" which contains
        the on_finish hook traceback, if any.
    """
    def __init__(self, name=None, clients=None, executor='serial'):
        self._G = nx.DiGraph()

        self.name = name or 'No name'
        self._logger = logging.getLogger(__name__)

        self._clients = clients or {}
        self.__exec_status = DAGStatus.WaitingRender

        if executor == 'serial':
            self._executor = executors.Serial()
        elif executor == 'parallel':
            self._executor = executors.Parallel()
        elif isinstance(executor, executors.Executor.Executor):
            self._executor = executor
        else:
            raise TypeError(
                'executor must be "serial", "parallel" or '
                'an instance of executors.Executor, got type {}'.format(
                    type(executor)))

        self._did_render = False

        self.on_finish = None
        self.on_failure = None
        self._available_callback_kwargs = {'dag': self}

        self._params = DAGConfiguration()

        # task access differ using .dag.differ
        self.differ = self._params.differ

    @property
    def executor(self):
        return self._executor

    @executor.setter
    def executor(self, value):
        self._executor = value

    @property
    def _exec_status(self):
        return self.__exec_status

    @_exec_status.setter
    def _exec_status(self, value):
        self._logger.debug('Setting %s status to %s', self, value)

        # The Task class is responsible for updating their status
        # (except for Executed and Errored, those are updated by the executor)
        # DAG should not set task status but only verify that after an attemp
        # to change DAGStatus, all Tasks have allowed states, otherwise there
        # is an error in either the Task or the Executor. we cannot raise an
        # exception here, since setting _exec_status happens might happen
        # right before catching an exception, but we still have to warn the
        # user that the DAG entered an inconsistent state. We only raise
        # an exception when trying to set an invalid value
        # NOTE: in some exec_status, it is ok to raise an exception, maybe we
        # should do it?

        if value == DAGStatus.WaitingRender:
            self.check_tasks_have_allowed_status({TaskStatus.WaitingRender},
                                                 value)

        # render errored
        elif value == DAGStatus.ErroredRender:
            allowed = {
                TaskStatus.WaitingExecution, TaskStatus.WaitingUpstream,
                TaskStatus.ErroredRender, TaskStatus.AbortedRender,
                TaskStatus.Skipped
            }
            self.check_tasks_have_allowed_status(allowed, value)

        # rendering ok, waiting execution
        elif value == DAGStatus.WaitingExecution:
            exec_values = set(task.exec_status for task in self.values())
            allowed = {
                TaskStatus.WaitingExecution, TaskStatus.WaitingUpstream,
                TaskStatus.Skipped
            }
            self.check_tasks_have_allowed_status(allowed, value)

        # attempted execution but failed
        elif value == DAGStatus.Executed:
            exec_values = set(task.exec_status for task in self.values())
            # check len(self) to prevent this from failing on an empty DAG
            if not exec_values <= {TaskStatus.Executed, TaskStatus.Skipped
                                   } and len(self):
                warnings.warn('The DAG "{}" entered in an inconsistent '
                              'state: trying to set DAG status to '
                              'DAGStatus.Executed but executor '
                              'returned tasks whose status is not '
                              'TaskStatus.Executed nor '
                              'TaskStatus.Skipped, returned '
                              'status: {}'.format(self.name, exec_values))
        elif value == DAGStatus.Errored:
            # no value validation since this state is also set then the
            # DAG executor ends up abrubtly
            pass
        else:
            raise RuntimeError('Unknown DAGStatus value: {}'.format(value))

        self.__exec_status = value

    def check_tasks_have_allowed_status(self, allowed, new_status):
        exec_values = set(task.exec_status for task in self.values())
        if not exec_values <= allowed:
            warnings.warn('The DAG "{}" entered in an inconsistent state: '
                          'trying to set DAG status to '
                          '{} but executor '
                          'returned tasks whose status is not in a '
                          'subet of {}. Returned '
                          'status: {}'.format(self.name, new_status, allowed,
                                              exec_values))

    @property
    def product(self):
        # NOTE: this allows declaring a dag as a dependency for one task,
        # maybe create a metaclass that applies to DAGs and Task
        # We have to rebuild it since tasks might have been added
        return MetaProduct([t.product for t in self.values()])

    @property
    def clients(self):
        return self._clients

    def pop(self, name):
        """Remove a task from the dag
        """
        t = self._G.nodes[name]['task']
        self._G.remove_node(name)
        return t

    def render(self, force=False, show_progress=True):
        """

        Render resolves all placeholders in tasks and determines whether
        a task should run or not based on the task.product metadata, this
        allows up-to-date tasks to be skipped.
        """
        g = self._to_graph()

        def unique(elements):
            elements_unique = []
            for elem in elements:
                if elem not in elements_unique:
                    elements_unique.append(elem)
            return elements_unique

        dags = unique([t.dag for t in g])

        # first render any other dags involved (this happens when some
        # upstream parameters come form other dags)
        # NOTE: for large compose dags it might be wasteful to render over
        # and over
        for dag in dags:
            if dag is not self:
                dag._render_current(force=force, show_progress=show_progress)

        # then, render this dag
        self._render_current(force=force, show_progress=show_progress)

        return self

    def _render_current(self, force, show_progress):
        """
        Render tasks, and update exec_status
        """
        # FIXME: should also render again if errored render, maybe change
        # _did_render for needs render which is on the first time
        # and when there's an error
        if not self._params.cache_rendered_status or not self._did_render:
            self._logger.info('Rendering DAG %s', self)

            if show_progress:
                tasks = tqdm(self.values(), total=len(self))

            exceptions = MessageCollector()
            warnings_ = MessageCollector()

            # reset all tasks status
            for task in self.values():
                task.exec_status = TaskStatus.WaitingRender

            for t in tasks:
                # no need to process task with AbortedRender
                if t.exec_status == TaskStatus.AbortedRender:
                    continue

                if show_progress:
                    tasks.set_description('Rendering DAG "{}"'.format(
                        self.name))

                with warnings.catch_warnings(record=True) as warnings_current:
                    try:
                        t.render(
                            force=force,
                            outdated_by_code=self._params.outdated_by_code)
                    except Exception:
                        tr = traceback.format_exc()
                        exceptions.append(message=tr, task_str=repr(t))

                if warnings_current:
                    w = [
                        str(a_warning.message)
                        for a_warning in warnings_current
                    ]
                    warnings_.append(task_str=t.name, message='\n'.join(w))

            if warnings_:
                # FIXME: maybe raise one by one to keep the warning type
                warnings.warn('Some tasks had warnings when rendering DAG '
                              '"{}":\n{}'.format(self.name, str(warnings_)))

            if exceptions:
                self._exec_status = DAGStatus.ErroredRender
                raise DAGRenderError('DAG render failed, the following '
                                     'tasks could not render '
                                     '(corresponding tasks aborted '
                                     'rendering):\n{}'.format(str(exceptions)))

        self._exec_status = DAGStatus.WaitingExecution

    def build(self, force=False, show_progress=True):
        """
        Runs the DAG in order so that all upstream dependencies are run for
        every task

        Parameters
        ----------
        force: bool, optional
            If True, it will run all tasks regardless of status, defaults to
            False

        Notes
        -----
        All dag-level clients are closed after calling this function

        Returns
        -------
        BuildReport
            A dict-like object with tasks as keys and dicts with task
            status as values
        """
        kwargs = callback_check(self._params.logging_factory,
                                available={'dag_name': self.name})

        res = self._params.logging_factory(**kwargs)

        if isinstance(res, Iterable):
            dag_logger = DAGLogger(*res)
        else:
            dag_logger = DAGLogger(handler=res)

        with dag_logger:
            report = self._build(force, show_progress)

        return report

    def _build(self, force, show_progress):
        # always render before building, the function might immediately
        # return if the user turned render status caching on
        self.render(force=force, show_progress=show_progress)

        if self._exec_status == DAGStatus.ErroredRender:
            raise DAGBuildError('Cannot build dag that failed to render, '
                                'fix rendering errors then build again. '
                                'To see the full traceback again, run '
                                'dag.render(force=True)')
        else:
            self._logger.info('Building DAG %s', self)

            tb = {}

            try:
                # within_dag flags when we execute a task in isolation
                # vs as part of a dag execution
                # FIXME: not passing force flag
                task_reports = self._executor(dag=self,
                                              show_progress=show_progress)

            # executors raise this error to signal that there was an error
            # building the dag, this allows us to run the on_failure hook,
            # but any other errors should not be caught (e.g.
            # a user might turn that setting off in the executor to start
            # a debugging session at the line of failure)
            except DAGBuildError as e:
                tb['build'] = traceback.format_exc()
                # error build dag, log exception and set status
                self._logger.exception('Failure when building DAG "{}"'.format(
                    self.name))
                self._exec_status = DAGStatus.Errored
                build_exception = e
            except DAGBuildEarlyStop:
                # early stop and empty on_failure, nothing left to do
                if self.on_failure is None:
                    # FIXME: remove this, only needed after rendering
                    self._clear_cached_status()
                    return
            else:
                # no error when building dag
                build_exception = None

            if build_exception is None:
                empty = [
                    TaskReport.empty_with_name(t.name) for t in self.values()
                    if t.exec_status == TaskStatus.Skipped
                ]

                build_report = BuildReport(task_reports + empty)
                self._logger.info(' DAG report:\n{}'.format(build_report))

                # try on_finish hook
                try:
                    self._run_on_finish(build_report)
                except Exception as e:
                    tb['on_finish'] = traceback.format_exc()
                    # on_finish error, log exception and set status
                    msg = ('Exception when running on_finish '
                           'for DAG "{}": {}'.format(self.name, e))
                    self._logger.exception(msg)
                    self._exec_status = DAGStatus.Errored

                    if isinstance(e, DAGBuildEarlyStop):
                        # early stop, nothing left to co
                        # FIXME: remove this, only needed after rendering
                        self._clear_cached_status()
                        return
                    else:
                        # otherwise raise exception
                        raise DAGBuildError(msg) from e
                else:
                    # DAG success and on_finish did not raise exception
                    self._exec_status = DAGStatus.Executed
                    # FIXME: remove this, only needed after rendering
                    self._clear_cached_status()
                    return build_report

            else:
                # DAG raised error, run on_failure hook
                try:
                    self._run_on_failure(tb)
                except Exception as e:
                    # error in hook, log exception
                    msg = ('Exception when running on_failure '
                           'for DAG "{}": {}'.format(self.name, e))
                    self._logger.exception(msg)

                    # do not raise exception if early stop
                    if isinstance(e, DAGBuildEarlyStop):
                        return
                    else:
                        raise DAGBuildError(msg) from e

                # on_failure hook executed, raise original exception
                # FIXME: remove this, only needed after rendering
                self._clear_cached_status()

                raise DAGBuildError(
                    'Failed to build DAG {}'.format(self)) from build_exception

    def _run_on_failure(self, tb):
        if self.on_failure:
            self._logger.debug('Executing on_failure hook '
                               'for dag "%s"', self.name)
            kwargs_available = copy(self._available_callback_kwargs)
            kwargs_available['traceback'] = tb

            kwargs = callback_check(self.on_failure, kwargs_available)
            self.on_failure(**kwargs)
        else:
            self._logger.debug('No on_failure hook for dag '
                               '"%s", skipping', self.name)

    def _run_on_finish(self, build_report):
        if self.on_finish:
            self._logger.debug('Executing on_finish hook '
                               'for dag "%s"', self.name)
            kwargs_available = copy(self._available_callback_kwargs)
            kwargs_available['report'] = build_report
            kwargs = callback_check(self.on_finish, kwargs_available)
            self.on_finish(**kwargs)
        else:
            self._logger.debug('No on_finish hook for dag '
                               '"%s", skipping', self.name)

    def build_partially(self, target, force=False, show_progress=True):
        """Partially build a dag until certain task
        """
        lineage = self[target]._lineage
        dag = deepcopy(self)

        to_pop = set(dag) - {target}

        if lineage:
            to_pop = to_pop - lineage

        for task in to_pop:
            dag.pop(task)

        return dag.build(force=force, show_progress=show_progress)

    def status(self, **kwargs):
        """Returns a table with tasks status
        """
        # FIXME: delete this, make dag.render() return this

        # self._clear_cached_status()

        self.render()

        return Table(
            [self._G.nodes[name]['task'].status(**kwargs) for name in self._G])

    def to_dict(self, include_plot=False):
        """Returns a dict representation of the dag's Tasks,
        only includes a few attributes.

        Parameters
        ----------
        include_plot: bool, optional
            If True, the path to a PNG file with the plot in "_plot"
        """
        # self._clear_cached_status()

        d = {name: self._G.nodes[name]['task'].to_dict() for name in self._G}

        if include_plot:
            d['_plot'] = self.plot(open_image=False)

        return d

    def to_markup(self, path=None, fmt='html', sections=None):
        """Returns a str (md or html) with the pipeline's description

        Parameters
        ----------
        sections : list
            Which sections to include, possible values are "plot", "status"
            and "source". Defaults to ["plot", "status"]
        """
        sections = sections or ['plot', 'status']

        if fmt not in ['html', 'md']:
            raise ValueError('fmt must be html or md, got {}'.format(fmt))

        if 'status' in sections:
            status = self.status().to_format('html')
        else:
            status = False

        if 'plot' in sections:
            path_to_plot = Path(self.plot())
            plot = image_bytes2html(path_to_plot.read_bytes())
        else:
            plot = False

        template_md = importlib_resources.read_text(resources, 'dag.md')
        out = Template(template_md).render(plot=plot,
                                           status=status,
                                           source='source' in sections,
                                           dag=self)

        if fmt == 'html':
            renderer = markup.HighlightRenderer()
            out = mistune.markdown(out, escape=False, renderer=renderer)

            # add css
            html = importlib_resources.read_text(resources,
                                                 'github-markdown.html')
            out = Template(html).render(content=out)

        if path is not None:
            Path(path).write_text(out)

        return out

    @requires(['pygraphviz'])
    def plot(self, output='tmp'):
        """Plot the DAG
        """
        if output in {'tmp', 'matplotlib'}:
            path = tempfile.mktemp(suffix='.png')
        else:
            path = output

        # self._clear_cached_status()

        # attributes docs:
        # https://graphviz.gitlab.io/_pages/doc/info/attrs.html

        # FIXME: add tests for this
        self.render()

        G = self._to_graph()

        for task, data in G.nodes(data=True):
            data['color'] = 'red' if task.product._is_outdated() else 'green'
            data['label'] = _task_short_repr(task)

        # https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
        # # http://graphviz.org/doc/info/attrs.html
        # NOTE: requires pygraphviz and pygraphviz
        G_ = nx.nx_agraph.to_agraph(G)
        G_.draw(path, prog='dot', args='-Grankdir=LR')

        # plot function uses _is_outdated, which casues caching, clear up
        # to avoid re-using this in any other operation
        self._clear_cached_status()

        if output == 'matplotlib':
            return path2fig(path)
        else:
            return path

    def _add_task(self, task):
        """Adds a task to the DAG
        """
        if task.name in self._G:
            raise ValueError('DAGs cannot have Tasks with repeated names, '
                             'there is a Task with name "{}" '
                             'already'.format(task.name))

        if task.name is not None:
            self._G.add_node(task.name, task=task)
        else:
            raise ValueError('Tasks must have a name, got None')

    def _to_graph(self, only_current_dag=False):
        """
        Converts the DAG to a Networkx DiGraph object. Since upstream
        dependencies are not required to come from the same DAG,
        this object might include tasks that are not included in the current
        object
        """
        # NOTE: delete this, use existing DiGraph object
        G = nx.DiGraph()

        for task in self.values():
            G.add_node(task)

            if only_current_dag:
                G.add_edges_from([(up, task) for up in task.upstream.values()
                                  if up.dag is self])
            else:
                G.add_edges_from([(up, task) for up in task.upstream.values()])

        return G

    def _add_edge(self, task_from, task_to):
        """Add an edge between two tasks
        """
        # if a new task is added, rendering is required again
        self._did_render = False

        if isiterable(task_from) and not isinstance(task_from, DAG):
            # if iterable, add all components as separate upstream tasks
            for a_task_from in task_from:

                # this happens when the task was originally declared in
                # another dag...
                if a_task_from.name not in self._G:
                    self._G.add_node(a_task_from.name, task=a_task_from)

                self._G.add_edge(a_task_from.name, task_to.name)

        else:
            # this happens when the task was originally declared in
            # another dag...
            if task_from.name not in self._G:
                self._G.add_node(task_from.name, task=task_from)

            # DAGs are treated like a single task
            self._G.add_edge(task_from.name, task_to.name)

    def _get_upstream(self, task_name):
        """Get upstream tasks given a task name (returns Task objects)
        """
        upstream = self._G.predecessors(task_name)
        return {u: self._G.nodes[u]['task'] for u in upstream}

    def _clear_cached_status(self):
        # NOTE: maybe make this a context manager and/or a decorator
        self._logger.debug('Clearing product status')
        # clearing out this way is only useful after building, but not
        # if the metadata changed since it wont be reloaded
        for task in self.values():
            task.product._clear_cached_status()

    def __getitem__(self, key):
        return self._G.nodes[key]['task']

    def __delitem__(self, key):
        # TODO: this implementation is correct but perhaps we should raise
        # warning if the deleted task has downstream dependencies, render
        # and build will no longer work
        return self._G.remove_node(key)

    def __iter__(self):
        """Iterate task names in topological order
        """
        # TODO: raise a warning if this any of this dag tasks have tasks
        # from other tasks as dependencies (they won't show up here)
        for name in nx.algorithms.topological_sort(self._G):
            yield name

    def __len__(self):
        return len(self._G)

    def __repr__(self):
        return '{}("{}")'.format(type(self).__name__, self.name)

    def _short_repr(self):
        return repr(self)

    # IPython integration
    # https://ipython.readthedocs.io/en/stable/config/integrating.html

    def _ipython_key_completions_(self):
        return list(self)

    # __getstate__ and __setstate__ are needed to make this picklable

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


def _single_product_short_repr(product):
    s = str(product._identifier)

    if len(s) > 20:
        s_short = ''

        t = ceil(len(s) / 20)

        for i in range(t):
            s_short += s[(20 * i):(20 * (i + 1))] + '\n'
    else:
        s_short = s

    return s_short


def _meta_product_short_repr(metaproduct):
    return ', '.join(
        [_single_product_short_repr(p) for p in metaproduct.products])


def _product_short_repr(product):
    if isinstance(product, MetaProduct):
        return _meta_product_short_repr(product)
    else:
        return _single_product_short_repr(product)


def _task_short_repr(task):
    def short(s):
        max_l = 30
        return s if len(s) <= max_l else s[:max_l - 3] + '...'

    return ('{} -> \n{}'.format(short(str(task.name)),
                                _product_short_repr(task.product)))
