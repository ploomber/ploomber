"""
DAG module

A DAG is collection of tasks that makes sure they are executed in
the right order
"""
from copy import copy
from pathlib import Path
import warnings
import logging
import collections
import tempfile

try:
    import importlib.resources as importlib_resources
except ImportError:
    # backported
    import importlib_resources


try:
    import mistune
except ImportError:
    mistune = None


try:
    import pygments
    from pygments import highlight
    from pygments.lexers import get_lexer_by_name
    from pygments.formatters import html
except ImportError:
    pygments = None

import networkx as nx
from tqdm.auto import tqdm
from jinja2 import Template

from ploomber.Table import Table
from ploomber.products import MetaProduct
from ploomber.util import image_bytes2html, isiterable, path2fig, requires
from ploomber.CodeDiffer import CodeDiffer
from ploomber import resources
from ploomber import executors


class HighlightRenderer(mistune.Renderer):
    """mistune renderer with syntax highlighting

    Notes
    -----
    Source: https://github.com/lepture/mistune#renderer
    """

    def block_code(self, code, lang):
        if not lang:
            return '\n<pre><code>%s</code></pre>\n' % \
                mistune.escape(code)
        lexer = get_lexer_by_name(lang, stripall=True)
        formatter = html.HtmlFormatter()
        return highlight(code, lexer, formatter)


class DAG(collections.abc.Mapping):
    """A collection of tasks with dependencies

    Parameters
    ----------
    name: str, optional
        A name to identify this DAG
    clients:  dict, optional
        A dictionary with classes as keys and clients as values, can be
        later modified using dag.clients[dag] = client
    differ: CodeDiffer
        An object to determine whether two pieces of code are the same and
        to output a diff, defaults to CodeDiffer() (default parameters)
    executor: str or ploomber.executors instance, optional
        The executor to use. The parallel executor is currently experimental
        and not recommended, use the serial excutor for now
    """
    def __init__(self, name=None, clients=None, differ=None,
                 on_task_finish=None, on_task_failure=None,
                 executor='serial'):
        self._G = nx.DiGraph()

        self.name = name or 'No name'
        self.differ = differ or CodeDiffer()
        self._logger = logging.getLogger(__name__)

        self._clients = clients or {}
        self._rendered = False

        if executor == 'serial':
            self._executor = executors.Serial()
        elif executor == 'parallel':
            self._executor = executors.Parallel()
        elif isinstance(executor, executors.Executor.Executor):
            self._executor = executor
        else:
            raise TypeError('executor must be "serial", "parallel" or '
                            'an instance of executors.Executor, got type {}'
                            .format(type(executor)))

        self._on_task_finish = on_task_finish
        self._on_task_failure = on_task_failure

    @property
    def product(self):
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

    def render(self, show_progress=True, force=False):
        """Render the graph
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
        for dag in dags:
            if dag is not self:
                dag._render_current(show_progress, force)

        # then, render this dag
        self._render_current(show_progress, force)

        return self

    def build(self, force=False, clear_cached_status=False):
        """
        Runs the DAG in order so that all upstream dependencies are run for
        every task

        Parameters
        ----------
        force: bool, optional
            If True, it will run all tasks regadless of status, defaults to
            False

        clear_cached_status: bool, optional
            If True, it will clear all cached status forcing a check on all
            tasks

        Returns
        -------
        BuildReport
            A dict-like object with tasks as keys and dicts with task
            status as values
        """
        if clear_cached_status:
            self._clear_cached_outdated_status()

        self.render()
        return self._executor(dag=self, force=force)

    def build_partially(self, target, clear_cached_status=False):
        """Partially build a dag until certain task
        """
        if clear_cached_status:
            self._clear_cached_outdated_status()

        lineage = self[target]._lineage
        dag = copy(self)

        to_pop = set(dag) - {target} - lineage

        for task in to_pop:
            dag.pop(task)

        dag.render()
        return self._executor(dag=dag)

    def status(self, clear_cached_status=False, **kwargs):
        """Returns a table with tasks status
        """
        if clear_cached_status:
            self._clear_cached_outdated_status()

        self.render()

        return Table([self._G.nodes[name]['task'].status(**kwargs)
                      for name in self._G])

    def to_dict(self, include_plot=False, clear_cached_status=False):
        """Returns a dict representation of the dag's Tasks,
        only includes a few attributes.

        Parameters
        ----------
        include_plot: bool, optional
            If True, the path to a PNG file with the plot in "_plot"
        """
        if clear_cached_status:
            self._clear_cached_outdated_status()

        d = {name: self._G.nodes[name]['task'].to_dict()
             for name in self._G}

        if include_plot:
            d['_plot'] = self.plot(open_image=False)

        return d

    def to_markup(self, path=None, fmt='html'):
        """Returns a str (md or html) with the pipeline's description
        """
        if fmt not in ['html', 'md']:
            raise ValueError('fmt must be html or md, got {}'.format(fmt))

        status = self.status().to_format('html')
        path_to_plot = Path(self.plot(clear_cached_status=True))
        plot = image_bytes2html(path_to_plot.read_bytes())

        template_md = importlib_resources.read_text(resources, 'dag.md')
        out = Template(template_md).render(plot=plot, status=status, dag=self)

        if fmt == 'html':
            if not mistune or not pygments:
                raise ImportError('mistune and pygments are '
                                  'required to export to HTML')

            renderer = HighlightRenderer()
            out = mistune.markdown(out, escape=False, renderer=renderer)

            # add css
            html = importlib_resources.read_text(resources,
                                                 'github-markdown.html')
            out = Template(html).render(content=out)
        if path is not None:
            Path(path).write_text(out)

        return out

    @requires(['pygraphviz'])
    def plot(self, output='tmp', clear_cached_status=False):
        """Plot the DAG
        """
        if output in {'tmp', 'matplotlib'}:
            path = tempfile.mktemp(suffix='.png')
        else:
            path = output

        if clear_cached_status:
            self._clear_cached_outdated_status()

        # attributes docs:
        # https://graphviz.gitlab.io/_pages/doc/info/attrs.html

        # FIXME: add tests for this
        self.render()

        G = self._to_graph()

        for n, data in G.nodes(data=True):
            data['color'] = 'red' if n.product._outdated() else 'green'
            data['label'] = n._short_repr()

        # https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
        # # http://graphviz.org/doc/info/attrs.html
        # NOTE: requires pygraphviz and pygraphviz
        G_ = nx.nx_agraph.to_agraph(G)
        G_.draw(path, prog='dot', args='-Grankdir=LR')

        if output == 'matplotlib':
            return path2fig(path)
        else:
            return path

    def diagnose(self):
        """Evaluate code quality
        """
        for task_name in self:

            doc = self[task_name].source.doc

            if doc is None or doc == '':
                warnings.warn('Task "{}" has no docstring'.format(task_name))

    def _render_current(self, show_progress, force):
        # only render the first time this is called, this means that
        # if the dag is modified, render won't have an effect, DAGs are meant
        # to be all set before rendering, but might be worth raising a warning
        # if trying to modify an already rendered DAG
        if not self._rendered or force:
            g = self._to_graph(only_current_dag=True)

            tasks = nx.algorithms.topological_sort(g)

            if show_progress:
                tasks = tqdm(tasks, total=len(g))

            for t in tasks:
                if show_progress:
                    tasks.set_description('Rendering DAG "{}"'
                                          .format(self.name))

                with warnings.catch_warnings(record=True) as warnings_:
                    try:
                        t.render()
                    except Exception as e:
                        raise type(e)('While rendering a Task in {}, check '
                                      'the full '
                                      'traceback above for details'
                                      .format(self)) from e

                if warnings_:
                    messages = [str(w.message) for w in warnings_]
                    warning = ('Task "{}" had the following warnings:\n\n{}'
                               .format(repr(t), '\n'.join(messages)))
                    warnings.warn(warning)

                self._rendered = True

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
                G.add_edges_from([(up, task) for up
                                  in task.upstream.values() if up.dag is self])
            else:
                G.add_edges_from([(up, task) for up in task.upstream.values()])

        return G

    def _add_edge(self, task_from, task_to):
        """Add an edge between two tasks
        """
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

    def _clear_cached_outdated_status(self):
        for task in self.values():
            task.product._clear_cached_outdated_status()

    def __getitem__(self, key):
        return self._G.nodes[key]['task']

    def __iter__(self):
        # TODO: raise a warning if this any of this dag tasks have tasks
        # from other tasks as dependencies (they won't show up here)
        for name in self._G:
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
        self._logger = logging.getLogger('{}.{}'.format(__name__,
                                                        type(self).__name__))
