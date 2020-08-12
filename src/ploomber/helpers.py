from copy import deepcopy
from pathlib import Path

from jinja2 import Template

from ploomber.tasks import PythonCallable
from ploomber.tasks.tasks import _Gather, _Partition
from ploomber.products import File
from ploomber.tasks.TaskGroup import TaskGroup


class PartitionedFile(File):
    @property
    def _path_to_stored_source_code(self):
        # point to the parent folder
        return Path(str(self._path_to_file.parent) + '.source')

    def save_metadata(self):
        # do not save metadata
        pass

    # I think i have to remove this
    def _outdated_code_dependency(self):
        return False


def partitioned_execution(upstream_partitioned,
                          downstream_callable,
                          downstream_prefix,
                          downstream_path,
                          partition_ids,
                          partition_template='partition={{id}}',
                          upstream_other=None):
    # make sure output is a File
    assert isinstance(upstream_partitioned.product, File)

    dag = upstream_partitioned.dag
    partition_template_w_suffix = Template(downstream_prefix + '_' +
                                           partition_template)
    partition_template_t = Template(partition_template)

    # instantiate null tasks
    nulls = [
        _Partition(product=PartitionedFile(
            Path(str(upstream_partitioned.product),
                 partition_template_t.render(id=id_))),
                   dag=dag,
                   name=Template('null_' + partition_template).render(id=id_))
        for id_ in partition_ids
    ]

    def make_file(id_):
        f = PartitionedFile(
            Path(downstream_path, partition_template_t.render(id=id_)))
        return f

    # TODO: validate downstream product is File
    tasks = [
        PythonCallable(downstream_callable,
                       product=make_file(id_),
                       dag=dag,
                       name=(partition_template_w_suffix.render(id=id_)),
                       params={
                           'upstream_key':
                           (Template('null_' +
                                     partition_template).render(id=id_))
                       }) for id_ in partition_ids
    ]

    gather = _Gather(source=downstream_callable,
                     product=File(downstream_path),
                     dag=dag,
                     name=downstream_prefix)

    # TODO: "fuse" operator that merges task chains and shows it like a single
    # task - maybe this should be like this and the dag.plot function take
    # care of simplifications for viz purposes?
    for null, task in zip(nulls, tasks):
        upstream_partitioned >> null >> task >> gather

        if upstream_other:
            upstream_other >> task

    return gather


def make_task_group(task_class,
                    task_kwargs,
                    dag,
                    name,
                    params_array,
                    namer=None):
    """
    Build a group of tasks of the same class that operate over an array of
    parameters but have the same source. This is better than using a for
    loop to create multiple tasks as it takes care of validation and proper
    object creation.

    Parameters
    ----------
    task_class : class
        The task class for all generated tasks

    task_kwargs : dict
        Task keyword arguments passed to the constructor, must not contain:
        dag, name, params, nor product, as this parameters are passed by this
        function. Must include source

    dag : ploomber.DAG
        The DAG object to add these tasks to

    name : str
        The name for the task group

    params_array : list
        Each element is passed to the "params" argument on each task. This
        determines the number of tasks to be created. "name" is added to each
        element.

    """
    # validate task_kwargs
    if 'dag' in task_kwargs:
        raise KeyError('dag should not be part of task_kwargs')

    if 'name' in task_kwargs:
        raise KeyError('name should not be part of task_kwargs')

    if 'params' in task_kwargs:
        raise KeyError('params should not be part of task_kwargs')

    if 'product' not in task_kwargs:
        raise KeyError('product should be in task_kwargs')

    # TODO: validate {{index}} appears in product - maybe all products
    # should have a way to extract which placeholders exist?

    tasks_all = []

    for i, params in enumerate(params_array):

        # each task should get a different deep copy, primarily cause they
        # should have a different product
        kwargs = deepcopy(task_kwargs)
        # params should also be different copies, otherwise if the same
        # grid is re-used in several tasks, modifying anything there will
        # have side-effects
        params = deepcopy(params)

        if namer:
            task_name = namer(params)
            params['name'] = task_name
        else:
            task_name = name + str(i)
            params['name'] = i

        t = task_class(**kwargs, dag=dag, name=task_name, params=params)
        tasks_all.append(t)

    return TaskGroup(tasks_all, treat_as_single_task=False, name=name)
