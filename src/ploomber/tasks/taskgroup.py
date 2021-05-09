"""
TaskGroup allows group tasks together. It enables syntactic sugar when adding
dependencies explicitly (e.g., (t1 + t2) >> t3). It also allows creating
multiple tasks at once using a parameters array.

There are two potential cases that we haven't implemented:
    1. Automatically generate an array of tasks to apply a transformation over
        file partitions. e.g., user supplies code that cannot be easily
        parallelized using numpy/pandas + the column(s) to use for
        partitioning. Then we partition in equal parts ensuring that all rows
        with the same id end up in the same partition, generate N tasks (one
        per partition) and output a final directory with the outputs of each
        task. Since parquet supports partitions, the parquet partitions can be
        used without having to join them.
    2. Same use case as before but this time, the tasks look like a single one
        from the DAGs perspective. Inside they are executed in a for loop. This
        can be useful to scale up batch computations in a single-machine.
        Often DS/ML practitioners run into memory errors because pandas/numpy
        have to load the entire dataset to memory, if we process the files
        in partitions, we can process the whole dataset in parts and avoid
        memory issues. The only pre-requisite for this would be for upstream
        dependencies to generate their product in parquet partitions.

These are nice-to-have features that require a considerable effort.

A few notes from a first implemenation attempt:
    * Although use cases (1) and (2) sound similar, from the dag perspective
        they are different (1) generates multiple taks, (2) generates a single
        task that looks like one from the outside. It might be better to
        have separate implementations, instead of a single TaskGroup
"""
from collections.abc import Mapping
from pathlib import Path
from copy import deepcopy, copy

from ploomber.products import File
from ploomber.util import isiterable, ParamGrid
from ploomber.products.mixins import SQLProductMixin


class TaskGroup:
    """
    A collection of Tasks, used internally for enabling operador overloading

    (task1 + task2) >> task3
    """
    def __init__(self, tasks):
        self.tasks = tasks

    def __iter__(self):
        for t in self.tasks:
            yield t

    def __len__(self):
        return len(self.tasks)

    def __add__(self, other):
        if isiterable(other):
            return TaskGroup(list(self.tasks) + list(other))
        else:
            return TaskGroup(list(self.tasks) + [other])

    def __radd__(self, other):
        if isiterable(other):
            return TaskGroup(list(other) + list(self.tasks))
        else:
            return TaskGroup([other] + list(self.tasks))

    def set_upstream(self, other):
        if isiterable(other):
            for t in self.tasks:
                for o in other:
                    t.set_upstream(other)
        else:
            for t in self.tasks:
                t.set_upstream(other)

    def __rshift__(self, other):
        other.set_upstream(self)
        # return other so a >> b >> c works
        return other

    @classmethod
    def from_params(cls,
                    task_class,
                    product_class,
                    product_primitive,
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
            dag, name, params, nor product, as this parameters are passed by
            this function. Must include source

        dag : ploomber.DAG
            The DAG object to add these tasks to

        name : str
            The name for the task group

        params_array : list
            Each element is passed to the "params" argument on each task. This
            determines the number of tasks to be created. "name" is added to
            each
            element.
        """
        # validate task_kwargs
        if 'dag' in task_kwargs:
            raise KeyError('dag should not be part of task_kwargs')

        if 'name' in task_kwargs:
            raise KeyError('name should not be part of task_kwargs')

        if 'params' in task_kwargs:
            raise KeyError('params should not be part of task_kwargs')

        if 'product' in task_kwargs:
            raise KeyError('product should not be part of task_kwargs')

        if 'source' not in task_kwargs:
            raise KeyError('source should be in task_kwargs')

        # TODO: validate {{index}} appears in product - maybe all products
        # should have a way to extract which placeholders exist?

        tasks_all = []

        for index, params in enumerate(params_array):

            # each task should get a different deep copy, primarily cause they
            # should have a different product
            kwargs = deepcopy(task_kwargs)
            # params should also be different copies, otherwise if the same
            # grid is re-used in several tasks, modifying anything there will
            # have side-effects
            params = deepcopy(params)

            # assign task name
            if namer:
                task_name = namer(params)
            else:
                task_name = name + str(index)

            # add index to product primitive
            if product_class is File or issubclass(product_class,
                                                   SQLProductMixin):
                product = _init_product(product_class, product_primitive,
                                        index)
            else:
                raise NotImplementedError('TaskGroup only sypported for '
                                          'File and SQL products. '
                                          f'{product_class} is not supported')

            t = task_class(product=product,
                           dag=dag,
                           name=task_name,
                           params=params,
                           **kwargs)
            tasks_all.append(t)

        return cls(tasks_all)

    @classmethod
    def from_grid(cls,
                  task_class,
                  product_class,
                  product_primitive,
                  task_kwargs,
                  dag,
                  name,
                  grid,
                  namer=None):
        params_array = ParamGrid(grid).product()
        return cls.from_params(task_class=task_class,
                               product_class=product_class,
                               product_primitive=product_primitive,
                               task_kwargs=task_kwargs,
                               dag=dag,
                               name=name,
                               params_array=params_array,
                               namer=namer)


def _init_product(product_class, product_primitive, index):
    if isinstance(product_primitive, Mapping):
        return {
            key: _init_product(product_class, primitive, index)
            for key, primitive in product_primitive.items()
        }
    elif isinstance(product_primitive, str):
        return _init_product_with_str(product_class, product_primitive, index)
    # is there a better way to check this? Sequence also matches str/bytes
    elif isinstance(product_primitive, (list, tuple)):
        return _init_product_with_sql_elements(product_class,
                                               product_primitive, index)
    else:
        raise NotImplementedError('TaskGroup only supported for task dict '
                                  'and str product primitives. Got '
                                  f'{product_primitive}, an object of type '
                                  f'{type(product_primitive).__name__}')


def _init_product_with_str(product_class, product_primitive, index):
    path = Path(product_primitive)
    suffix = ''.join(path.suffixes)
    filename = path.name.replace(suffix, '')
    filename_with_index = f'{filename}-{index}{suffix}'
    return product_class(path.parent / filename_with_index)


def _init_product_with_sql_elements(product_class, product_primitive, index):
    # this could be [schema, name, type] or just [name, type]
    index_to_change = 1 if len(product_primitive) == 3 else 0
    updated = copy(product_primitive)
    updated[index_to_change] = product_primitive[index_to_change] + f'-{index}'
    return product_class(updated)
