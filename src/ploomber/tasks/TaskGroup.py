from ploomber.util import isiterable

# FIXME: restrict for now TaskGroup treated as a single task to be a source
# node, otherwise handling the upstream logic becomes messy:
# the dag.plot function should see a single edge but in reality there should
# be many going from the upstream dependencies to each one of the tasks
# in the group, currently plotting and execution have the same source of truth:
# the nx.DiGraph object so we will have to make some changes - probably
# just at the plot level: the DAG should still see edges to all the subtasks
# for execution to run successfully but the plot function should only
# draw a single circle


# FIXME: behavior is very different when treat_as_single_task is on,
# better to make it to separate classes (they should not subclass from Task
# though) - TaskGroup as multuple tasks definitely no, since it is just a proxy
# for adding many tasks to a dag, for the other case, seems like we will have
# to override most of the methods anyway and subclassing will make this
# complex, better just to implement whatever methods are needed and disclose
# whih Task-like behavior does not work
class TaskGroup:
    """
    A collection of Tasks, used internally for enabling operador overloading

    (task1 + task2) >> task3
    """

    # TODO: implement mapping interface, task.upstream should return
    # a TaskGroup object, make ipython autocomplete work in getitem,
    # also add logic to verify if all keys were used, to detect tasks
    # where not all upstream dependencies are used. That means, they should
    # not be dependencies. Checking should be implemented in getitem,
    # but possibly in pop, popitem

    def __init__(self, tasks, treat_as_single_task=False, name=None):
        self.tasks = tasks
        # self.treat_as_single_task = treat_as_single_task

        # # name is only required when treat_as_single_task is True
        # if treat_as_single_task and name is None:
        #     raise ValueError('name cannot be None if treat_as_single_task is True')

        # # should only exist when treat_as_single_task is True
        # dags = []

        # for t in tasks:
        #     if t.dag not in dags:
        #         dags.append(t.dag)

        # if len(dags) > 1:
        #     raise ValueError('All tasks must be part of the same DAG')

        # self.dag = dags[0]

        # self.name = name

        # # if treat_as_single_task we have to delete the tasks from the
        # # dag and add the TaskGroup
        # if treat_as_single_task:
        #     for t in self.tasks:
        #         self.dag.pop(t.name)

        # self.dag._add_task(self)

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
        # if self.treat_as_single_task:
        #     if isiterable(other):
        #         for o in other:
        #             self.dag._add_edge(o, self)
        #     else:
        #         self.dag._add_edge(other, self)
        # else:
        if isiterable(other):
            for t in self.tasks:
                for o in other:
                    t.set_upstream(other)
        else:
            for t in self.tasks:
                t.set_upstream(other)

    # FIXME: implement render

    def __rshift__(self, other):
        other.set_upstream(self)
        # return other so a >> b >> c works
        return other
