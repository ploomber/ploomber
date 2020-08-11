import networkx as nx
from itertools import chain


class SuperDAG:
    """DAG-like interface to make multiple dags behave like one
    """
    def __init__(self, dags):
        G = nx.DiGraph()

        for task in chain(*(dag.values() for dag in dags)):
            G.add_node(task)
            for upstream in task.upstream:
                G.add_edge(task.dag[upstream], task)

        dags = []

        for task in nx.algorithms.topological_sort(G):
            if task.dag not in dags:
                dags.append(task.dag)

        self.G = G
        self.dags = dags

    def plot(self):
        G_ = nx.nx_agraph.to_agraph(self.G)
        G_.draw('pip.png', prog='dot', args='-Grankdir=LR')

    def build(self):
        for dag in self.dags:
            dag.build()

    def render(self):
        for dag in self.dags:
            dag.render()
