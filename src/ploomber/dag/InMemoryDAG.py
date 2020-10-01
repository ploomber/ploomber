class InMemoryDAG:
    def __init__(self, dag):
        dag.render()

        self.dag = dag
        self.root_nodes = [
            name for name, degree in dag._G.in_degree() if not degree
        ]

    def build(self, root_params):
        outs = {}

        extra = set(root_params) - set(self.root_nodes)
        missing = set(self.root_nodes) - set(root_params)

        if missing or extra:
            raise ValueError

        for task_name in self.dag:
            task = self.dag[task_name]
            params = task.params.to_dict()

            # if root node, replace input params (but do not replace upstream
            # not product)
            if task_name in self.root_nodes:
                params = {**params, **root_params[task_name]}

            # replace params with output
            if 'upstream' in params:
                params['upstream'] = {
                    k: outs[k]
                    for k, v in params['upstream'].items()
                }

            outs[task_name] = task.source.primitive(**params)

        return outs
