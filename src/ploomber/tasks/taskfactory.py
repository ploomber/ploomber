class TaskFactory:
    """Utility class for reducing boilerplate code
    """
    def __init__(self, task_class, product_class, dag):
        self.task_class = task_class
        self.product_class = product_class
        self.dag = dag

    def make(self, task_arg, product_arg, name, params=None):
        product = self.product_class(product_arg)
        return self.task_class(task_arg,
                               product=product,
                               dag=self.dag,
                               name=name,
                               params=params)
