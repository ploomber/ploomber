class NotebookSource:
    """
    A source object representing a jupyter notebook (or any format supported
    by jupytext)
    """
    def __init__(self, path):
        pass

    def get_parameters(self):
        """
        Returns a dictionary with the declared parameters (variables in a cell
        tagged as "parameters")
        """
        pass

    def to_python(self):
        """
        Returns the Python representation for this notebook
        """
        pass

    def post_init_validate():
        """
        Validate notebook after initialization (run pyflakes to detect
        syntax errors)
        """
        # NOTE: what happens if I pass source code with errors to parso?
        # maybe we don't need to use pyflakes after all
        # we can also use compile. can pyflakes detect things that
        # compile cannot?
        pass

    def post_render_validate(self):
        """
        Validate params passed againts parameters in the notebook
        """
        pass
