"""
A module for defining micro pipelines. You can use it to define and run
pipelines inside a Jupyter notebook
"""
from ploomber.micro._micro import dag_from_functions, grid

__all__ = ["dag_from_functions", "grid"]
