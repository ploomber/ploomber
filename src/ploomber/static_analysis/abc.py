"""
Extractors read "upstream" and "product" variables defined in sources
"""
import abc


class Extractor(abc.ABC):
    def __init__(self, code):
        self.code = code

    @abc.abstractmethod
    def extract_upstream(self):
        pass

    @abc.abstractmethod
    def extract_product(self):
        pass


class NotebookExtractor(Extractor):
    def __init__(self, parameters_cell):
        self.parameters_cell = parameters_cell

        if self.parameters_cell is None:
            raise ValueError('Notebook does not have a cell with the tag '
                             '"parameters"')
