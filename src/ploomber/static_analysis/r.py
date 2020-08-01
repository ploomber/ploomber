from ploomber.static_analysis.abstract import NotebookExtractor


class RNotebookExtractor(NotebookExtractor):
    def extract_upstream(self):
        return extract_upstream(self.parameters_cell)

    def extract_product(self):
        return extract_product(self.parameters_cell)


def extract_product(code_cell):
    pass


def extract_upstream(code_cell):
    pass
