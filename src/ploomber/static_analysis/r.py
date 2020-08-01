from ploomber.static_analysis.parser.parser import Parser
from ploomber.static_analysis.parser.RLexer import RLexer
from ploomber.static_analysis.abstract import NotebookExtractor


class RNotebookExtractor(NotebookExtractor):
    def extract_upstream(self):
        code = self.parameters_cell.split('\n')[0]
        parser = Parser(list(RLexer(code)))
        exp = parser.parse()
        return set(exp.right.to_python())

    def extract_product(self):
        code = self.parameters_cell.split('\n')[1]
        parser = Parser(list(RLexer(code)))
        exp = parser.parse()
        return exp.right.to_python()
