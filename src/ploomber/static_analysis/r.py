from ploomber.static_analysis.parser.parser import Parser
from ploomber.static_analysis.parser.RLexer import RLexer
from ploomber.static_analysis.abc import NotebookExtractor


def naive_parsing(code, var_name):
    """
    Our current R parser can only deal with a single statement (one line)
    at a time. So we parse on a per-line basis and look for the variable
    we want, this will be replaced for a more efficient implementation
    once we improve the parser
    """
    for code in code.split('\n'):
        if code != '':
            parser = Parser(list(RLexer(code)))
            exp = parser.parse()

            if exp.left.value == var_name:
                return exp.right.to_python()


class RNotebookExtractor(NotebookExtractor):
    def extract_upstream(self):
        parsed = naive_parsing(self.parameters_cell, 'upstream')
        return parsed if not parsed else set(parsed)

    def extract_product(self):
        return naive_parsing(self.parameters_cell, 'product')
