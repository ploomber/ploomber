from jinja2 import Environment

from ploomber.static_analysis.abstract import Extractor
from ploomber.static_analysis.jinja import find_variable_access


class StringExtractor(Extractor):
    """
    Extract variables from a string
    """
    def extract_upstream(self):
        """Extract upstream keys used in a templated SQL script
        """
        env = Environment()
        ast = env.parse(self.code)
        return find_variable_access(ast, variable='upstream')

    def extract_product(self):
        raise NotImplementedError('extract_product is not implemented in '
                                  '{}'.format(type(self).__name__))
