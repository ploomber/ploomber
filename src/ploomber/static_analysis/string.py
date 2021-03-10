from ploomber.static_analysis.abc import Extractor
from ploomber.static_analysis.jinja import JinjaExtractor


class StringExtractor(Extractor):
    """
    Extract variables from a string
    """
    def extract_upstream(self):
        """Extract upstream keys used in a templated SQL script
        """
        extractor = JinjaExtractor(self.code)
        return extractor.find_variable_access(variable='upstream')

    def extract_product(self):
        raise NotImplementedError('extract_product is not implemented in '
                                  '{}'.format(type(self).__name__))
