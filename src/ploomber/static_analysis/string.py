from jinja2 import Template

from ploomber.static_analysis.abstract import Extractor
from ploomber.static_analysis.jinja import JinjaUpstreamIntrospector


class StringExtractor(Extractor):
    """
    Extract variables from a string
    """
    def extract_upstream(self):
        """Extract upstream keys used in a templated SQL script
        """
        upstream = JinjaUpstreamIntrospector()
        params = {'upstream': upstream}
        Template(self.code).render(params)
        return set(upstream.keys) if len(upstream.keys) else None

    def extract_product(self):
        raise NotImplementedError('extract_product is not implemented in '
                                  '{}'.format(type(self).__name__))
