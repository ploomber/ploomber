from ploomber import products
from ploomber.static_analysis.abstract import Extractor
from ploomber.static_analysis.jinja import JinjaExtractor


class SQLExtractor(Extractor):
    """

    Parameters
    ----------
    code : str or Placeholder
        SQL code
    """
    def __init__(self, code):
        self._jinja_extractor = JinjaExtractor(code)
        self._product = None
        self._extracted_product = False

    def extract_upstream(self):
        """Extract upstream keys used in a templated SQL script
        """
        return self._jinja_extractor.find_variable_access(variable='upstream')

    def extract_product(self, raise_if_none=True):
        """
        Extract an object from a SQL template that defines as product variable:

        {% set product = SOME_CLASS(...) %}

        Where SOME_CLASS is a class defined in ploomber.products. If no product
        variable is defined, returns None
        """
        product = self._jinja_extractor.find_variable_assignment(
            variable='product')

        if product is None:
            if raise_if_none:
                raise ValueError("Couldn't extract 'product' from code:\n" +
                                 self._jinja_extractor.get_code_as_str())
        else:
            # validate product
            try:
                # get the class name used
                class_ = getattr(products, product.node.name)
                # get the arg passed to the class
                arg = product.args[0].as_const()
                # try to initialize object
                return class_(arg)
            except Exception as e:
                exc = ValueError("Found a variable named 'product' in "
                                 "code: {} but it does not appear to "
                                 "be a valid SQL product, verify it ".format(
                                     self._jinja_extractor.code))
                raise exc from e
