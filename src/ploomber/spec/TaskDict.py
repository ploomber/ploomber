

class TaskDict:
    def __init__(self, data, meta):
        self.data = data
        self.meta = meta
        self.validate()

    def validate(self):

        if self.meta['infer_upstream'] and self.data.get('upstream'):
            raise ValueError('Error validating task "{}", if '
                             'meta.infer_upstream is set to True, tasks '
                             'should not have an "upstream" key'
                             .format(self.data))

        if self.meta['extract_product'] and self.data.get('product'):
            raise ValueError('Error validating task "{}", if '
                             'meta.extract_product is set to True, tasks '
                             'should not have a "product" key'
                             .format(self.data))

    def init(self):
        """Returns a Task instance
        """
        pass
