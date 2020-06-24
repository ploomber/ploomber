from collections.abc import MutableMapping


class TaskDict(MutableMapping):
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

    def init(self, dag):
        """Returns a Task instance
        """
        pass

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __iter__(self):
        for e in self.data:
            yield e

    def __len__(self):
        return len(self.data)
