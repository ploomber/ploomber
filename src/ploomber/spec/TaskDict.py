from pathlib import Path
from collections.abc import MutableMapping, Iterable, Mapping

from ploomber import tasks, products


class TaskDict(MutableMapping):
    """Converts a dictionaty to a Task object
    NOTE: document schema
    """
    def __init__(self, data, meta):
        # FIXME: make sure data and meta are immutable structures
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
        task_dict = self.data
        upstream = _pop_upstream(task_dict)
        class_ = get_task_class(task_dict)

        product = _pop_product(task_dict, self.meta)
        source_raw = task_dict.pop('source')
        name_raw = task_dict.pop('name', None)

        task = class_(source=Path(source_raw),
                      product=product,
                      name=name_raw or source_raw,
                      dag=dag,
                      **task_dict)

        return task, upstream

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


def _make_iterable(o):
    if isinstance(o, Iterable) and not isinstance(o, str):
        return o
    elif o is None:
        return []
    else:
        return [o]


def _pop_upstream(task_dict):
    upstream = task_dict.pop('upstream', None)
    return _make_iterable(upstream)


def _pop_product(task_dict, meta):
    product_raw = task_dict.pop('product')

    product_class = meta.get('product_class')

    if 'product_class' in task_dict:
        CLASS = getattr(products, task_dict.pop('product_class'))
    elif product_class:
        CLASS = getattr(products, product_class)
    else:
        CLASS = products.File

    if isinstance(product_raw, Mapping):
        return {key: CLASS(value) for key, value in product_raw.items()}
    else:
        return CLASS(product_raw)


def get_task_class(task_dict):
    """
    Pops 'class' key if it exists

    Task class is determined by the 'class' key, if missing. Defaults
    are used by inspecting the 'source' key: NoteboonRunner (.py),
    SQLScript (.sql) and BashScript (.sh).
    """
    class_name = task_dict.pop('class', None)

    if class_name:
        class_ = getattr(tasks, class_name)
    else:
        suffix = Path(task_dict['source']).suffix

        if suffix2class.get(suffix):
            class_ = suffix2class[suffix]
        else:
            raise KeyError('No default task class available for task with '
                           'source: '
                           '"{}". Default class is only available for '
                           'files with extensions {}, otherwise you should '
                           'set an explicit class key'
                           .format(task_dict['source'], set(suffix2class)))

    return class_


suffix2class = {
    '.py': tasks.NotebookRunner,
    '.ipynb': tasks.NotebookRunner,
    '.sql': tasks.SQLScript,
    '.sh': tasks.ShellScript
}


def get_value_at(d, dotted_path):
    current = d

    for key in dotted_path.split('.'):
        try:
            current = current[key]
        except KeyError:
            return None

    return current
