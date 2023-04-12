# adding this to make sure relative imports work fine
from .util import util_touch


def function(product, upstream):
    # to make this depend on the raw task
    upstream["raw"]
    util_touch(str(product))
