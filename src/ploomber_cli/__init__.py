import warnings
from sys import version_info
if version_info < (3, 7):
    warnings.warn('Ploomber 0.20 will not support Python 3.6.\n'
                  'Please either downgrade ploomber by running '
                  '`pip install ploomber==0.19` or '
                  'upgrade your Python version to 3.7+.')
