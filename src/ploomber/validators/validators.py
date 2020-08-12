import warnings
from functools import partial, wraps
from collections.abc import Mapping


class Assert:
    """
    An object to collect assertions and print results after they've all been
    evaluated

    Examples
    --------
    >>> from ploomber.validators import Assert
    >>> assert_ = Assert()
    >>> assert_(True, 'This wont be displayed')
    >>> assert_(False, 'This will be displayed')
    >>> assert_(False, 'This will also be displayed')
    >>> assert_.check() # raise an exception, show all error messages
    """
    def __init__(self):
        self.messages_error = []
        self.messages_warning = []

    def __call__(self, expression, error_message):
        if not expression:
            self.messages_error.append(error_message)

    def warn(self, expression, warning_message):
        if not expression:
            warnings.warn(warning_message)
            self.messages_warning.append(warning_message)

    def __len__(self):
        return len(self.messages_error)

    def __iter__(self):
        for msg in self.messages_error:
            yield msg

    def __repr__(self):
        return 'Assert oject with {} error messages'.format(len(self))

    def __str__(self):
        if not self.messages_error:
            str_ = 'No errors found'
        elif len(self.messages_error) == 1:
            str_ = '1 error found: {}'.format(self.messages_error[0])
        else:
            str_ = ('{} errors found: \n * {}'.format(
                len(self.messages_error), '\n * '.join(self.messages_error)))

        if len(self.messages_warning) == 1:
            str_ += '\n\n 1 warning: {}'.format(self.messages_warning[0])
        elif len(self.messages_warning) > 1:
            str_ += ('\n\n {} warnings: \n * {}'.format(
                len(self.messages_warning),
                '\n * '.join(self.messages_warning)))

        return str_

    def check(self):
        """
        Raise AsserionError with all error messages if there is at least one
        error
        """
        if len(self):
            raise AssertionError(str(self))


def validator(fn):

    # TODO: verify fn signature

    @wraps(fn)
    def wrapped(**kwargs):
        if 'assert_' in kwargs:
            raise TypeError('Do not include the assert_ parameter in '
                            'validator functions')

        if 'data' in kwargs:
            raise TypeError('Do not include the data parameter in '
                            'validator functions')

        return partial(fn, **kwargs)

    return wrapped


@validator
def validate_schema(assert_,
                    data,
                    schema,
                    optional=None,
                    on_unexpected_cols='raise'):
    """Check if a data frame complies with a schema

    Parameters
    ----------
    data : pandas.DataFrame
        Data frame to test
    schema : list or dict
        List with column names (will only validate names)
        or dict with column names as keys, dtypes as values (will validate
        names and dtypes)
    optional : list, optional
        List of optional column names, it won't warn nor raise any errors if
        they appear
    on_unexpected_cols : str, optional
        One of 'warn', 'raise' or None. If 'warn', it will warn on extra
        columns, if 'raise' it will raise an error, if None it will completely
        ignore extra columns
    """
    if on_unexpected_cols not in {'warn', 'raise', None}:
        raise ValueError("'on_unexpected_cols' must be one of 'warn', 'raise' "
                         "or None")

    optional = set() if optional is None else set(optional)
    cols = set(data.columns)
    expected = set(schema)

    missing = expected - cols
    unexpected = cols - expected - optional

    msg = 'validate_schema: missing columns {missing}.'.format(missing=missing)
    assert_(not missing, msg)

    if on_unexpected_cols is not None:
        msg = ('validate_schema: unexpected columns {unexpected}'.format(
            unexpected=unexpected))
        caller = assert_ if on_unexpected_cols == 'raise' else assert_.warn
        caller(not unexpected, msg)

    if isinstance(schema, Mapping):
        # validate column types (as many as you can)
        dtypes = data.dtypes.astype(str).to_dict()

        for name, dtype in dtypes.items():
            expected = schema.get(name)

            if expected is not None:
                msg = ('validate_schema: wrong dtype for column "{name}". '
                       'Expected: "{expected}". Got: "{dtype}"'.format(
                           name=name, expected=expected, dtype=dtype))
                assert_(dtype == expected, msg)

    return assert_


@validator
def validate_values(assert_, data, values):
    data_cols = data.columns

    for column, (kind, params) in values.items():
        if column not in data_cols:
            assert_.warn(False,
                         ('validate_values: declared spec for column "{}" but'
                          ' it does not appear in the data').format(column))
        elif kind == 'unique':
            expected = set(params)
            unique = set(data[column].unique())
            msg = ('validate_values:: expected unique values of  "{}" to be a'
                   ' subset of {}, got: {}'.format(column, expected, unique))
            assert_(expected >= unique, msg)
        elif kind == 'range':
            if len(params) != 2:
                raise ValueError('If kind is range, you must provide two '
                                 'values, got {}'.format(params))
            min_expected, max_expected = params
            min_ = data[column].min()
            max_ = data[column].max()
            msg = ('validate_values: expected range of "{}" to be [{}, {}], '
                   'got [{}, {}]'.format(column, min_expected, max_expected,
                                         min_, max_))
            assert_(min_expected <= min_ and max_ <= max_expected, msg)
        else:
            raise ValueError('Got invalid kind, must be "unique" or "range"')


def data_frame_validator(df, validators):
    """

    Examples
    --------
    >>> from ploomber.validators import data_frame_validator
    >>> from ploomber.validators import validate_schema, validate_values
    >>> import pandas as pd
    >>> import numpy as np
    >>> df = pd.DataFrame({'x': np.random.rand(3), 'y': np.random.rand(3),
    ...                    'z': [0, 1, 2], 'i': ['a', 'b', 'c']})
    >>> data_frame_validator(df,
    ...                     [validate_schema(schema={'x': 'int', 'z': 'int'}),
    ...                      validate_values(values={'z': ('range', (0, 1)),
    ...                                              'i': ('unique', {'a'}),
    ...                                              'j': ('unique', {'b'})}
    ...                                      )])
    """
    assert_ = Assert()

    for validator in validators:
        validator(assert_=assert_, data=df)

    if len(assert_):
        raise AssertionError(str(assert_))

    return True
