from functools import partial, wraps


class Assert:

    def __init__(self):
        self.messages_error = []
        self.messages_warning = []

    def __call__(self, expression, error_message):
        if not expression:
            self.messages_error.append(error_message)

    def warn(self, expression, warning_message):
        if not expression:
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
            str_ = ('{} errors found: \n * {}'
                    .format(len(self.messages_error),
                            '\n * '.join(self.messages_error)))

        if len(self.messages_warning) == 1:
            str_ += '\n\n 1 warning: {}'.format(self.messages_warning[0])
        elif len(self.messages_warning) > 1:
            str_ += ('\n\n {} warnings: \n * {}'
                     .format(len(self.messages_warning),
                             '\n * '.join(self.messages_error)))

        return str_


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
def validate_schema(assert_, data, schema, error_on_extra_cols=False):
    """Check if a data frame complies with a schema
    """
    cols = set(data.columns)
    expected = set(schema)
    missing = expected - cols
    unexpected = cols - expected

    msg = 'Missing columns: {missing}.'.format(missing=missing)
    assert_(not missing, msg)

    msg = 'Unexpected columns {unexpected}'.format(unexpected=unexpected)
    caller = assert_ if error_on_extra_cols else assert_.warn
    caller(not unexpected, msg)

    # validate column types (as many as you can)
    dtypes = data.dtypes.astype(str).to_dict()

    for name, dtype in dtypes.items():
        expected = schema.get(name)

        if expected is not None:
            msg = ('Wrong dtype for column "{name}". '
                   'Expected: "{expected}". Got: "{dtype}"'
                   .format(name=name, expected=expected, dtype=dtype))
            assert_(dtype == expected, msg)

    return assert_


def data_frame_validator(df, validators):
    """

    Examples
    --------
    >>> import pandas as pd
    >>> import numpy as np
    >>> df = pd.DataFrame({'x': np.random.rand(10), 'y': np.random.rand(10)})
    >>> data_frame_validator(df,
    ...                     [validate_schema(schema={'x': 'int', 'z': 'int'})])
    """
    assert_ = Assert()

    for validator in validators:
        validator(assert_=assert_, data=df)

    if len(assert_):
        raise AssertionError(str(assert_))
