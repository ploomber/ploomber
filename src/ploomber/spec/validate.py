def keys(valid, passed, required=None, name='spec'):
    passed = set(passed)

    if valid:
        extra = passed - set(valid)

        if extra:
            raise KeyError("Error validating {}, the following keys aren't "
                           "valid: {}. Valid keys are: {}"
                           .format(name, extra, valid))

    if required:
        missing = set(required) - passed

        if missing:
            raise KeyError("Error validating {}, the following required "
                           "keys are missing: {}".format(name, missing))
