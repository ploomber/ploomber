from ploomber.exceptions import MissingKeysValidationError, ValidationError
from ploomber.io import pretty_print


def keys(valid, passed, required=None, name="spec"):
    passed = set(passed)

    if valid:
        extra = passed - set(valid)

        if extra:
            raise ValidationError(
                "Error validating {}, the following keys aren't "
                "valid: {}. Valid keys are: {}".format(
                    name, pretty_print.iterable(extra), pretty_print.iterable(valid)
                )
            )

    if required:
        missing = set(required) - passed

        if missing:
            raise MissingKeysValidationError(
                f"Error validating {name}. Missing "
                f"keys: { pretty_print.iterable(missing)}",
                missing_keys=missing,
            )
