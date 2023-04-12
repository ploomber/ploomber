from difflib import get_close_matches

from ploomber import tasks
from ploomber import products

_NORMALIZED_TASKS = {name.upper(): name for name in tasks.__all__}
_NORMALIZED_PRODUCTS = {name.upper(): name for name in products.__all__}
_NORMALIZED = {**_NORMALIZED_TASKS, **_NORMALIZED_PRODUCTS}

_KEY2CLASS_TASKS = {name: getattr(tasks, name) for name in tasks.__all__}
_KEY2CLASS_PRODUCTS = {name: getattr(products, name) for name in products.__all__}
_KEY2CLASS = {**_KEY2CLASS_TASKS, **_KEY2CLASS_PRODUCTS}


def _suggest_class_name(name: str, options):
    name = name.upper()

    close_commands = get_close_matches(name, options)

    if close_commands:
        return options[close_commands[0]]
    else:
        return None


def _normalize_input(name):
    return name.upper().replace("-", "").replace("_", "").replace(" ", "")


def get_suggestion(key_str, mapping=None):
    key_str_normalized = _normalize_input(key_str)
    mapping = mapping or _NORMALIZED
    return _suggest_class_name(key_str_normalized, mapping)


def str_to_class(key_str):
    return _KEY2CLASS.get(key_str, None)


def validate_task_class_name(value):
    """
    Validates if a string is a valid Task class name (e.g., SQLScipt).
    Raises a ValueError if not.
    """
    if value not in _KEY2CLASS_TASKS:
        suggestion = get_suggestion(value, mapping=_NORMALIZED_TASKS)
        msg = f"{value!r} is not a valid Task class name"

        if suggestion:
            msg += f". Did you mean {suggestion!r}?"

        raise ValueError(msg)

    return _KEY2CLASS_TASKS[value]


def validate_product_class_name(value):
    """
    Validates if a string is a valid Product class name (e.g., File).
    Raises a ValueError if not.
    """
    if value not in _KEY2CLASS_PRODUCTS:
        suggestion = get_suggestion(value, mapping=_NORMALIZED_PRODUCTS)
        msg = f"{value!r} is not a valid Product class name"

        if suggestion:
            msg += f". Did you mean {suggestion!r}?"

        raise ValueError(msg)

    return _KEY2CLASS_PRODUCTS[value]
