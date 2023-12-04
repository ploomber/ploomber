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


def _import_class(dotted_path, base_class=None):
    """
    Import a class from a dotted path string. Optionally, check if the class
    is a subclass of a specified base class.
    """
    module_path, class_name = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    klass = getattr(module, class_name)
    # Ensure klass is a type
    if not isinstance(klass, type):
        raise TypeError(f"{klass} is not a class")
    # If a base class is provided, check if klass is a subclass of it
    if base_class is not None:
        if not issubclass(klass, base_class):
            raise TypeError(f"{klass} is not a subclass of {base_class}")
    return klass


def _validate_class(value, base_class, key2class, normalized):
    """
    Validates if a string is a valid class name (e.g., SQLScipt).
    Raises a ValueError if not.
    """
    # Dotted path to class
    if "." in value:
        return _import_class(value, base_class=base_class)
    # Ploomber class
    if value in key2class:
        return key2class[value]
    else:
        suggestion = get_suggestion(value, mapping=normalized)
        msg = f"{value!r} is not a valid class name"
        if suggestion:
            msg += f". Did you mean {suggestion!r}?"
        raise ValueError(msg)


def validate_task_class_name(value):
    """
    Validates if a string is a valid Task class name (e.g., SQLScipt).
    Raises a ValueError if not.
    """
    return _validate_class(
        value,
        base_class=tasks.Task,
        key2class=_KEY2CLASS_TASKS,
        normalized=_NORMALIZED_TASKS,
    )


def validate_product_class_name(value):
    """
    Validates if a string is a valid Product class name (e.g., File).
    Raises a ValueError if not.
    """
    return _validate_class(
        value,
        base_class=products.Product,
        key2class=_KEY2CLASS_PRODUCTS,
        normalized=_NORMALIZED_PRODUCTS,
    )
