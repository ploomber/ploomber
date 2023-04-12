from pathlib import Path


def iterable(obj, repr_=False):
    if repr_:
        sorted_ = sorted(repr(element) for element in obj)
    else:
        sorted_ = sorted(f"'{element}'" for element in obj)

    if len(sorted_) > 1:
        sorted_[-1] = f"and {sorted_[-1]}"

    return ", ".join(sorted_)


def them_or_name(obj):
    return "them" if len(obj) > 1 else f"'{list(obj)[0]}'"


def trailing_dot(obj):
    return " ".join(f"{e}." for e in obj)


def try_relative_path(path):
    if not isinstance(path, (str, Path)):
        return path

    path_ = Path(path)

    if path_.is_absolute():
        try:
            return str(path_.relative_to(Path().resolve()))
        except ValueError:
            return str(path)
    else:
        return str(path)
