def iterable(obj):
    sorted_ = sorted(f"'{element}'" for element in obj)

    if len(sorted_) > 1:
        sorted_[-1] = f'and {sorted_[-1]}'

    return ", ".join(sorted_)


def them_or_name(obj):
    return 'them' if len(obj) > 1 else f"'{list(obj)[0]}'"


def trailing_dot(obj):
    return ' '.join(f'{e}.' for e in obj)
