from typing import (Any, Optional)


# Validate the input of type string
def str_param(item: Any, name: str) -> str:
    if not isinstance(item, str):
        raise TypeError(f"TypeError: Variable not supported/wrong type: "
                        f"{item}, {name}, should be a str")

    return item


def opt_str_param(name: str, item: Any) -> Optional[str]:
    # Can leverage regular string function
    if item is not None and not isinstance(item, str):
        raise TypeError(f"TypeError: Variable not supported/wrong type: "
                        f"{item}, {name}, should be a str")
    return item
