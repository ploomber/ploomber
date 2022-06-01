from pathlib import Path


def create_file(product):
    """This function creates a file
    """
    Path(product).write_text('some text')