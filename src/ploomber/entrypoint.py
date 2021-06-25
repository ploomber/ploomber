from pathlib import Path

from ploomber.util.dotted_path import load_dotted_path


class EntryPoint:
    """
    Handles common operations on the 4 types of entry points. Exposes a
    pathlib.Path-like interface
    """
    Directory = 'directory'
    Pattern = 'pattern'
    File = 'file'
    DottedPath = 'dotted-path'
    ModulePath = 'module-path'

    def __init__(self, value):
        self.value = value
        self.type = find_entry_point_type(value)

    def exists(self):
        if self.type == self.Pattern:
            return True
        elif self.type in {self.Directory, self.File}:
            return Path(self.value).exists()
        elif self.type == self.DottedPath:
            return load_dotted_path(self.value, raise_=False) is not None

    def is_dir(self):
        return self.type == self.Directory

    @property
    def suffix(self):
        return None if self.type not in {self.File, self.ModulePath} else Path(
            self.value).suffix

    def __repr__(self):
        return repr(self.value)

    def __str__(self):
        return str(self.value)


# TODO: this only used to override default behavior
# when using the jupyter extension, but they have to be integrated with the CLI
# to provide consistent behavior. The problem is that logic implemented
# in _process_file_dir_or_glob and _process_factory_dotted_path
# also contains some CLI specific parts that we don't require here
def find_entry_point_type(entry_point):
    """
    Step 1: If not ENTRY_POINT is defined nor a value is passed, a default
    value is used (pipeline.yaml for CLI, recursive lookup for Jupyter client).
    If ENTRY_POINT is defined, this simply overrides the default value, but
    passing a value overrides the default value. Once the value is determined.

    Step 2: If value is a valid directory, DAG is loaded from such directory,
    if it's a file, it's loaded from that file (spec), finally, it's
    interpreted as a dotted path
    """
    type_ = try_to_find_entry_point_type(entry_point)

    if type_:
        return type_
    else:
        raise ValueError(
            'Could not determine the entry point type from value: '
            f'{entry_point!r}. Expected '
            'an existing file with extension .yaml or .yml, '
            'existing directory, glob-like pattern '
            '(i.e., *.py) or dotted path '
            '(i.e., module.sub_module.factory_function). Verify your input.')


def try_to_find_entry_point_type(entry_point):
    if entry_point is None:
        return None
    elif '*' in entry_point:
        return EntryPoint.Pattern
    elif '::' in entry_point:
        return EntryPoint.ModulePath
    elif Path(entry_point).exists():
        if Path(entry_point).is_dir():
            return EntryPoint.Directory
        else:
            return EntryPoint.File
    elif '.' in entry_point and Path(entry_point).suffix not in {
            '.yaml', '.yml'
    }:
        return EntryPoint.DottedPath
