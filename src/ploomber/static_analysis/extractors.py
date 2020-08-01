from ploomber.static_analysis.python import PythonNotebookExtractor
from ploomber.static_analysis.r import RNotebookExtractor
from ploomber.static_analysis.sql import SQLExtractor

_EXTRACTOR_FOR_LANGUAGE = {
    'python': PythonNotebookExtractor,
    # uppercase to match the name from the jupyter kernel
    'R': RNotebookExtractor,
    'sql': SQLExtractor
}

# FIXME: tmp, will delete once the refactoring is finished
_EXTRACTOR_FOR_SUFFIX = {
    '.sql': SQLExtractor,
    '.ipynb': PythonNotebookExtractor,
    '.py': PythonNotebookExtractor,
}


def extractor_for_language(language):
    if language not in _EXTRACTOR_FOR_LANGUAGE:
        raise KeyError(
            '"{}" is unsupported, supported languages are {}'.format(
                language, list(_EXTRACTOR_FOR_LANGUAGE)))

    return _EXTRACTOR_FOR_LANGUAGE[language]
