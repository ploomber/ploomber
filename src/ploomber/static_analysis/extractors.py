from ploomber.static_analysis.python import PythonNotebookExtractor
from ploomber.static_analysis.r import RNotebookExtractor
from ploomber.static_analysis.sql import SQLExtractor

_EXTRACTOR_FOR_LANGUAGE = {
    'python': PythonNotebookExtractor,
    'r': RNotebookExtractor,
    'sql': SQLExtractor
}


def extractor_class_for_language(language):
    if language not in _EXTRACTOR_FOR_LANGUAGE:
        raise NotImplementedError(
            '"{}" is unsupported, supported languages are {}'.format(
                language, list(_EXTRACTOR_FOR_LANGUAGE)))

    return _EXTRACTOR_FOR_LANGUAGE[language]
