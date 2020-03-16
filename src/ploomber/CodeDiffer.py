"""
Utils for comparing source code
"""
import io
import tokenize
import warnings
from difflib import Differ

try:
    import sqlparse
except ImportError:
    sqlparse = None


try:
    import autopep8
except ImportError:
    autopep8 = None


try:
    import parso
except ImportError:
    parso = None


def normalize_null(code):
    return code


def normalize_sql(code):
    if not sqlparse:
        raise ImportError('sqlparse is required for normalizing SQL code')

    return None if code is None else sqlparse.format(code,
                                                     keyword_case='upper',
                                                     identifier_case='lower',
                                                     strip_comments=True,
                                                     reindent=True,
                                                     indent_with=4)


def _delete_python_comments(code):
    tokens = tokenize.generate_tokens(io.StringIO(code).readline)
    tokens = [(num, val) for num, val, _, _, _ in tokens
              if num != tokenize.COMMENT]
    return tokenize.untokenize(tokens)


def normalize_python(code):
    # TODO: we should really be comparing the tree between a, b but this
    # works for now

    if code is None:
        return None

    code = _delete_python_comments(code)

    if not autopep8 or not parso:
        raise ImportError('autopep8 and parso are required for normalizing '
                          'Python code: pip install autopep8 parso')

    try:
        doc_node = parso.parse(code).children[0].get_doc_node()
    except Exception as e:
        warnings.warn('Could not remove docstring from Python code: {}'
                      .format(e))
    else:
        if doc_node is not None:
            code = code.replace(doc_node.get_code(), '')

    code = autopep8.fix_code(code)

    return code


def diff_strings(a, b):
    """Compute the diff between two strings
    """
    d = Differ()

    if a is None and b is None:
        return '[Both a and b are None]'

    out = ''

    if a is None:
        out += '[a is None]\n'
    elif b is None:
        out += '[a is None]\n'

    a = '' if a is None else a
    b = '' if b is None else b

    result = d.compare(a.splitlines(keepends=True),
                       b.splitlines(keepends=True))
    out += ''.join(result)

    return out


class CodeDiffer:
    LANGUAGES = ['python', 'sql']
    NORMALIZERS = {None: normalize_null, 'python': normalize_python,
                   'sql': normalize_sql}

    def code_is_different(self, a, b, language=None):
        normalizer = self._get_normalizer(language)

        a_norm = normalizer(a)
        b_norm = normalizer(b)

        return a_norm != b_norm

    def get_diff(self, a, b, language=None):
        normalizer = self._get_normalizer(language)

        a = normalizer(a)
        b = normalizer(b)

        diff = diff_strings(a, b)

        if language is not None:
            diff = '[Code was normalized]\n' + diff

        return diff

    def _get_normalizer(self, language):
        if language in self.NORMALIZERS:
            return self.NORMALIZERS[language]
        else:
            return normalize_null
