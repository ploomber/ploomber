"""
Utils for comparing source code
"""
import io
import tokenize
from difflib import Differ
import parso
import ast

try:
    import sqlparse
except ImportError:
    sqlparse = None

try:
    import autopep8
except ImportError:
    autopep8 = None
from ploomber.products.serializeparams import remove_non_serializable_top_keys


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
    if code is None:
        return None

    try:
        ast.parse(code)

    except SyntaxError:
        pass

    else:
        code = _delete_python_comments(code)

        if not autopep8 or not parso:
            raise ImportError(
                'autopep8 and parso are required for normalizing '
                'Python code: pip install autopep8 parso')

        node = parso.parse(code).children[0]

        if node.type == 'decorated':
            node = node.children[-1]

        try:
            doc_node = node.get_doc_node()
        except Exception:
            # function without docstring...
            doc_node = None

        if doc_node is not None:
            code = code.replace('\n' + doc_node.get_code(), '')

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
    NORMALIZERS = {
        None: normalize_null,
        'py': normalize_python,
        'sql': normalize_sql
    }

    def is_different(self, a, b, a_params, b_params, extension=None):
        """
        Compares code and params to determine if it's changed. Ignores top-keys
        in a_params or b_params if they're no JSON serializable.

        Parameters
        ----------
        a : str
            Code to compare

        b : str
            Code to compare

        a_params : dict
            Params passed to a

        b_params : dict
            Params passed to b

        extension : str, default=None
            Code extension. Used to normalize code to prevent changes such
            as whitespace to trigger false positives. Normalization only
            available for .py and .sql, other languages are compared as is

        Returns
        -------
        result : bool
            True if code is different (different code or params),
            False if they are the same (same code and params)

        diff : str
            A diff view of the differences

        Notes
        -----
        Params comparison is ignored if either a_params or b_params is None
        """
        # TODO: this can be more efficient. ie only compare source code
        # if params are the same and only get diff if result is True
        normalizer = self._get_normalizer(extension)
        a_norm = normalizer(a)
        b_norm = normalizer(b)

        if a_params is None or b_params is None:
            outdated_params = False
        else:
            a_params_ = remove_non_serializable_top_keys(a_params)
            b_params_ = remove_non_serializable_top_keys(b_params)
            outdated_params = (a_params_ != b_params_)

        result = outdated_params or (a_norm != b_norm)
        # TODO: improve diff view, also show a params diff view. probably
        # we need to normalize them first (maybe using pprint?) then take
        # the diff
        diff = self.get_diff(a_norm, b_norm, normalize=False)

        return result, diff

    def get_diff(self, a, b, extension=None, normalize=True):
        """Get the diff view
        """
        # TODO: remove normalize param, we are already normalizing in the
        # is_different method
        if normalize:
            normalizer = self._get_normalizer(extension)

            a = normalizer(a)
            b = normalizer(b)

        diff = diff_strings(a, b)

        if extension is not None:
            diff = '[Code was normalized]\n' + diff

        return diff

    def _get_normalizer(self, extension):
        """Get the normalizer function for a given extension
        """
        if extension in self.NORMALIZERS:
            return self.NORMALIZERS[extension]
        else:
            return normalize_null
