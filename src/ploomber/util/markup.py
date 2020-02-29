from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import html
import mistune


class HighlightRenderer(mistune.Renderer):
    """mistune renderer with syntax highlighting

    Notes
    -----
    Source: https://github.com/lepture/mistune#renderer
    """

    def block_code(self, code, lang):
        if not lang:
            return '\n<pre><code>%s</code></pre>\n' % \
                mistune.escape(code)
        lexer = get_lexer_by_name(lang, stripall=True)
        formatter = html.HtmlFormatter()
        return highlight(code, lexer, formatter)
