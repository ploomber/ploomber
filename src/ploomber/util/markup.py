from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import html
import mistune


try:
    mistune_version = int(mistune.__version__[0])
except ValueError:
    mistune_version = 0


mistune_recent = mistune_version >= 2


def markdown_to_html(md):
    """
    Convert markdown to HTML with syntax highlighting, works with old and
    new versions of mistune
    """
    if mistune_recent:

        class HighlightRenderer(mistune.HTMLRenderer):
            def block_code(self, code, lang=None):
                if lang:
                    lexer = get_lexer_by_name(lang, stripall=True)
                    formatter = html.HtmlFormatter()
                    return highlight(code, lexer, formatter)
                return "<pre><code>" + mistune.escape(code) + "</code></pre>"

        markdown = mistune.create_markdown(renderer=HighlightRenderer(escape=False))
        return markdown(md)
    else:

        class HighlightRenderer(mistune.Renderer):
            """mistune renderer with syntax highlighting

            Notes
            -----
            Source: https://github.com/lepture/mistune#renderer
            """

            def block_code(self, code, lang):
                if not lang:
                    return "\n<pre><code>%s</code></pre>\n" % mistune.escape(code)
                lexer = get_lexer_by_name(lang, stripall=True)
                formatter = html.HtmlFormatter()
                return highlight(code, lexer, formatter)

        renderer = HighlightRenderer()
        return mistune.markdown(md, escape=False, renderer=renderer)
