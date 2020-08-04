import string
from ploomber.static_analysis.parser.tokens import (Integer, BinaryOperator,
                                                    Assignment, Name, Operator,
                                                    String, Null)
from ploomber.static_analysis.parser.abstract import Lexer


class RLexer(Lexer):
    """
    Parses a code string and returns one token at a time

    Notes
    -----
    R data structures: http://adv-r.had.co.nz/Data-structures.html
    """
    def __init__(self, text):
        self.text = text
        self.pos = 0
        self.current_char = self.text[0]

    @property
    def next_char(self):
        return self.text[self.pos + 1]

    def comes_next(self, s):
        return self.text[self.pos:self.pos + len(s)] == s

    def advance(self, n=1):
        advanced = self.text[self.pos:self.pos + n]

        self.pos += n

        if self.pos > len(self.text) - 1:
            self.current_char = None
        else:
            self.current_char = self.text[self.pos]

        return advanced

    def skip_whitespace(self):
        while self.current_char is not None and self.current_char.isspace():
            self.advance()

    def read_name(self):
        result = ''

        while (self.current_char is not None
               and self.current_char in string.ascii_letters):
            result += self.current_char
            self.advance()

        return result

    def read_string(self):
        self.advance()

        result = ''

        while (self.current_char is not None
               and self.current_char not in ['"', "'"]):
            result += self.current_char
            self.advance()

        self.advance()

        return result

    def read_integer(self):
        result = ''

        while self.current_char is not None and self.current_char.isdigit():
            result += self.current_char
            self.advance()

        return int(result)

    def __iter__(self):
        while self.current_char is not None:

            if self.current_char.isspace():
                self.skip_whitespace()
                continue

            elif self.comes_next('list'):
                op = Operator('list')
                self.advance(n=len('list'))
                yield op

            elif self.comes_next('NULL'):
                null = Null()
                self.advance(n=len('NULL'))
                yield null

            # Vector definiton start
            elif self.current_char == 'c' and self.next_char == '(':
                self.advance(n=2)
                yield Operator('c(')

            elif self.current_char in string.ascii_letters:
                yield Name(self.read_name())

            # Assignment (<-)
            elif self.current_char == '<' and self.next_char == '-':
                self.advance(n=2)
                yield Assignment('<-')

            elif self.current_char == '=':
                self.advance()
                yield Assignment('=')

            # Other operators
            elif self.current_char in ['(', ')', ',']:
                op = Operator(self.current_char)
                self.advance()
                yield op

            elif self.current_char in ['"', "'"]:
                yield String(self.read_string())

            elif self.current_char.isdigit():
                yield Integer(self.read_integer())

            elif self.current_char in ['+', '-', '*']:
                op = BinaryOperator(self.current_char)
                self.advance()
                yield op
            else:
                raise SyntaxError('Could not parse parameters in R notebook. '
                                  'Verify they have the right format: '
                                  '"upstream = list(\'a\', \'b\')" or '
                                  '"product = list(name=\'path/to/file\')"')
