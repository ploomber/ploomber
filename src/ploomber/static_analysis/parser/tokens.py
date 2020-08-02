"""
Tokens, given the limited scope our parser, we can reuse them for several
languages
"""


class Token:
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.value == other.value

    def __repr__(self):
        return '{}("{}")'.format(type(self).__name__, self.value)

    def to_python(self):
        return self.value


class Operator(Token):
    pass


class Name(Token):
    pass


class Integer(Token):
    pass


class Null(Token):
    def __init__(self):
        self.value = None


class String(Token):
    pass


# FIXME: remove, just use Operator
class BinaryOperator(Token):
    pass


class Assignment(Token):
    pass
