import itertools
from ploomber.static_analysis.parser.tokens import Assignment, Name, Operator, Null


class Parser:
    """
    The current implementation is very simple and does not even
    build an AST. It just parses one statement at a time. Fine for our
    purposes with some limitations.

    Parameters
    ----------
    tokens : list
        Tokens, obtained from a Lexer
    """

    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    @property
    def current_token(self):
        return self.tokens[self.pos]

    @property
    def next_token(self):
        return self.tokens[self.pos + 1]

    def get_tail(self, exclude_next=True):
        return self.tokens[self.pos + 1 + int(exclude_next) :]

    def parse(self):
        """The current implementation can only parse one expression at a time"""
        if not isinstance(self.current_token, Name):
            raise SyntaxError("First token must be a valid name")

        if not isinstance(self.next_token, Assignment):
            raise SyntaxError("Second token must be an assignment")

        return Expression(
            self.current_token, self.next_token, build_node(self.get_tail())
        )


def get_slicer(elements, size):
    elements = iter(elements)

    while True:
        slice_ = list(itertools.islice(elements, size))

        if not slice_:
            return

        if len(slice_) == size - 1:
            slice_.append(None)

        yield slice_


class Node:
    pass


class Expression(Node):
    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right

    def __repr__(self):
        return "{} {} {}".format(self.left, self.op, self.right)


class ListNode(Node):
    def __init__(self, tokens):
        self.elements = [value for value, comma in get_slicer(tokens, size=2)]

    def to_python(self):
        return [e.value for e in self.elements]


class DictionaryNode(Node):
    def __init__(self, tokens):
        self.elements = [
            (key, value) for key, equal, value, comma in get_slicer(tokens, size=4)
        ]

    def to_python(self):
        return {key.value: value.value for key, value in self.elements}


def build_node(tokens):
    if tokens[0] == Operator("list"):
        elements = tokens[2:-1]

        if isinstance(elements[0], Name):
            return DictionaryNode(elements)
        else:
            return ListNode(elements)

    elif tokens[0] == Null():
        return tokens[0]
    else:
        raise SyntaxError(
            "Variables should be assigned to values of type "
            'list (e.g. list("a", "b"))  or NULL'
        )
