class AST:
    pass


class Variable(AST):
    def __init__(self, name):
        self.name = name


class BinaryOperator(AST):
    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right