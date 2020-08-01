import abc


class Lexer(abc.ABC):
    @abc.abstractclassmethod
    def __init__(self, text):
        pass

    @abc.abstractclassmethod
    def __iter__(self):
        pass
