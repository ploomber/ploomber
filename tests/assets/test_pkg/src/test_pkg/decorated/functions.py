from test_pkg.decorated.decorators import my_decorator


def function():
    pass


@my_decorator
def decorated_function():
    pass


@my_decorator
@my_decorator
def double_decorated_function():
    pass
