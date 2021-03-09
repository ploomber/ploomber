"""
Utlity functions to run Ploomber tests. This is not a public module
"""


def assert_no_extra_attributes_in_class(abstract_class,
                                        concrete_class,
                                        allowed=None):
    """
    Ploomber makes heavy use of abstract classes to provide a uniform API for
    tasks, products, metadata, etc. When defining abstract classes, the
    interpreter refuses to instantiate an object where the concrete class
    misses implementation for abstract methods. However, it does not complain
    if the concrete class implements *extra methods*

    This has been problematic to remove old code. As we simplify the API,
    sometimes concrete classes become outdated. For example, before creating
    Metadata, all metadata logic was embedded in the Product objects, when
    we made the change, we removed some abstract methods from the Product class
    but it took a long time to realize that we sould've removed these mehods
    from the MetaProduct class as well. We use this function to alert us
    when there are things we can remove.

    The other case also happens: we add functionality to concrete classes but
    we do not do it in the abstract class, when this happens we have to decide
    whether to add them to the abstract class (recommended) or make an
    exception in such case, those new methods should be named with double
    leading underscore to be ignored by this check and to prevent polluting the
    public interface. Single leading underscore methods are checked to allow
    abstract classes define its own private API, which is also important
    for consistency, even if the end user does not use these methods.

    Convention:
        - no leading underscode: public API
        - one leading underscore: private Ploomber API. Not meant to be used by
            end-users but can be user by developers
        - two leading underscores: private class API. Not meant to be used
            outside the implementation of the class itself. Abstract classes
            should not define these, these are intended to carry logic
            specific to concrete classes

    NOTE: maybe a better alternative to allowed is to create an abstract
    class that adds new abstract methods
    """
    allowed = allowed or set()

    # allow "private" methods
    preffixes = [
        '_{}__'.format(class_.__name__) for class_ in concrete_class.__bases__
    ] + ['__', '_', '_{}__'.format(concrete_class.__name__)]

    extra_attrs = {
        attr
        for attr in set(dir(concrete_class)) - set(dir(abstract_class))
        if not any(attr.startswith(p) for p in preffixes)
    } - allowed

    if extra_attrs:
        raise ValueError('The following methods/attributes in {} '
                         'are not part of the {} interface: {}'.format(
                             concrete_class.__name__, abstract_class.__name__,
                             extra_attrs))
