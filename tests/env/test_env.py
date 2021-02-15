import importlib
import platform
from pathlib import Path
import getpass
import inspect
import pickle

import pytest
import yaml

from ploomber.env.env import Env
from ploomber.env.decorators import with_env, load_env
from ploomber.env import validate
from ploomber.env.EnvDict import EnvDict
from ploomber.env.expand import (EnvironmentExpander, expand_raw_dictionary,
                                 cast_if_possible, iterate_nested_dict,
                                 expand_raw_dictionaries)
from ploomber import repo


def test_env_repr_and_str(cleanup_env):
    env = Env({'a': 1})
    assert repr(env) == "Env({'a': 1})"
    assert str(env) == "{'a': 1}"


def test_env_repr_and_str_when_loaded_from_file(tmp_directory, cleanup_env):
    path_env = Path('env.yaml')
    path_env.write_text(yaml.dump({'a': 1}))
    env = Env()
    assert repr(env) == "Env({'a': 1}) (from file: %s)" % str(
        path_env.resolve())
    assert str(env) == "{'a': 1}"


def test_includes_path_in_repr_if_init_from_file(cleanup_env, tmp_directory):
    Path('env.yaml').write_text('a: 1')
    env = Env('env.yaml')

    assert 'env.yaml' in repr(env)


def test_init_with_arbitrary_name(cleanup_env, tmp_directory):
    Path('some_environment.yaml').write_text('a: 1')
    assert Env('some_environment.yaml')


def test_init_with_null_value(cleanup_env, tmp_directory):
    Path('env.yaml').write_text('a: null')
    assert Env('env.yaml')


def test_init_with_absolute_path(cleanup_env, tmp_directory):
    Path('env.yaml').write_text('a: 1')
    assert Env(Path(tmp_directory, 'env.yaml'))


def test_includes_function_module_and_name_if_decorated(cleanup_env):
    @with_env({'a': 1})
    def my_fn(env):
        return env

    # NOTE: pytest sets the module name to the current filename
    assert 'test_env.my_fn' in repr(my_fn())


def test_cannot_start_env_if_one_exists_already(cleanup_env):
    Env({'a': 1})

    with pytest.raises(RuntimeError):
        Env({'a': 2})


def test_can_initialize_env_after_failed_attempt(cleanup_env):
    try:
        # underscores are not allowed, this will fail, but before raising
        # the exception, the instance (created in __new__) must be discarded
        Env({'_a': 1})
    except ValueError:
        pass

    # if we can initialize another object, it means the previous call was
    # corerctly discarded
    assert Env({'a': 1}).a == 1


def test_context_manager(cleanup_env):

    with Env({'a': 1}) as env:
        value = env.a

    # should be able to initialize another env now
    Env({'a': 2})

    assert value == 1


def test_load_env_with_name(tmp_directory, cleanup_env):
    Path('env.some_name.yaml').write_text(yaml.dump({'a': 1}))
    Env('env.some_name.yaml')


def test_load_env_default_name(tmp_directory, cleanup_env):
    Path('env.yaml').write_text(yaml.dump({'a': 1}))
    Env()


def test_load_env_hostname(tmp_directory, cleanup_env):
    name = 'env.{}.yaml'.format(platform.node())
    Path(name).write_text(yaml.dump({'a': 1}))
    Env()


def test_path_returns_Path_objects(cleanup_env):
    env = Env(
        {'path': {
            'a': '/tmp/path/file.txt',
            'b': '/another/path/file.csv'
        }})
    assert isinstance(env.path.a, Path)
    assert isinstance(env.path.b, Path)


def test_automatically_creates_path(cleanup_env, tmp_directory):
    Env({'path': {'home': 'some_path/'}})
    assert Path('some_path').exists() and Path('some_path').is_dir()


def test_path_expandsuser(cleanup_env):
    env = Env({'path': {'home': '~'}})
    assert env.path.home == Path('~').expanduser()


def test_init_with_module_key(cleanup_env):
    env = Env({'_module': 'test_pkg'})

    expected = Path(importlib.util.find_spec('test_pkg').origin).parent
    assert env._module == expected


def test_init_with_nonexistent_package(cleanup_env):
    with pytest.raises(ValueError) as exc_info:
        Env({'_module': 'i_do_not_exist'})

    expected = ('Could not resolve _module "i_do_not_exist", '
                'it is not a valid module nor a directory')
    assert exc_info.value.args[0] == expected


def test_init_with_file(tmp_directory, cleanup_env):
    Path('not_a_package').touch()

    with pytest.raises(ValueError) as exc_info:
        Env({'_module': 'not_a_package'})

    expected = ('Could not resolve _module "not_a_package", '
                'expected a module or a directory but got a file')
    assert exc_info.value.args[0] == expected


def test_module_is_here_placeholder_raises_error_if_init_w_dict(cleanup_env):
    with pytest.raises(ValueError) as exc_info:
        Env({'_module': '{{here}}'})

    expected = '_module cannot be {{here}} if not loaded from a file'
    assert exc_info.value.args[0] == expected


def test_module_with_here_placeholder(tmp_directory, cleanup_env):
    Path('env.yaml').write_text('_module: "{{here}}"')
    env = Env()
    assert env._module == Path(tmp_directory).resolve()


def test_expand_version(cleanup_env):
    env = Env({'_module': 'test_pkg', 'version': '{{version}}'})
    assert env.version == 'VERSION'


def test_expand_git(monkeypatch, cleanup_env):
    def mockreturn(module_path):
        return {'git_location': 'some_version_string'}

    monkeypatch.setattr(repo, 'get_git_info', mockreturn)

    env = Env({'_module': 'test_pkg', 'git': '{{git}}'})
    assert env.git == 'some_version_string'


def test_can_create_env_from_dict(cleanup_env):
    e = Env({'a': 1})
    assert e.a == 1


def test_can_instantiate_env_if_located_in_sample_dir(tmp_sample_dir,
                                                      cleanup_env):
    Env()


def test_can_instantiate_env_if_located_in_sample_subdir(
        tmp_sample_subdir, cleanup_env):
    Env()


def test_raise_file_not_found_if(cleanup_env):
    msg = ('Could not find file "env.non_existing.yaml" '
           'in the current working directory nor 6 levels up')
    with pytest.raises(FileNotFoundError, match=msg):
        Env('env.non_existing.yaml')


def test_with_env_decorator(cleanup_env):
    @with_env({'a': 1})
    def my_fn(env, b):
        return env.a, b

    assert (1, 2) == my_fn(2)


def test_with_env_modifies_signature(cleanup_env):
    @with_env({'a': 1})
    def my_fn(env, b):
        return env.a, b

    assert tuple(inspect.signature(my_fn).parameters) == ('b', )


# TODO: try even more nested
def test_with_env_casts_paths(cleanup_env):
    @with_env({'path': {'data': '/some/path'}})
    def my_fn(env):
        return env.path.data

    returned = my_fn(env__path__data='/another/path')

    assert returned == Path('/another/path')


def test_with_env_fails_if_no_env_arg(cleanup_env):
    with pytest.raises(RuntimeError):

        @with_env({'a': 1})
        def my_fn(not_env):
            pass


def test_with_env_fails_if_fn_takes_no_args(cleanup_env):
    with pytest.raises(RuntimeError):

        @with_env({'a': 1})
        def my_fn():
            pass


def test_replace_defaults(cleanup_env):
    @with_env({'a': {'b': 1}})
    def my_fn(env, c):
        return env.a.b + c

    assert my_fn(1, env__a__b=100) == 101


def test_with_env_without_args(tmp_directory, cleanup_env):
    Path('env.yaml').write_text('key: value')

    @with_env
    def my_fn(env):
        return 1

    assert my_fn() == 1


def test_env_dict_is_available_upon_decoration():
    @with_env({'a': 1})
    def make(env, param, optional=1):
        pass

    assert make._env_dict['a'] == 1


def test_replacing_defaults_also_expand(monkeypatch, cleanup_env):
    @with_env({'user': 'some_user'})
    def my_fn(env):
        return env.user

    def mockreturn():
        return 'expanded_username'

    monkeypatch.setattr(getpass, 'getuser', mockreturn)

    assert my_fn(env__user='{{user}}') == 'expanded_username'


def test_replacing_raises_error_if_key_does_not_exist():
    @with_env({'a': {'b': 1}})
    def my_fn(env, c):
        return env.a.b + c

    with pytest.raises(KeyError):
        my_fn(1, env__c=100)


def test_with_env_shows_name_and_module_if_invalid_env(cleanup_env):
    with pytest.raises(RuntimeError) as excinfo:

        @with_env({'_a': 1})
        def some_function(env):
            pass

    # NOTE: pytest sets the module name to the current filename
    assert 'test_env.some_function' in str(excinfo.getrepr())


def test_with_env_shows_function_names_if_env_exists(cleanup_env):
    @with_env({'a': 1})
    def first(env):
        pass

    @with_env({'a': 1})
    def second(env):
        first()

    with pytest.raises(RuntimeError) as excinfo:
        second()

    # NOTE: pytest sets the module name to the current filename
    assert 'test_env.first' in str(excinfo.getrepr())
    assert 'test_env.second' in str(excinfo.getrepr())


def test_get_all_dict_keys():
    got = validate.get_keys_for_dict({'a': 1, 'b': {'c': {'d': 10}}})
    assert set(got) == {'a', 'b', 'c', 'd'}


def test_double_underscore_raises_error():
    msg = r"Keys cannot have double underscores, got: \['b\_\_c'\]"
    with pytest.raises(ValueError, match=msg):
        Env({'a': {'b__c': 1}})


def test_leading_underscore_in_top_key_raises_error(cleanup_env):
    msg = ("Error validating env.\nTop-level keys cannot start with "
           "an underscore, except for {'_module'}. Got: ['_a']")
    with pytest.raises(ValueError) as exc_info:
        Env({'_a': 1})

    assert exc_info.value.args[0] == msg


def test_can_decorate_w_load_env_without_initialized_env():
    @load_env
    def fn(env):
        pass


def test_load_env_modifies_signature(cleanup_env):
    @load_env
    def fn(env):
        pass

    assert tuple(inspect.signature(fn).parameters) == ()


def test_load_env_decorator(cleanup_env):
    Env({'a': 10})

    @load_env
    def fn(env):
        return env.a

    assert fn() == 10


# def test_iterate_nested_dict():
#     env = {'a': 1, 'b': 2, 'c': {'d': 1}}
#     list(expand.iterate_nested_dict(env))


def test_expand_tags(monkeypatch, tmp_directory):
    def mockreturn():
        return 'username'

    monkeypatch.setattr(getpass, "getuser", mockreturn)

    # this is required for {{root}}
    Path('setup.py').touch()

    raw = {
        'a': '{{user}}',
        'b': {
            'c': '{{user}} {{user}}'
        },
        'cwd': '{{cwd}}',
        'root': '{{root}}',
    }
    expander = EnvironmentExpander(preprocessed={})
    env_expanded = expander.expand_raw_dictionary(raw)

    assert env_expanded == {
        'a': 'username',
        'b': {
            'c': 'username username'
        },
        'cwd': str(Path(tmp_directory).resolve()),
        'root': str(Path(tmp_directory).resolve()),
    }


def test_error_if_no_project_root(tmp_directory):
    raw = {'root': '{{root}}'}
    expander = EnvironmentExpander(preprocessed={})

    with pytest.raises(ValueError) as excinfo:
        expander.expand_raw_dictionary(raw)

    assert 'Failed to expand {{root}}' in str(excinfo.value)


def test_here_placeholder(tmp_directory, cleanup_env):
    Path('env.yaml').write_text(yaml.dump({'here': '{{here}}'}))
    env = Env()
    assert env.here == str(Path(tmp_directory).resolve())


def test_serialize_env_dict():
    # this tests an edge case due to EnvDict's implementation: to enable
    # accessing values in the underlying dictionary as attributes, we are
    # customizing __getattr__, however, when an object is unserialized,
    # Python tries to look for __getstate__ (which triggers calling
    # __getattr__), since it cannot find it, it will go to __getitem__
    # (given the current implementation of __getattr__). But __getitem__
    # uses self.preprocessed. At unserialization time, this attribute does
    # not exist yet!, which will cause another call to __getattr__. To avoid
    # this recursive loop, we have to prevent special methods to call
    # __getitem__ if they do not exist - EnvDict and Env objects are not
    # expected to be serialized but we have fix it anyway
    env = EnvDict({'a': 1})
    assert pickle.loads(pickle.dumps(env))


def test_replace_flatten_key_env_dict():
    env = EnvDict({'a': 1})
    new_env = env._replace_flatten_key(2, 'env__a')
    assert new_env.a == 2 and env is not new_env  # must return a copy


def test_replace_nested_flatten_key_env_dict():
    env = EnvDict({'a': {'b': 1}})
    new_env = env._replace_flatten_key(2, 'env__a__b')
    assert new_env.a.b == 2 and env is not new_env  # must return a copy


def test_replace_nested_flatten_keys_env_dict():
    env = EnvDict({'a': {'b': 1, 'c': 1}})
    new_env = env._replace_flatten_keys({'env__a__b': 2, 'env__a__c': 2})
    assert (new_env.a.b == 2 and new_env.a.c == 2
            and env is not new_env)  # must return a copy


def test_error_when_flatten_key_doesnt_exist():
    env = EnvDict({'a': 1})
    with pytest.raises(KeyError):
        env._replace_flatten_key(2, 'env__b')


@pytest.mark.parametrize(
    'data, keys',
    [
        [{
            'a': 1
        }, ('a', )],
        # added this to fix an edge case
        [{
            'a': {
                'b': 1
            }
        }, ('a', 'b')],
    ])
def test_env_dict_initialized_with_env_dict(data, keys):
    env = EnvDict(EnvDict(data))

    for key in keys:
        env = env[key]


def test_env_dict_initialized_with_replaced_env_dict():
    a = EnvDict({'a': {'b': 1}})
    a_mod = a._replace_flatten_keys({'env__a__b': 2})
    b = EnvDict(a_mod)

    # make sure the new object has the updated values
    assert b['a']['b'] == 2


def test_expand_raw_dictionary():
    mapping = {'key': 'value'}
    d = {'some_setting': '{{key}}'}
    assert expand_raw_dictionary(d, mapping) == {'some_setting': 'value'}


def test_expand_raw_dictionaries():
    mapping = {'key': 'value'}
    d = [{'some_setting': '{{key}}'}, {'another_setting': '{{key}}'}]
    assert expand_raw_dictionaries(d, mapping) == [
        {
            'some_setting': 'value',
        },
        {
            'another_setting': 'value'
        },
    ]


def test_expand_raw_dict_nested():
    mapping = {'key': 'value'}
    d = {
        'section': {
            'some_settting': '{{key}}'
        },
        'list': ['{{key}}', '{{key}}']
    }
    assert (expand_raw_dictionary(d, mapping) == {
        'section': {
            'some_settting': 'value'
        },
        'list': ['value', 'value']
    })


def test_expand_raw_dict_error_if_missing_key():
    mapping = {'another_key': 'value'}
    d = {'some_settting': '{{key}}'}

    with pytest.raises(KeyError):
        expand_raw_dictionary(d, mapping)


def test_iterate_nested_dict():
    numbers = [1, 2, 3]
    c = {'c': numbers}
    b = {'b': c}
    g = iterate_nested_dict({'a': b})

    parent, key, value, preffix = next(g)
    assert parent is numbers and key == 0 and value == 1, preffix == [
        'a', 'b', 'c', 0
    ]

    parent, key, value, preffix = next(g)
    assert parent is numbers and key == 1 and value == 2, preffix == [
        'a', 'b', 'c', 1
    ]

    parent, key, value, preffix = next(g)
    assert parent is numbers and key == 2 and value == 3, preffix == [
        'a', 'b', 'c', 2
    ]


@pytest.mark.parametrize('value, expected', [('True', True), ('false', False),
                                             ('100', 100), ('0.11', 0.11),
                                             ('string', 'string'),
                                             (True, True), (False, False),
                                             (10, 10), (10.1, 10.1)])
def test_cast_if_possible(value, expected):
    assert cast_if_possible(value) == expected


def test_replace_value_casts_if_possible():
    env = EnvDict({'a': False, 'b': 1, 'c': 1.1})
    env._replace_value('True', ['a'])
    env._replace_value('2', ['b'])
    env._replace_value('2.2', ['c'])
    assert env.a is True
    assert env.b == 2
    assert env.c == 2.2


def test_attribute_error_message():
    env = EnvDict({'a': 1})

    with pytest.raises(AttributeError) as excinfo_attr:
        env.aa

    with pytest.raises(KeyError) as excinfo_key:
        env['aa']

    assert str(excinfo_attr.value
               ) == "EnvDict({'a': 1}) object has no atttribute 'aa'"
    assert str(
        excinfo_key.value) == '"EnvDict({\'a\': 1}) object has no key \'aa\'"'


@pytest.mark.parametrize('content, type_',
                         [['a', 'str'], ['- a', 'list'], ['', 'NoneType']])
def test_error_when_loaded_obj_is_not_dict(content, type_, tmp_directory):
    path = Path(tmp_directory, 'file.yaml')
    path.write_text(content)

    with pytest.raises(ValueError) as excinfo:
        EnvDict('file.yaml')

    expected = ("Expected object loaded from 'file.yaml' to be "
                "a dict but got '{}' instead, "
                "verify the content").format(type_)
    assert str(excinfo.value) == expected


# TODO: test {{here}} allowed in _module
