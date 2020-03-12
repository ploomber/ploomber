import platform
from pathlib import Path
import getpass

import pytest
import yaml

from ploomber.env.env import Env
from ploomber.env.decorators import with_env, load_env
from ploomber.env import validate, expand
from ploomber.env.EnvDict import _get_name
from ploomber import repo


def test_load_env_with_name(tmp_directory, cleanup_env):
    Path('env.some_name.yaml').write_text(yaml.dump({'a': 1}))
    Env.start('env.some_name.yaml')


def test_load_env_default_name(tmp_directory, cleanup_env):
    Path('env.yaml').write_text(yaml.dump({'a': 1}))
    Env.start()


def test_load_env_hostname(tmp_directory, cleanup_env):
    name = 'env.{}.yaml'.format(platform.node())
    Path(name).write_text(yaml.dump({'a': 1}))
    Env.start()


def test_path_returns_Path_objects(cleanup_env):
    env = Env.start({'path': {'a': '/tmp/path/file.txt',
                              'b': '/another/path/file.csv'}})
    assert isinstance(env.path.a, Path)
    assert isinstance(env.path.b, Path)


def test_init_with_module_key(cleanup_env):
    env = Env.start({'module': 'sample_project'})
    assert env.module == 'sample_project'


def test_init_with_nonexistent_package(cleanup_env):
    with pytest.raises(ImportError):
        Env.start({'module': 'i_do_not_exist'})


def test_expand_version(cleanup_env):
    env = Env.start({'module': 'sample_project', 'version': '{{version}}'})
    assert env.version == '0.1dev'


def test_expand_git(monkeypatch, cleanup_env):
    def mockreturn(module_path):
        return {'git_location': 'some_version_string'}

    monkeypatch.setattr(repo, 'get_env_metadata', mockreturn)

    env = Env.start({'module': 'sample_project', 'git': '{{git}}'})
    assert env.git == 'some_version_string'


def test_can_create_env_from_dict(cleanup_env):
    e = Env.start({'a': 1})
    assert e.a == 1


def test_assigns_default_name():
    assert _get_name('path/to/env.yaml') == 'root'


def test_can_extract_name():
    assert _get_name('path/to/env.my_name.yaml') == 'my_name'


def test_raises_error_if_wrong_format():
    with pytest.raises(ValueError):
        _get_name('path/to/wrong.my_name.yaml')


def test_can_instantiate_env_if_located_in_sample_dir(tmp_sample_dir,
                                                      cleanup_env):
    Env.start()


def test_can_instantiate_env_if_located_in_sample_subdir(tmp_sample_subdir,
                                                         cleanup_env):
    Env.start()


def test_raise_file_not_found_if(cleanup_env):
    msg = ('Could not find file "env.non_existing.yaml" '
           'in the current working directory nor 6 levels up')
    with pytest.raises(FileNotFoundError, match=msg):
        Env.start('env.non_existing.yaml')


def test_with_env_decorator(cleanup_env):
    @with_env({'a': 1})
    def my_fn(env, b):
        return env.a, b

    assert (1, 2) == my_fn(2)


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
        def my_fn(a):
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


def test_with_env_without_args(cleanup_env):
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


def test_get_all_dict_keys():
    got = validate.get_keys_for_dict({'a': 1, 'b': {'c': {'d': 10}}})
    assert set(got) == {'a', 'b', 'c', 'd'}


def test_double_underscore_raises_error():
    msg = r"Keys cannot have double underscores, got: \['b\_\_c'\]"
    with pytest.raises(ValueError, match=msg):
        Env.start({'a': {'b__c': 1}})


def test_leading_underscore_in_top_key_raises_error():
    msg = r"Top-level keys cannot start with an underscore, got: \['\_a'\]"
    with pytest.raises(ValueError, match=msg):
        Env.start({'_a': 1})


def test_can_decorate_w_load_env_without_initialized_env():
    @load_env
    def fn(env):
        pass


def test_load_env_decorator(cleanup_env):
    Env.start({'a': 10})

    @load_env
    def fn(env):
        return env.a

    assert fn() == 10


def test_modify_all_values_in_dict():
    env = {'a': 1, 'b': 2, 'c': {'d': 1}}
    env_mod = expand.modify_values(env, lambda x, _: x + 1)

    assert env_mod == {'a': 2, 'b': 3, 'c': {'d': 2}}
    # original dict is not modified
    assert env == {'a': 1, 'b': 2, 'c': {'d': 1}}


# def test_iterate_nested_dict():
#     env = {'a': 1, 'b': 2, 'c': {'d': 1}}
#     list(expand.iterate_nested_dict(env))


def test_expand_tags(monkeypatch):

    def mockreturn():
        return 'username'

    monkeypatch.setattr(getpass, "getuser", mockreturn)

    env = {'a': '{{user}}', 'b': {'c': '{{user}} {{user}}'}}
    env_expanded = expand.modify_values(env, expand.EnvironmentExpander(env))

    assert env_expanded == {'a': 'username', 'b': {'c': 'username username'}}


def test_here_placeholder(tmp_directory, cleanup_env):
    Path('env.yaml').write_text(yaml.dump({'here': '{{here}}'}))
    env = Env.start()
    assert env.here == str(Path(tmp_directory, 'env.yaml').resolve())


# TODO: {{here}} allowed in _module
# TODO: test invalid YAML shows error message