"""
Script for creating new releases

Maybe I should switch to this:
https://blog.mozilla.org/warner/2012/01/31/version-string-management-in-python-introducing-python-versioneer/

Deps: click, twine, keyring

Packaging projects guide:
    https://packaging.python.org/tutorials/packaging-projects/
Twine docs:
    https://github.com/pypa/twine
"""
import ast
import re
from subprocess import call as _call
from functools import reduce
import datetime

import click

TESTING = False
PACKAGE = 'src/ploomber'
PACKAGE_NAME = 'ploomber'


def replace_in_file(path_to_file, original, replacement):
    """Replace string in file
    """
    with open(path_to_file, 'r+') as f:
        content = f.read()
        updated = content.replace(original, replacement)
        f.seek(0)
        f.write(updated)
        f.truncate()


def read_file(path_to_file):
    with open(path_to_file, 'r') as f:
        content = f.read()

    return content


def call(*args, **kwargs):
    """Mocks call function for testing
    """
    if TESTING:
        print(args, kwargs)
        return 0
    else:
        return _call(*args, **kwargs)


class Versioner(object):
    """Utility functions to manage versions
    """
    @classmethod
    def current_version(cls):
        """Returns the current version in __init__.py
        """
        _version_re = re.compile(r'__version__\s+=\s+(.*)')

        with open('{package}/__init__.py'.format(package=PACKAGE), 'rb') as f:
            VERSION = str(
                ast.literal_eval(
                    _version_re.search(f.read().decode('utf-8')).group(1)))

        return VERSION

    @classmethod
    def release_version(cls):
        """
        Returns a release version number
        e.g. 2.4.4dev -> v.2.2.4
        """
        current = cls.current_version()

        if 'dev' not in current:
            raise ValueError('Current version is not a dev version')

        return current.replace('dev', '')

    @classmethod
    def bump_up_version(cls):
        """
        Gets gets a release version and returns a the next value value.
        e.g. 1.2.5 -> 1.2.6dev
        """
        # Get current version
        current = cls.current_version()

        if 'dev' in current:
            raise ValueError('Current version is dev version, new dev '
                             'versions can only be made from release versions')

        # Get Z from X.Y.Z and sum 1
        tokens = current.split('.')

        # if just released a major version, add a 0 so we bump up a subversion
        # e.g. from 0.8 -> 0.8.0, then new dev version becomes 0.8.1dev
        if len(tokens) == 2:
            tokens.append('0')

        new_subversion = int(tokens[-1]) + 1

        # Replace new_subversion in current version
        elements = current.split('.')
        elements[-1] = new_subversion
        new_version = reduce(lambda x, y: str(x) + '.' + str(y),
                             elements) + 'dev'

        return new_version

    @classmethod
    def commit_version(cls, new_version, tag=False):
        """
        Replaces version in  __init__ and optionally creates a tag in the git
        repository (also saves a commit)
        """
        current = cls.current_version()

        # replace new version in __init__.py
        replace_in_file('{package}/__init__.py'.format(package=PACKAGE),
                        current, new_version)

        # Create tag
        if tag:
            # Run git add and git status
            click.echo('Adding new changes to the repository...')
            call(['git', 'add', '--all'])
            call(['git', 'status'])

            # Commit repo with updated dev version
            click.echo('Creating new commit release version...')
            msg = 'Release {}'.format(new_version)
            call(['git', 'commit', '-m', msg])

            click.echo('Creating tag {}...'.format(new_version))
            message = '{} release {}'.format(PACKAGE_NAME, new_version)
            call(['git', 'tag', '-a', new_version, '-m', message])

            click.echo('Pushing tags...')
            call(['git', 'push', 'origin', new_version])

    @classmethod
    def update_changelog_release(cls, new_version):
        current = cls.current_version()

        # update CHANGELOG header
        header_current = '{ver}\n'.format(ver=current) + '-' * len(current)
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        header_new = '{ver} ({today})\n'.format(ver=new_version, today=today)
        header_new = header_new + '-' * len(header_new)
        replace_in_file('CHANGELOG.rst', header_current, header_new)

    @classmethod
    def add_changelog_dev_section(cls, dev_version):
        # add new CHANGELOG section
        start_current = 'Changelog\n========='
        start_new = (('Changelog\n=========\n\n{dev_version}\n'.format(
            dev_version=dev_version) + '-' * len(dev_version)) + '\n')
        replace_in_file('CHANGELOG.rst', start_current, start_new)


@click.group()
def cli():
    """Automates release a new version and uploading it to PyPI

    1. MANUAL: Merge whatever you want to publish to master
    2. MANUAL: Update your CHANGELOG.rst
    2. CREATE A NEW VERSION: python versioneer.py new
    3. PUBLISH: python versioneer.py release [TAG] --production

    To install from test:
    pip install --index-url https://test.pypi.org/simple/ --no-deps [PKG]
    """
    pass


@cli.command(
    help=
    'Sets a new version for the project: Updates __version__, changelog and commits'
)
def new():
    """
    Create a new version for the project: updates __init__.py, CHANGELOG,
    creates new commit for released version (creating a tag) and commits
    to a new dev version
    """
    current = Versioner.current_version()
    release = Versioner.release_version()

    release = click.prompt('Current version in app.yaml is {current}. Enter'
                           ' release version'.format(current=current,
                                                     release=release),
                           default=release,
                           type=str)

    Versioner.update_changelog_release(release)

    changelog = read_file('CHANGELOG.rst')

    click.confirm('\nCHANGELOG.rst:\n\n{}\n Continue?'.format(changelog),
                  'done',
                  abort=True)

    # Replace version number and create tag
    click.echo('Commiting release version: {}'.format(release))
    Versioner.commit_version(release, tag=True)

    # Create a new dev version and save it
    bumped_version = Versioner.bump_up_version()

    click.echo('Creating new section in CHANGELOG...')
    Versioner.add_changelog_dev_section(bumped_version)
    click.echo('Commiting dev version: {}'.format(bumped_version))
    Versioner.commit_version(bumped_version)

    # Run git add and git status
    click.echo('Adding new changes to the repository...')
    call(['git', 'add', '--all'])
    call(['git', 'status'])

    # Commit repo with updated dev version
    click.echo('Creating new commit with new dev version...')
    msg = 'Bumps up project to version {}'.format(bumped_version)
    call(['git', 'commit', '-m', msg])
    call(['git', 'push'])

    click.echo('Version {} was created, you are now in {}'.format(
        release, bumped_version))


@cli.command(help='Merges changes in dev with master')
def tomaster():
    """
    Merges dev with master and pushes
    """
    click.echo('Checking out master...')
    call(['git', 'checkout', 'master'])

    click.echo('Merging master with dev...')
    call(['git', 'merge', 'dev'])

    click.echo('Pushing changes...')
    call(['git', 'push'])


@cli.command(help='Publishes to PyPI')
@click.argument('tag')
@click.option('--production', is_flag=True)
def release(tag, production):
    """
    Merges dev with master and pushes
    """
    click.echo('Checking out tag {}'.format(tag))
    call(['git', 'checkout', tag])

    current = Versioner.current_version()

    click.confirm('Version in {} tag is {}. Do you want to continue?'.format(
        tag, current))

    # create distribution
    call(['rm', '-rf', 'dist/'])
    call(['python', 'setup.py', 'sdist', 'bdist_wheel'])

    click.echo('Publishing to PyPI...')

    if not production:
        call([
            'twine', 'upload', '--repository-url',
            'https://test.pypi.org/legacy/', 'dist/*'
        ])
    else:
        call(['twine', 'upload', 'dist/*'])


if __name__ == '__main__':
    cli()
