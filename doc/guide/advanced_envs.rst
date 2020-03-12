Another use case for this: add {{git}} as metadata for artifacts, but for those we want always to use the dirty dag, although that is a bit more advanced maybe just use git here and explain version and forcing the dirty tag in an advanced tutorial

this is more advanced if we want it to work right, do not include it here

` and `{{version}}` 

`{{version}}` works in a different way, it looks for an `__init__.py` are reads it to find a line with the pattern `__version__ = 'SOME_VERSION` and resolves to whatever value is equal to `__version__`, in this case, `SOME_VERSION`. Let's see an example:

Now using `{{version}}` (remember we moved to the commit with version 0.1):

# create a sample repository
! git init
# first version
! echo "__version__ = '0.1'" > __init__.py
! git add --all
! git commit -m 'set version to 0.1'
# set __version__ to 0.2 and commit
! echo "__version__ = '0.2'" > __init__.py
! git add --all
! git commit -m 'set version to 0.2'