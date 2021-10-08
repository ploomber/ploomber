Auto reloading code in Jupyter
------------------------------

When you import a module in Python (e.g., ``from module import my_function``),
the system caches the code and subsequent changes to ``my_funcion`` won't take
effect even if you run the import statement again until you restar the kernel,
which is inconvenient if you are iterating on some code stored in an external
file.

To overcome such limitation, you can insert the following at the top of your
notebook, before any ``import`` statements:


.. code-block:: python
    :class: text-editor

    # auto reload modules
    %load_ext autoreload
    %autoreload 2

Once executed, any updates to imported modules will take effect if you change
the source code. Note that this feature has some `limitations. <https://ipython.readthedocs.io/en/stable/config/extensions/autoreload.html#caveats>`_