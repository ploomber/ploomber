Should tasks generate products?
-------------------------------

Yes. Tasks must generate at least one product; this is typically a file but can be a table or view in a database.

If you find yourself trying to write a task that generates no outputs, consider the following options:

1. Merge the code that does not generate outputs with upstream tasks that generate outputs.
2. Use the ``on_finish`` hook to execute code after a task executes successfully (click :doc:`here <../cookbook/hooks>` to learn more).