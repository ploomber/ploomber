Cell tags
---------

Scripts (with the ``%%`` separator) and notebooks support cell tags. Tags
help identify cells for several purposes; the two most common ones are:

1. Notebook parameters: Ploomber uses this cell to know where to inject parameters from ``pipeline.yaml``. In most cases, you don't need to manually tag cells since Ploomber does it automatically.
2. Cell filtering: When generating reports, you may want to selectively hide specific cells from the output report.

For an example of adding cell tags, see the :ref:`parametrizing-notebooks` section.