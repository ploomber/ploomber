# ---
# jupyter:
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# Add description here

# + tags=["parameters"]
# extract_upstream=True in your pipeline.yaml file, if this task has
# dependencies, list them them here (e.g. upstream = ['some_task']), otherwise
# leave as None
upstream = None

# extract_product=False in your pipeline.yaml file, leave this as None, the
# value in the YAML spec  will be added here during task execution
product = None

# +
# your code here...
from pathlib import Path

Path(product).touch()