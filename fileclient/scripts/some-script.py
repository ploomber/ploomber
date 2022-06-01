# ---
# jupyter:
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
from pathlib import Path

# %% tags=["parameters"]
upstream = None
product = None

# %%
Path(product['file']).write_text('some text')
