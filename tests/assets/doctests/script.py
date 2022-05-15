from pathlib import Path

# %% tags=["parameters"]
product = None

# %%
Path(product['data']).touch()
Path(product['another']).touch()
