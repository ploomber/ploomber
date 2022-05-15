from pathlib import Path

# %% tags=["parameters"]

# %%
Path(product['data']).touch()
Path(product['another']).touch()