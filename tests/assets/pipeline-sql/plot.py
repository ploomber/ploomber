import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine
from config import get_uri

# + tags=["parameters"]
upstream = None
product = None

# +
engine = create_engine(get_uri())
df = pd.read_sql('SELECT * FROM transformed', engine)
sns.distplot(df.value_per_customer)
