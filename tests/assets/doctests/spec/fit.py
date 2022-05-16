# %% [markdown]
# Train a model

# %%
import pickle

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.ensemble import RandomForestClassifier
from sklearn_evaluation import plot

# %% tags=["parameters"]
upstream = ['join']
product = None

# %%
df = pd.read_parquet(str(upstream['join']))
X = df.drop('target', axis='columns')
y = df.target

# %%
X_train, X_test, y_train, y_test = train_test_split(X,
                                                    y,
                                                    test_size=0.33,
                                                    random_state=42)

# %%
clf = RandomForestClassifier()
clf.fit(X_train, y_train)

# %%
y_pred = clf.predict(X_test)

# %%
print(classification_report(y_test, y_pred))

# %%
plot.confusion_matrix(y_test, y_pred)

# %%
with open(product['model'], 'wb') as f:
    pickle.dump(clf, f)
