# + tags=["parameters"]
upstream = NULL
product = list(nb='output/data.ipynb', data='output/data.csv')
# -

data(iris)
colnames(iris) = list('sepal_length', 'sepal_width','petal_length', 'petal_width', 'class')
write.csv(iris, product$data)