# + tags=["parameters"]
upstream = list('clean')
product = list(nb='output/plot.ipynb')
# -

# +
df = read.csv(upstream$clean$data)
head(df)

# +
hist(df$sepal_length)