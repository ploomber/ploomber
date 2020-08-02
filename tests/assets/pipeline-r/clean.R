# + tags=["parameters"]
upstream = list('raw')
product = list(nb='output/clean.ipynb', data='output/clean.csv')
# -


df = read.csv(upstream$raw$data)

write.csv(df, product$data)