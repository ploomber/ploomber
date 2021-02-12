def square(upstream):
    df = upstream['get']
    df['square'] = df.x**2
    return df


def cube(upstream):
    df = upstream['get']
    df['cube'] = df.x**3
    return df
