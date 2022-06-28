from pyspark.sql import functions as F


def sql_tuple_from_list(l: list) -> str:
    return str(tuple(l)).replace(",)", ")")


def bound(v):
    """Transforms the input value to the range between 0 and 1."""
    return 1 / (1 + 1 / F.log1p(v))


def apply(self, func, *args, **kwargs):
    """Apply a function on the DataFrame.

    To use this function, set "DataFrame.apply = apply"
    """
    return func(self, *args, **kwargs)


def pipe(data, *funcs):
    """Pipe a value through a sequence of functions"""
    for func in funcs:
        data = func(data)
    return data
