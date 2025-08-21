import polars as pl
from polars import col as c
import polars.selectors as cs

def total_nadac() -> pl.Expr:
    return (c.unit_price.cast(pl.Float64) * c.qty).round(2).alias('nadac')

def margin() -> pl.Expr:
    return (c.total - total_nadac()).round(2).alias('margin')
