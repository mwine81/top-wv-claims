import polars as pl
from polars import col as c
import polars.selectors as cs
from helpers import load_medispan
from ftc import get_ftc_gpis_list

def total_nadac() -> pl.Expr:
    return (c.unit_price.cast(pl.Float64) * c.qty).round(2).alias('nadac')

def margin() -> pl.Expr:
    return (c.total - total_nadac()).round(2).alias('margin')

def is_quallent() -> pl.Expr:
    return c.labeler.str.contains('(?i)quallent')

def is_quallent_gpis() -> pl.Expr:
    return c.gpi.is_in(
    load_medispan()
    .filter(is_quallent())
    .select(c.gpi)
    .unique()
    .collect(engine='streaming')
    .to_series()
    .to_list()
    ).alias('is_quallent_gpi')

def is_quallent_ndcs() -> pl.Expr:
    return c.ndc.is_in(
    load_medispan()
    .filter(is_quallent())
    .select(c.ndc)
    .unique()
    .collect(engine='streaming')
    .to_series()
    .to_list()
    ).alias('is_quallent_ndc')

def margin_per_unit() -> pl.Expr:
    return ((c.margin / c.qty).round(4)).alias('margin_per_unit')

def median_gpi_qty() -> pl.Expr:
    return c.qty.median().over(c.gpi).alias('median_qty')

def standardize_margin() -> pl.Expr:
    return (margin_per_unit() * median_gpi_qty()).round(2).alias('margin_standarized')

def total_gpi() -> pl.Expr:
    return c.total.sum().over(c.gpi).cast(pl.Int64).alias('total_gpi')

def pct_of_total() -> pl.Expr:
    return ((c.total_gpi / c.total_gpi.sum()).round(4).alias('pct'))

def is_esi_reporting() -> pl.Expr:
    return c.source.str.contains('(?i)esi')

def is_ftc() -> pl.Expr:
    return c.gpi.is_in(get_ftc_gpis_list()).alias('is_ftc')

