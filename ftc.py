import polars as pl
from polars import col as c
import polars.selectors as cs
from helpers import load_medispan

def _load_ftc_table() -> pl.LazyFrame:
    """
    load ftc drug table
    """
    return (
    pl.scan_csv('data/drug_brand_list.csv')
    .rename(lambda x: x.lower().replace('"','').replace(' ','_'))
    )

def _brand_regex_str() -> str:
    """
    extract brand names from ftc table
    """
    ftc = _load_ftc_table()
    brand_name = '|'.join(ftc.select(c.brand_equivalent.str.split(',').list.eval(pl.element().str.strip_chars().str.to_lowercase()).list.join('|').alias('brand_name')).collect(engine='streaming').to_series().to_list())
    return brand_name

def get_ftc_gpis_list() -> list[str]:
    """
    Get unique GPIs from the Medispan dataset that match FTC brand names.
    """
    return (
        load_medispan()
        .filter(c.product.str.contains(f'(?i){_brand_regex_str()}'))
        .filter(cs.matches('gpi.*2.*code').is_in(['99','96','93']).not_())
        .select(c.gpi)
        .unique()
        .collect(engine='streaming')
        .to_series()
        .to_list()
    )   

def ftc_drug_list() -> pl.LazyFrame:
    """
    Get unique drug names from the Medispan dataset that match FTC brand names.
    """
    return (
        load_medispan()
        .filter(c.gpi.is_in(get_ftc_gpis_list()))
        .select(c.gpi_10_generic_name.alias('generic_name'))
        .unique()
        .sort('generic_name')
    )
