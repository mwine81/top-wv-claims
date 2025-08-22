#%%
from config import CLAIMS_DIR, NADAC_FILES, STATE_DATA_SET_STATES, YEAR_RANGE, MEDISPAN_FILE
import polars as pl
from polars import col as c
import polars.selectors as cs
import re
from expressions import margin, is_quallent
from helpers import state_files_to_load, add_medispan
from pathlib import Path
from rich.console import Console
from console import console


def load_nadac_table() -> pl.LazyFrame:
    """
    Loads NADAC data from parquet files, filters for matching effective and as_of dates,
    selects columns defined in NadacTable, and sorts by 'ndc' and 'effective_date'.
    Returns a Polars LazyFrame.
    """
    return (
        pl.scan_parquet(NADAC_FILES)
        .filter(c.effective_date == c.as_of)
        .with_columns([
            c.unit_price.cast(pl.Float64).round(4),  # Ensure unit_price is Float64
            c.effective_date.cast(pl.Date),
        ])
        .drop('as_of')
        .select(c.ndc,c.unit_price,c.effective_date)
        .sort(by=['effective_date'])
    )


def load_data(states: list[str] | None = STATE_DATA_SET_STATES, YEARS_RANGE: list[int] =YEAR_RANGE, nadac_tolerance: str = '52w') -> pl.LazyFrame:
    data = (
        pl.scan_parquet(state_files_to_load(states))
        .pipe(add_medispan, cols_to_add=['gpi'], join_col='ndc')
        .sort(c.dos)
        .join_asof(load_nadac_table(), left_on='dos', right_on='effective_date', by='ndc', strategy='backward', tolerance=nadac_tolerance)
        .with_columns(margin())
        # filters
        .filter(c.margin.is_not_null())
        .filter(c.dos.dt.year().is_in(YEARS_RANGE))
        .filter(c.ndc.is_not_null())
    )
    return data
