from pathlib import Path
from config import CLAIMS_DIR, STATE_DATA_SET_STATES, MEDISPAN_FILE
import re
import polars as pl
from polars import col as c
import polars.selectors as cs



def state_files_to_load(states: list[str] | None = STATE_DATA_SET_STATES) -> list[Path]:
    """
    Returns a list of file paths from the CLAIMS_DIR directory that match the specified state names.
    Args:
        states (list[str] | None): A list of state names to filter files by. If None, defaults to STATE_DATA_SET_STATES.
    Returns:
        list[Path]: A list of Path objects representing files in CLAIMS_DIR that match the given states.
                    If states is None, returns all files in CLAIMS_DIR.
    Notes:
        - The function performs a case-insensitive search for state names within file names.
        - Only files (not directories) are included in the returned list.
    """
    
    if states is not None:
        state_pattern = '|'.join([s.lower() for s in states])
        return [s for s in CLAIMS_DIR.iterdir() if re.search(rf'(?i)({state_pattern})', s.name)]
    
    return [s for s in CLAIMS_DIR.iterdir() if s.is_file()]


def load_medispan() -> pl.LazyFrame:
    return pl.scan_parquet(MEDISPAN_FILE)

def add_medispan(lf, cols_to_add: list[str] = [], join_col = 'ndc') ->pl.LazyFrame:
    
    select_col = list(set([join_col] + cols_to_add))
    m = load_medispan().select(select_col)
    return lf.join(m, on=join_col, how='left')
