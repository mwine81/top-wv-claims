from data_process import load_data
import polars as pl
from polars import col as c
import polars.selectors as cs
from console import console
from expressions import is_esi_reporting, standardize_margin, is_quallent_gpis, is_quallent_ndcs, total_gpi, median_gpi_qty
from helpers import load_medispan


def query_high_low_quantile(lower_quantile: float, upper_quantile: float) -> pl.LazyFrame:
    return (
    load_data()
    .filter(c.affiliate.is_not_null())
    .filter(c.dos.dt.year().is_in([2023,2024]))
    .group_by(c.affiliate)
    .agg(c.margin.quantile(lower_quantile).alias('low') ,c.margin.quantile(upper_quantile).alias('high'))
    .with_columns(((c.high + c.low).round(2)).alias('net'))
    .with_columns(c.net.pct_change().round(2).alias('%_diff'))
    )

def calculate_standardized_margin_comparison_quallent():
    base = (
    load_data()
    # filter for esi reportings only to ensure price is based on esi network contracting
    .filter(is_esi_reporting())
    # filter for reporting within qualient gpis
    .filter(is_quallent_gpis())
    # flag qualient ndcs to partition data based on qualient vs non qualient
    .with_columns(is_quallent_ndcs())
    # standardize the margin over nadac so margin is scaled to a standard qty
    .with_columns(standardize_margin())
    # add total spend for the gpi for future analysis
    .with_columns(total_gpi())
    )

    def add_median_nadac(lf):
        """
        helper function to add median nadac into a lazyframe for the specified data
        """
        nadac = base.group_by(c.gpi).agg(c.unit_price.median().alias('median_nadac'))
        data = lf.join(nadac, on='gpi', how='left').with_columns((c.median_nadac * c.median_qty).round(2).alias('median_nadac'))
        return data

    return (
    base
    # group data
    .group_by(c.gpi,c.total_gpi, is_quallent_ndcs().alias('is_qualient'), median_gpi_qty())
    # aggregate to standardized median marging at gpi level based on if the dispensing was a qualiant NDC or not
    .agg(c.margin_standarized.median().round(2),)
    .collect(engine='streaming')
    # pivot the data so qualient (True) vs non-qualient (False) are on the same line
    .pivot(on = 'is_qualient', index=['gpi', 'total_gpi', 'median_qty'])
    # calculate the standarized difference between groupings (positive value indiates higher margin for qualient products)
    .with_columns((c.true - c.false).round(2).alias('difference'))
    # filter out observations where gpis do not have both a qualient and non-qualient observation
    .filter(c.difference.is_not_null())
    .lazy()
    # add in product name
    .join(load_medispan().select(c.gpi,c.generic_name).unique(), on='gpi', how='left')
    # add in median nadac
    .pipe(add_median_nadac)
    .drop('total_gpi')
    .sort(c.difference, descending=True)
    )

if __name__ == "__main__":
    console.log(query_high_low_quantile(0.01,0.99).collect(engine='streaming').sort('affiliate'))
    

