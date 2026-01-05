# src/insider_flow/assets/debug.py
from dagster import asset
import polars as pl

@asset
def test_polars_setup():
    """
    A simple asset to verify Polars and Dagster are working together.
    """
    data = {
        "ticker": ["AAPL", "MSFT", "GOOGL"],
        "price": [150.5, 300.2, 2500.0],
        "is_tech": [True, True, True]
    }
    
    df = pl.DataFrame(data)
    
    # Perform a simple polars operation
    summary = df.select([
        pl.col("ticker"),
        (pl.col("price") * 1.1).alias("projected_price")
    ])
    
    # Dagster logs
    print(f"Created DataFrame with shape: {summary.shape}")
    return summary.to_dicts()