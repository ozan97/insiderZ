from dagster import asset, AssetExecutionContext
import polars as pl
from ..utils import get_data_path, get_storage_options
from ..partitions import daily_partitions_def


@asset(
    group_name="analytics",
    description="Filters trades for high-conviction signals (C-Suite, High Value, Clusters).",
    partitions_def=daily_partitions_def 
)
def high_conviction_signals(context: AssetExecutionContext, parsed_insider_trades: pl.DataFrame):
    
    if parsed_insider_trades.height == 0:
        return pl.DataFrame()

    # 1. Clean Titles for "C-Suite" detection
    # We look for CEO, CFO, Chief Executive, etc.
    c_suite_regex = r"(?i)\b(CEO|CFO|CHIEF EXECUTIVE|CHIEF FINANCIAL|PRESIDENT)\b"
    
    # 2. Add Scoring Columns
    scored = parsed_insider_trades.with_columns([
        
        # Score 1: Is C-Suite? (3 Points)
        pl.when(pl.col("owner_title").str.contains(c_suite_regex))
        .then(3)
        .otherwise(0)
        .alias("score_role"),
        
        # Score 2: High Value > $100k (2 Points)
        pl.when(pl.col("total_value") > 100_000)
        .then(2)
        .otherwise(0)
        .alias("score_value_mid"),

        # Score 3: Whale Value > $500k (3 Points)
        pl.when(pl.col("total_value") > 500_000)
        .then(3)
        .otherwise(0)
        .alias("score_value_high")
    ])

    # 3. Calculate "Cluster Buys" (Bonus 2 Points)
    # Group by Ticker + Date. If count > 1, it means multiple people bought.
    # We join this back to the main dataframe.
    clusters = scored.group_by(["ticker", "filing_date"]).len().rename({"len": "daily_buyer_count"})
    
    scored = scored.join(clusters, on=["ticker", "filing_date"], how="left")
    
    scored = scored.with_columns(
        pl.when(pl.col("daily_buyer_count") > 1)
        .then(2)
        .otherwise(0)
        .alias("score_cluster")
    )

    # 4. Total Score
    final_df = scored.with_columns(
        (pl.col("score_role") + pl.col("score_value_mid") + pl.col("score_value_high") + pl.col("score_cluster"))
        .alias("conviction_score")
    )

    # 5. Filter for the "Good Stuff" (Score >= 5)
    # Examples of Score 5: 
    # - CEO (3) + Buys $150k (2) = 5
    # - Director (0) + Buys $500k (2+3) = 5
    # - Two Directors (0) + Buy $150k each (2) + Cluster (2) = 4 (Close, but maybe noise)
    high_conviction = final_df.filter(pl.col("conviction_score") >= 5)

    # Sort by Score descending
    high_conviction = high_conviction.sort("conviction_score", descending=True)

    context.log.info(f"Filtered {len(parsed_insider_trades)} trades down to {len(high_conviction)} High Conviction signals.")

    # Save to Gold Layer
    save_path = get_data_path("processed/gold_signals.parquet")
    high_conviction.write_parquet(save_path, storage_options=get_storage_options())
    
    return high_conviction