from dagster import asset, AssetExecutionContext
import polars as pl
from ..utils import save_dataframe 
from ..partitions import daily_partitions_def

@asset(
    group_name="analytics",
    description="Filters trades for high-conviction BUY signals",
    partitions_def=daily_partitions_def
)
def high_conviction_buy_signals(context: AssetExecutionContext, parsed_insider_trades: pl.DataFrame):
    
    if parsed_insider_trades.height == 0:
        return pl.DataFrame()

    # 1. Filter and AGGREGATE immediately
    # We group by the unique identifiers of a person's filing to collapse multiple lots
    buys_df = (
        parsed_insider_trades
        .filter(pl.col("transaction_code") == "P")
        .group_by(["filing_date", "ticker", "company_name", "owner_name", "owner_title", "accession_number"])
        .agg([
            pl.col("total_value").sum(),
            pl.col("shares").sum(),
            pl.col("price_per_share").mean(),
            pl.col("transaction_date").min(),
            pl.col("transaction_code").first()
        ])
    )
    
    if buys_df.height == 0:
        return pl.DataFrame()

    # 2. Clean Titles
    c_suite_regex = r"(?i)\b(CEO|CFO|CHIEF EXECUTIVE|CHIEF FINANCIAL|PRESIDENT)\b"
    
    # 3. Add Scoring Columns (Based on aggregated totals)
    scored = buys_df.with_columns([
        pl.when(pl.col("owner_title").str.contains(c_suite_regex))
        .then(3).otherwise(0).alias("score_role"),
        
        pl.when(pl.col("total_value") > 100_000)
        .then(2).otherwise(0).alias("score_value_mid"),

        pl.when(pl.col("total_value") > 500_000)
        .then(3).otherwise(0).alias("score_value_high")
    ])

    # 4. Correct Cluster Logic
    # We count UNIQUE owners per ticker/date, not total rows.
    clusters = (
        scored
        .group_by(["ticker", "filing_date"])
        .agg(pl.col("owner_name").n_unique().alias("unique_owner_count"))
    )
    
    scored = scored.join(clusters, on=["ticker", "filing_date"], how="left")
    
    scored = scored.with_columns(
        pl.when(pl.col("unique_owner_count") > 1)
        .then(2).otherwise(0).alias("score_cluster")
    )

    # 5. Total Score
    final_df = scored.with_columns(
        (pl.col("score_role") + pl.col("score_value_mid") + pl.col("score_value_high") + pl.col("score_cluster"))
        .alias("conviction_score")
    )

    # 6. Filter and Save
    high_conviction = final_df.filter(pl.col("conviction_score") >= 5)
    high_conviction = high_conviction.sort("conviction_score", descending=True)

    date_str = context.partition_key
    save_dataframe(high_conviction, f"processed/gold_signals_buy_{date_str}.parquet")
    
    return high_conviction


@asset(
    group_name="analytics",
    description="Filters trades for high-conviction SELL signals",
    partitions_def=daily_partitions_def
)
def high_conviction_sell_signals(context: AssetExecutionContext, parsed_insider_trades: pl.DataFrame):
    
    if parsed_insider_trades.height == 0:
        return pl.DataFrame()

    # Only Open Market Sales ('S')
    sells_df = parsed_insider_trades.filter(pl.col("transaction_code") == "S")
    
    if sells_df.height == 0:
        return pl.DataFrame()

    # 1. Clean Titles
    c_suite_regex = r"(?i)\b(CEO|CFO|CHIEF EXECUTIVE|CHIEF FINANCIAL|PRESIDENT)\b"
    
    # 2. Add Scoring Columns
    scored = sells_df.with_columns([
        pl.when(pl.col("owner_title").str.contains(c_suite_regex))
        .then(3).otherwise(0).alias("score_role"),
        
        pl.when(pl.col("total_value") > 100_000)
        .then(2).otherwise(0).alias("score_value_mid"),

        # Higher threshold for sales logic (>$1M)
        pl.when(pl.col("total_value") > 1_000_000)
        .then(3).otherwise(0).alias("score_value_high")
    ])

    # 3. Cluster Sells
    clusters = scored.group_by(["ticker", "filing_date"]).len().rename({"len": "cluster_count"})
    scored = scored.join(clusters, on=["ticker", "filing_date"], how="left")
    
    scored = scored.with_columns(
        pl.when(pl.col("cluster_count") > 1)
        .then(2).otherwise(0).alias("score_cluster")
    )

    # 4. Total Score
    final_df = scored.with_columns(
        (pl.col("score_role") + pl.col("score_value_mid") + pl.col("score_value_high") + pl.col("score_cluster"))
        .alias("conviction_score")
    )

    # 5. Filter
    high_conviction = final_df.filter(pl.col("conviction_score") >= 5)
    high_conviction = high_conviction.sort("conviction_score", descending=True)

    context.log.info(f"Filtered {len(sells_df)} sells down to {len(high_conviction)} High Conviction SELL signals.")

    date_str = context.partition_key
    save_dataframe(high_conviction, f"processed/gold_signals_sell_{date_str}.parquet")
    
    return high_conviction