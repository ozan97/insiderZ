from dagster import asset, AssetExecutionContext
import polars as pl
from .utils import get_data_path, get_storage_options
from ..partitions import daily_partitions_def

# ==========================================
# ASSET 1: BUY SIGNALS (Long Strategy)
# ==========================================
@asset(
    group_name="analytics",
    description="Filters trades for high-conviction BUY signals",
    partitions_def=daily_partitions_def
)
def high_conviction_signals(context: AssetExecutionContext, parsed_insider_trades: pl.DataFrame):
    
    if parsed_insider_trades.height == 0:
        return pl.DataFrame()

    # We only want Open Market Purchases ('P')
    buys_df = parsed_insider_trades.filter(pl.col("transaction_code") == "P")
    
    if buys_df.height == 0:
        return pl.DataFrame()

    # 1. Clean Titles for "C-Suite" detection
    c_suite_regex = r"(?i)\b(CEO|CFO|CHIEF EXECUTIVE|CHIEF FINANCIAL|PRESIDENT)\b"
    
    # 2. Add Scoring Columns
    scored = buys_df.with_columns([
        
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
    high_conviction = final_df.filter(pl.col("conviction_score") >= 5)
    high_conviction = high_conviction.sort("conviction_score", descending=True)

    context.log.info(f"Filtered {len(buys_df)} buys down to {len(high_conviction)} High Conviction BUY signals.")

    # Save to Gold Layer (Partitioned by Date)
    date_str = context.partition_key
    save_path = get_data_path(f"processed/gold_signals_{date_str}.parquet")
    high_conviction.write_parquet(save_path, storage_options=get_storage_options())
    
    return high_conviction


# ==========================================
# ASSET 2: SELL SIGNALS (Short Strategy)
# ==========================================
@asset(
    group_name="analytics",
    description="Filters trades for high-conviction SELL signals",
    partitions_def=daily_partitions_def
)
def high_conviction_sell_signals(context: AssetExecutionContext, parsed_insider_trades: pl.DataFrame):
    
    if parsed_insider_trades.height == 0:
        return pl.DataFrame()

    # We only want Open Market Sales ('S')
    sells_df = parsed_insider_trades.filter(pl.col("transaction_code") == "S")
    
    if sells_df.height == 0:
        return pl.DataFrame()

    # 1. Clean Titles
    c_suite_regex = r"(?i)\b(CEO|CFO|CHIEF EXECUTIVE|CHIEF FINANCIAL|PRESIDENT)\b"
    
    # 2. Add Scoring Columns
    scored = sells_df.with_columns([
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

        # Score 3: Whale Dump > $1M (3 Points) - Higher threshold for sales often makes sense
        pl.when(pl.col("total_value") > 1_000_000)
        .then(3)
        .otherwise(0)
        .alias("score_value_high")
    ])

    # 3. Calculate "Cluster Sells"
    clusters = scored.group_by(["ticker", "filing_date"]).len().rename({"len": "daily_seller_count"})
    
    scored = scored.join(clusters, on=["ticker", "filing_date"], how="left")
    
    scored = scored.with_columns(
        pl.when(pl.col("daily_seller_count") > 1)
        .then(2)
        .otherwise(0)
        .alias("score_cluster")
    )

    # 4. Total Score
    final_df = scored.with_columns(
        (pl.col("score_role") + pl.col("score_value_mid") + pl.col("score_value_high") + pl.col("score_cluster"))
        .alias("conviction_score")
    )

    # 5. Filter for Score >= 5
    high_conviction = final_df.filter(pl.col("conviction_score") >= 5)
    high_conviction = high_conviction.sort("conviction_score", descending=True)

    context.log.info(f"Filtered {len(sells_df)} sells down to {len(high_conviction)} High Conviction SELL signals.")

    # Save to Gold Layer (Partitioned by Date, distinct filename)
    date_str = context.partition_key
    save_path = get_data_path(f"processed/gold_signals_sell_{date_str}.parquet")
    high_conviction.write_parquet(save_path, storage_options=get_storage_options())
    
    return high_conviction