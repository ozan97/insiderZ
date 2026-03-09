import polars as pl
from dagster import asset, AssetExecutionContext
from ..utils import save_dataframe, load_all_parquet
from ..partitions import daily_partitions_def


@asset(
    group_name="analytics",
    description=(
        "Cumulative insider profiles built from all scored_trades partitions. "
        "One row per owner_cik with name, roles, tickers, and trade stats."
    ),
    partitions_def=daily_partitions_def,
)
def insider_profiles(context: AssetExecutionContext, scored_trades: pl.DataFrame):
    """
    Appends today's scored_trades into the cumulative profiles table.
    We load existing profiles, union with today's raw data, and re-aggregate.
    """
    if scored_trades.height == 0:
        return pl.DataFrame()

    # Load any previously saved profiles parquets to merge with
    existing = load_all_parquet("profiles/insider_profiles_*.parquet")

    # Build a mini-summary from today's trades
    today = _build_profile_fragment(scored_trades)

    # Union with existing profiles and re-aggregate
    if existing is not None and existing.height > 0:
        combined = pl.concat([existing, today], how="diagonal_relaxed")
    else:
        combined = today

    profiles = (
        combined
        .group_by("owner_cik")
        .agg([
            pl.col("owner_name").last(),
            pl.col("tickers").flatten().unique(),
            pl.col("companies").flatten().unique(),
            pl.col("roles").flatten().unique(),
            pl.col("total_buys").sum(),
            pl.col("total_sells").sum(),
            pl.col("total_buy_value").sum(),
            pl.col("total_sell_value").sum(),
            pl.col("first_trade_date").min(),
            pl.col("last_trade_date").max(),
            pl.col("is_officer").max(),
            pl.col("is_director").max(),
            pl.col("is_ten_percent_owner").max(),
        ])
    )

    date_str = context.partition_key
    save_dataframe(profiles, f"profiles/insider_profiles_{date_str}.parquet")

    context.log.info(f"Built profiles for {profiles.height} unique insiders (cumulative).")
    return profiles


def _build_profile_fragment(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate a batch of scored_trades into the profile schema."""
    return (
        df
        .group_by("owner_cik")
        .agg([
            pl.col("owner_name").last(),
            pl.col("ticker").unique().alias("tickers"),
            pl.col("company_name").unique().alias("companies"),
            pl.col("owner_title").drop_nulls().unique().alias("roles"),
            (pl.col("transaction_code") == "P").sum().alias("total_buys"),
            (pl.col("transaction_code") == "S").sum().alias("total_sells"),
            pl.col("total_value")
              .filter(pl.col("transaction_code") == "P")
              .sum()
              .alias("total_buy_value"),
            pl.col("total_value")
              .filter(pl.col("transaction_code") == "S")
              .sum()
              .alias("total_sell_value"),
            pl.col("transaction_date").min().alias("first_trade_date"),
            pl.col("transaction_date").max().alias("last_trade_date"),
            pl.col("is_officer").max(),
            pl.col("is_director").max(),
            pl.col("is_ten_percent_owner").max(),
        ])
    )