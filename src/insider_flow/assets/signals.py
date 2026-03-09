from dagster import asset, AssetExecutionContext
import polars as pl
from ..utils import save_dataframe 
from ..partitions import daily_partitions_def

C_SUITE_REGEX = r"(?i)\b(CEO|CFO|CHIEF EXECUTIVE|CHIEF FINANCIAL|PRESIDENT)\b"


@asset(
    group_name="analytics",
    description="Scores ALL trades (buys and sells) with a unified conviction model. No filtering — all trades are preserved.",
    partitions_def=daily_partitions_def
)
def scored_trades(context: AssetExecutionContext, parsed_insider_trades: pl.DataFrame):

    if parsed_insider_trades.height == 0:
        return pl.DataFrame()

    # 1. Aggregate: collapse multiple transaction lots per person per filing
    agg_df = (
        parsed_insider_trades
        .group_by([
            "filing_date", "ticker", "company_name",
            "owner_cik", "owner_name", "owner_title",
            "issuer_cik", "accession_number", "transaction_code",
            "is_director", "is_officer", "is_ten_percent_owner",
        ])
        .agg([
            pl.col("total_value").sum(),
            pl.col("shares").sum(),
            pl.col("price_per_share").mean(),
            pl.col("transaction_date").min(),
        ])
    )

    if agg_df.height == 0:
        return pl.DataFrame()

    # 2. Role scoring
    scored = agg_df.with_columns([
        # C-suite: CEO / CFO / President
        pl.when(pl.col("owner_title").str.contains(C_SUITE_REGEX))
          .then(3).otherwise(0).alias("score_role"),
        # Board member
        pl.when(pl.col("is_director"))
          .then(1).otherwise(0).alias("score_director"),
        # Activist / founder (10%+ owner)
        pl.when(pl.col("is_ten_percent_owner"))
          .then(2).otherwise(0).alias("score_10pct"),
    ])

    # 3. Value scoring (tiered — highest matching tier only)
    scored = scored.with_columns(
        pl.when(pl.col("total_value") > 1_000_000).then(3)
          .when(pl.col("total_value") > 500_000).then(2)
          .when(pl.col("total_value") > 100_000).then(1)
          .otherwise(0).alias("score_value")
    )

    # 4. Cluster scoring: ≥2 unique owners trading the same ticker on the same day
    clusters = (
        scored
        .group_by(["ticker", "filing_date", "transaction_code"])
        .agg(pl.col("owner_cik").n_unique().alias("cluster_size"))
    )
    scored = scored.join(clusters, on=["ticker", "filing_date", "transaction_code"], how="left")

    scored = scored.with_columns(
        pl.when(pl.col("cluster_size") >= 2)
          .then(2).otherwise(0).alias("score_cluster")
    )

    # 5. Total conviction score
    scored = scored.with_columns(
        (
            pl.col("score_role")
            + pl.col("score_director")
            + pl.col("score_10pct")
            + pl.col("score_value")
            + pl.col("score_cluster")
        ).alias("conviction_score")
    )

    scored = scored.sort("conviction_score", descending=True)

    date_str = context.partition_key
    save_dataframe(scored, f"processed/scored_trades_{date_str}.parquet")

    buy_count = scored.filter(pl.col("transaction_code") == "P").height
    sell_count = scored.filter(pl.col("transaction_code") == "S").height
    context.log.info(
        f"Scored {scored.height} trades ({buy_count} buys, {sell_count} sells) for {date_str}"
    )

    return scored