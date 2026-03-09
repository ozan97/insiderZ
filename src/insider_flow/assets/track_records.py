import yfinance as yf
import polars as pl
import time
from datetime import datetime, timedelta
from dagster import asset, AssetExecutionContext
from ..utils import save_dataframe, load_all_parquet
from ..partitions import daily_partitions_def

RETURN_HORIZONS = [30, 60, 90]


@asset(
    group_name="analytics",
    description=(
        "Retrospective enrichment: for trades that are 90+ days old, "
        "compute forward returns at 30/60/90-day horizons."
    ),
    partitions_def=daily_partitions_def,
)
def forward_return_analysis(context: AssetExecutionContext, scored_trades: pl.DataFrame):
    """
    Looks at today's scored_trades partition and — if the trades are old enough —
    fetches historical prices to compute what happened 30/60/90 days after each trade.

    Since each partition is a filing date, we check whether 90 days have passed
    since the transaction_date. If not, we still save what we can (30d, 60d)
    and mark the rest as null.
    """
    if scored_trades.height == 0:
        return pl.DataFrame()

    today = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    max_horizon = max(RETURN_HORIZONS)

    # Only compute returns for trades where at least 30 days have passed
    eligible = scored_trades.filter(
        (pl.lit(today) - pl.col("transaction_date")).dt.total_days() >= min(RETURN_HORIZONS)
    )

    if eligible.height == 0:
        context.log.info("No trades old enough for return analysis yet.")
        return pl.DataFrame()

    # Collect unique (ticker, transaction_date) pairs to minimize API calls
    price_requests = (
        eligible
        .select("ticker", "transaction_date")
        .unique()
        .to_dicts()
    )

    context.log.info(f"Fetching price history for {len(price_requests)} ticker/date pairs...")

    price_cache: dict[str, pl.DataFrame] = {}
    returns_rows = []

    for req in price_requests:
        ticker_sym = req["ticker"]
        trade_date = req["transaction_date"]

        # Fetch price history (cached per ticker)
        if ticker_sym not in price_cache:
            try:
                time.sleep(0.3)
                start = trade_date - timedelta(days=5)
                end = trade_date + timedelta(days=max_horizon + 10)
                hist = yf.Ticker(ticker_sym).history(start=start, end=end)
                if hist.empty:
                    price_cache[ticker_sym] = pl.DataFrame()
                else:
                    hist = hist.reset_index()
                    price_cache[ticker_sym] = pl.from_pandas(
                        hist[["Date", "Close"]].rename(columns={"Date": "date", "Close": "close"})
                    ).with_columns(pl.col("date").cast(pl.Date))
            except Exception:
                price_cache[ticker_sym] = pl.DataFrame()

        hist_df = price_cache[ticker_sym]
        if hist_df.height == 0:
            continue

        # Find the closing price on or just after trade_date
        base_row = hist_df.filter(pl.col("date") >= trade_date).sort("date").head(1)
        if base_row.height == 0:
            continue
        base_price = base_row["close"][0]

        row = {
            "ticker": ticker_sym,
            "transaction_date": trade_date,
            "base_price": float(base_price),
        }

        for horizon in RETURN_HORIZONS:
            target_date = trade_date + timedelta(days=horizon)
            if target_date > today:
                row[f"price_{horizon}d"] = None
                row[f"return_{horizon}d"] = None
                continue

            future_row = hist_df.filter(pl.col("date") >= target_date).sort("date").head(1)
            if future_row.height == 0:
                row[f"price_{horizon}d"] = None
                row[f"return_{horizon}d"] = None
            else:
                future_price = float(future_row["close"][0])
                row[f"price_{horizon}d"] = future_price
                row[f"return_{horizon}d"] = (future_price - base_price) / base_price

        returns_rows.append(row)

    if not returns_rows:
        context.log.info("Could not compute returns for any trades.")
        return pl.DataFrame()

    returns_df = pl.DataFrame(returns_rows)

    # Join back to the scored trades
    result = eligible.join(
        returns_df,
        on=["ticker", "transaction_date"],
        how="left",
    )

    date_str = context.partition_key
    save_dataframe(result, f"processed/forward_returns_{date_str}.parquet")
    context.log.info(f"Computed forward returns for {returns_df.height} ticker/date pairs.")

    return result


@asset(
    group_name="analytics",
    description=(
        "Per-insider track records: win rate, avg return by horizon, "
        "computed from all available forward_return_analysis partitions."
    ),
    partitions_def=daily_partitions_def,
)
def insider_track_records(context: AssetExecutionContext, forward_return_analysis: pl.DataFrame):
    """
    Aggregates forward returns per owner_cik to build a track record.
    """
    # Load all historical forward_return files to get cumulative data
    existing = load_all_parquet("processed/forward_returns_*.parquet")

    if forward_return_analysis.height > 0:
        if existing is not None and existing.height > 0:
            all_returns = pl.concat([existing, forward_return_analysis], how="diagonal_relaxed")
        else:
            all_returns = forward_return_analysis
    elif existing is not None and existing.height > 0:
        all_returns = existing
    else:
        return pl.DataFrame()

    # Deduplicate (same trade might appear in multiple partition runs)
    all_returns = all_returns.unique(
        subset=["owner_cik", "ticker", "transaction_date", "accession_number"]
    )

    # Only consider rows where we have at least the 30d return
    with_returns = all_returns.filter(pl.col("return_30d").is_not_null())

    if with_returns.height == 0:
        return pl.DataFrame()

    # Compute per-insider track records
    # For buys: a "win" is return > 0. For sells: a "win" is return < 0.
    records = (
        with_returns
        .with_columns([
            pl.when(pl.col("transaction_code") == "P")
              .then(pl.col("return_30d") > 0)
              .otherwise(pl.col("return_30d") < 0)
              .alias("win_30d"),
            pl.when(
                (pl.col("transaction_code") == "P") & pl.col("return_90d").is_not_null()
            ).then(pl.col("return_90d") > 0)
             .when(
                (pl.col("transaction_code") == "S") & pl.col("return_90d").is_not_null()
            ).then(pl.col("return_90d") < 0)
             .otherwise(None)
             .alias("win_90d"),
        ])
        .group_by("owner_cik")
        .agg([
            pl.col("owner_name").last(),
            pl.len().alias("total_trades_evaluated"),
            # 30-day metrics
            pl.col("win_30d").mean().alias("win_rate_30d"),
            pl.col("return_30d").mean().alias("avg_return_30d"),
            # 90-day metrics (only where available)
            pl.col("win_90d").drop_nulls().mean().alias("win_rate_90d"),
            pl.col("return_90d").drop_nulls().mean().alias("avg_return_90d"),
            # Best and worst trades
            pl.col("return_30d").max().alias("best_return_30d"),
            pl.col("return_30d").min().alias("worst_return_30d"),
        ])
    )

    records = records.sort("win_rate_30d", descending=True, nulls_last=True)

    date_str = context.partition_key
    save_dataframe(records, f"profiles/track_records_{date_str}.parquet")
    context.log.info(f"Built track records for {records.height} insiders.")

    return records
