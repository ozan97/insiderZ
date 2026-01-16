import yfinance as yf
import polars as pl
import time
from dagster import asset, AssetExecutionContext
from ..utils import save_dataframe
from ..partitions import daily_partitions_def

@asset(
    group_name="analytics",
    description="Enriches buys with 'Ahab Score' (Insider Conviction + Deep Value Fundamentals).",
    partitions_def=daily_partitions_def
)
def enriched_signals(context: AssetExecutionContext, high_conviction_buy_signals: pl.DataFrame):
    
    if high_conviction_buy_signals.height == 0:
        return pl.DataFrame()

    tickers = high_conviction_buy_signals["ticker"].unique().to_list()
    context.log.info(f"Fetching fundamentals for {len(tickers)} tickers...")
    
    fundamentals_data = []

    for ticker_symbol in tickers:
        try:
            time.sleep(0.5) # Be polite
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            
            current_price = info.get("currentPrice", 0)
            high_52 = info.get("fiftyTwoWeekHigh", 0)
            
            dip_pct = 0.0
            if high_52 > 0 and current_price > 0:
                dip_pct = (high_52 - current_price) / high_52

            data = {
                "ticker": ticker_symbol,
                "pe_ratio": info.get("trailingPE", 999), 
                "pb_ratio": info.get("priceToBook", 999),
                "debt_to_equity": info.get("debtToEquity", 999),
                "profit_margins": info.get("profitMargins", -1.0),
                "dip_from_52w_high": dip_pct
            }
            fundamentals_data.append(data)
            
        except Exception as e:
            # context.log.warning(f"Failed {ticker_symbol}: {e}")
            continue

    if not fundamentals_data:
        return high_conviction_buy_signals

    fund_df = pl.DataFrame(fundamentals_data, schema_overrides={
        "ticker": pl.String,
        "pe_ratio": pl.Float64,
        "pb_ratio": pl.Float64,
        "debt_to_equity": pl.Float64,
        "profit_margins": pl.Float64,
        "dip_from_52w_high": pl.Float64
    })
    enriched = high_conviction_buy_signals.join(fund_df, on="ticker", how="left")
    enriched = enriched.with_columns([
        pl.col("pe_ratio").cast(pl.Float64, strict=False).fill_null(999),
        pl.col("debt_to_equity").cast(pl.Float64, strict=False).fill_null(999),
        pl.col("dip_from_52w_high").cast(pl.Float64, strict=False).fill_null(0),
        pl.col("profit_margins").cast(pl.Float64, strict=False).fill_null(-1),
    ])
    enriched = enriched.with_columns([
        # Insider Score
        pl.when(pl.col("total_value") > 1_000_000).then(5)
          .when(pl.col("total_value") > 500_000).then(3)
          .when(pl.col("total_value") > 100_000).then(1)
          .otherwise(0).alias("score_val_elite"),
          
        pl.when(pl.col("score_cluster") > 0).then(3).otherwise(0).alias("score_cluster_elite"),
        
        # Fundamental Score
        pl.when(pl.col("pe_ratio") < 15).then(2).otherwise(0).alias("f_score_pe"),
        pl.when(pl.col("debt_to_equity") < 100).then(1).otherwise(0).alias("f_score_debt"),
        pl.when(pl.col("dip_from_52w_high") > 0.30).then(2).otherwise(0).alias("f_score_dip"),
        pl.when(pl.col("profit_margins") > 0).then(1).otherwise(0).alias("f_score_profit")
    ])
    enriched = enriched.with_columns(
        (
            pl.col("score_role") +         
            pl.col("score_val_elite") +    
            pl.col("score_cluster_elite") +
            pl.col("f_score_pe") +         
            pl.col("f_score_debt") +       
            pl.col("f_score_dip") +        
            pl.col("f_score_profit")       
        ).alias("ahab_score")
    )
    enriched = enriched.sort("ahab_score", descending=True)

    date_str = context.partition_key
    save_dataframe(enriched, f"processed/enriched_signals_{date_str}.parquet")
    
    elite_count = enriched.filter(pl.col("ahab_score") >= 10).height
    context.log.info(f"Generated {len(enriched)} signals. {elite_count} are ELITE (Score 10+).")
    
    return enriched