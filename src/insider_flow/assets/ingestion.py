import polars as pl
import io
from dagster import asset, AssetExecutionContext
from datetime import datetime
from ..resources import SECClient
from ..utils import SEC_BASE_URL, save_dataframe
from ..partitions import daily_partitions_def

SCHEMA_COLS = ["cik", "company_name", "form_type", "date_filed", "filename"]

@asset(
    group_name="ingestion",
    description="Downloads the Daily Master Index from SEC and filters for Form 4",
    partitions_def=daily_partitions_def # <--- 1. Bind to partitions
)
def daily_form4_list(context: AssetExecutionContext, sec_client: SECClient):
    """
    Downloads the master index for the specific partition date.
    """
    
    # 2. Get the date from Dagster (Deterministic)
    # context.partition_key is a string like "2026-01-05"
    target_date_str = context.partition_key
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()

    # 3. Handle Weekends (Optimization)
    if target_date.weekday() > 4: 
        context.log.info(f"Partition {target_date} is a weekend. No SEC index available. Skipping.")
        return pl.DataFrame(schema=SCHEMA_COLS)

    # 4. Fetch
    url = sec_client.get_daily_index_url(target_date)
    try:
        raw_text = sec_client.get_content(url)
    except Exception as e:
        # If SEC returns 404 (e.g., holiday), log it and return empty
        context.log.warning(f"Failed to fetch index for {target_date}: {e}")
        return pl.DataFrame(schema=SCHEMA_COLS)

    # 5. Parse
    lines = raw_text.splitlines()
    start_idx = 0
    for idx, line in enumerate(lines):
        if "CIK|Company Name" in line:
            start_idx = idx + 2
            break
            
    # Handle case where file might be malformed or empty
    if start_idx >= len(lines):
        context.log.warning("Could not find data start in index file.")
        return pl.DataFrame(schema=SCHEMA_COLS)

    clean_csv_data = "\n".join(lines[start_idx:])
    
    # Read CSV
    df = pl.read_csv(
        io.StringIO(clean_csv_data), 
        separator="|", 
        has_header=False, 
        new_columns=SCHEMA_COLS,
        truncate_ragged_lines=True,
        schema_overrides={"cik": pl.String, "date_filed": pl.String}
    )

    # Convert types
    df = df.with_columns(
        pl.col("date_filed").str.strptime(pl.Date, "%Y%m%d")
    )

    # Filter for Form 4
    form4_df = df.filter(
        pl.col("form_type").is_in(["4", "4/A"])
    )

    # Enrich URL
    form4_df = form4_df.with_columns(
        (SEC_BASE_URL + pl.col("filename")).alias("file_url")
    )
    
    context.log.info(f"Found {len(form4_df)} Insider Trading filings for {target_date}")

    # 6. Save (using updated utils)
    save_path = save_dataframe(form4_df, f"raw/form4_index_{target_date_str}.parquet")

    context.log.info(f"Saved {len(form4_df)} rows to {save_path}")
    
    return form4_df