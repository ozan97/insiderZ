import polars as pl
import io
import os
from dagster import asset, AssetExecutionContext
from datetime import datetime, timedelta
from ..resources import SECClient

# Define the column names for the SEC master index
SCHEMA_COLS = ["cik", "company_name", "form_type", "date_filed", "filename"]

@asset(
    group_name="ingestion",
    description="Downloads the Daily Master Index from SEC and filters for Form 4"
)
def daily_form4_list(context: AssetExecutionContext, sec_client: SECClient):
    """
    1. Calculates 'Yesterday's' date.
    2. Downloads the master.idx file.
    3. Parses it using Polars (forcing types).
    4. Filters for '4' (Insider Trade) and '4/A' (Amendment).
    """
    
    # 1. Determine Date
    target_date = datetime.now() - timedelta(days=1)
    
    # Skip weekends
    if target_date.weekday() > 4: 
        target_date -= timedelta(days=target_date.weekday() - 4)

    context.log.info(f"Targeting date: {target_date.date()}")

    # 2. Get the Index URL and Content
    url = sec_client.get_daily_index_url(target_date)
    try:
        raw_text = sec_client.get_content(url)
    except Exception as e:
        context.log.error(f"Failed to fetch index: {e}")
        return pl.DataFrame(schema=SCHEMA_COLS)

    # 3. Clean and Parse Data
    lines = raw_text.splitlines()
    start_idx = 0
    for idx, line in enumerate(lines):
        if "CIK|Company Name" in line:
            start_idx = idx + 2 # Skip header and separator
            break
            
    clean_csv_data = "\n".join(lines[start_idx:])
    
    # Load into Polars with STRICT typing
    df = pl.read_csv(
        io.StringIO(clean_csv_data), 
        separator="|", 
        has_header=False, 
        new_columns=SCHEMA_COLS,
        truncate_ragged_lines=True,
        # Critical: Force CIK and Date to be strings initially to avoid inference errors
        schema_overrides={"cik": pl.String, "date_filed": pl.String}
    )

    # Convert date string to actual Date object
    df = df.with_columns(
        pl.col("date_filed").str.strptime(pl.Date, "%Y%m%d")
    )

    # 4. Filter for Form 4
    form4_df = df.filter(
        pl.col("form_type").is_in(["4", "4/A"])
    )

    # 5. Enrich with full URL
    base_url = "https://www.sec.gov/Archives/"
    form4_df = form4_df.with_columns(
        (base_url + pl.col("filename")).alias("file_url")
    )

    context.log.info(f"Found {len(form4_df)} Insider Trading filings for {target_date.date()}")
    
    # 6. Save to Parquet
    date_str = target_date.strftime("%Y-%m-%d")
    file_name = f"form4_index_{date_str}.parquet"
    save_path = os.path.join("data", "raw", file_name)
    
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    form4_df.write_parquet(save_path)
    
    context.log.info(f"Saved {len(form4_df)} rows to {save_path}")
    
    return form4_df