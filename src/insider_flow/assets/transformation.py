import os
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import polars as pl
from typing import List
from ..parsing import parse_filing
from ..utils import get_data_path, get_storage_options # Import utils

@asset(
    group_name="transformation",
    description="Parses raw text filings into structured insider trades."
)
def parsed_insider_trades(context: AssetExecutionContext, raw_form4_filings: List[str]):
    
    if not raw_form4_filings:
        return Output(pl.DataFrame(), metadata={"count": 0})

    sample_path = raw_form4_filings[0]
    # Handle cloud paths vs local paths for date extraction
    # gs://bucket/raw/filings/2026-01-02/file.txt
    parts = sample_path.replace("\\", "/").split("/")
    filing_date_str = parts[-2] 
    
    all_trades = []
    
    for i, file_path in enumerate(raw_form4_filings):
        # Now parse_filing uses fsspec, so it handles gs:// automatically
        trades = parse_filing(file_path, filing_date_str)
        all_trades.extend([t.model_dump() for t in trades])
        
        if i % 50 == 0:
            context.log.info(f"Parsed {i}/{len(raw_form4_filings)} filings...")

    if not all_trades:
        return Output(pl.DataFrame(), metadata={"count": 0})

    df = pl.DataFrame(all_trades)
    
    output_filename = f"trades_{filing_date_str}.parquet"
    save_path = get_data_path(f"processed/{output_filename}") # Use helper
    
    # Create dir only if local
    if "gs://" not in save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    # Write with credentials
    df.write_parquet(save_path, storage_options=get_storage_options())
    
    # Preview logic (unchanged)
    preview_md = df.head(10).to_pandas().to_markdown(index=False)

    return Output(
        value=df, 
        metadata={
            "row_count": len(df), 
            "path": save_path,
            "preview": MetadataValue.md(preview_md)
        }
    )