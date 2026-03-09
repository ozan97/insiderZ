import os
import fsspec
from concurrent.futures import ThreadPoolExecutor, as_completed
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import polars as pl
from typing import List
from ..parsing import parse_filing
from ..utils import get_fsspec_options, save_dataframe 
from ..partitions import daily_partitions_def

@asset(
    group_name="transformation",
    description="Parses raw text filings into structured insider trades.",
    partitions_def=daily_partitions_def 
)
def parsed_insider_trades(context: AssetExecutionContext, raw_form4_filings: List[str]):
    
    if not raw_form4_filings:
        return Output(pl.DataFrame(), metadata={"count": 0})

    root_path = raw_form4_filings[0]
    
    # 1. Setup Filesystem for Scanning (Read-Side)
    fsspec_opts = get_fsspec_options()
        
    protocol = "gs" if "gs://" in root_path else "file"
    fs = fsspec.filesystem(protocol, **fsspec_opts)

    context.log.info(f"Scanning directory: {root_path} with protocol {protocol}")

    all_files = []
    try:
        # 2. Glob files
        if "gs://" in root_path:
            # Strip 'gs://' for the glob search
            search_path = root_path.replace("gs://", "")
            found_paths = fs.glob(f"{search_path}/*.txt")
            # Re-add 'gs://'
            all_files = [f"gs://{p}" for p in found_paths]
        else:
            # Local filesystem
            import glob
            all_files = glob.glob(os.path.join(root_path, "*.txt"))
            
    except Exception as e:
        context.log.error(f"Failed to list files in {root_path}: {e}")
        if "Anonymous caller" in str(e):
            context.log.error("Check if GCP_SERVICE_ACCOUNT_JSON env var is set.")
        return Output(pl.DataFrame(), metadata={"count": 0, "error": str(e)})

    context.log.info(f"Found {len(all_files)} files to parse.")

    if not all_files:
         return Output(pl.DataFrame(), metadata={"count": 0})

    # 3. Use Partition Key for Date (Cleaner than parsing path)
    filing_date_str = context.partition_key # e.g., "2026-01-05"

    # Build a dedicated read-side filesystem once, pass into every parse_filing call
    read_opts = get_fsspec_options()
    read_fs = fsspec.filesystem(protocol, **read_opts)

    all_trades = []
    total_files = len(all_files)
    parsed_count = 0
    
    # 4. Parallel Parse Loop
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(parse_filing, fp, filing_date_str, read_fs): fp
            for fp in all_files
        }
        for future in as_completed(futures):
            trades = future.result()
            if trades:
                all_trades.extend([t.model_dump() for t in trades])
            parsed_count += 1
            if parsed_count % 100 == 0:
                context.log.info(f"Parsed {parsed_count}/{total_files} filings...")

    if not all_trades:
        context.log.warning("Parsed 0 trades from all files.")
        return Output(pl.DataFrame(), metadata={"count": 0})

    df = pl.DataFrame(all_trades)
    df = df.unique(subset=["accession_number", "owner_name", "transaction_date", "shares", "price_per_share"])
    
    # 5. Save using Utils (Write-Side)
    output_filename = f"trades_{filing_date_str}.parquet"
    save_path = save_dataframe(df, f"processed/{output_filename}")
    
    # 6. Preview
    preview_md = df.head(10).to_pandas().to_markdown(index=False)

    return Output(
        value=df, 
        metadata={
            "row_count": len(df), 
            "path": save_path,
            "preview": MetadataValue.md(preview_md)
        }
    )