import os
import fsspec
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import polars as pl
from typing import List
from ..parsing import parse_filing
from ..utils import get_data_path, get_storage_options
from ..partitions import daily_partitions_def

@asset(
    group_name="transformation",
    description="Parses raw text filings into structured insider trades.",
    partitions_def=daily_partitions_def 
)
def parsed_insider_trades(context: AssetExecutionContext, raw_form4_filings: List[str]):
    
    if not raw_form4_filings:
        return Output(pl.DataFrame(), metadata={"count": 0})

    # The upstream asset returns the FOLDER path
    root_path = raw_form4_filings[0]
    
    # 1. Determine Filesystem & Credentials
    opts = get_storage_options()
    
    # Polars uses 'google_application_credentials', but gcsfs/fsspec wants 'token'
    fsspec_opts = opts.copy()
    if "google_application_credentials" in opts:
        fsspec_opts["token"] = opts["google_application_credentials"]

    protocol = "gs" if "gs://" in root_path else "file"
    
    # Pass the corrected options to fsspec
    fs = fsspec.filesystem(protocol, **fsspec_opts)

    context.log.info(f"Scanning directory: {root_path} with protocol {protocol}")

    all_files = []
    try:
        # 2. Glob files
        if "gs://" in root_path:
            # Strip 'gs://' for the glob search
            search_path = root_path.replace("gs://", "")
            # glob returns 'bucket/folder/file.txt'
            found_paths = fs.glob(f"{search_path}/*.txt")
            # We must re-add 'gs://' so downstream functions know it's cloud
            all_files = [f"gs://{p}" for p in found_paths]
        else:
            # Local filesystem
            import glob
            all_files = glob.glob(os.path.join(root_path, "*.txt"))
            
    except Exception as e:
        context.log.error(f"Failed to list files in {root_path}: {e}")
        # Hint for debugging
        if "Anonymous caller" in str(e):
            context.log.error("Check if gcp_key.json exists and USE_CLOUD=True is set.")
        return Output(pl.DataFrame(), metadata={"count": 0, "error": str(e)})

    context.log.info(f"Found {len(all_files)} files to parse.")

    if not all_files:
         return Output(pl.DataFrame(), metadata={"count": 0})

    # 3. Determine Filing Date (metadata)
    sample_path = all_files[0]
    parts = sample_path.replace("\\", "/").split("/")
    filing_date_str = parts[-2] 

    all_trades = []
    
    # 4. Parse Loop
    for i, file_path in enumerate(all_files):
        # We pass the same opts logic to the parser via utils, but parse_filing needs to handle it
        trades = parse_filing(file_path, filing_date_str)
        if trades:
            all_trades.extend([t.model_dump() for t in trades])
        
        if (i + 1) % 50 == 0:
            context.log.info(f"Parsed {i + 1}/{len(all_files)} filings...")

    if not all_trades:
        context.log.warning("Parsed 0 trades from all files.")
        return Output(pl.DataFrame(), metadata={"count": 0})

    # 5. Save Output
    df = pl.DataFrame(all_trades)
    
    output_filename = f"trades_{filing_date_str}.parquet"
    save_path = get_data_path(f"processed/{output_filename}")
    
    if "gs://" not in save_path:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
    
    # Polars is happy with 'google_application_credentials', so we use the original 'opts'
    df.write_parquet(save_path, storage_options=opts)
    
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