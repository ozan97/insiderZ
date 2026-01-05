import os
from dagster import asset, AssetExecutionContext, Output, MetadataValue
import polars as pl
from typing import List
from ..parsing import parse_filing

@asset(
    group_name="transformation",
    description="Parses raw text filings into structured insider trades."
)
def parsed_insider_trades(context: AssetExecutionContext, raw_form4_filings: List[str]):
    
    if not raw_form4_filings:
        context.log.warning("No files to parse.")
        return Output(pl.DataFrame(), metadata={"count": 0})

    # ... (Date logic remains the same)
    sample_path = raw_form4_filings[0]
    path_parts = sample_path.replace("\\", "/").split("/") 
    filing_date_str = path_parts[-2] 
    
    all_trades = []
    
    # ... (Loop logic remains the same)
    for i, file_path in enumerate(raw_form4_filings):
        trades = parse_filing(file_path, filing_date_str)
        all_trades.extend([t.model_dump() for t in trades])
        
        if i % 50 == 0:
            context.log.info(f"Parsed {i}/{len(raw_form4_filings)} filings...")

    if not all_trades:
        return Output(pl.DataFrame(), metadata={"count": 0})

    # Create DataFrame
    df = pl.DataFrame(all_trades)
    
    # Save to Processed Data Lake
    output_filename = f"trades_{filing_date_str}.parquet"
    output_path = os.path.join("data", "processed", output_filename)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.write_parquet(output_path)
    
    # Note: Polars to_pandas() is needed because Dagster's markdown renderer likes standard types
    preview_md = df.head(10).to_pandas().to_markdown(index=False)

    return Output(
        value=df, # The actual data passed to downstream assets
        metadata={
            "row_count": len(df), # Numeric metric (graphed over time)
            "path": output_path,
            "columns": MetadataValue.json(df.columns),
            "preview": MetadataValue.md(preview_md) # The visual table
        }
    )