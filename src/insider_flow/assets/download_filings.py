import os
import gcsfs
from concurrent.futures import ThreadPoolExecutor, as_completed
from dagster import asset, AssetExecutionContext
from datetime import datetime
import polars as pl
from ..resources import SECClient
from ..utils import get_data_path, USE_CLOUD, get_storage_options

SEC_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data"

def process_filing(row, date_folder, sec_client, fs, context):
    """
    Helper function to process a single row.
    Returns the path if downloaded, None if skipped/failed.
    """
    try:
        cik = str(row["cik"])
        original_path = row["filename"]
        accession_with_dashes_txt = original_path.split("/")[-1] 
        accession_no_dashes = accession_with_dashes_txt.replace("-", "").replace(".txt", "")
        
        # 1. Construct Canonical URL
        canonical_url = f"{SEC_ARCHIVES_URL}/{cik}/{accession_no_dashes}/{accession_with_dashes_txt}"

        # 2. Define Output Path
        output_filepath = get_data_path(f"raw/filings/{date_folder}/{cik}_{accession_with_dashes_txt}")
        
        # 3. Check Existence (Fast fail)
        exists = False
        if USE_CLOUD and fs:
            # Strip gs:// for fs.exists checks usually
            check_path = output_filepath.replace("gs://", "").split("/", 1)[-1] if "gs://" in output_filepath else output_filepath
            if fs.exists(output_filepath): 
                exists = True
        elif os.path.exists(output_filepath):
            exists = True

        if exists:
            return None # Skipped

        # 4. Download
        content_bytes = sec_client.get_content(canonical_url, as_bytes=True)
        
        # 5. Write
        if USE_CLOUD and fs:
            with fs.open(output_filepath, "wb") as f:
                f.write(content_bytes)
        else:
            os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
            with open(output_filepath, "wb") as f:
                f.write(content_bytes)
            
        return output_filepath

    except Exception as e:
        # Log failure but don't crash the thread
        # We can't use context.log inside a thread easily without issues, 
        # so we print or return the error to be logged later.
        return f"ERROR: {str(e)}"

@asset(
    group_name="ingestion",
    description="Downloads raw filings in PARALLEL."
)
def raw_form4_filings(context: AssetExecutionContext, sec_client: SECClient, daily_form4_list: pl.DataFrame):
    
    if daily_form4_list.is_empty():
        return []

    # --- Date Logic ---
    try:
        target_date = daily_form4_list["date_filed"][0]
        if not isinstance(target_date, (datetime, type(datetime.now().date()))):
             target_date_str = str(target_date)
             target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    except Exception as e:
        context.log.error(f"Could not infer target date: {e}")
        target_date = datetime.now().date()

    date_folder = target_date.strftime("%Y-%m-%d")
    
    # --- Filesystem Setup ---
    fs = None
    if USE_CLOUD:
        opts = get_storage_options()
        fs = gcsfs.GCSFileSystem(token=opts.get("google_application_credentials"))
    
    downloaded_count = 0
    rows = list(daily_form4_list.iter_rows(named=True))
    total_files = len(rows)

    context.log.info(f"Starting parallel download for {total_files} files with 5 workers...")

    # --- PARALLEL EXECUTION ---
    # 5 workers to stay under the SECs 10 req/sec limit.
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_filing, row, date_folder, sec_client, fs, context) 
            for row in rows
        ]
        
        # Process results as they complete
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            
            if result and result.startswith("ERROR"):
                context.log.warning(result)
            elif result:
                downloaded_count += 1
            
            # Log progress every 50 files
            if (i + 1) % 200 == 0:
                context.log.info(f"Processed {i + 1}/{total_files} filings...")

    context.log.info(f"Successfully downloaded {downloaded_count} new raw filings.")
    
    # We return a list of paths (or just a dummy list since downstream re-reads directory)
    # Re-listing the directory is safer for downstream consistency
    return [get_data_path(f"raw/filings/{date_folder}")]