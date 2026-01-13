import os
import gcsfs
from concurrent.futures import ThreadPoolExecutor, as_completed
from dagster import asset, AssetExecutionContext
import polars as pl
from ..resources import SECClient
from ..utils import get_data_path, USE_CLOUD, get_storage_options
from ..partitions import daily_partitions_def

SEC_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data"

def process_filing(row, date_folder, sec_client, fs):
    """
    Helper function to process a single row.
    Returns the path if downloaded, None if skipped, Error string if failed.
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
            # GCS check
            # fs.exists expects 'bucket/path', so we strip 'gs://' just in case
            check_path = output_filepath.replace("gs://", "")
            if fs.exists(check_path): 
                exists = True
        elif os.path.exists(output_filepath):
            # Local check
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
        return f"ERROR: {str(e)}"

@asset(
    group_name="ingestion",
    description="Downloads raw filings in PARALLEL.",
    partitions_def=daily_partitions_def
)
def raw_form4_filings(context: AssetExecutionContext, sec_client: SECClient, daily_form4_list: pl.DataFrame):
    
    # Fast exit if input is empty (e.g. weekends)
    if daily_form4_list.is_empty():
        return []

    # --- UPDATED DATE LOGIC ---
    # Use the partition key directly. It is guaranteed to be "YYYY-MM-DD".
    # This is safer than parsing the DataFrame.
    date_folder = context.partition_key
    # --------------------------
    
    # --- Filesystem Setup ---
    fs = None
    if USE_CLOUD:
        opts = get_storage_options()
        # gcsfs uses 'token' for the credentials dict/path
        fs = gcsfs.GCSFileSystem(token=opts.get("google_application_credentials"))
    
    downloaded_count = 0
    rows = list(daily_form4_list.iter_rows(named=True))
    total_files = len(rows)

    context.log.info(f"Starting parallel download for {total_files} files (Target: {date_folder})")

    # --- PARALLEL EXECUTION ---
    with ThreadPoolExecutor(max_workers=6) as executor:
        # Submit tasks
        futures = [
            executor.submit(process_filing, row, date_folder, sec_client, fs) 
            for row in rows
        ]
        
        # Process results
        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            
            if result and result.startswith("ERROR"):
                context.log.warning(result)
            elif result:
                downloaded_count += 1
            
            # Log progress less frequently to keep logs clean
            if (i + 1) % 100 == 0:
                context.log.info(f"Processed {i + 1}/{total_files} filings...")

    context.log.info(f"Successfully downloaded {downloaded_count} new raw filings.")
    
    # Return the folder path so downstream knows where to look
    return [get_data_path(f"raw/filings/{date_folder}")]