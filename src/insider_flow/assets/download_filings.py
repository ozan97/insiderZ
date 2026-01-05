import os
from dagster import asset, AssetExecutionContext
from datetime import datetime
import polars as pl
from ..resources import SECClient
# URL definition moved here for clarity
SEC_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data"

@asset(
    group_name="ingestion",
    description="Downloads the raw .txt filings using the canonical SEC folder structure."
)
def raw_form4_filings(context: AssetExecutionContext, sec_client: SECClient, daily_form4_list: pl.DataFrame):
    
    if daily_form4_list.is_empty():
        context.log.warning("daily_form4_list is empty, skipping raw filing download.")
        return []

    # Logic to determine storage folder date
    try:
        target_date = daily_form4_list["date_filed"][0]
        if not isinstance(target_date, (datetime, type(datetime.now().date()))):
             target_date_str = str(target_date)
             target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    except Exception as e:
        context.log.error(f"Could not infer target date: {e}. Using current date.")
        target_date = datetime.now().date()

    date_folder = target_date.strftime("%Y-%m-%d")
    output_dir = os.path.join("data", "raw", "filings", date_folder)
    os.makedirs(output_dir, exist_ok=True)
    
    downloaded_files = []
    
    # Iterate through rows
    for row in daily_form4_list.iter_rows(named=True):
        cik = str(row["cik"]) # Ensure string to keep it safe
        
        # Original filename from index: edgar/data/101382/0000101382-26-000025.txt
        original_path = row["filename"]
        
        # Extract Accession Number (filename part)
        # 0000101382-26-000025.txt
        accession_with_dashes_txt = original_path.split("/")[-1] 
        
        # Create Accession without dashes for the folder name
        # 0000101382-26-000025.txt -> 000010138226000025
        accession_no_dashes = accession_with_dashes_txt.replace("-", "").replace(".txt", "")
        
        # Construct the Canonical URL (The one you found)
        # https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/{accession_with_dashes}.txt
        canonical_url = f"{SEC_ARCHIVES_URL}/{cik}/{accession_no_dashes}/{accession_with_dashes_txt}"

        # Clean output filename
        output_filename = f"{cik}_{accession_with_dashes_txt}"
        output_filepath = os.path.join(output_dir, output_filename)
        
        if os.path.exists(output_filepath):
            # context.log.debug(f"Skipping {output_filename}, already exists.")
            downloaded_files.append(output_filepath)
            continue

        try:
            # Download using the canonical URL
            content_bytes = sec_client.get_content(canonical_url, as_bytes=True)
            
            with open(output_filepath, "wb") as f:
                f.write(content_bytes)
                
            downloaded_files.append(output_filepath)
            
            if len(downloaded_files) % 10 == 0:
                context.log.info(f"Downloaded {len(downloaded_files)} files...")

        except Exception as e:
            context.log.warning(f"Canonical URL failed: {canonical_url}. Trying Index URL...")
            # Fallback: Try the URL provided by the Index (just in case)
            try:
                fallback_url = row["file_url"]
                content_bytes = sec_client.get_content(fallback_url, as_bytes=True)
                with open(output_filepath, "wb") as f:
                    f.write(content_bytes)
                downloaded_files.append(output_filepath)
                context.log.info(f"Recovered using fallback URL for {cik}")
            except Exception as e2:
                 context.log.error(f"Failed to download {cik} via both methods: {e2}")

    context.log.info(f"Successfully downloaded {len(downloaded_files)} raw Form 4 filings to {output_dir}")
    return downloaded_files