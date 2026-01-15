import os
import json
import fsspec
import polars as pl
from dotenv import load_dotenv

load_dotenv()

# SEC Configuration
SEC_USER_AGENT = "InsiderFlow kire.min@xitroo.de" 
SEC_BASE_URL = "https://www.sec.gov/Archives"
# Cloud Configuration
USE_CLOUD = os.getenv("USE_CLOUD", "False") == "True"
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "insider-flow-lake")

def get_storage_options():
    """
    Returns the credentials for Polars/Pandas.
    """

    if USE_CLOUD:
        return {"google_application_credentials": json.loads(os.getenv("GCP_SERVICE_ACCOUNT_JSON", "{}"))}
    else:
        return {}

def get_data_path(relative_path: str) -> str:
    """
    Converts 'raw/filings/file.txt' into:
    - 'data/raw/filings/file.txt' (Local)
    - 'gs://my-bucket/raw/filings/file.txt' (Cloud)
    """
    # Normalize path separators for Windows compatibility
    if USE_CLOUD:
        return f"gs://{GCS_BUCKET_NAME}/{relative_path}"
    else:
        return os.path.join("data", relative_path).replace("\\", "/")
    
def save_dataframe(df: pl.DataFrame, relative_path: str) -> str:
    """
    Saves a Polars DataFrame to Parquet, handling Cloud vs Local logic 
    and Dictionary credentials automatically.
    """
    full_path = get_data_path(relative_path)
    opts = get_storage_options()
    
    # Check if Cloud Path
    if "://" in full_path:
        fsspec_opts = opts.copy()
        
        # Specific fix for GCSFS + Dictionary Credentials
        if "google_application_credentials" in opts and isinstance(opts["google_application_credentials"], dict):
            fsspec_opts["token"] = opts["google_application_credentials"]

        # Write to Cloud via fsspec pipe
        with fsspec.open(full_path, "wb", **fsspec_opts) as f:
            df.write_parquet(f)
    else:
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        df.write_parquet(full_path)
        
    return full_path