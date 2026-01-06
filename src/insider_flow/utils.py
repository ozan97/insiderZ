import os

# SEC Configuration
SEC_USER_AGENT = "InsiderFlow kire.min@xitroo.de" 
SEC_BASE_URL = "https://www.sec.gov/Archives"
# Cloud Configuration
# We read this from Environment Variable. Default to False (Local).
USE_CLOUD = os.getenv("USE_CLOUD", "False") == "True"
GCS_BUCKET_NAME = "insider-flow-lake"

def get_storage_options():
    """
    Returns the credentials for Polars/DuckDB/Pandas.
    """
    # 1. If running locally and wanting to use Cloud, use the JSON key
    if USE_CLOUD and os.path.exists("gcp_key.json"):
        return {"google_application_credentials": "gcp_key.json"}
    
    # 2. If running locally or inside Cloud Run or GitHub Actions, it auto-detects credentials.
    return {}

def get_data_path(relative_path: str) -> str:
    """
    Converts 'raw/filings/file.txt' into:
    - 'data/raw/filings/file.txt' (Local)
    - 'gs://my-bucket/raw/filings/file.txt' (Cloud)
    """
    # Normalize path separators for Windows compatibility
    clean_path = relative_path.replace("\\", "/")
    
    if USE_CLOUD:
        return f"gs://{GCS_BUCKET_NAME}/{clean_path}"
    else:
        return os.path.join("data", clean_path)