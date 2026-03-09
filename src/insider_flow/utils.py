import json
import os
from typing import Any

import fsspec
import polars as pl
from dotenv import load_dotenv

load_dotenv()

# SEC Configuration
SEC_USER_AGENT = "InsiderFlow kire.min@xitroo.de"
SEC_BASE_URL = "https://www.sec.gov/Archives"

# Cloud Configuration
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "insider-flow-lake")
LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", "data")


def _env_bool(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


USE_CLOUD = _env_bool("USE_CLOUD", default=False)


def _normalize_relative_path(relative_path: str) -> str:
    normalized = relative_path.replace("\\", "/").strip()
    return normalized.lstrip("/")


def _read_service_account_json() -> dict[str, Any]:
    raw = os.getenv("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        return {}

    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "GCP_SERVICE_ACCOUNT_JSON must be valid JSON (service account object)."
        ) from exc

    if not isinstance(value, dict):
        raise ValueError("GCP_SERVICE_ACCOUNT_JSON must decode to a JSON object.")

    return value


def get_storage_options(use_cloud: bool | None = None, require_credentials: bool = False) -> dict[str, Any]:
    """
    Returns cloud storage options compatible with Polars/Pandas.

    - Local mode returns an empty dict.
    - Cloud mode returns a `google_application_credentials` dict when available.
    """
    cloud_mode = USE_CLOUD if use_cloud is None else use_cloud
    if not cloud_mode:
        return {}

    credentials = _read_service_account_json()
    if credentials:
        return {"google_application_credentials": credentials}

    if require_credentials:
        raise ValueError(
            "Cloud mode is enabled but GCP_SERVICE_ACCOUNT_JSON is empty. "
            "Set USE_CLOUD=False for local mode or provide credentials."
        )

    return {}


def get_fsspec_options(use_cloud: bool | None = None, require_credentials: bool = False) -> dict[str, Any]:
    """
    Returns options ready for fsspec APIs.

    Includes `token` for GCSFS when service account JSON is present.
    """
    opts = get_storage_options(use_cloud=use_cloud, require_credentials=require_credentials)
    fsspec_opts = opts.copy()

    credentials = opts.get("google_application_credentials")
    if isinstance(credentials, dict):
        fsspec_opts["token"] = credentials

    return fsspec_opts


def get_data_path(relative_path: str, use_cloud: bool | None = None) -> str:
    """
    Converts `raw/filings/file.txt` into:
    - `data/raw/filings/file.txt` (local)
    - `gs://<bucket>/raw/filings/file.txt` (cloud)
    """
    normalized_path = _normalize_relative_path(relative_path)
    cloud_mode = USE_CLOUD if use_cloud is None else use_cloud

    if cloud_mode:
        return f"gs://{GCS_BUCKET_NAME}/{normalized_path}"

    return os.path.join(LOCAL_DATA_DIR, normalized_path).replace("\\", "/")


def save_dataframe(df: pl.DataFrame, relative_path: str, *, use_cloud: bool | None = None) -> str:
    """
    Saves a Polars DataFrame to parquet for local or GCS targets.
    """
    full_path = get_data_path(relative_path, use_cloud=use_cloud)
    cloud_path = full_path.startswith("gs://")

    if cloud_path:
        fsspec_opts = get_fsspec_options(use_cloud=True, require_credentials=True)
        with fsspec.open(full_path, "wb", **fsspec_opts) as file_obj:
            df.write_parquet(file_obj)
    else:
        directory = os.path.dirname(full_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        df.write_parquet(full_path)

    return full_path


def load_all_parquet(relative_glob: str, *, use_cloud: bool | None = None) -> pl.DataFrame | None:
    """
    Reads all parquet files matching a glob pattern and concatenates them.
    Returns None if no files are found.
    """
    import glob as globmod

    full_pattern = get_data_path(relative_glob, use_cloud=use_cloud)
    cloud = full_pattern.startswith("gs://")

    if cloud:
        fsspec_opts = get_fsspec_options(use_cloud=True)
        fs = fsspec.filesystem("gs", **fsspec_opts)
        search = full_pattern.replace("gs://", "")
        paths = fs.glob(search)
        if not paths:
            return None
        frames = []
        for p in paths:
            with fs.open(p, "rb") as f:
                frames.append(pl.read_parquet(f))
        return pl.concat(frames, how="diagonal_relaxed")
    else:
        paths = globmod.glob(full_pattern)
        if not paths:
            return None
        frames = [pl.read_parquet(p) for p in paths]
        return pl.concat(frames, how="diagonal_relaxed")