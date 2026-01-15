from dagster import Definitions, load_assets_from_modules
from .resources import SECClient
from . import assets

all_assets = load_assets_from_modules([
    assets.ingestion, 
    assets.download_filings,
    assets.transformation,
    assets.signals,
    assets.enrichtment
])


defs = Definitions(
    assets=all_assets,
    resources={
        "sec_client": SECClient(),
    },
)