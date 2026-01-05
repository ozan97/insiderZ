from dagster import Definitions, load_assets_from_modules
from . import assets
from .resources import SECClient
from .assets import ingestion, download_filings, transformation 

all_assets = load_assets_from_modules([
    assets.debug, 
    assets.ingestion, 
    assets.download_filings,
    assets.transformation
])

defs = Definitions(
    assets=all_assets,
    resources={
        "sec_client": SECClient(),
    },
)