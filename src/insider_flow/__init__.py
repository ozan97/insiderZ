from dagster import (
    Definitions, 
    load_assets_from_modules, 
    define_asset_job, 
    build_schedule_from_partitioned_job
)
from .resources import SECClient
from .assets import ingestion, download_filings, transformation, signals, enrichtment


all_assets = load_assets_from_modules([
    assets.ingestion, 
    assets.download_filings,
    assets.transformation,
    assets.signals,
    assets.enrichtment
])


daily_update_job = define_asset_job(
    name="daily_update_job", 
    selection=all_assets
)
daily_schedule = build_schedule_from_partitioned_job(
    job=daily_update_job,
    hour_of_day=9, 
    minute_of_hour=0,  
    description="Daily Morning Run for Insider Trades"
)

defs = Definitions(
    assets=all_assets,
    jobs=[daily_update_job],
    schedules=[daily_schedule],
    resources={
        "sec_client": SECClient(),
    },
)