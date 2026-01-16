from dagster import (
    Definitions, 
    load_assets_from_modules, 
    define_asset_job, 
    build_schedule_from_partitioned_job
)
from .resources import SECClient
import assets


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
    cron_schedule="0 9 * * 1-5", 
    description="Daily Morning Run for Insider Trades",
    execution_timezone="Europe/Zurich"
)

defs = Definitions(
    assets=all_assets,
    jobs=[daily_update_job],
    schedules=[daily_schedule],
    resources={
        "sec_client": SECClient(),
    },
)