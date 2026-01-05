# src/insider_flow/__init__.py
from dagster import Definitions, load_assets_from_modules
from . import assets

# This automatically finds all assets (tasks) in the assets folder
all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
)