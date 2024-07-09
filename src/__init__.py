from dagster import Definitions, load_assets_from_modules, AutoMaterializePolicy

from src.assets import lower_level, routing, top_level
from src.jobs import (
    community_job,
    education_job,
    food_job,
    greenspace_job,
    health_job,
    lower_level_assets_job,
    retail_job,
    services_job,
    sustenance_job,
    transport_job,
)

lower_level_assets = load_assets_from_modules([lower_level])
top_level_assets = load_assets_from_modules([top_level])
routing_assets = load_assets_from_modules([routing])


defs = Definitions(
    assets=[*lower_level_assets, *top_level_assets, *routing_assets],
    jobs=[
        lower_level_assets_job,
        community_job,
        education_job,
        food_job,
        greenspace_job,
        health_job,
        retail_job,
        services_job,
        sustenance_job,
        transport_job,
    ],
)
