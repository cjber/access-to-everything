from dagster import define_asset_job

from src.assets.lower_level import (
    dentists,
    gpprac,
    process_hospitals,
    overture,
    pharmacies,
    process_postcodes,
)
from src.assets.routing import (
    community_distances,
    education_distances,
    food_distances,
    greenspace_distances,
    health_distances,
    oproad,
    retail_distances,
    services_distances,
    sustenance_distances,
    transport_distances,
)
from src.assets.top_level import (
    community,
    education,
    food,
    process_greenspace,
    process_health,
    retail,
    services,
    sustenance,
    transport,
)

lower_level_assets_job = define_asset_job(
    "lower_level_assets_job",
    selection=[dentists, gpprac, process_hospitals, overture, pharmacies, process_postcodes, oproad],
)
community_job = define_asset_job(
    "community_job", selection=[community, community_distances]
)
education_job = define_asset_job(
    "education_job", selection=[education, education_distances]
)
food_job = define_asset_job("food_job", selection=[food, food_distances])
greenspace_job = define_asset_job(
    "greenspace_job", selection=[process_greenspace, greenspace_distances]
)
health_job = define_asset_job("health_job", selection=[process_health, health_distances])
retail_job = define_asset_job("retail_job", selection=[retail, retail_distances])
services_job = define_asset_job(
    "services_job", selection=[services, services_distances]
)
sustenance_job = define_asset_job(
    "sustenance_job", selection=[sustenance, sustenance_distances]
)
transport_job = define_asset_job(
    "transport_job", selection=[transport, transport_distances]
)
