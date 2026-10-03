from dagster import Definitions
from .assets import (
    hackatime_warehouse_mirror,
    hackatime_warehouse_app_indexes,
    hcer_public_github_data_warehouse_mirror,
    hcb_warehouse_mirror,
    auth_warehouse_mirror,
    sling_replication_resource,
    _program_assets,
)

defs = Definitions(
    assets=[
        hackatime_warehouse_mirror,
        hackatime_warehouse_app_indexes,
        hcer_public_github_data_warehouse_mirror,
        hcb_warehouse_mirror,
        auth_warehouse_mirror,
        *_program_assets,
    ],
    resources={
        "sling": sling_replication_resource,
    },
)
