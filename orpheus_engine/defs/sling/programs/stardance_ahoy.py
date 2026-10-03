import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


stardance_ahoy_db_connection = SlingConnectionResource(
    name="STARDANCE_AHOY_DB",
    type="postgres",
    connection_string=EnvVar("STARDANCE_AHOY_COOLIFY_URL"),
)


stardance_ahoy_replication_config = {
    "source": "STARDANCE_AHOY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "stardance_ahoy.{stream_table}",
    },

    "streams": {
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",
        },
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",
        },
    }
}



@dg.asset(
    name="stardance_ahoy_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def stardance_ahoy_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates Stardance Ahoy analytics DB → warehouse with incremental sync."""
    context.log.info("Starting Stardance Ahoy → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, stardance_ahoy_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=stardance_ahoy_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
