import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


flavortown_ahoy_db_connection = SlingConnectionResource(
    name="FLAVORTOWN_AHOY_DB",
    type="postgres",
    connection_string=EnvVar("FLAVORTOWN_AHOY_COOLIFY_URL"),
)


flavortown_ahoy_replication_config = {
    "source": "FLAVORTOWN_AHOY_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "flavortown_ahoy.{stream_table}",
    },

    "streams": {
        # Insert-only; started_at/time are only non-leading columns of composite indexes,
        # so key off the indexed bigint PK instead.
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "id",
        },
    }
}



@dg.asset(
    name="flavortown_ahoy_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def flavortown_ahoy_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates FlavorTown Ahoy analytics DB → warehouse with incremental sync."""
    context.log.info("Starting FlavorTown Ahoy → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, flavortown_ahoy_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=flavortown_ahoy_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
