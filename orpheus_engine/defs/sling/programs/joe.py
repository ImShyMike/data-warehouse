import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


joe_db_connection = SlingConnectionResource(
    name="JOE_DB",
    type="postgres",
    connection_string=EnvVar("JOE_COOLIFY_URL"),
)


joe_replication_config = {
    "source": "JOE_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "fraud_joe.{stream_table}",
    },

    "streams": {
        # --- Cases & case activity ---
        "public.cases": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.case_status_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.case_comments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.case_assignees": None,  # No update key, small table

        # --- Fraudpheus threads & messages ---
        "public.fraudpheus_messages": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.fraudpheus_thread_status_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.fraudpheus_v2_threads": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.fraudpheus_v2_messages": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

        # --- Users & profiles ---
        "public.user": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.profiles": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.permissions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

    }
}



@dg.asset(
    name="joe_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def joe_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates Joe (fraud case management) DB → warehouse."""
    context.log.info("Starting Joe → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, joe_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=joe_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
