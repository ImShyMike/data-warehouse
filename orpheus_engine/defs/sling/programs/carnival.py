import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


carnival_db_connection = SlingConnectionResource(
    name="CARNIVAL_DB",
    type="postgres",
    connection_string=EnvVar("CARNIVAL_COOLIFY_URL"),
)


carnival_replication_config = {
    "source": "CARNIVAL_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "carnival.{stream_table}",
    },

    "streams": {
        "public.user": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
            "select": [
                "id", "name", "email", "slack_id", "role",
                "verification_status", "hackatime_user_id",
                "hackatime_connected_at", "is_frozen", "frozen_reason",
                "frozen_at", "created_at", "updated_at",
            ],
        },
        "public.project": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
            "select": [
                "id", "creator_id", "name", "description", "category",
                "status", "code_url", "video_url", "playable_demo_url",
                "hackatime_project_name", "hackatime_started_at",
                "hackatime_stopped_at", "hackatime_total_seconds",
                "hours_spent_seconds", "approved_hours", "bounty_project_id",
                "started_on_carnival_at", "submitted_at",
                "created_at", "updated_at",
            ],
        },
        "public.project_hackatime_project": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
            "select": [
                "id", "project_id", "name", "is_default",
                "first_devlog_id", "created_at", "updated_at",
            ],
        },
    },
}



@dg.asset(
    name="carnival_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def carnival_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the analytics-safe Carnival DB columns into the warehouse."""
    context.log.info("Starting Carnival → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, carnival_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=carnival_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
