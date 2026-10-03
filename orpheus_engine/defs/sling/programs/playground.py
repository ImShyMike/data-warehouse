import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


playground_db_connection = SlingConnectionResource(
    name="PLAYGROUND_DB",
    type="postgres",
    connection_string=EnvVar("PLAYGROUND_DATABASE_URL"),
)


playground_replication_config = {
    "source": "PLAYGROUND_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "playground.{stream_table}",
    },

    "streams": {
        "public.audit_events": None,
        "public.claims": None,
        "public.coding_hours": None,
        "public.projects": None,
        "public.redemptions": None,
        "public.ships": None,
        "public.solid_cable_messages": {"disabled": True},
        "public.users": {
            "select": [
                "id", "admin", "airtable_record_id", "ban_reason",
                "banned_at", "birthday", "created_at", "email", "first_name",
                "hackatime_trust_level", "hackatime_user_id", "hca_id",
                "last_name", "slack_id", "synced_at", "updated_at",
                "verification_status", "ysws_eligible", "display_name",
                "display_name_source", "desktop_trash", "session_version",
                "banana_peel_out", "coding_hours_synced_at",
                "slack_prompt_dismissed_at",
            ],
        },
    },
}



@dg.asset(
    name="playground_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def playground_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Playground DB → warehouse in a single shot."""
    context.log.info("Starting Playground → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=playground_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
