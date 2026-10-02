import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


offtrack_db_connection = SlingConnectionResource(
    name="OFFTRACK_DB",
    type="postgres",
    connection_string=EnvVar("OFFTRACK_COOLIFY_URL"),
)


offtrack_replication_config = {
    "source": "OFFTRACK_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "offtrack.{stream_table}",
    },
    "streams": {
        "public.*": None,
        "public._prisma_migrations": {"disabled": True},
        "public.users": {
            "select": [
                "id", "hackclub_sub", "email", "name", "slug", "profile_image_url",
                "slack_id", "verification_status", "role", "coins", "token_type",
                "token_expires_at", "expires_in_seconds", "scope",
                "created_at", "updated_at", "hackatime_token_expires_at",
                "hackatime_connected_at", "hackatime_total_hours",
                "hackatime_github_username",
            ],  # Excludes access_token, refresh_token, raw_token,
                # raw_profile, hackatime_access_token, hackatime_refresh_token
        },
    },
}



@dg.asset(
    name="offtrack_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def offtrack_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Off-Track DB → warehouse in a single shot."""
    context.log.info("Starting Off-Track → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=offtrack_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
