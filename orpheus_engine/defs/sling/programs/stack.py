import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


stack_db_connection = SlingConnectionResource(
    name="STACK_DB",
    type="postgres",
    connection_string=EnvVar("STACK_COOLIFY_URL"),
)


stack_replication_config = {
    "source": "STACK_DB",
    "target": "WAREHOUSE_DB",
    "defaults": {
        "mode": "full-refresh",
        "object": "stack.{stream_table}",
    },
    "streams": {
        "public.*": None,
        "public._prisma_migrations": {"disabled": True},
        "public.users": {
            "select": [
                "id", "hackclub_sub", "email", "name", "slug", "profile_image_url",
                "slack_id", "verification_status", "role", "token_type",
                "token_expires_at", "expires_in_seconds", "scope",
                "created_at", "updated_at", "password_set_at", "coins",
                "hackatime_token_expires_at", "hackatime_connected_at",
                "hackatime_total_hours", "bricks", "hackatime_github_username",
            ],  # Excludes access_token, refresh_token, raw_token, password_hash,
                # raw_profile, hackatime_access_token, hackatime_refresh_token
        },
    },
}



@dg.asset(
    name="stack_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def stack_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Stack DB → warehouse in a single shot."""
    context.log.info("Starting Stack → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=stack_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
