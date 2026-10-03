import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


shrink_db_connection = SlingConnectionResource(
    name="SHRINK_DB",
    type="postgres",
    connection_string=EnvVar("SHRINK_DATABASE_URL"),
)


shrink_replication_config = {
    "source": "SHRINK_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "shrink.{stream_table}",
    },

    "streams": {
        "public.users": {
            "select": [
                "-hca_subject", "-display_name", "-avatar_url", "-slack_id",
                "-verification_status", "-eligibility", "-eligibility_at",
                "-birthdate", "-role", "-hca_token_encrypted",
                "-hca_token_expires_at", "-hackatime_token_encrypted",
                "-hackatime_linked_at",
            ],
        },
        "public.hackatime_days": None,
    },
}



@dg.asset(
    name="shrink_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def shrink_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the SHRINK DB → warehouse in a single shot."""
    context.log.info("Starting SHRINK → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=shrink_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
