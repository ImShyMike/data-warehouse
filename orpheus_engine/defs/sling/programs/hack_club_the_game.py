import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


hack_club_the_game_db_connection = SlingConnectionResource(
    name="HACK_CLUB_THE_GAME_DB",
    type="postgres",
    connection_string=EnvVar("HACK_CLUB_THE_GAME_COOLIFY_URL"),
)


hack_club_the_game_replication_config = {
    "source": "HACK_CLUB_THE_GAME_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "hack_club_the_game.{stream_table}",
    },

    "streams": {
        "public.*": None,
        # Exclude sensitive/internal tables
        "public.one_time_passwords": {"disabled": True},
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.blazer_audits": {"disabled": True},
        "public.blazer_checks": {"disabled": True},
        "public.blazer_dashboard_queries": {"disabled": True},
        "public.blazer_dashboards": {"disabled": True},
        "public.blazer_queries": {"disabled": True},
        "public.versions": {"disabled": True},
        # Users: exclude encrypted auth tokens
        "public.users": {
            "select": [
                "-account_access_token", "-hackatime_access_token",
                "-onboarding_completed", "-can_overspend",
                "-referral_share_code", "-phone_number", "-is_debt",
                "-is_shop_approved",
            ],
        },
    },
}



@dg.asset(
    name="hack_club_the_game_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def hack_club_the_game_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Hack Club: The Game DB → warehouse in a single shot."""
    context.log.info("Starting Hack Club: The Game → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=hack_club_the_game_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
