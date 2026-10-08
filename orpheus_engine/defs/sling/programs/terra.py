import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import Nothing
from .._helpers import _sling_connection_url


terra_db_connection = SlingConnectionResource(
    name="TERRA_DB",
    type="postgres",
    connection_string=_sling_connection_url("TERRA_DATABASE_URL"),
)


terra_replication_config = {
    "source": "TERRA_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "terra.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # --- Disabled: entirely encrypted PII ---
        "public.user_addresses": {"disabled": True},

        # --- Sensitive: exclude credential columns ---
        "public.hackatime_accounts": {
            "select": ["-access_token_enc", "-api_key_enc"],
        },
        "public.chat_slack_accounts": {
            "select": ["-access_token_enc"],
        },
        "public.github_accounts": {
            "select": ["-access_token_enc"],
        },
    },
}


@dg.asset(
    name="terra_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def terra_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the Terra DB → warehouse in a single shot."""
    context.log.info("Starting Terra → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=terra_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
