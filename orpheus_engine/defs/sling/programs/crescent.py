import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


crescent_db_connection = SlingConnectionResource(
    name="CRESCENT_DB",
    type="postgres",
    connection_string=EnvVar("CRESCENT_DATABASE_URL"),
)


crescent_replication_config = {
    "source": "CRESCENT_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "crescent.{stream_table}",
    },

    "streams": {
        "public.active_storage_attachments": None,
        "public.active_storage_blobs": None,
        "public.active_storage_variant_records": {
            "select": ["-variation_digest"],
        },
        "public.activity_events": None,
        "public.adjustments": None,
        "public.airtable_syncs": None,
        "public.announcement_blocks": None,
        "public.blazer_audits": None,
        "public.blazer_checks": None,
        "public.blazer_dashboard_queries": None,
        "public.blazer_dashboards": None,
        "public.blazer_queries": None,
        "public.card_definitions": None,
        "public.card_offers": None,
        "public.checklist_completions": None,
        "public.flipper_features": None,
        "public.flipper_gates": None,
        "public.guides": None,
        "public.hcb_connections": {
            "select": ["-access_token", "-refresh_token", "-token_expires_at"],
        },
        "public.hcb_grants": None,
        "public.hcb_ledger_entries": None,
        "public.orders": None,
        "public.project_hackatime_links": None,
        "public.projects": None,
        "public.queue_snapshots": None,
        "public.rate_weeks": None,
        "public.review_escalations": None,
        "public.review_metrics": None,
        "public.reviewer_metrics": None,
        "public.reviews": {
            "select": ["-action_items_digest"],
        },
        "public.settings": None,
        "public.ships": {
            "select": ["-acknowledged_action_items_digest"],
        },
        "public.shop_item_price_changes": None,
        "public.shop_items": None,
        "public.user_activity_days": None,
        "public.user_notes": None,
        "public.users": {
            "select": [
                "-hca_access_token", "-hca_refresh_token",
                "-hca_token_expires_at",
            ],
        },
        "public.versions": None,
        "public.warehouse_packages": None,
    },
}



@dg.asset(
    name="crescent_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def crescent_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Crescent DB → warehouse in a single shot."""
    context.log.info("Starting Crescent → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=crescent_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
