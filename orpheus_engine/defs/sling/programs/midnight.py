import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _sling_connection_url
from .._helpers import _ensure_incremental_target_indexes


midnight_db_connection = SlingConnectionResource(
    name="MIDNIGHT_DB",
    type="postgres",
    connection_string=_sling_connection_url("MIDNIGHT_K8S_URL"),
)


midnight_replication_config = {
    "source": "MIDNIGHT_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "midnight.{stream_table}",
    },

    "streams": {
        "public.projects": {
            "mode": "incremental",
            "primary_key": ["project_id"],
            "update_key": "updated_at",
            "select": [
                "project_id", "user_id", "project_type", "created_at",
                "updated_at", "airtable_rec_id", "approved_hours",
                "description", "hours_justification", "now_hackatime_hours",
                "now_hackatime_projects", "playable_url", "project_title",
                "repo_url", "screenshot_url", "is_locked", "is_fraud",
            ],
        },
        "public.submissions": {
            "mode": "incremental",
            "primary_key": ["submission_id"],
            "update_key": "updated_at",
            "select": [
                "submission_id", "project_id", "playable_url",
                "screenshot_url", "description", "repo_url", "approved_hours",
                "hours_justification", "approval_status", "reviewed_by",
                "reviewed_at", "created_at", "updated_at",
            ],
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["user_id"],
            "update_key": "updated_at",
            "select": [
                "user_id", "email", "first_name", "last_name", "birthday",
                "role", "onboard_complete", "onboarded_at", "address_line_1",
                "address_line_2", "city", "state", "country", "zip_code",
                "airtable_rec_id", "created_at", "updated_at",
                "hackatime_account", "raffle_pos", "referral_code",
                "slack_user_id", "is_fraud",
            ],
        },
        "public.user_sessions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
            "select": [
                "id", "user_id", "is_verified", "verified_at",
                "expires_at", "created_at",
            ],
        },
    },
}



@dg.asset(
    name="midnight_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def midnight_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates analytics-safe Midnight DB columns into the warehouse."""
    context.log.info("Starting Midnight → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, midnight_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=midnight_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
