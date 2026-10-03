import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


blueprint_db_connection = SlingConnectionResource(
    name="BLUEPRINT_DB",
    type="postgres",
    connection_string=EnvVar("BLUEPRINT_COOLIFY_URL"),
)


blueprint_replication_config = {
    "source": "BLUEPRINT_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "blueprint.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # --- Incremental: id + updated_at ---
        "public.ai_reviews": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.airtable_syncs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.allowed_emails": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_dashboard_queries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_dashboards": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.blazer_queries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.build_reviews": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.design_reviews": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.disco_recommendations": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.email_tracks": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_features": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.flipper_gates": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.follows": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.guild_signups": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.guilds": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.hcb_grants": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.hcb_transactions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.journal_entries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.kudos": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.manual_ticket_adjustments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.packages": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.privileged_session_expiries": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.project_grants": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_items": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_orders": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.stored_recommendations": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.task_lists": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
            "select": [
                "id", "avatar", "slack_id", "username", "timezone_raw",
                "is_banned", "created_at", "updated_at", "email", "is_mcg",
                "github_username", "last_active", "github_installation_id",
                "referrer_id", "identity_vault_id", "ysws_verified",
                "internal_notes", "free_stickers_claimed", "ban_type",
                "birthday", "is_pro", "admin", "reviewer", "fulfiller",
                "idv_country", "shopkeeper", "last_impersonated_at",
                "last_impersonation_ended_at", "first_synced_to_airtable",
                "hcb_integration_enabled", "hcb_token_expires_at",
            ],  # Excludes identity_vault_access_token, hcb_access_token, hcb_refresh_token
        },

        # --- Incremental: id + special timestamp ---
        "public.ahoy_events": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "time",
        },
        "public.ahoy_visits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "started_at",
        },

        # --- Incremental: append-only with created_at ---
        "public.versions": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

        # --- Incremental: append-only tables ---
        "public.active_storage_attachments": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.active_storage_blobs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.project_user_views": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "first_viewed_at",
        },
        "public.blazer_audits": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },

        # --- Disabled: sensitive data ---
        "public.one_time_passwords": {"disabled": True},

        # --- Disabled: Rails infrastructure tables ---
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.solid_cache_entries": {"disabled": True},
        "public.solid_queue_blocked_executions": {"disabled": True},
        "public.solid_queue_claimed_executions": {"disabled": True},
        "public.solid_queue_failed_executions": {"disabled": True},
        "public.solid_queue_jobs": {"disabled": True},
        "public.solid_queue_pauses": {"disabled": True},
        "public.solid_queue_processes": {"disabled": True},
        "public.solid_queue_ready_executions": {"disabled": True},
        "public.solid_queue_recurring_executions": {"disabled": True},
        "public.solid_queue_recurring_tasks": {"disabled": True},
        "public.solid_queue_scheduled_executions": {"disabled": True},
        "public.solid_queue_semaphores": {"disabled": True},

        # --- Full-refresh: no suitable update key ---
        # active_storage_variant_records (no timestamp, 41K rows)
    }
}



@dg.asset(
    name="blueprint_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def blueprint_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Blueprint DB → warehouse in a single shot."""
    context.log.info("Starting Blueprint → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, blueprint_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=blueprint_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
