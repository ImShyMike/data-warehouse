import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


siege_db_connection = SlingConnectionResource(
    name="SIEGE_DB",
    type="postgres",
    connection_string=EnvVar("SIEGE_COOLIFY_URL"),
)


siege_replication_config = {
    "source": "SIEGE_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "siege.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # --- Incremental: id + updated_at ---
        "public.addresses": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.ballots": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.global_bets": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.hackatime_days": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.meeple_cosmetics": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.meeples": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.personal_bets": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.projects": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.shop_purchases": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.user_weeks": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.votes": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
        "public.users": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
            # Siege has no token/credential/otp columns anywhere (Slack OAuth
            # only, verified against information_schema 2026-06-12); the list
            # pins today's columns so future app migrations can't leak secrets.
            "select": [
                "id", "slack_id", "email", "name", "team_id", "team_name",
                "created_at", "updated_at", "is_admin", "rank", "coins",
                "status", "idv_rec", "referrer_id", "main_device",
                "display_name", "audit_logs", "on_fraud_team",
                "ruby_unlocked", "emerald_unlocked", "amethyst_unlocked",
                "current_runes",
            ],
        },

        # --- Incremental: append-only with created_at ---
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

        # --- Disabled: Rails/queue infrastructure tables ---
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
        "public.pg_stat_statements": {"disabled": True},
        "public.pg_stat_statements_info": {"disabled": True},
    },
}



@dg.asset(
    name="siege_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def siege_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Siege DB → warehouse in a single shot."""
    context.log.info("Starting Siege → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, siege_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=siege_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
