import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


construct_db_connection = SlingConnectionResource(
    name="CONSTRUCT_DB",
    type="postgres",
    connection_string=EnvVar("CONSTRUCT_COOLIFY_URL"),
)


construct_replication_config = {
    "source": "CONSTRUCT_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "construct.{stream_table}",
    },

    "streams": {
        "public.devlog": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
            "select": [
                "id", "userId", "projectId", "timeSpent", "deleted",
                "createdAt", "updatedAt", "lapseId",
            ],
        },
        "public.project": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updatedAt",
            "select": [
                "-printedBy", "-editorFileType", "-editorUrl",
                "-uploadedFileUrl", "-modelFile",
            ],
        },
        "public.ship": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "timestamp",
            "select": [
                "-editorFileType", "-editorUrl", "-uploadedFileUrl",
                "-modelFile",
            ],
        },
        "public.user": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "lastLoginAt",
            "select": [
                "-idvId", "-profilePicture", "-idvToken", "-printer",
            ],
        },
        "public.legion_review": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "timestamp",
            "select": [
                "-feedback", "-notes",
            ],
        },
        "public.t1_review": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "timestamp",
            "select": [
                "-feedback", "-notes",
            ],
        },
        "public.t2_review": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "timestamp",
            "select": [
                "-feedback", "-notes", "-image",
            ],
        },
        "public.schema_migrations": {"disabled": True},
        "public.ar_internal_metadata": {"disabled": True},
        "public.session": {"disabled": True},
        "public.ovenpheus_log": {"disabled": True},
        "public.impersonate_audit_log": {"disabled": True},
        "public.currency_audit_log": {"disabled": True},
    },
}



@dg.asset(
    name="construct_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def construct_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the analytics-safe Construct DB columns into the warehouse."""
    context.log.info("Starting Construct → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, construct_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=construct_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
