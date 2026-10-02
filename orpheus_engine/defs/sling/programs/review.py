import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _ensure_incremental_target_indexes


review_db_connection = SlingConnectionResource(
    name="REVIEW_DB",
    type="postgres",
    connection_string=EnvVar("REVIEW_COOLIFY_URL"),
)


review_replication_config = {
    "source": "REVIEW_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "review.{stream_table}",
    },

    "streams": {
        "public.*": None,
        # ysws_reviews has id + updated_at - use incremental sync
        "public.ysws_reviews": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },
    }
}



@dg.asset(
    name="review_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def review_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Review DB → warehouse in a single shot."""
    context.log.info("Starting Review → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, review_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=review_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
