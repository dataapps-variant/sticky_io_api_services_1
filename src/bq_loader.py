"""
All BigQuery interaction: creating tables, loading order rows, upserting them
idempotently with MERGE, reading the incremental watermark, and recording
backfill progress so a re-run resumes where it left off.

Authentication uses Application Default Credentials - on Cloud Run this is the
Job's own service account. There is no key file. Grant that service account:
  * roles/bigquery.dataEditor  (on the dataset)
  * roles/bigquery.jobUser     (on the project)
"""
import logging
from typing import Dict, List, Optional

from google.cloud import bigquery

logger = logging.getLogger(__name__)


# ---- schemas ---------------------------------------------------------------
ORDERS_SCHEMA = [
    bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("company", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("time_stamp", "STRING"),
    bigquery.SchemaField("time_of_sale", "TIMESTAMP"),
    bigquery.SchemaField("date_of_sale", "DATE"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
    bigquery.SchemaField("raw_order", "STRING"),   # full order as JSON text
]

STATE_SCHEMA = [
    bigquery.SchemaField("company", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("mode", "STRING"),
    bigquery.SchemaField("window_key", "STRING"),   # e.g. "2025-11" or "incremental"
    bigquery.SchemaField("source", "STRING"),       # "original" / "updated"
    bigquery.SchemaField("status", "STRING"),       # "completed"
    bigquery.SchemaField("orders_loaded", "INT64"),
    bigquery.SchemaField("watermark_time_stamp", "STRING"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
]

STATE_TABLE = "_etl_state"


class BQLoader:
    def __init__(self, project: str, dataset: str, location: str):
        self.project = project
        self.dataset = dataset
        self.location = location
        self.client = bigquery.Client(project=project)

    # ---- naming ----
    def _fq(self, table: str) -> str:
        return f"{self.project}.{self.dataset}.{table}"

    def orders_table(self, app_name: str) -> str:
        return f"orders_{app_name}"

    def staging_table(self, app_name: str) -> str:
        return f"orders_{app_name}_staging"

    # ---- setup ----
    def ensure_dataset(self) -> None:
        ds_id = f"{self.project}.{self.dataset}"
        try:
            self.client.get_dataset(ds_id)
        except Exception:
            ds = bigquery.Dataset(ds_id)
            ds.location = self.location
            self.client.create_dataset(ds, exists_ok=True)
            logger.info(f"Created dataset {ds_id} in {self.location}")

    def ensure_orders_table(self, app_name: str) -> None:
        table_id = self._fq(self.orders_table(app_name))
        try:
            self.client.get_table(table_id)
            return
        except Exception:
            pass
        table = bigquery.Table(table_id, schema=ORDERS_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date_of_sale"
        )
        table.clustering_fields = ["order_id"]
        self.client.create_table(table, exists_ok=True)
        logger.info(f"Created table {table_id} (partitioned by date_of_sale, clustered by order_id)")

    def ensure_state_table(self) -> None:
        table_id = self._fq(STATE_TABLE)
        try:
            self.client.get_table(table_id)
            return
        except Exception:
            pass
        self.client.create_table(bigquery.Table(table_id, schema=STATE_SCHEMA), exists_ok=True)
        logger.info(f"Created state table {table_id}")

    # ---- staging load + MERGE ----
    def reset_staging(self, app_name: str) -> None:
        """Empty the staging table before a new window."""
        staging_id = self._fq(self.staging_table(app_name))
        table = bigquery.Table(staging_id, schema=ORDERS_SCHEMA)
        self.client.delete_table(staging_id, not_found_ok=True)
        self.client.create_table(table, exists_ok=True)

    def append_to_staging(self, app_name: str, rows: List[Dict]) -> None:
        """Load a chunk of rows into staging with a load job (append)."""
        if not rows:
            return
        staging_id = self._fq(self.staging_table(app_name))
        job_config = bigquery.LoadJobConfig(
            schema=ORDERS_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = self.client.load_table_from_json(rows, staging_id, job_config=job_config)
        job.result()  # wait
        if job.errors:
            raise RuntimeError(f"Staging load errors: {job.errors}")

    def merge_staging_into_orders(self, app_name: str) -> int:
        """
        Upsert staging -> orders on order_id. Last write wins (matches the
        original ROW_NUMBER de-dup by newest). Returns rows in staging.
        """
        target = self._fq(self.orders_table(app_name))
        staging = self._fq(self.staging_table(app_name))
        # De-duplicate staging by order_id (keep newest loaded_at) before merging.
        merge_sql = f"""
        MERGE `{target}` T
        USING (
          SELECT * EXCEPT(rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY order_id ORDER BY loaded_at DESC
            ) AS rn
            FROM `{staging}`
          ) WHERE rn = 1
        ) S
        ON T.order_id = S.order_id
        WHEN MATCHED THEN UPDATE SET
          company = S.company, source = S.source, time_stamp = S.time_stamp,
          time_of_sale = S.time_of_sale, date_of_sale = S.date_of_sale,
          loaded_at = S.loaded_at, raw_order = S.raw_order
        WHEN NOT MATCHED THEN INSERT ROW
        """
        self.client.query(merge_sql, location=self.location).result()
        count_sql = f"SELECT COUNT(1) AS c FROM `{staging}`"
        rows = list(self.client.query(count_sql, location=self.location).result())
        return rows[0].c if rows else 0

    # ---- wide append (one real column per field) ----
    def append_flat_rows(self, app_name: str, rows: List[Dict]) -> None:
        """
        Append wide rows into orders_<app>, creating a real column for every
        field. New fields seen in future automatically add new columns
        (ALLOW_FIELD_ADDITION). Business fields load as STRING to avoid type
        clashes; the base helper columns keep their proper types.
        """
        if not rows:
            return
        table_id = self._fq(self.orders_table(app_name))

        base_names = {f.name for f in ORDERS_SCHEMA}
        keys = set()
        for r in rows:
            keys.update(r.keys())

        schema = list(ORDERS_SCHEMA)  # typed base columns
        for k in sorted(keys):
            if k not in base_names:
                schema.append(bigquery.SchemaField(k, "STRING"))

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )
        job = self.client.load_table_from_json(rows, table_id, job_config=job_config)
        job.result()
        if job.errors:
            raise RuntimeError(f"Append load errors: {job.errors}")

    # ---- watermark & state ----
    def get_watermark(self, app_name: str, source: str = "original") -> Optional[str]:
        """Max time_stamp already loaded for this source (the incremental cursor)."""
        target = self._fq(self.orders_table(app_name))
        sql = f"""
          SELECT MAX(time_stamp) AS w
          FROM `{target}`
          WHERE source = @source AND time_stamp IS NOT NULL
        """
        job = self.client.query(
            sql,
            location=self.location,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source)
            ]),
        )
        rows = list(job.result())
        return rows[0].w if rows and rows[0].w else None

    def record_state(
        self, company: str, mode: str, window_key: str, source: str,
        orders_loaded: int, watermark: Optional[str],
    ) -> None:
        table = self._fq(STATE_TABLE)
        # Remove any prior row for this (company, mode, window_key, source), then insert.
        del_sql = f"""
          DELETE FROM `{table}`
          WHERE company=@c AND mode=@m AND window_key=@w AND source=@s
        """
        params = [
            bigquery.ScalarQueryParameter("c", "STRING", company),
            bigquery.ScalarQueryParameter("m", "STRING", mode),
            bigquery.ScalarQueryParameter("w", "STRING", window_key),
            bigquery.ScalarQueryParameter("s", "STRING", source),
        ]
        self.client.query(
            del_sql, location=self.location,
            job_config=bigquery.QueryJobConfig(query_parameters=params),
        ).result()
        insert_sql = f"""
          INSERT INTO `{table}`
          (company, mode, window_key, source, status, orders_loaded,
           watermark_time_stamp, updated_at)
          VALUES (@c, @m, @w, @s, 'completed', @n, @wm, CURRENT_TIMESTAMP())
        """
        self.client.query(
            insert_sql, location=self.location,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("c", "STRING", company),
                bigquery.ScalarQueryParameter("m", "STRING", mode),
                bigquery.ScalarQueryParameter("w", "STRING", window_key),
                bigquery.ScalarQueryParameter("s", "STRING", source),
                bigquery.ScalarQueryParameter("n", "INT64", orders_loaded),
                bigquery.ScalarQueryParameter("wm", "STRING", watermark),
            ]),
        ).result()

    def completed_windows(self, company: str, mode: str, source: str) -> set:
        """Return the set of window_keys already completed (for resume)."""
        table = self._fq(STATE_TABLE)
        sql = f"""
          SELECT window_key FROM `{table}`
          WHERE company=@c AND mode=@m AND source=@s AND status='completed'
        """
        job = self.client.query(
            sql, location=self.location,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("c", "STRING", company),
                bigquery.ScalarQueryParameter("m", "STRING", mode),
                bigquery.ScalarQueryParameter("s", "STRING", source),
            ]),
        )
        return {r.window_key for r in job.result()}
