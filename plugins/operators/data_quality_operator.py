
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    template_fields = ("logical_date",)

    def __init__(
        self,
        postgres_conn_id: str,
        logical_date: str,
        source_table: str,
        reject_table: str,
        max_null_ratio: float = 0.05,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.logical_date = logical_date
        self.source_table = source_table
        self.reject_table = reject_table
        self.max_null_ratio = max_null_ratio

    def execute(self, context):
        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        total_sql = f"""
            SELECT COUNT(*)
            FROM {self.source_table}
            WHERE logical_date = %s
        """

        null_sql = f"""
            SELECT COUNT(*)
            FROM {self.source_table}
            WHERE logical_date = %s
              AND (
                    device_id IS NULL
                 OR event_timestamp IS NULL
                 OR event_date IS NULL
                 OR device_id = ''
                 OR event_timestamp = ''
                 OR event_date = ''
              )
        """

        critical_null_reject_sql = f"""
            INSERT INTO {self.reject_table}
            SELECT
                s.device_id,
                s.event_timestamp,
                s.event_date,
                s.event_type,
                s.metric_value,
                s.logical_date,
                s.ingestion_ts,
                s.source_file,
                'CRITICAL_NULL' AS reject_reason,
                NOW() AS rejected_at
            FROM {self.source_table} s
            WHERE s.logical_date = %s
              AND (
                    s.device_id IS NULL
                 OR s.event_timestamp IS NULL
                 OR s.event_date IS NULL
                 OR s.device_id = ''
                 OR s.event_timestamp = ''
                 OR s.event_date = ''
              )
        """

        orphan_reject_sql = f"""
            INSERT INTO {self.reject_table}
            SELECT
                s.device_id,
                s.event_timestamp,
                s.event_date,
                s.event_type,
                s.metric_value,
                s.logical_date,
                s.ingestion_ts,
                s.source_file,
                'ORPHAN_DEVICE' AS reject_reason,
                NOW() AS rejected_at
            FROM {self.source_table} s
            LEFT JOIN core.device_master d
              ON s.device_id = d.device_id
            WHERE s.logical_date = %s
              AND s.device_id IS NOT NULL
              AND s.device_id <> ''
              AND d.device_id IS NULL
        """

        delete_previous_rejects_sql = f"""
            DELETE FROM {self.reject_table}
            WHERE logical_date = %s
        """

        total_rows = pg.get_first(total_sql, parameters=(self.logical_date,))[0]
        null_rows = pg.get_first(null_sql, parameters=(self.logical_date,))[0]
        null_ratio = (null_rows / total_rows) if total_rows else 0

        self.log.info(
            "dq_metrics logical_date=%s total_rows=%s null_rows=%s null_ratio=%.4f",
            self.logical_date,
            total_rows,
            null_rows,
            null_ratio,
        )

        pg.run(delete_previous_rejects_sql, parameters=(self.logical_date,))
        pg.run(critical_null_reject_sql, parameters=(self.logical_date,))
        pg.run(orphan_reject_sql, parameters=(self.logical_date,))

        rejected_sql = f"""
            SELECT COUNT(*)
            FROM {self.reject_table}
            WHERE logical_date = %s
        """
        rejected_rows = pg.get_first(rejected_sql, parameters=(self.logical_date,))[0]

        self.log.info(
            "dq_rejections logical_date=%s rejected_rows=%s",
            self.logical_date,
            rejected_rows,
        )

        if null_ratio > self.max_null_ratio:
            raise AirflowFailException(
                f"DQ failed: critical null ratio {null_ratio:.2%} exceeds threshold {self.max_null_ratio:.2%}"
            )