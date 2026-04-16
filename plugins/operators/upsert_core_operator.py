
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class UpsertCoreOperator(BaseOperator):
    template_fields = ("logical_date",)

    def __init__(
        self,
        postgres_conn_id: str,
        logical_date: str,
        source_table: str,
        target_table: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.logical_date = logical_date
        self.source_table = source_table
        self.target_table = target_table

    def execute(self, context):
        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        upsert_sql = f"""
            INSERT INTO {self.target_table} (
                device_id,
                event_timestamp,
                event_date,
                event_type,
                metric_value,
                logical_date,
                updated_at
            )
            SELECT
                s.device_id,
                s.event_timestamp::timestamp,
                s.event_date::date,
                s.event_type,
                NULLIF(s.metric_value, '')::numeric,
                s.logical_date,
                NOW()
            FROM {self.source_table} s
            LEFT JOIN {self.source_table} sr
              ON 1 = 0
            WHERE s.logical_date = %s
              AND NOT EXISTS (
                    SELECT 1
                    FROM staging.telemetry_rejects r
                    WHERE r.logical_date = s.logical_date
                      AND r.device_id IS NOT DISTINCT FROM s.device_id
                      AND r.event_timestamp IS NOT DISTINCT FROM s.event_timestamp
                      AND r.event_type IS NOT DISTINCT FROM s.event_type
                )
            ON CONFLICT (device_id, event_timestamp, event_type)
            DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                event_date = EXCLUDED.event_date,
                logical_date = EXCLUDED.logical_date,
                updated_at = NOW()
        """

        count_sql = f"""
            SELECT COUNT(*)
            FROM {self.target_table}
            WHERE logical_date = %s
        """

        before_count = pg.get_first(count_sql, parameters=(self.logical_date,))[0]
        pg.run(upsert_sql, parameters=(self.logical_date,))
        after_count = pg.get_first(count_sql, parameters=(self.logical_date,))[0]

        self.log.info(
            "core_upsert_complete logical_date=%s before_count=%s after_count=%s",
            self.logical_date,
            before_count,
            after_count,
        )