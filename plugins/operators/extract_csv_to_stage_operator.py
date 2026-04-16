
from datetime import datetime

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from plugins.hooks.csv_hook import CsvHook


class ExtractCsvToStageOperator(BaseOperator):
    template_fields = ("file_path", "logical_date")

    def __init__(
        self,
        postgres_conn_id: str,
        file_path: str,
        staging_table: str,
        logical_date: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.file_path = file_path
        self.staging_table = staging_table
        self.logical_date = logical_date

    def execute(self, context):
        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        csv_hook = CsvHook(self.file_path)
        rows = csv_hook.read_rows()

        if not rows:
            self.log.info("No rows found in file %s", self.file_path)
            return

        conn = pg.get_conn()
        cur = conn.cursor()

        headers = list(rows[0].keys())

        schema_name, table_name = self.staging_table.split(".")
        existing_cols_sql = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
        """
        cur.execute(existing_cols_sql, (schema_name, table_name))
        existing_cols = {r[0] for r in cur.fetchall()}

        for col in headers:
            if col not in existing_cols:
                alter_sql = f'ALTER TABLE {self.staging_table} ADD COLUMN "{col}" TEXT'
                self.log.info("Adding new column to staging: %s", col)
                cur.execute(alter_sql)

        metadata_cols = ["logical_date", "ingestion_ts", "source_file"]
        all_cols = headers + metadata_cols

        values = []
        for row in rows:
            values.append(
                tuple(row.get(col) for col in headers) + (
                    self.logical_date,
                    datetime.utcnow(),
                    self.file_path,
                )
            )

        column_sql = ",".join([f'"{c}"' for c in all_cols])
        insert_sql = f"""
            INSERT INTO {self.staging_table} ({column_sql})
            VALUES %s
        """

        execute_values(cur, insert_sql, values, page_size=5000)
        conn.commit()

        self.log.info(
            "staging_load_complete logical_date=%s rows=%s file=%s",
            self.logical_date,
            len(values),
            self.file_path,
        )