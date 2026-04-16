
import csv
from pathlib import Path


class CsvHook:
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)

    def read_rows(self):
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")

        with self.file_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def get_headers(self):
        rows = self.read_rows()
        return list(rows[0].keys()) if rows else []