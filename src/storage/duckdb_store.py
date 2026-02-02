
import duckdb
import pandas as pd
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

class LeadStore:
    """
    V5 Storage Engine: DuckDB.
    Replaces purely CSV-based storage with an in-process SQL database.
    """
    
    def __init__(self, db_path="data/lead_intel.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        """Initialize DB directory and connection."""
        return

    def _safe_table(self, name: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", name or "")
        return cleaned or "table"

    def save_dataframe(self, df: pd.DataFrame, table: str, replace: bool = True):
        """Save DataFrame into DuckDB with dynamic schema."""
        if df.empty:
            return
        table = self._safe_table(table)
        self.con.register("df_view", df)
        if replace:
            self.con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM df_view")
        else:
            self.con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df_view WHERE 1=0")
            self.con.execute(f"INSERT INTO {table} SELECT * FROM df_view")
        self.con.unregister("df_view")

    def save_raw(self, df: pd.DataFrame):
        """Save raw leads to DuckDB."""
        if df.empty:
            return
        self.save_dataframe(df, "leads_raw", replace=True)
        logger.info(f"💾 Stored {len(df)} raw leads in DuckDB")

    def save_master(self, df: pd.DataFrame):
        """Save master leads (overwrite or merge strategy)."""
        if df.empty:
            return
        self.save_dataframe(df, "leads_master", replace=True)
        logger.info(f"💾 Stored {len(df)} master leads in DuckDB")

    def export_csv(self, table="leads_master", path="outputs/targets.csv"):
        """Export table to CSV."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"COPY {table} TO '{path}' (HEADER, DELIMITER ',')")

    def export_parquet(self, table="leads_master", path="outputs/targets.parquet"):
        """Export table to Parquet."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")

    def close(self):
        self.con.close()
