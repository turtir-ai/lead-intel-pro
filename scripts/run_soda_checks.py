import shutil
import subprocess
from pathlib import Path

import duckdb

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _load_csv_table(con, csv_path: Path, table_name: str) -> None:
    if not csv_path.exists():
        return
    con.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS "
        f"SELECT * FROM read_csv_auto('{csv_path}', HEADER=TRUE, SAMPLE_SIZE=-1)"
    )


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    db_path = project_root / "data" / "lead_intel.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    targets_csv = project_root / "outputs" / "crm" / "targets_master.csv"
    leads_csv = project_root / "data" / "processed" / "leads_master.csv"
    if not targets_csv.exists() and not leads_csv.exists():
        logger.error("No CSV outputs found for Soda checks.")
        return 1

    con = duckdb.connect(str(db_path))
    try:
        _load_csv_table(con, targets_csv, "targets_master")
        _load_csv_table(con, leads_csv, "leads_master")
    finally:
        con.close()

    if not shutil.which("soda"):
        logger.error("Soda CLI not found. Install soda-core to enable checks.")
        return 1

    config_path = project_root / "soda" / "configuration.yml"
    checks_path = project_root / "soda" / "checks.yml"
    if not config_path.exists() or not checks_path.exists():
        logger.error("Soda config/checks missing.")
        return 1

    cmd = [
        "soda",
        "scan",
        "-d",
        "lead_intel",
        "-c",
        str(config_path),
        str(checks_path),
    ]
    logger.info("Running Soda checks...")
    result = subprocess.run(cmd, check=False)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
