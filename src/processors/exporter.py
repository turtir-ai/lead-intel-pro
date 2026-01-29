import os

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


class Exporter:
    def __init__(self, output_dir="outputs/crm"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def export_targets(self, leads, tag=""):
        df = pd.DataFrame(leads)
        if df.empty:
            return None

        suffix = f"{tag}" if tag else ""
        master_path = os.path.join(self.output_dir, f"targets_master{suffix}.csv")
        df.to_csv(master_path, index=False)

        top100_path = os.path.join(self.output_dir, f"top100{suffix}.csv")
        df.sort_values("score", ascending=False).head(100).to_csv(top100_path, index=False)

        xray_path = os.path.join(self.output_dir, f"linkedin_xray_queries{suffix}.csv")
        xray_df = pd.DataFrame({"query": [self._xray_query(name) for name in df["company"].fillna("")]})
        xray_df.to_csv(xray_path, index=False)

        logger.info(f"Exported {len(df)} leads to {master_path}")
        return master_path

    def _xray_query(self, company_name):
        cleaned = company_name.replace('"', "").strip()
        if not cleaned:
            return ""
        return f'site:linkedin.com/company "{cleaned}"'
