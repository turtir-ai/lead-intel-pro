import os

import pandas as pd
import yaml

from src.utils.logger import get_logger

logger = get_logger(__name__)


class Exporter:
    def __init__(self, output_dir="outputs/crm", scoring_config=None):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # GPT Audit Fix: Load and enforce scoring.yaml export filters
        self.scoring_config = scoring_config or self._load_scoring_config()
        self.export_cfg = self.scoring_config.get("export", {})
        
    def _load_scoring_config(self):
        """Load scoring.yaml if available."""
        config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "scoring.yaml")
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f)
            except Exception as e:
                logger.warning(f"Failed to load scoring config: {e}")
        return {}

    def export_targets(self, leads, tag=""):
        df = pd.DataFrame(leads)
        if df.empty:
            return None
        
        # GPT Audit Fix: Apply export filters from scoring.yaml
        df = self._apply_export_filters(df)
        
        if df.empty:
            logger.warning("No leads passed export filters")
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
    
    def _apply_export_filters(self, df):
        """Apply export filters from scoring.yaml - GPT Audit Fix."""
        original_count = len(df)
        
        # 1. Filter by allowed source types
        allowed_sources = set(self.export_cfg.get("allowed_source_types", []))
        if allowed_sources and "source_type" in df.columns:
            df = df[df["source_type"].isin(allowed_sources)]
            logger.info(f"Source type filter: {original_count} -> {len(df)}")
        
        # 2. Filter by minimum score
        min_score = self.export_cfg.get("min_score", 0)
        if min_score and "score" in df.columns:
            before = len(df)
            df = df[df["score"] >= min_score]
            logger.info(f"Min score filter ({min_score}): {before} -> {len(df)}")
        
        # 3. Exclude by name keywords
        exclude_keywords = self.export_cfg.get("exclude_name_keywords", [])
        if exclude_keywords and "company" in df.columns:
            before = len(df)
            pattern = "|".join(exclude_keywords)
            df = df[~df["company"].str.lower().str.contains(pattern, na=False)]
            logger.info(f"Name exclusion filter: {before} -> {len(df)}")
        
        # 4. Require reachability (website/email)
        require_reach = self.export_cfg.get("require_reachability", False)
        reach_level = self.export_cfg.get("require_reachability_level", "website")
        if require_reach:
            before = len(df)
            if reach_level == "website" and "website" in df.columns:
                df = df[df["website"].notna() & (df["website"] != "") & (df["website"] != "[]")]
            elif reach_level == "email" and "emails" in df.columns:
                df = df[df["emails"].notna() & (df["emails"] != "") & (df["emails"] != "[]")]
            logger.info(f"Reachability filter ({reach_level}): {before} -> {len(df)}")
        
        # 5. Filter out parts suppliers (keep only non-flagged)
        if "is_parts_supplier" in df.columns:
            before = len(df)
            parts_suppliers = df[df["is_parts_supplier"] == True]
            df = df[df["is_parts_supplier"] != True]
            if len(parts_suppliers) > 0:
                # Export parts suppliers separately
                parts_path = os.path.join(self.output_dir, "parts_suppliers.csv")
                parts_suppliers.to_csv(parts_path, index=False)
                logger.info(f"Parts supplier filter: {before} -> {len(df)} (saved {len(parts_suppliers)} to parts_suppliers.csv)")
        
        # 6. Filter out low-grade entities (C grade with no website)
        if "entity_grade" in df.columns and "website" in df.columns:
            before = len(df)
            # Keep A and B grades, only keep C if they have website
            mask = (df["entity_grade"].isin(["A", "B"])) | \
                   ((df["entity_grade"] == "C") & df["website"].notna() & (df["website"] != "") & (df["website"] != "[]"))
            df = df[mask]
            logger.info(f"Grade filter (C without website removed): {before} -> {len(df)}")
        
        return df

    def _apply_region_quotas(self, df):
        """GPT Fix #6: Apply region quotas to prevent over-representation."""
        quotas = self.export_cfg.get("region_quotas", {})
        if not quotas or "country" not in df.columns:
            return df
        
        # Define region mappings
        SOUTH_AMERICA = {"brazil", "argentina", "colombia", "ecuador", "peru", "chile", "paraguay", "uruguay", "venezuela", "bolivia"}
        NORTH_AMERICA = {"mexico", "usa", "united states", "canada"}
        EUROPE = {"germany", "italy", "spain", "portugal", "france", "uk", "poland", "romania", "greece", "turkey"}
        ASIA = {"india", "bangladesh", "pakistan", "vietnam", "indonesia", "china", "thailand", "sri lanka"}
        AFRICA = {"egypt", "morocco", "tunisia", "south africa", "ethiopia", "kenya"}
        
        def get_region(country):
            c = (country or "").lower().strip()
            if c in SOUTH_AMERICA:
                return "south_america"
            elif c in NORTH_AMERICA:
                return "north_america"
            elif c in EUROPE:
                return "europe"
            elif c in ASIA:
                return "asia"
            elif c in AFRICA:
                return "africa"
            return "other"
        
        df = df.copy()
        df["_region"] = df["country"].apply(get_region)
        
        # Sort by score within each region
        if "score" in df.columns:
            df = df.sort_values("score", ascending=False)
        
        # Apply quotas
        result_dfs = []
        for region, quota in quotas.items():
            region_df = df[df["_region"] == region].head(quota)
            result_dfs.append(region_df)
            logger.info(f"Region quota {region}: {len(df[df['_region'] == region])} -> {len(region_df)} (quota: {quota})")
        
        # Include regions not in quotas (no limit)
        quota_regions = set(quotas.keys())
        other_df = df[~df["_region"].isin(quota_regions)]
        result_dfs.append(other_df)
        
        result = pd.concat(result_dfs, ignore_index=True)
        result = result.drop(columns=["_region"])
        
        return result

    def _xray_query(self, company_name):
        cleaned = company_name.replace('"', "").strip()
        if not cleaned:
            return ""
        return f'site:linkedin.com/company "{cleaned}"'
