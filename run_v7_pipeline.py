#!/usr/bin/env python3
"""
V7 AUTONOMOUS HUNTER - Unified Pipeline Runner

Bu script tüm V7 modüllerini entegre ederek tek komutla çalışır:

1. BULK DISCOVERY (Phase 1) - PDF/Excel hazine dosyaları bulma
2. HARVEST - Tüm kaynaklardan ham lead toplama
3. ROLE CLASSIFICATION - Müşteri vs Tedarikçi ayrımı
4. WEBSITE RESOLUTION - Gerçek şirket websitesi bulma
5. SCENTING (Phase 2-3) - OEM ve stenter kanıtı arama
6. DEEP VALIDATION - Website, keyword, contact doğrulama
7. TIER CLASSIFICATION - Satışa hazırlık sınıflandırması
8. EXPORT - CRM-ready çıktı üretme

Kullanım:
    python run_v7_pipeline.py --full              # Tam pipeline
    python run_v7_pipeline.py --discover          # Sadece bulk discovery
    python run_v7_pipeline.py --validate          # Mevcut lead'leri doğrula
    python run_v7_pipeline.py --export            # Sadece export
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.collectors.brave_scenter import BraveScenter
from src.processors.lead_role_classifier import LeadRoleClassifier
from src.processors.deep_validator import DeepValidator, TierExporter
from src.processors.data_cleaner import DataCleaner
from src.processors.dedupe import LeadDedupe
from src.utils.logger import get_logger

logger = get_logger(__name__)


# Configuration
CONFIG = {
    "output_dir": "outputs/crm",
    "staging_dir": "data/staging",
    "cache_dir": "data/cache",
    "verified_csv": "/Users/dev/Documents/germany/Doğrulama/Onaylanmış Stenter Yedek Parça Müşteri Listesi - Table 1.csv",
    "targets_master": "outputs/crm/targets_master.csv",
}


class V7Pipeline:
    """
    V7 Autonomous Hunter Pipeline.
    
    Integrates all modules for complete lead intelligence.
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or CONFIG
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize modules
        self.scenter = BraveScenter()
        self.role_classifier = LeadRoleClassifier()
        self.deep_validator = DeepValidator()
        self.data_cleaner = DataCleaner()
        self.deduper = LeadDedupe()
        
        # Ensure directories
        for dir_key in ["output_dir", "staging_dir", "cache_dir"]:
            os.makedirs(self.config[dir_key], exist_ok=True)
        
        # Stats
        self.stats = {
            "phase": "",
            "total_leads": 0,
            "after_dedupe": 0,
            "after_role_filter": 0,
            "websites_resolved": 0,
            "sce_sales_ready": 0,
            "tier_1": 0,
            "tier_2": 0,
            "tier_3": 0,
        }
    
    def run_full_pipeline(
        self,
        discover: bool = True,
        validate: bool = True,
        limit: Optional[int] = None,
    ) -> str:
        """
        Run the complete V7 pipeline.
        
        Args:
            discover: Run bulk discovery phase
            validate: Run deep validation
            limit: Limit number of leads to process
            
        Returns:
            Path to final output file
        """
        logger.info("=" * 70)
        logger.info("V7 AUTONOMOUS HUNTER PIPELINE")
        logger.info("=" * 70)
        
        # Phase 1: Bulk Discovery (optional)
        if discover:
            self._phase1_bulk_discovery()
        
        # Phase 2: Load & Merge Leads
        leads = self._phase2_load_leads()
        self.stats["total_leads"] = len(leads)
        logger.info(f"Loaded {len(leads)} total leads")
        
        # Apply limit if specified
        if limit:
            leads = leads[:limit]
            logger.info(f"Limited to {len(leads)} leads for testing")
        
        # Phase 3: Deduplicate
        leads = self._phase3_dedupe(leads)
        self.stats["after_dedupe"] = len(leads)
        
        # Phase 4: Role Classification & Noise Filter
        leads = self._phase4_role_filter(leads)
        self.stats["after_role_filter"] = len(leads)
        
        # Phase 5: Scenting (Website Resolution + Evidence)
        leads = self._phase5_scenting(leads)
        
        # Phase 6: Deep Validation (optional)
        if validate:
            leads = self._phase6_deep_validation(leads)
        
        # Phase 7: Export
        output_path = self._phase7_export(leads)
        
        # Print summary
        self._print_summary()
        
        return output_path
    
    def _phase1_bulk_discovery(self) -> List[Dict]:
        """Phase 1: Search for bulk lead sources (PDFs, Excel files)."""
        self.stats["phase"] = "bulk_discovery"
        logger.info("\n" + "-" * 50)
        logger.info("PHASE 1: BULK DISCOVERY")
        logger.info("-" * 50)
        
        # Search for treasure files
        files = self.scenter.phase1_bulk_discovery(region="global")
        
        # Save discovered files
        if files:
            output_path = os.path.join(self.config["staging_dir"], f"discovered_files_{self.timestamp}.csv")
            pd.DataFrame(files).to_csv(output_path, index=False)
            logger.info(f"Saved {len(files)} discovered files to {output_path}")
        
        return files
    
    def _phase2_load_leads(self) -> List[Dict]:
        """Phase 2: Load leads from all sources."""
        self.stats["phase"] = "load_leads"
        logger.info("\n" + "-" * 50)
        logger.info("PHASE 2: LOAD & MERGE LEADS")
        logger.info("-" * 50)
        
        all_leads = []
        
        # Source 1: Verified customers (gold standard)
        verified_csv = self.config.get("verified_csv")
        if verified_csv and os.path.exists(verified_csv):
            verified_df = pd.read_csv(verified_csv)
            
            # Normalize columns
            col_map = {
                "Şirket Adı": "company",
                "Ülke": "country",
                "Neden Onaylı? (Kanıt/Makine)": "evidence_reason",
                "Hedef Ürün (HS Kodu)": "hs_code",
            }
            verified_df = verified_df.rename(columns=col_map)
            verified_df["source_type"] = "verified_list"
            verified_df["source"] = "manual_verification"
            
            verified_leads = verified_df.to_dict(orient="records")
            all_leads.extend(verified_leads)
            logger.info(f"Loaded {len(verified_leads)} verified leads")
        
        # Source 2: Pipeline targets_master
        targets_csv = self.config.get("targets_master")
        if targets_csv and os.path.exists(targets_csv):
            targets_df = pd.read_csv(targets_csv)
            targets_df["source_type"] = "pipeline"
            
            # Only take essential columns
            keep_cols = ["company", "country", "website", "emails", "phones", 
                        "context", "source_type", "source"]
            available_cols = [c for c in keep_cols if c in targets_df.columns]
            targets_df = targets_df[available_cols]
            
            targets_leads = targets_df.to_dict(orient="records")
            all_leads.extend(targets_leads)
            logger.info(f"Loaded {len(targets_leads)} pipeline leads")
        
        return all_leads
    
    def _phase3_dedupe(self, leads: List[Dict]) -> List[Dict]:
        """Phase 3: Deduplicate leads."""
        self.stats["phase"] = "dedupe"
        logger.info("\n" + "-" * 50)
        logger.info("PHASE 3: DEDUPLICATION")
        logger.info("-" * 50)
        
        # Simple name-based dedupe
        seen = set()
        unique_leads = []
        
        for lead in leads:
            company = str(lead.get("company", "")).lower().strip()
            if not company or company in seen:
                continue
            seen.add(company)
            unique_leads.append(lead)
        
        logger.info(f"Dedupe: {len(leads)} -> {len(unique_leads)} unique leads")
        return unique_leads
    
    def _phase4_role_filter(self, leads: List[Dict]) -> List[Dict]:
        """Phase 4: Filter by role (keep customers, drop suppliers)."""
        self.stats["phase"] = "role_filter"
        logger.info("\n" + "-" * 50)
        logger.info("PHASE 4: ROLE CLASSIFICATION")
        logger.info("-" * 50)
        
        filtered = []
        dropped_suppliers = []
        
        for lead in leads:
            # Classify role
            role_result = self.role_classifier.classify(lead)
            lead["role"] = role_result.role
            lead["role_confidence"] = role_result.confidence
            
            # Check noise filter
            company = lead.get("company", "")
            context = lead.get("context", "")
            is_noise = self.data_cleaner.is_noise(company)
            is_non_customer = self.data_cleaner.is_non_customer(company, context)
            
            # Keep only CUSTOMER and UNKNOWN roles (drop SUPPLIER/INTERMEDIARY)
            if role_result.role in ["CUSTOMER", "UNKNOWN"] and not is_noise and not is_non_customer:
                filtered.append(lead)
            else:
                dropped_suppliers.append(lead)
        
        # Save dropped suppliers for review
        if dropped_suppliers:
            dropped_path = os.path.join(self.config["staging_dir"], f"dropped_suppliers_{self.timestamp}.csv")
            pd.DataFrame(dropped_suppliers).to_csv(dropped_path, index=False)
            logger.info(f"Dropped {len(dropped_suppliers)} suppliers/intermediaries (saved to {dropped_path})")
        
        logger.info(f"Role filter: {len(leads)} -> {len(filtered)} customers")
        return filtered
    
    def _phase5_scenting(self, leads: List[Dict]) -> List[Dict]:
        """Phase 5: Brave scenting (website resolution + evidence)."""
        self.stats["phase"] = "scenting"
        logger.info("\n" + "-" * 50)
        logger.info("PHASE 5: BRAVE SCENTING")
        logger.info("-" * 50)
        
        if not os.environ.get("BRAVE_API_KEY"):
            logger.warning("BRAVE_API_KEY not set - skipping scenting")
            return leads
        
        # Run scenting on all leads
        scented_leads = self.scenter.scent_leads_batch(leads)
        
        # Count results
        websites_resolved = sum(1 for l in scented_leads if l.get("website_source") == "brave_navigation")
        sce_ready = sum(1 for l in scented_leads if l.get("sce_sales_ready"))
        
        self.stats["websites_resolved"] = websites_resolved
        self.stats["sce_sales_ready"] = sce_ready
        
        logger.info(f"Scenting complete: {websites_resolved} websites resolved, {sce_ready} SCE sales ready")
        
        return scented_leads
    
    def _phase6_deep_validation(self, leads: List[Dict]) -> List[Dict]:
        """Phase 6: Deep validation (website check, keywords, contacts)."""
        self.stats["phase"] = "deep_validation"
        logger.info("\n" + "-" * 50)
        logger.info("PHASE 6: DEEP VALIDATION")
        logger.info("-" * 50)
        
        validated_leads = self.deep_validator.validate_batch(leads)
        
        # Count tiers
        self.stats["tier_1"] = sum(1 for l in validated_leads if l.get("tier") == 1)
        self.stats["tier_2"] = sum(1 for l in validated_leads if l.get("tier") == 2)
        self.stats["tier_3"] = sum(1 for l in validated_leads if l.get("tier") == 3)
        
        logger.info(f"Validation complete: T1={self.stats['tier_1']}, T2={self.stats['tier_2']}, T3={self.stats['tier_3']}")
        
        return validated_leads
    
    def _phase7_export(self, leads: List[Dict]) -> str:
        """Phase 7: Export final leads."""
        self.stats["phase"] = "export"
        logger.info("\n" + "-" * 50)
        logger.info("PHASE 7: EXPORT")
        logger.info("-" * 50)
        
        output_dir = self.config["output_dir"]
        
        # Export all leads
        all_path = os.path.join(output_dir, f"v7_all_leads_{self.timestamp}.csv")
        df = pd.DataFrame(leads)
        df.to_csv(all_path, index=False)
        logger.info(f"Exported all {len(leads)} leads to {all_path}")
        
        # Export by tier
        tier_files = TierExporter.export_by_tier(leads, output_dir, self.timestamp)
        
        # Export sales-ready (SCE or Tier 1)
        sales_ready = [
            l for l in leads 
            if l.get("sce_sales_ready") or l.get("tier") == 1
        ]
        if sales_ready:
            sales_path = os.path.join(output_dir, f"v7_sales_ready_{self.timestamp}.csv")
            pd.DataFrame(sales_ready).to_csv(sales_path, index=False)
            logger.info(f"Exported {len(sales_ready)} sales-ready leads to {sales_path}")
        
        return all_path
    
    def _print_summary(self):
        """Print pipeline summary with P0/P1 metrics."""
        logger.info("\n" + "=" * 70)
        logger.info("V7 PIPELINE SUMMARY")
        logger.info("=" * 70)
        
        total = self.stats['total_leads']
        after_dedupe = self.stats['after_dedupe']
        after_filter = self.stats['after_role_filter']
        tier1 = self.stats['tier_1']
        tier2 = self.stats['tier_2']
        tier3 = self.stats['tier_3']
        
        logger.info(f"Total leads loaded:     {total}")
        logger.info(f"After dedupe:           {after_dedupe}")
        logger.info(f"After role filter:      {after_filter}")
        logger.info(f"Websites resolved:      {self.stats['websites_resolved']}")
        logger.info(f"SCE Sales Ready:        {self.stats['sce_sales_ready']}")
        logger.info("-" * 50)
        logger.info(f"Tier 1 (Full):          {tier1}")
        logger.info(f"Tier 2 (Promising):     {tier2}")
        logger.info(f"Tier 3 (Research):      {tier3}")
        logger.info("=" * 70)
        
        # P0: Key metrics for tracking improvement
        if after_filter > 0:
            tier1_rate = (tier1 / after_filter) * 100
            websites_rate = (self.stats['websites_resolved'] / after_filter) * 100
            sce_rate = (self.stats['sce_sales_ready'] / after_filter) * 100
            
            logger.info("\n📊 KEY METRICS (P0 Tracking):")
            logger.info(f"  Tier 1 Rate:          {tier1_rate:.1f}%")
            logger.info(f"  Websites Resolved:    {websites_rate:.1f}%")
            logger.info(f"  SCE Sales Ready:      {sce_rate:.1f}%")
        
        # Scenter stats
        scenter_stats = self.scenter.get_stats()
        logger.info("\nBrave Scenter Stats:")
        for key, val in scenter_stats.items():
            logger.info(f"  {key}: {val}")
        
        # Validator stats (now includes fail_reasons)
        validator_stats = self.deep_validator.get_stats()
        logger.info("\nDeep Validator Stats:")
        for key, val in validator_stats.items():
            if key == "fail_reasons" and isinstance(val, dict):
                logger.info(f"  {key}:")
                for reason, count in sorted(val.items(), key=lambda x: -x[1]):
                    logger.info(f"    - {reason}: {count}")
            else:
                logger.info(f"  {key}: {val}")
        
        # P0: Save metrics to file for tracking
        self._save_metrics()
    
    def _save_metrics(self):
        """P0: Save run metrics to CSV for tracking improvement over time."""
        import csv
        
        metrics_file = os.path.join(self.config["output_dir"], "pipeline_metrics.csv")
        
        total = self.stats['total_leads']
        after_filter = self.stats['after_role_filter']
        tier1 = self.stats['tier_1']
        
        metrics = {
            "timestamp": self.timestamp,
            "total_leads": total,
            "after_dedupe": self.stats['after_dedupe'],
            "after_role_filter": after_filter,
            "websites_resolved": self.stats['websites_resolved'],
            "sce_sales_ready": self.stats['sce_sales_ready'],
            "tier_1": tier1,
            "tier_2": self.stats['tier_2'],
            "tier_3": self.stats['tier_3'],
            "tier_1_rate": f"{(tier1 / after_filter * 100):.1f}" if after_filter > 0 else "0",
            "websites_rate": f"{(self.stats['websites_resolved'] / after_filter * 100):.1f}" if after_filter > 0 else "0",
        }
        
        # Append to CSV
        file_exists = os.path.exists(metrics_file)
        with open(metrics_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=metrics.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(metrics)
        
        logger.info(f"\n📈 Metrics saved to {metrics_file}")


def main():
    parser = argparse.ArgumentParser(
        description="V7 Autonomous Hunter Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_v7_pipeline.py --full              # Run complete pipeline
    python run_v7_pipeline.py --full --limit 50   # Test with 50 leads
    python run_v7_pipeline.py --discover          # Only bulk discovery
    python run_v7_pipeline.py --validate          # Only validation (skip discovery)
        """
    )
    
    parser.add_argument("--full", action="store_true", help="Run complete pipeline")
    parser.add_argument("--discover", action="store_true", help="Run bulk discovery only")
    parser.add_argument("--validate", action="store_true", help="Skip discovery, run validation")
    parser.add_argument("--limit", type=int, help="Limit number of leads to process")
    parser.add_argument("--no-validation", action="store_true", help="Skip deep validation")
    
    args = parser.parse_args()
    
    pipeline = V7Pipeline()
    
    if args.discover:
        pipeline._phase1_bulk_discovery()
    elif args.full or args.validate:
        pipeline.run_full_pipeline(
            discover=not args.validate,
            validate=not args.no_validation,
            limit=args.limit,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
