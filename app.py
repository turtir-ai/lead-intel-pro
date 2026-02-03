"""
LeadIntel Pro V8 - Unified Pipeline
====================================

Tüm P0/P1/P2 iyileştirmelerini içeren entegre pipeline:

P0 (Kritik):
- phonenumbers kütüphanesi ile telefon extraction (SVG artifact fix)
- SSL fallback (HTTPS -> HTTP) + fail_reason tracking
- Email blocklist filtering (noreply, example.com vb.)

P1 (Bu Hafta):
- Directory path pattern detection (GOTS, member pages)
- Enhanced role classification

P2 (Sonraki):
- Evidence context window capture
- Proximity scoring (OEM+keyword)
- Pipeline metrics tracking

Kullanım:
    python app.py v8                    # Tam V8 pipeline
    python app.py v8 --limit 50         # Test için 50 lead
    python app.py v8 --skip-discovery   # Discovery'siz
    python app.py all                   # Legacy pipeline
"""

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import yaml
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.collectors.bettercotton_members import BetterCottonMembers
from src.collectors.competitor_harvester import CompetitorHarvester
from src.collectors.competitor_websearch import CompetitorWebSearch
from src.collectors.egypt_textile_export_council import EgyptTextileExportCouncil
from src.collectors.exhibitor_list import ExhibitorListCollector
from src.collectors.fairs_harvester import FairsHarvester
from src.collectors.gots_directory import GotsCertifiedSuppliers
from src.collectors.texbrasil_companies import TexbrasilCompanies
from src.collectors.trade_fetcher import TradeFetcher
from src.collectors.oekotex_directory import OekoTexDirectory
from src.collectors.bluesign_partners import BluesignPartners
from src.collectors.amith_directory import AmithDirectory
from src.collectors.abit_directory import AbitDirectory
from src.collectors.known_manufacturers import KnownManufacturersCollector
from src.collectors.discovery.brave_search import BraveSearchClient, SourceDiscovery
# GPT V10.4: South America collectors
from src.collectors.colombiatex_harvester import ColombiatexHarvester
from src.collectors.emitex_harvester import EmitexHarvester
from src.collectors.peru_moda_harvester import PeruModaHarvester
from src.collectors.itmf_bootstrap import ITMFBootstrap
from src.processors.dedupe import LeadDedupe
from src.processors.enricher import Enricher
from src.processors.entity_extractor import EntityExtractor
from src.processors.entity_quality_gate_v2 import EntityQualityGateV2
from src.processors.lead_role_classifier import LeadRoleClassifier
from src.processors.exporter import Exporter
# Phase 1: Data Quality Foundation
from src.processors.data_cleaner import DataCleaner
from src.processors.entity_validator import EntityValidator
from src.processors.pdf_processor import PdfProcessor
# Phase 3: Advanced Discovery
from src.collectors.latam_sources import LATAMSourcesOrchestrator
from src.collectors.discovery.network_sniffer import GOTSDirectorySniffer
from src.processors.scorer import Scorer
# GPT V10.4: SCE scoring and quality reporting
from src.processors.sce_scorer import SCEScorer
from src.processors.quality_reporter import QualityReporter
from src.processors.enrichment_queue import EnrichmentQueue
# V8: Advanced scenting and deep validation
from src.collectors.brave_scenter import BraveScenter
from src.processors.deep_validator import DeepValidator, TierExporter
# GPT V3: Import fix functions
try:
    from gpt_v3_fix import fix_schema, apply_noise_filter, enhanced_role_classify, validate_sce_sales_ready, export_split
except ImportError:
    # Fallback if gpt_v3_fix not available
    fix_schema = lambda df: df
    apply_noise_filter = lambda df: (df, pd.DataFrame())
    enhanced_role_classify = lambda row: row.get("role", "UNKNOWN")
    validate_sce_sales_ready = lambda row: row.get("sce_sales_ready", False)
    export_split = lambda df: (df, pd.DataFrame(), pd.DataFrame(), df)

from src.utils.logger import get_logger

logger = get_logger(__name__)


# =============================================================================
# V8 PIPELINE CLASS
# =============================================================================

class V8Pipeline:
    """
    LeadIntel Pro V8 - Unified Pipeline with all P0/P1/P2 improvements.
    
    Features:
    - 9-phase pipeline: Discovery -> Load -> Dedupe -> Role -> Scenting -> Heuristic -> SCE -> Validation -> Export
    - phonenumbers library for phone extraction (no SVG artifacts)
    - SSL fallback with fail_reason tracking
    - Email blocklist filtering
    - Directory URL detection
    - Evidence context windows
    - Proximity scoring
    - Multi-source collection
    - Heuristic + SCE dual scoring
    - Metrics tracking
    """
    
    def __init__(self, config: Dict = None):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Load configurations
        self.targets = load_config("config/targets.yaml")
        self.scoring_config = load_config("config/scoring.yaml")
        self.sources_config = load_config("config/sources.yaml")
        self.settings = load_config("config/settings.yaml")
        
        # Default paths
        self.config = config or {
            "output_dir": "outputs/crm",
            "staging_dir": "data/staging",
            "processed_dir": "data/processed",
            "cache_dir": "data/cache",
            "verified_csv": None,
            "targets_master": "outputs/crm/targets_master.csv",
        }
        
        # Initialize all modules
        self.scenter = BraveScenter()
        self.role_classifier = LeadRoleClassifier()
        self.deep_validator = DeepValidator()
        self.data_cleaner = DataCleaner()
        self.deduper = LeadDedupe()
        self.scorer = Scorer(self.targets, self.scoring_config)
        self.sce_scorer = SCEScorer()
        self.exporter = Exporter()
        
        # Ensure directories
        for dir_key in ["output_dir", "staging_dir", "processed_dir", "cache_dir"]:
            if dir_key in self.config:
                os.makedirs(self.config[dir_key], exist_ok=True)
        
        # Stats
        self.stats = {
            "phase": "",
            "total_leads": 0,
            "after_dedupe": 0,
            "after_role_filter": 0,
            "websites_resolved": 0,
            "sce_sales_ready": 0,
            "heuristic_high": 0,
            "tier_1": 0,
            "tier_2": 0,
            "tier_3": 0,
        }
    
    def run(
        self,
        discover: bool = True,
        validate: bool = True,
        limit: Optional[int] = None,
        source_file: Optional[str] = None,
    ) -> str:
        """
        Run the complete V8 pipeline.
        
        Args:
            discover: Run bulk discovery phase
            validate: Run deep validation
            limit: Limit number of leads to process
            source_file: Optional source CSV to load leads from
            
        Returns:
            Path to final output file
        """
        logger.info("=" * 70)
        logger.info("🚀 LeadIntel Pro V8 - UNIFIED PIPELINE")
        logger.info("=" * 70)
        logger.info(f"Timestamp: {self.timestamp}")
        logger.info(f"Options: discover={discover}, validate={validate}, limit={limit}")
        
        # Phase 1: Bulk Discovery (optional)
        if discover:
            self._phase1_bulk_discovery()
        
        # Phase 2: Load Leads
        if source_file:
            leads = self._load_from_file(source_file)
        else:
            leads = self._phase2_load_leads()
        
        self.stats["total_leads"] = len(leads)
        logger.info(f"📊 Loaded {len(leads)} total leads")
        
        # Apply limit if specified
        if limit:
            leads = leads[:limit]
            logger.info(f"⚠️ Limited to {len(leads)} leads for testing")
        
        # Phase 3: Deduplicate
        leads = self._phase3_dedupe(leads)
        self.stats["after_dedupe"] = len(leads)
        
        # Phase 4: Role Classification & Noise Filter
        leads = self._phase4_role_filter(leads)
        self.stats["after_role_filter"] = len(leads)
        
        # Phase 5: Brave Scenting (Website + Evidence)
        leads = self._phase5_scenting(leads)
        
        # Phase 6: Heuristic Scoring
        leads = self._phase6_heuristic_scoring(leads)
        
        # Phase 7: SCE Scoring
        leads = self._phase7_sce_scoring(leads)
        
        # Phase 8: Deep Validation (optional)
        if validate:
            leads = self._phase8_deep_validation(leads)
        
        # Phase 9: Export
        output_path = self._phase9_export(leads)
        
        # Print summary with P0 metrics
        self._print_summary()
        
        return output_path
    
    def _phase1_bulk_discovery(self) -> List[Dict]:
        """Phase 1: Search for bulk lead sources."""
        self.stats["phase"] = "bulk_discovery"
        logger.info("\n" + "-" * 50)
        logger.info("📁 PHASE 1: BULK DISCOVERY")
        logger.info("-" * 50)
        
        files = self.scenter.phase1_bulk_discovery(region="global")
        
        if files:
            output_path = os.path.join(self.config["staging_dir"], f"discovered_files_{self.timestamp}.csv")
            pd.DataFrame(files).to_csv(output_path, index=False)
            logger.info(f"Saved {len(files)} discovered files to {output_path}")
        
        return files
    
    def _load_from_file(self, filepath: str) -> List[Dict]:
        """Load leads from a specific file."""
        logger.info(f"Loading leads from: {filepath}")
        df = pd.read_csv(filepath)
        return df.to_dict(orient="records")
    
    def _phase2_load_leads(self) -> List[Dict]:
        """Phase 2: Load leads from all sources."""
        self.stats["phase"] = "load_leads"
        logger.info("\n" + "-" * 50)
        logger.info("📥 PHASE 2: LOAD LEADS")
        logger.info("-" * 50)
        
        all_leads = []
        
        # Source 1: Verified customers (gold standard)
        verified_csv = self.config.get("verified_csv")
        if verified_csv and os.path.exists(verified_csv):
            verified_df = pd.read_csv(verified_csv)
            col_map = {
                "Şirket Adı": "company",
                "Ülke": "country",
                "Neden Onaylı? (Kanıt/Makine)": "evidence_reason",
            }
            verified_df = verified_df.rename(columns=col_map)
            verified_df["source_type"] = "verified_list"
            verified_df["source"] = "manual_verification"
            verified_leads = verified_df.to_dict(orient="records")
            all_leads.extend(verified_leads)
            logger.info(f"✅ Loaded {len(verified_leads)} verified leads")
        
        # Source 2: Pipeline targets_master
        targets_csv = self.config.get("targets_master")
        if targets_csv and os.path.exists(targets_csv):
            targets_df = pd.read_csv(targets_csv)
            targets_df["source_type"] = "pipeline"
            targets_leads = targets_df.to_dict(orient="records")
            all_leads.extend(targets_leads)
            logger.info(f"📋 Loaded {len(targets_leads)} pipeline leads")
        
        # Source 3: Raw staging leads
        raw_csv = os.path.join(self.config["staging_dir"], "leads_raw.csv")
        if os.path.exists(raw_csv):
            raw_df = pd.read_csv(raw_csv)
            raw_leads = raw_df.to_dict(orient="records")
            all_leads.extend(raw_leads)
            logger.info(f"📄 Loaded {len(raw_leads)} raw staging leads")
        
        # Source 4: Enriched leads
        enriched_csv = os.path.join(self.config["staging_dir"], "leads_enriched.csv")
        if os.path.exists(enriched_csv):
            try:
                enriched_df = pd.read_csv(enriched_csv, on_bad_lines='skip')
                enriched_leads = enriched_df.to_dict(orient="records")
                all_leads.extend(enriched_leads)
                logger.info(f"🔍 Loaded {len(enriched_leads)} enriched leads")
            except Exception as e:
                logger.warning(f"⚠️ Could not load enriched CSV: {e}")
        
        return all_leads
    
    def _phase3_dedupe(self, leads: List[Dict]) -> List[Dict]:
        """Phase 3: Deduplicate leads."""
        self.stats["phase"] = "dedupe"
        logger.info("\n" + "-" * 50)
        logger.info("🔄 PHASE 3: DEDUPLICATION")
        logger.info("-" * 50)
        
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
        logger.info("👤 PHASE 4: ROLE CLASSIFICATION")
        logger.info("-" * 50)
        
        filtered = []
        dropped = []
        
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
            
            # Keep CUSTOMER and UNKNOWN roles
            if role_result.role in ["CUSTOMER", "UNKNOWN"] and not is_noise and not is_non_customer:
                filtered.append(lead)
            else:
                lead["drop_reason"] = f"role={role_result.role}, noise={is_noise}, non_cust={is_non_customer}"
                dropped.append(lead)
        
        if dropped:
            dropped_path = os.path.join(self.config["staging_dir"], f"dropped_{self.timestamp}.csv")
            pd.DataFrame(dropped).to_csv(dropped_path, index=False)
            logger.info(f"Dropped {len(dropped)} leads (saved to {dropped_path})")
        
        logger.info(f"Role filter: {len(leads)} -> {len(filtered)} customers")
        return filtered
    
    def _phase5_scenting(self, leads: List[Dict]) -> List[Dict]:
        """Phase 5: Brave scenting (website + evidence)."""
        self.stats["phase"] = "scenting"
        logger.info("\n" + "-" * 50)
        logger.info("🔍 PHASE 5: BRAVE SCENTING")
        logger.info("-" * 50)
        
        if not os.environ.get("BRAVE_API_KEY"):
            logger.warning("⚠️ BRAVE_API_KEY not set - skipping scenting")
            return leads
        
        scented_leads = self.scenter.scent_leads_batch(leads)
        
        websites_resolved = sum(1 for l in scented_leads if l.get("website_source") == "brave_navigation")
        sce_ready = sum(1 for l in scented_leads if l.get("sce_sales_ready"))
        
        self.stats["websites_resolved"] = websites_resolved
        self.stats["sce_sales_ready"] = sce_ready
        
        logger.info(f"✅ Scenting complete: {websites_resolved} websites, {sce_ready} SCE ready")
        
        return scented_leads
    
    def _phase6_heuristic_scoring(self, leads: List[Dict]) -> List[Dict]:
        """Phase 6: Heuristic keyword-based scoring."""
        self.stats["phase"] = "heuristic_scoring"
        logger.info("\n" + "-" * 50)
        logger.info("📊 PHASE 6: HEURISTIC SCORING")
        logger.info("-" * 50)
        
        scored_leads = []
        high_score_count = 0
        
        for lead in leads:
            scored = self.scorer.score_lead(lead)
            scored_leads.append(scored)
            
            if scored.get("score", 0) >= 70:
                high_score_count += 1
        
        self.stats["heuristic_high"] = high_score_count
        logger.info(f"✅ Heuristic scoring complete: {high_score_count} high-score leads (>=70)")
        
        return scored_leads
    
    def _phase7_sce_scoring(self, leads: List[Dict]) -> List[Dict]:
        """Phase 7: SCE (Stenter Customer Evidence) scoring."""
        self.stats["phase"] = "sce_scoring"
        logger.info("\n" + "-" * 50)
        logger.info("🎯 PHASE 7: SCE SCORING")
        logger.info("-" * 50)
        
        scored_leads, sce_stats = self.sce_scorer.score_batch(leads)
        
        logger.info(f"✅ SCE scoring complete:")
        logger.info(f"  - Sales Ready: {sce_stats.get('sales_ready', 0)}")
        logger.info(f"  - High Confidence: {sce_stats.get('high_confidence', 0)}")
        logger.info(f"  - Medium Confidence: {sce_stats.get('medium_confidence', 0)}")
        
        return scored_leads
    
    def _phase8_deep_validation(self, leads: List[Dict]) -> List[Dict]:
        """Phase 8: Deep validation with P0 improvements."""
        self.stats["phase"] = "deep_validation"
        logger.info("\n" + "-" * 50)
        logger.info("🔬 PHASE 8: DEEP VALIDATION (P0 Enhanced)")
        logger.info("-" * 50)
        logger.info("  ✓ phonenumbers library (E.164 format)")
        logger.info("  ✓ SSL fallback (HTTPS -> HTTP)")
        logger.info("  ✓ Email blocklist filtering")
        
        validated_leads = self.deep_validator.validate_batch(leads)
        
        self.stats["tier_1"] = sum(1 for l in validated_leads if l.get("tier") == 1)
        self.stats["tier_2"] = sum(1 for l in validated_leads if l.get("tier") == 2)
        self.stats["tier_3"] = sum(1 for l in validated_leads if l.get("tier") == 3)
        
        logger.info(f"✅ Validation: T1={self.stats['tier_1']}, T2={self.stats['tier_2']}, T3={self.stats['tier_3']}")
        
        return validated_leads
    
    def _phase9_export(self, leads: List[Dict]) -> str:
        """Phase 9: Export final leads with multiple formats."""
        self.stats["phase"] = "export"
        logger.info("\n" + "-" * 50)
        logger.info("📤 PHASE 9: EXPORT")
        logger.info("-" * 50)
        
        output_dir = self.config["output_dir"]
        
        # Export all leads
        all_path = os.path.join(output_dir, f"v8_all_{self.timestamp}.csv")
        df = pd.DataFrame(leads)
        df.to_csv(all_path, index=False)
        logger.info(f"📁 Exported all {len(leads)} leads to {all_path}")
        
        # Export by tier
        tier_files = TierExporter.export_by_tier(leads, output_dir, self.timestamp)
        
        # Export sales-ready (SCE or Tier 1)
        sales_ready = [l for l in leads if l.get("sce_sales_ready") or l.get("tier") == 1]
        if sales_ready:
            sales_path = os.path.join(output_dir, f"v8_sales_ready_{self.timestamp}.csv")
            pd.DataFrame(sales_ready).to_csv(sales_path, index=False)
            logger.info(f"🎯 Exported {len(sales_ready)} sales-ready leads")
        
        # Export high-score leads (heuristic >= 70)
        high_score = [l for l in leads if l.get("score", 0) >= 70]
        if high_score:
            hs_path = os.path.join(output_dir, f"v8_high_score_{self.timestamp}.csv")
            pd.DataFrame(high_score).to_csv(hs_path, index=False)
            logger.info(f"⭐ Exported {len(high_score)} high-score leads")
        
        # Export by region
        for region in ["Brazil", "Turkey", "Egypt", "Pakistan", "Bangladesh"]:
            region_leads = [l for l in leads if str(l.get("country", "")).lower() == region.lower()]
            if region_leads:
                region_path = os.path.join(output_dir, f"v8_{region.lower()}_{self.timestamp}.csv")
                pd.DataFrame(region_leads).to_csv(region_path, index=False)
                logger.info(f"🌍 Exported {len(region_leads)} {region} leads")
        
        return all_path
    
    def _print_summary(self):
        """Print pipeline summary with P0 metrics."""
        logger.info("\n" + "=" * 70)
        logger.info("📊 V8 PIPELINE SUMMARY")
        logger.info("=" * 70)
        
        total = self.stats['total_leads']
        after_dedupe = self.stats['after_dedupe']
        after_filter = self.stats['after_role_filter']
        tier1 = self.stats['tier_1']
        tier2 = self.stats['tier_2']
        tier3 = self.stats['tier_3']
        heuristic_high = self.stats.get('heuristic_high', 0)
        
        logger.info(f"Total leads loaded:     {total}")
        logger.info(f"After dedupe:           {after_dedupe}")
        logger.info(f"After role filter:      {after_filter}")
        logger.info(f"Websites resolved:      {self.stats['websites_resolved']}")
        logger.info(f"SCE Sales Ready:        {self.stats['sce_sales_ready']}")
        logger.info(f"Heuristic High (>=70):  {heuristic_high}")
        logger.info("-" * 50)
        logger.info(f"Tier 1 (Full):          {tier1}")
        logger.info(f"Tier 2 (Promising):     {tier2}")
        logger.info(f"Tier 3 (Research):      {tier3}")
        logger.info("=" * 70)
        
        # P0: Key metrics
        if after_filter > 0:
            tier1_rate = (tier1 / after_filter) * 100
            websites_rate = (self.stats['websites_resolved'] / after_filter) * 100
            sce_rate = (self.stats['sce_sales_ready'] / after_filter) * 100
            heuristic_rate = (heuristic_high / after_filter) * 100
            
            logger.info("\n📈 KEY METRICS (P0 Tracking):")
            logger.info(f"  Tier 1 Rate:          {tier1_rate:.1f}%")
            logger.info(f"  Websites Resolved:    {websites_rate:.1f}%")
            logger.info(f"  SCE Sales Ready:      {sce_rate:.1f}%")
            logger.info(f"  Heuristic High:       {heuristic_rate:.1f}%")
        
        # Scenter stats
        scenter_stats = self.scenter.get_stats()
        logger.info("\n🔍 Brave Scenter Stats:")
        for key, val in scenter_stats.items():
            logger.info(f"  {key}: {val}")
        
        # Validator stats (includes fail_reasons)
        validator_stats = self.deep_validator.get_stats()
        logger.info("\n🔬 Deep Validator Stats:")
        for key, val in validator_stats.items():
            if key == "fail_reasons" and isinstance(val, dict):
                logger.info(f"  {key}:")
                for reason, count in sorted(val.items(), key=lambda x: -x[1]):
                    logger.info(f"    - {reason}: {count}")
            else:
                logger.info(f"  {key}: {val}")
        
        # Save metrics
        self._save_metrics()
    
    def _save_metrics(self):
        """Save run metrics to CSV for tracking."""
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
        
        file_exists = os.path.exists(metrics_file)
        with open(metrics_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=metrics.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(metrics)
        
        logger.info(f"\n📈 Metrics saved to {metrics_file}")


# =============================================================================
# LEGACY FUNCTIONS (backward compatibility)
# =============================================================================

def load_config(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

def ensure_dirs():
    for path in [
        "data/staging",
        "data/processed",
        "outputs",
        "outputs/crm",
        "outputs/reports",
        "outputs/evidence",
    ]:
        os.makedirs(path, exist_ok=True)

def _target_country_labels(targets):
    """Extract all country labels from targets config"""
    labels = []
    # Check old format (target_regions)
    for _, data in (targets or {}).get("target_regions", {}).items():
        labels.extend(data.get("labels", []))
    # Check new format (south_america, north_africa, etc.)
    region_keys = ["south_america", "north_africa", "south_asia", "turkey", "other_markets"]
    for region_key in region_keys:
        region = (targets or {}).get(region_key, {})
        countries = region.get("countries", {})
        if isinstance(countries, dict):
            for _, country_data in countries.items():
                labels.extend(country_data.get("labels", []))
    return labels

def _target_country_iso3(targets):
    """Extract all country ISO3 codes from targets config"""
    codes = []
    # Check old format (target_regions)
    for _, data in (targets or {}).get("target_regions", {}).items():
        codes.extend(data.get("countries", []))
    # Check new format (south_america, north_africa, etc.)
    region_keys = ["south_america", "north_africa", "south_asia", "turkey", "other_markets"]
    for region_key in region_keys:
        region = (targets or {}).get(region_key, {})
        countries = region.get("countries", {})
        if isinstance(countries, dict):
            for _, country_data in countries.items():
                code = country_data.get("code")
                if code:
                    codes.append(code)
    return codes

def apply_env(settings):
    api_keys = settings.setdefault("api_keys", {})
    if os.getenv("Brave_API_KEY"):
        api_keys["brave"] = os.getenv("Brave_API_KEY")
    if os.getenv("Comtrade_API_KEY"):
        api_keys["un_comtrade"] = os.getenv("Comtrade_API_KEY")
    return settings

def merge_discovered_sources(sources):
    discovered_path = "data/staging/discovered_sources.yaml"
    if not os.path.exists(discovered_path):
        return sources
    discovered = load_config(discovered_path)
    for key in ["fairs", "directories"]:
        for item in discovered.get(key, []):
            if not any(src.get("url") == item.get("url") for src in sources.get(key, [])):
                sources.setdefault(key, []).append(item)
    return sources

def discover_sources(settings, sources):
    discovery_cfg = sources.get("discovery", {})
    if not discovery_cfg.get("enabled", False):
        logger.info("Discovery disabled.")
        return
    # Get Brave API key from env or settings
    brave_api_key = os.environ.get("BRAVE_API_KEY") or settings.get("api_keys", {}).get("brave")
    client = BraveSearchClient(brave_api_key, discovery_cfg)
    discoverer = SourceDiscovery(client, disallow_domains=discovery_cfg.get("disallow_domains"))
    results = discoverer.discover(discovery_cfg.get("queries", []), max_results=discovery_cfg.get("max_results", 10))
    os.makedirs("data/staging", exist_ok=True)
    with open("data/staging/discovered_sources.yaml", "w") as f:
        yaml.safe_dump(results, f)
    logger.info("Discovery complete. Saved to data/staging/discovered_sources.yaml")

def harvest(targets, competitors, settings, policies, sources):
    logger.info("Stage: lead-harvest")
    ensure_dirs()
    sources = merge_discovered_sources(sources)
    harvester = CompetitorHarvester(settings=settings, policies=policies)
    fairs_harvester = FairsHarvester(settings=settings, policies=policies)
    pdf_processor = PdfProcessor()
    extractor = EntityExtractor()
    bettercotton = BetterCottonMembers(settings=settings, policies=policies)
    gots = GotsCertifiedSuppliers(settings=sources.get("gots", {}))
    # Get Brave API key from env or settings
    brave_api_key = os.environ.get("BRAVE_API_KEY") or settings.get("api_keys", {}).get("brave")
    websearch = CompetitorWebSearch(
        brave_api_key, settings=sources.get("discovery", {}), policies=policies
    )
    egypt_tec = EgyptTextileExportCouncil(
        brave_api_key, settings=sources.get("discovery", {}), policies=policies
    )
    exhibitor_lists = ExhibitorListCollector(settings=settings, policies=policies)
    texbrasil = TexbrasilCompanies(settings=settings, policies=policies)

    all_leads = []

    for comp in competitors.get("competitors", []):
        logger.info(f"Harvesting competitor: {comp.get('name', 'unknown')}")
        contents = harvester.harvest_competitor(comp)
        if not comp.get("customer_source", True):
            continue
        for entry in contents:
            companies = extractor.extract_companies(entry.get("content", ""), strict=True)
            for company in companies:
                all_leads.append(
                    {
                        "company": company,
                        "source": entry.get("url"),
                        "context": entry.get("content", "")[:2000],
                        "snippet": entry.get("snippet", ""),
                        "source_type": "competitor",
                        "source_name": comp.get("name", ""),
                        "competitor": comp.get("name", ""),
                    }
                )

    search_cfg = sources.get("competitor_websearch", {})
    web_leads = websearch.harvest(competitors.get("competitors", []), search_cfg)
    all_leads.extend(web_leads)

    fair_leads = fairs_harvester.harvest_sources(sources, targets)
    for lead in fair_leads:
        lead["context"] = (lead.get("context") or "")[:2000]
        all_leads.append(lead)

    bc_cfg = sources.get("bettercotton", {})
    if bc_cfg.get("enabled"):
        bc_countries = _target_country_labels(targets)
        bc_leads = bettercotton.harvest(
            bc_cfg.get("url", "https://bettercotton.org/membership/find-members/"),
            max_pages=bc_cfg.get("max_pages", 10),
            country_filter=bc_countries,
            include_categories=bc_cfg.get("include_categories", []),
            use_xlsx=bc_cfg.get("use_xlsx", False),
            member_list_url=bc_cfg.get("member_list_url"),
        )
        all_leads.extend(bc_leads)

    gots_cfg = sources.get("gots", {})
    if gots_cfg.get("enabled"):
        gots_leads = gots.harvest(_target_country_iso3(targets))
        all_leads.extend(gots_leads)

    tec_cfg = sources.get("egypt_tec", {})
    if tec_cfg.get("enabled"):
        tec_leads = egypt_tec.harvest(
            tec_cfg.get("search_query"),
            max_results=tec_cfg.get("max_results", 30),
        )
        all_leads.extend(tec_leads)

    brazil_cfg = sources.get("brazil", {})
    for key in ("febratex", "febratextil"):
        cfg = brazil_cfg.get(key, {}) if isinstance(brazil_cfg, dict) else {}
        if cfg.get("enabled"):
            leads = exhibitor_lists.harvest(
                cfg.get("url"),
                source_name=cfg.get("name", key),
                country=cfg.get("country", "Brazil"),
            )
            all_leads.extend(leads)

    tex_cfg = brazil_cfg.get("texbrasil", {}) if isinstance(brazil_cfg, dict) else {}
    if tex_cfg.get("enabled"):
        leads = texbrasil.harvest(
            base_url=tex_cfg.get("base_url", "https://texbrasil.com.br"),
            sitemap_url=tex_cfg.get("sitemap_url"),
            max_pages=int(tex_cfg.get("max_pages", 200)),
            country=tex_cfg.get("country", "Brazil"),
        )
        all_leads.extend(leads)

    # OEKO-TEX Directory
    oekotex_cfg = sources.get("oekotex", {})
    if oekotex_cfg.get("enabled"):
        oekotex = OekoTexDirectory(settings=oekotex_cfg)
        oekotex_leads = oekotex.harvest(_target_country_iso3(targets))
        all_leads.extend(oekotex_leads)

    # bluesign Partners
    bluesign_cfg = sources.get("bluesign", {})
    if bluesign_cfg.get("enabled"):
        bluesign = BluesignPartners(settings=bluesign_cfg, policies=policies)
        bluesign_leads = bluesign.harvest(_target_country_iso3(targets))
        all_leads.extend(bluesign_leads)

    # AMITH Morocco
    amith_cfg = sources.get("amith", {})
    if amith_cfg.get("enabled"):
        amith = AmithDirectory(settings=amith_cfg, policies=policies)
        amith_leads = amith.harvest()
        all_leads.extend(amith_leads)

    # ABIT Brazil
    abit_cfg = sources.get("abit", {})
    if abit_cfg.get("enabled"):
        abit = AbitDirectory(settings=abit_cfg, policies=policies)
        abit_leads = abit.harvest()
        all_leads.extend(abit_leads)

    # === V5: Known Manufacturers from targets.yaml ===
    known_mfg_collector = KnownManufacturersCollector(targets_config=targets)
    known_mfg_leads = known_mfg_collector.harvest()
    all_leads.extend(known_mfg_leads)
    logger.info(f"Added {len(known_mfg_leads)} known manufacturers from config")

    # === GPT V10.4: South America Collectors ===
    sa_cfg = sources.get("south_america", {})
    
    # Colombiatex
    if sa_cfg.get("colombiatex", {}).get("enabled", True):
        try:
            colombiatex = ColombiatexHarvester(settings=settings, policies=policies)
            colombiatex_leads = colombiatex.harvest()
            all_leads.extend(colombiatex_leads)
            logger.info(f"Colombiatex: {len(colombiatex_leads)} leads harvested")
        except Exception as e:
            logger.warning(f"Colombiatex harvest failed: {e}")
    
    # Emitex Argentina
    if sa_cfg.get("emitex", {}).get("enabled", True):
        try:
            emitex = EmitexHarvester(settings=settings, policies=policies)
            emitex_leads = emitex.harvest()
            all_leads.extend(emitex_leads)
            logger.info(f"Emitex Argentina: {len(emitex_leads)} leads harvested")
        except Exception as e:
            logger.warning(f"Emitex harvest failed: {e}")
    
    # Peru Moda / ADEX
    if sa_cfg.get("peru_moda", {}).get("enabled", True):
        try:
            peru_moda = PeruModaHarvester(settings=settings, policies=policies)
            peru_leads = peru_moda.harvest()
            all_leads.extend(peru_leads)
            logger.info(f"Peru Moda/ADEX: {len(peru_leads)} leads harvested")
        except Exception as e:
            logger.warning(f"Peru Moda harvest failed: {e}")
    
    # ITMF/EURATEX Association Bootstrap
    if sa_cfg.get("itmf_bootstrap", {}).get("enabled", False):
        try:
            itmf = ITMFBootstrap(settings=settings, policies=policies)
            itmf_leads = itmf.harvest()
            all_leads.extend(itmf_leads)
            logger.info(f"ITMF Bootstrap: {len(itmf_leads)} leads harvested")
        except Exception as e:
            logger.warning(f"ITMF Bootstrap failed: {e}")

    pdf_results = pdf_processor.process_all_pdfs()
    for source, content in pdf_results.items():
        companies = extractor.extract_companies(content, strict=False)
        for company in companies:
            all_leads.append(
                {
                    "company": company,
                    "source": source,
                    "context": content[:2000],
                    "snippet": content[:200],
                    "source_type": "pdf",
                    "source_name": source,
                    "competitor": "PDF_Catalog",
                }
            )

    df = pd.DataFrame(all_leads)
    if not df.empty:
        df = df.drop_duplicates(subset=["company", "source"])
        df.to_csv("data/staging/leads_raw.csv", index=False)
        logger.info(f"Harvested {len(df)} raw leads.")
    else:
        logger.info("No leads harvested.")

def enrich(targets, settings, sources, policies):
    logger.info("Stage: enrich")
    if not os.path.exists("data/staging/leads_raw.csv"):
        logger.warning("No raw leads found.")
        return
    df = pd.read_csv("data/staging/leads_raw.csv")
    leads = df.to_dict(orient="records")
    enricher = Enricher(targets_config=targets, settings=settings, sources=sources, policies=policies)

    enrich_cfg = (settings or {}).get("enrichment", {})
    checkpoint_every = int(enrich_cfg.get("checkpoint_every", 50))
    resume = bool(enrich_cfg.get("resume", False))
    output_path = "data/staging/leads_enriched.csv"

    existing_keys = set()
    if resume and os.path.exists(output_path):
        try:
            existing = pd.read_csv(output_path)
            for _, row in existing.iterrows():
                key = f"{row.get('company','')}|{row.get('source','')}"
                existing_keys.add(key)
            logger.info(f"Resuming enrichment; {len(existing_keys)} leads already processed.")
        except Exception:
            existing_keys = set()
    else:
        if os.path.exists(output_path):
            os.remove(output_path)

    buffer = []
    processed = 0
    for lead in leads:
        key = f"{lead.get('company','')}|{lead.get('source','')}"
        if key in existing_keys:
            continue
        enriched = enricher.enrich_one(lead)
        buffer.append(enriched)
        processed += 1
        if len(buffer) >= checkpoint_every:
            pd.DataFrame(buffer).to_csv(output_path, mode="a", header=not os.path.exists(output_path), index=False)
            buffer = []
            logger.info(f"Enriched {processed} new leads (checkpoint).")

    if buffer:
        pd.DataFrame(buffer).to_csv(output_path, mode="a", header=not os.path.exists(output_path), index=False)

    total = len(existing_keys) + processed
    logger.info(f"Enriched {processed} new leads. Total enriched: {total}.")

def dedupe(settings=None):
    logger.info("Stage: dedupe")
    if not os.path.exists("data/staging/leads_enriched.csv"):
        logger.warning("No enriched leads found.")
        return
    df = pd.read_csv("data/staging/leads_enriched.csv", on_bad_lines='warn')
    leads = df.to_dict(orient="records")
    
    # === PHASE 1: Data Quality Foundation ===
    logger.info(f"Starting Phase 1: Data Cleaning on {len(leads)} leads")
    
    # Step 1: Noise filtering and domain validation
    data_quality_cfg = settings.get("data_quality", {}) if settings else {}
    cleaner = DataCleaner(config=data_quality_cfg.get("noise_filter"))
    cleaned_leads, rejected_noise = cleaner.clean_dataset(leads)
    
    cleaning_stats = cleaner.get_stats(len(leads), cleaned_leads, rejected_noise)
    logger.info(f"Noise Filter: {cleaning_stats['cleaned_count']} kept, "
               f"{cleaning_stats['rejected_count']} rejected "
               f"({cleaning_stats['noise_rate']}% noise rate)")
    logger.info(f"Domain Validator: {cleaning_stats['domains_cleared']} invalid domains cleared, "
               f"{cleaning_stats['needs_discovery']} need website discovery")
    
    # Save rejected noise for audit
    if rejected_noise:
        pd.DataFrame(rejected_noise).to_csv("outputs/crm/dropped_noise.csv", index=False)
        logger.info(f"Saved {len(rejected_noise)} noise entries to outputs/crm/dropped_noise.csv")
    
    # Step 2: Entity classification
    validator = EntityValidator(config=data_quality_cfg.get("entity_classification"))
    validated_leads, skipped_entities = validator.batch_validate(cleaned_leads)
    
    entity_dist = validator.get_distribution(cleaned_leads)
    logger.info(f"Entity Classification: {entity_dist['total']} leads classified")
    for entity_type, pct in entity_dist['percentages'].items():
        count = entity_dist['counts'][entity_type]
        logger.info(f"  - {entity_type}: {count} ({pct}%)")
    
    logger.info(f"Processable: {len(validated_leads)}, Skipped: {len(skipped_entities)}")
    
    # Save skipped entities for audit
    if skipped_entities:
        pd.DataFrame(skipped_entities).to_csv("outputs/crm/skipped_entities.csv", index=False)
        logger.info(f"Saved {len(skipped_entities)} skipped entities to outputs/crm/skipped_entities.csv")
    
    # Use validated leads for rest of pipeline
    quality_leads = validated_leads
    
    # === V5: Apply Quality Gate V2 AFTER Phase 1 ===
    quality_gate = EntityQualityGateV2()
    final_quality_leads = []
    rejected_count = 0
    rejection_reasons = {}
    
    for lead in quality_leads:
        # grade_entity expects a lead dict, not individual params
        grade, reason = quality_gate.grade_entity(lead)
        lead["entity_quality"] = grade
        lead["quality_reason"] = reason
        
        if grade != "REJECT":
            final_quality_leads.append(lead)
        else:
            rejected_count += 1
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
    
    logger.info(f"Quality Gate V2: {len(final_quality_leads)} passed, {rejected_count} rejected")
    for reason, count in sorted(rejection_reasons.items(), key=lambda x: -x[1])[:10]:
        logger.info(f"  - {reason}: {count}")
    
    # === V5: Apply Role Classifier ===
    # GPT V10.4: Updated to handle 4-tuple return (customers, intermediaries, brands, unknown)
    classifier = LeadRoleClassifier()
    customers, intermediaries, brands, unknown = classifier.classify_leads(final_quality_leads)
    logger.info(f"Role Classification: CUSTOMER={len(customers)}, INTERMEDIARY={len(intermediaries)}, BRAND={len(brands)}, UNKNOWN={len(unknown)}")
    
    # Mark roles
    for lead in customers:
        lead["lead_role"] = "CUSTOMER"
    for lead in intermediaries:
        lead["lead_role"] = "INTERMEDIARY"
    for lead in brands:
        lead["lead_role"] = "BRAND"
    for lead in unknown:
        lead["lead_role"] = "UNKNOWN"
    
    # GPT V10.4: Exclude brands from main output (they are not stenter customers)
    all_classified = customers + intermediaries + unknown
    logger.info(f"Excluding {len(brands)} BRAND leads from main output")
    
    deduper = LeadDedupe()
    merged, audit = deduper.dedupe(all_classified)
    pd.DataFrame(merged).to_csv("data/processed/leads_master.csv", index=False)
    pd.DataFrame(audit).to_csv("outputs/dedupe_audit.csv", index=False)
    logger.info(f"Dedupe complete: {len(merged)} leads retained.")

def brave_discovery(settings=None):
    """Phase 2: Brave Search Discovery & Evidence Collection"""
    logger.info("Stage: brave discovery & evidence")
    
    if not os.path.exists("data/processed/leads_master.csv"):
        logger.warning("No master leads found. Run dedupe first.")
        return
    
    # Load settings
    brave_cfg = settings.get("brave_discovery", {}) if settings else {}
    
    if not brave_cfg.get("enabled", True):
        logger.info("Brave discovery disabled in settings")
        return
    
    # Import Brave client
    from src.processors.brave_integration import BraveSearchClient
    
    brave_client = BraveSearchClient()
    
    if not brave_client.api_key:
        logger.error("Brave API key not configured. Set Brave_API_KEY or BRAVE_API_KEY env variable.")
        return
    
    # Load leads
    df = pd.read_csv("data/processed/leads_master.csv")
    leads = df.to_dict(orient="records")
    
    logger.info(f"Processing {len(leads)} leads for Brave discovery")
    
    # Phase 2A: Website Discovery
    if brave_cfg.get("website_discovery", {}).get("enabled", True):
        logger.info("=== Phase 2A: Website Discovery ===")
        
        # Filter leads needing discovery
        needs_discovery = [
            lead for lead in leads 
            if not lead.get('website') or lead.get('needs_discovery')
        ]
        
        logger.info(f"Found {len(needs_discovery)} leads needing website discovery")
        
        if needs_discovery:
            batch_size = brave_cfg.get("website_discovery", {}).get("batch_size", 50)
            max_leads = brave_cfg.get("website_discovery", {}).get("max_leads_per_run", 200)
            
            # Limit batch
            to_process = needs_discovery[:max_leads]
            logger.info(f"Processing {len(to_process)} leads (max: {max_leads})")
            
            # Batch discover
            updated_leads = brave_client.batch_discover(to_process)
            
            # Update original leads list
            for updated in updated_leads:
                for i, lead in enumerate(leads):
                    if lead.get('company') == updated.get('company'):
                        leads[i] = updated
                        break
    
    # Phase 2B: Evidence Search
    if brave_cfg.get("evidence_search", {}).get("enabled", True):
        logger.info("=== Phase 2B: SCE Evidence Search ===")
        
        batch_size = brave_cfg.get("evidence_search", {}).get("batch_size", 50)
        max_leads = brave_cfg.get("evidence_search", {}).get("max_leads_per_run", 200)
        
        # Limit batch for testing
        to_process = leads[:max_leads]
        logger.info(f"Searching evidence for {len(to_process)} leads (max: {max_leads})")
        
        # Batch evidence search
        leads = brave_client.batch_evidence_search(to_process) + leads[max_leads:]
    
    # Save updated leads
    pd.DataFrame(leads).to_csv("data/processed/leads_master.csv", index=False)
    
    # Stats
    stats = brave_client.get_stats()
    discovered_websites = sum(1 for l in leads if l.get('website_source') == 'brave_discovery')
    has_evidence = sum(1 for l in leads if l.get('sce_has_evidence'))
    strong_evidence = sum(1 for l in leads if l.get('sce_confidence') == 'strong')
    
    logger.info(f"Brave Discovery Complete:")
    logger.info(f"  - API calls made: {stats['calls_made']}")
    logger.info(f"  - Websites discovered: {discovered_websites}")
    logger.info(f"  - Leads with evidence: {has_evidence}")
    logger.info(f"  - Strong evidence: {strong_evidence}")

def phase3_discovery(settings=None, sources=None):
    """Phase 3: Advanced Discovery - Network Sniffing + LATAM + PDF Extraction"""
    logger.info("=== Phase 3: Advanced Discovery ===")
    
    # Load settings
    phase3_enabled = settings.get("phase3_discovery", {}).get("enabled", True) if settings else True
    
    if not phase3_enabled:
        logger.info("Phase 3 disabled in settings")
        return
    
    all_leads = []
    
    # 1. Network Sniffing - GOTS Directory
    logger.info("--- Phase 3A: Network Sniffing (GOTS) ---")
    network_cfg = sources.get("network_sniffing", {}) if sources else {}
    
    if network_cfg.get("enabled", True):
        gots_cfg = network_cfg.get("sources", {}).get("gots_directory", {})
        
        if gots_cfg.get("enabled", True):
            try:
                sniffer = GOTSDirectorySniffer()
                gots_leads = sniffer.harvest_gots_members()
                
                logger.info(f"Network sniffing collected {len(gots_leads)} GOTS companies")
                all_leads.extend(gots_leads)
                
            except Exception as e:
                logger.error(f"GOTS network sniffing failed: {e}")
    
    # 2. LATAM Sources Collection
    logger.info("--- Phase 3B: LATAM Sources ---")
    latam_cfg = sources.get("latam_sources", {}) if sources else {}
    
    if latam_cfg.get("enabled", True):
        try:
            orchestrator = LATAMSourcesOrchestrator()
            delay = latam_cfg.get("rate_limit", {}).get("delay_between_sources", 2.0)
            
            latam_leads = orchestrator.collect_all(delay_between_sources=delay)
            
            logger.info(f"LATAM collection gathered {len(latam_leads)} companies")
            all_leads.extend(latam_leads)
            
        except Exception as e:
            logger.error(f"LATAM sources collection failed: {e}")
    
    # 3. PDF Extraction
    logger.info("--- Phase 3C: PDF Exhibitor Extraction ---")
    pdf_cfg = sources.get("pdf_sources", {}) if sources else {}
    
    if pdf_cfg.get("enabled", True):
        input_dir = pdf_cfg.get("input_dir", "data/inputs")
        
        if os.path.exists(input_dir):
            processor = PdfProcessor(data_dir=input_dir)
            
            # Find all PDFs in input directory
            pdf_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
            
            if pdf_files:
                logger.info(f"Found {len(pdf_files)} PDF files to process")
                
                for pdf_file in pdf_files:
                    try:
                        pdf_path = os.path.join(input_dir, pdf_file)
                        companies = processor.extract_exhibitor_table(pdf_path)
                        
                        logger.info(f"Extracted {len(companies)} companies from {pdf_file}")
                        all_leads.extend(companies)
                        
                    except Exception as e:
                        logger.error(f"Error processing {pdf_file}: {e}")
            else:
                logger.info("No PDF files found in input directory")
        else:
            logger.warning(f"PDF input directory not found: {input_dir}")
    
    # Save Phase 3 leads
    if all_leads:
        output_path = "data/staging/leads_phase3.csv"
        pd.DataFrame(all_leads).to_csv(output_path, index=False)
        logger.info(f"Saved {len(all_leads)} Phase 3 leads to {output_path}")
        
        # Stats by source
        source_stats = {}
        for lead in all_leads:
            source = lead.get('source_name', 'unknown')
            source_stats[source] = source_stats.get(source, 0) + 1
        
        logger.info("Phase 3 Collection Summary:")
        for source, count in sorted(source_stats.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  - {source}: {count} companies")
    else:
        logger.warning("No leads collected in Phase 3")
    
    return all_leads

def score_and_export(targets, scoring, settings=None):
    logger.info("Stage: score + export")
    if not os.path.exists("data/processed/leads_master.csv"):
        logger.warning("No master leads found.")
        return
    df = pd.read_csv("data/processed/leads_master.csv")
    trade_path = scoring.get("trade_priority_path", "data/processed/country_priority_comtrade.csv")
    country_priority = {}
    if os.path.exists(trade_path):
        try:
            trade_df = pd.read_csv(trade_path)
            for _, row in trade_df.iterrows():
                country_priority[str(row.get("country_iso3", "")).upper()] = float(
                    row.get("import_value", 0)
                )
        except Exception:
            country_priority = {}

    scorer = Scorer(targets, scoring, country_priority=country_priority)
    scored = [scorer.score_lead(lead) for lead in df.to_dict(orient="records")]

    # === GPT V10.4: Apply SCE (Stenter Customer Evidence) Scoring ===
    sce_scorer = SCEScorer()
    scored, sce_stats = sce_scorer.score_batch(scored)
    logger.info(f"SCE Scoring: {sce_stats['sales_ready']} sales-ready, "
               f"{sce_stats['high_confidence']} high confidence")

    export_cfg = scoring.get("export", {}) if isinstance(scoring, dict) else {}
    allowed_sources = set(export_cfg.get("allowed_source_types", []) or [])
    min_score = export_cfg.get("min_score")
    exclude_name_keywords = [k.lower() for k in (export_cfg.get("exclude_name_keywords") or [])]
    require_reachability = bool(export_cfg.get("require_reachability", False))
    reachability_level = str(export_cfg.get("require_reachability_level", "any")).lower()

    filtered = scored
    if allowed_sources:
        filtered = [lead for lead in filtered if lead.get("source_type") in allowed_sources]
    if min_score is not None:
        filtered = [lead for lead in filtered if lead.get("score", 0) >= float(min_score)]
    if exclude_name_keywords:
        filtered = [
            lead
            for lead in filtered
            if not any(
                kw in str(lead.get("company", "")).lower() for kw in exclude_name_keywords
            )
        ]

    exporter = Exporter()
    if filtered != scored:
        exporter.export_targets(scored, tag="_all")

    sales_ready = filtered
    
    # === V5: Prioritize CUSTOMER role and Grade A entities ===
    # Filter by entity quality
    grade_a = [lead for lead in sales_ready if lead.get("entity_quality") == "A"]
    grade_b = [lead for lead in sales_ready if lead.get("entity_quality") == "B"]
    grade_c = [lead for lead in sales_ready if lead.get("entity_quality") == "C"]
    
    logger.info(f"Entity Quality: A={len(grade_a)}, B={len(grade_b)}, C={len(grade_c)}")
    
    # Filter by role
    customers_only = [lead for lead in sales_ready if lead.get("lead_role") == "CUSTOMER"]
    intermediaries = [lead for lead in sales_ready if lead.get("lead_role") == "INTERMEDIARY"]
    
    logger.info(f"Role Distribution: CUSTOMER={len(customers_only)}, INTERMEDIARY={len(intermediaries)}")
    
    # === V5: Premium leads = Grade A + CUSTOMER role ===
    premium_leads = [
        lead for lead in sales_ready 
        if lead.get("entity_quality") == "A" and lead.get("lead_role") == "CUSTOMER"
    ]
    logger.info(f"Premium Leads (Grade A + CUSTOMER): {len(premium_leads)}")
    
    needs_enrichment = []
    if require_reachability:
        def _has_reachability(lead):
            has_contact = bool(lead.get("emails") or lead.get("phones"))
            has_website = bool(lead.get("website") or lead.get("websites"))
            if reachability_level == "contact":
                return has_contact
            if reachability_level == "website":
                return has_website
            return has_contact or has_website

        sales_ready = [lead for lead in filtered if _has_reachability(lead)]
        needs_enrichment = [lead for lead in filtered if not _has_reachability(lead)]

    exporter.export_targets(sales_ready)
    if needs_enrichment:
        exporter.export_targets(needs_enrichment, tag="_needs_enrichment")

    # Competitor customers export
    competitor_customers = [
        lead for lead in scored if lead.get("source_type") in ("competitor", "competitor_search")
    ]
    if competitor_customers:
        exporter.export_targets(competitor_customers, tag="_competitor_customers")

    # Lookalike customers based on trade priority countries
    lookalike_cfg = export_cfg
    top_n = int(lookalike_cfg.get("lookalike_top_countries", 5))
    top_countries = [
        row[0]
        for row in sorted(country_priority.items(), key=lambda x: x[1], reverse=True)[:top_n]
    ]
    def _country_iso3_from_lead(lead):
        country = str(lead.get("country", "")).strip().upper()
        if country in top_countries:
            return country
        # try map from labels
        for _, data in targets.get("target_regions", {}).items():
            for iso3, label in zip(data.get("countries", []), data.get("labels", [])):
                if str(label).strip().lower() == str(lead.get("country", "")).strip().lower():
                    return iso3
        return ""

    lookalikes = [
        lead
        for lead in scored
        if lead.get("source_type") in ("gots", "bettercotton")
        and _country_iso3_from_lead(lead) in top_countries
    ]
    if lookalikes:
        exporter.export_targets(lookalikes, tag="_lookalikes")

    # === GPT V10.4: Generate Quality Report ===
    reporter = QualityReporter()
    report = reporter.generate_report(scored, sample_size=50, run_name="pipeline_run")
    logger.info(f"Quality Report generated with {len(report['recommendations'])} recommendations")
    
    # === GPT V10.4: Export SCE Sales-Ready leads ===
    sales_ready_sce = [lead for lead in scored if lead.get("sce_sales_ready")]
    if sales_ready_sce:
        exporter.export_targets(sales_ready_sce, tag="_sce_sales_ready")
        logger.info(f"Exported {len(sales_ready_sce)} SCE sales-ready leads")
    
    # === GPT V3: Apply post-processing fixes ===
    logger.info("Applying GPT V3 post-processing fixes...")
    try:
        targets_path = "outputs/crm/targets_master.csv"
        if os.path.exists(targets_path):
            df_fix = pd.read_csv(targets_path)
            original_count = len(df_fix)
            
            # Patch A: Schema fix
            df_fix = fix_schema(df_fix)
            
            # Patch B: Noise filter
            df_fix, noise_df = apply_noise_filter(df_fix)
            if len(noise_df) > 0:
                noise_df.to_csv("outputs/crm/dropped_noise.csv", index=False)
            
            # Patch C: Enhanced role classification
            df_fix['role'] = df_fix.apply(enhanced_role_classify, axis=1)
            
            # Patch D: SCE validation
            df_fix['sce_sales_ready_validated'] = df_fix.apply(validate_sce_sales_ready, axis=1)
            
            # Patch E: Export split
            customers, channels, unknown, sales_ready_v3 = export_split(df_fix)
            
            logger.info(f"GPT V3 Fix: {original_count} -> {len(df_fix)} leads")
            logger.info(f"  Customers: {len(customers)}, Channels: {len(channels)}, Unknown: {len(unknown)}")
            logger.info(f"  Sales Ready (validated): {len(sales_ready_v3)}")
    except Exception as e:
        logger.warning(f"GPT V3 fix failed: {e}")

def trade_rank(targets, settings):
    logger.info("Stage: trade-prioritize")
    fetcher = TradeFetcher(settings)
    hs_codes = [item["code"] for item in targets.get("hs_codes", [])]
    comtrade = fetcher.fetch_comtrade_data(hs_codes, targets.get("target_regions", {}))
    eurostat = fetcher.fetch_eurostat_data(hs_codes)
    if comtrade.get("rankings"):
        df = pd.DataFrame(comtrade["rankings"])
        trade_cfg = settings.get("trade", {})
        df["period"] = trade_cfg.get("period", "2022")
        df["flow"] = "Import"
        df["partner"] = "World"
        df["cmd_codes"] = ",".join([str(code) for code in hs_codes])
        df.to_csv("data/processed/country_priority_comtrade.csv", index=False)
    if eurostat.get("rankings"):
        pd.DataFrame(eurostat["rankings"]).to_csv(
            "data/processed/country_priority_comext.csv", index=False
        )

def main():
    parser = argparse.ArgumentParser(
        description="LeadIntel Pro - Unified Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
V8 Pipeline (Recommended):
    python app.py v8                    # Full V8 pipeline with all improvements
    python app.py v8 --limit 50         # Test with 50 leads
    python app.py v8 --skip-discovery   # Skip bulk discovery phase
    python app.py v8 --skip-validation  # Skip deep validation phase
    python app.py v8 --source FILE.csv  # Load leads from specific file

Legacy Pipeline:
    python app.py all                   # Run all legacy stages
    python app.py harvest               # Only harvest stage
    python app.py dedupe                # Only dedupe stage
        """
    )
    parser.add_argument(
        "stage",
        choices=["v8", "discover", "harvest", "enrich", "dedupe", "brave", "phase3", "score", "trade-rank", "ui", "all"],
        help="Pipeline stage to run (v8 = new unified pipeline)",
    )
    parser.add_argument("--limit", type=int, help="Limit number of leads to process")
    parser.add_argument("--skip-discovery", action="store_true", help="Skip bulk discovery phase")
    parser.add_argument("--skip-validation", action="store_true", help="Skip deep validation phase")
    parser.add_argument("--source", type=str, help="Source CSV file to load leads from")
    
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    
    # V8 Pipeline (new unified approach)
    if args.stage == "v8":
        pipeline = V8Pipeline()
        output = pipeline.run(
            discover=not args.skip_discovery,
            validate=not args.skip_validation,
            limit=args.limit,
            source_file=args.source,
        )
        logger.info(f"\n🎉 V8 Pipeline complete! Output: {output}")
        return
    
    # Legacy pipeline stages
    targets = load_config("config/targets.yaml")
    competitors = load_config("config/competitors.yaml")
    settings = load_config("config/settings.yaml")
    policies = load_config("config/policies.yaml")
    sources = load_config("config/sources.yaml")
    scoring = load_config("config/scoring.yaml")
    settings = apply_env(settings)
    sources = merge_discovered_sources(sources)
    ensure_dirs()

    if args.stage in ("discover", "all"):
        discover_sources(settings, sources)
    if args.stage in ("harvest", "all"):
        harvest(targets, competitors, settings, policies, sources)
    if args.stage in ("enrich", "all"):
        enrich(targets, settings, sources, policies)
    if args.stage in ("dedupe", "all"):
        dedupe(settings=settings)
    if args.stage in ("brave", "all"):
        brave_discovery(settings=settings)
    if args.stage in ("phase3", "all"):
        phase3_discovery(settings=settings, sources=sources)
    if args.stage in ("score", "all"):
        score_and_export(targets, scoring, settings=settings)
    if args.stage in ("trade-rank", "all"):
        trade_rank(targets, settings)
    if args.stage == "ui":
        logger.info("Launching Review UI...")
        os.system("streamlit run src/ui/app_streamlit.py")

if __name__ == "__main__":
    main()
