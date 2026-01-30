#!/usr/bin/env python3
"""
Lead Intel v2 - Full Autonomous Pipeline

End-to-end B2B lead intelligence system:
1. AutoDiscover: Find new sources with Brave Search
2. Harvest: Collect leads from all sources
3. Enrich: Add website, social media info
4. Dedupe: Remove duplicates
5. Score: Rank by potential
6. Export: CRM-ready output

Usage:
    python run_pipeline.py              # Full pipeline
    python run_pipeline.py --discover   # Include new source discovery
    python run_pipeline.py --harvest    # Only harvest + process
    python run_pipeline.py --status     # Show current status
"""

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)

# Fix Brave API key name
if os.environ.get("Brave_API_KEY"):
    os.environ["BRAVE_API_KEY"] = os.environ["Brave_API_KEY"]

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


class LeadIntelPipeline:
    """
    Full autonomous B2B lead intelligence pipeline.
    """
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.start_time = datetime.now()
        self.stats = {
            "sources_discovered": 0,
            "leads_harvested": 0,
            "leads_enriched": 0,
            "leads_deduped": 0,
            "leads_scored": 0,
            "leads_exported": 0,
        }
    
    def log_stage(self, stage: str):
        """Log stage start."""
        elapsed = (datetime.now() - self.start_time).seconds
        logger.info("="*60)
        logger.info(f"[{elapsed}s] STAGE: {stage}")
        logger.info("="*60)
    
    # =========================================================================
    # STAGE 0: AUTODISCOVER (Optional)
    # =========================================================================
    
    def discover_new_sources(self, countries: list = None, max_queries: int = 10):
        """Discover new lead sources using Brave Search."""
        self.log_stage("AUTODISCOVER - Finding New Sources")
        
        try:
            from src.autodiscover.engine import AutoDiscoverEngine
            
            engine = AutoDiscoverEngine()
            
            # Check for Brave API key
            api_key = os.environ.get("BRAVE_API_KEY")
            if not api_key:
                logger.warning("BRAVE_API_KEY not set - skipping discovery")
                return 0
            
            logger.info(f"Brave API key found: {api_key[:10]}...")
            
            # Run discovery
            sources = engine.discover(
                countries=countries or ["Egypt", "Morocco", "Tunisia", "Brazil"],
                max_queries=max_queries
            )
            
            self.stats["sources_discovered"] = len(sources)
            logger.info(f"Discovered {len(sources)} potential sources")
            
            # Diagnose top sources
            diagnosed = 0
            for domain, info in list(engine.state["discovered"].items())[:3]:
                if domain not in engine.state["diagnosed"]:
                    url = info.get("url", "")
                    if url:
                        try:
                            result = engine.process_url(url)
                            if result.get("success"):
                                diagnosed += 1
                                logger.info(f"Generated adapter for {domain}")
                        except Exception as e:
                            logger.warning(f"Failed to process {domain}: {e}")
            
            return len(sources)
            
        except Exception as e:
            logger.error(f"Discovery failed: {e}")
            return 0
    
    # =========================================================================
    # STAGE 1: HARVEST
    # =========================================================================
    
    def harvest(self):
        """Harvest leads from all configured sources."""
        self.log_stage("HARVEST - Collecting Leads")
        
        import yaml
        from src.collectors.oekotex_directory import OekoTexDirectory
        from src.collectors.texbrasil_companies import TexbrasilCompanies
        from src.collectors.gots_directory import GotsCertifiedSuppliers
        from src.collectors.bettercotton_members import BetterCottonMembers
        from src.collectors.brueckner_monforts import OEMCustomerHunter
        from src.collectors.turkey_textile import TurkeyTextileCollector
        from src.collectors.fair_exhibitors import FairExhibitorCollector
        from src.collectors.competitor_customer_intel import CompetitorCustomerIntel
        from src.collectors.import_intelligence import ImportIntelligence
        from src.collectors.precision_finder import PrecisionCustomerFinder
        from src.collectors.regional_collector import RegionalCollector
        from src.collectors.auto_discover import AutoDiscover
        
        all_leads = []
        
        # Load targets config
        targets_path = self.project_root / "config" / "targets.yaml"
        with open(targets_path, "r") as f:
            targets_config = yaml.safe_load(f)
        
        # Get all target countries
        target_countries = []
        for region, data in targets_config.get("target_regions", {}).items():
            target_countries.extend(data.get("countries", []))
        
        # 0. PRECISION CUSTOMER FINDER - NOKTA ATIŞI!
        try:
            logger.info("🎯 Precision Customer Finder - 16 ürün için nokta atışı...")
            finder = PrecisionCustomerFinder()
            leads = finder.harvest()
            logger.info(f"  Precision Leads: {len(leads)} leads")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"Precision finder failed: {e}")
        
        # 0.5. REGIONAL COLLECTOR - GÜNEY AMERİKA + KUZEY AFRİKA ÖNCELİKLİ!
        try:
            logger.info("🌎 REGIONAL COLLECTOR - South America + North Africa priority...")
            regional = RegionalCollector()
            
            # South America (Brazil, Argentina, Colombia, Mexico, Peru)
            logger.info("  🌎 Collecting South America...")
            sa_leads = regional.collect_south_america()
            logger.info(f"    South America: {len(sa_leads)} leads")
            all_leads.extend(sa_leads)
            
            # North Africa (Egypt, Morocco, Tunisia, Algeria)
            logger.info("  🌍 Collecting North Africa...")
            na_leads = regional.collect_north_africa()
            logger.info(f"    North Africa: {len(na_leads)} leads")
            all_leads.extend(na_leads)
            
            # South Asia (Pakistan, India, Bangladesh)
            logger.info("  🌏 Collecting South Asia...")
            asia_leads = regional.collect_south_asia()
            logger.info(f"    South Asia: {len(asia_leads)} leads")
            all_leads.extend(asia_leads)
            
        except Exception as e:
            logger.error(f"Regional collector failed: {e}")
        
        # 0.55. SA DIRECTORY COLLECTOR - AITE Ecuador + Febratex Brazil (ChatGPT Audit Fix)
        try:
            logger.info("🌎 SA DIRECTORY COLLECTOR - Ecuador AITE + Brazil Febratex...")
            from src.collectors.sa_directory_collector import SouthAmericaDirectoryCollector
            sa_directory = SouthAmericaDirectoryCollector()
            sa_dir_leads = sa_directory.harvest()
            logger.info(f"    SA Directories: {len(sa_dir_leads)} leads (with website: {len([l for l in sa_dir_leads if l.get('website')])})")
            all_leads.extend(sa_dir_leads)
        except Exception as e:
            logger.error(f"SA Directory collector failed: {e}")
        
        # 0.6. AUTO-DISCOVER - Yeni fuar ve kaynakları keşfet
        try:
            logger.info("🔍 Auto-Discover: Finding new fairs and directories...")
            discoverer = AutoDiscover()
            
            # Discover new fairs for priority regions
            new_fairs = discoverer.discover_new_fairs(["south_america", "north_africa"])
            logger.info(f"    New fairs discovered: {len(new_fairs)}")
            
            # Auto-scrape top discovered fairs
            for fair in new_fairs[:3]:
                exhibitors = discoverer.auto_scrape_exhibitors(
                    fair.get("url", ""),
                    fair.get("name", "Unknown")
                )
                if exhibitors:
                    for ex in exhibitors:
                        ex["source_type"] = "auto_discovered_fair"
                    all_leads.extend(exhibitors)
                    logger.info(f"    Scraped {len(exhibitors)} from {fair.get('name', 'fair')}")
                    
        except Exception as e:
            logger.error(f"Auto-discover failed: {e}")
        
        # 1. OEM CUSTOMER HUNTING (Brückner & Monforts) - HIGH PRIORITY!
        try:
            logger.info("🎯 Hunting Brückner & Monforts customers...")
            collector = OEMCustomerHunter()
            leads = collector.harvest()
            logger.info(f"  OEM Customers: {len(leads)} leads")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"OEM Customer hunt failed: {e}")
        
        # 1.5. COMPETITOR CUSTOMER INTEL (Interspare & XTY/Elinmac) - ULTRA HIGH PRIORITY!
        try:
            logger.info("🔥 Hunting Interspare & XTY customer references...")
            collector = CompetitorCustomerIntel()
            leads = collector.harvest()
            logger.info(f"  Competitor Customers: {len(leads)} leads")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"Competitor customer hunt failed: {e}")
        
        # 1.6. IMPORT INTELLIGENCE - Show priority report
        try:
            logger.info("📊 Analyzing import priorities...")
            intel = ImportIntelligence()
            priority_scores = intel.get_country_priority_scores()
            top_5 = sorted(priority_scores.items(), key=lambda x: -x[1])[:5]
            logger.info(f"  Top importers: {', '.join([f'{k}:{v:.0f}' for k,v in top_5])}")
        except Exception as e:
            logger.warning(f"Import intelligence failed: {e}")
        
        # 2. TURKEY TEXTILE COMPANIES - #1 PRIORITY MARKET
        try:
            logger.info("🇹🇷 Collecting Turkish textile companies...")
            collector = TurkeyTextileCollector()
            leads = collector.harvest()
            logger.info(f"  Turkey: {len(leads)} leads")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"Turkey harvest failed: {e}")
        
        # 3. TRADE FAIR EXHIBITORS - ACTIVE BUYERS
        try:
            logger.info("🎪 Collecting trade fair exhibitors...")
            collector = FairExhibitorCollector()
            leads = collector.harvest()
            logger.info(f"  Fairs: {len(leads)} leads")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"Fair harvest failed: {e}")
        
        # 4. OEKO-TEX CERTIFIED COMPANIES
        try:
            logger.info("🏅 Harvesting OEKO-TEX certified companies...")
            collector = OekoTexDirectory()
            leads = collector.harvest(target_iso3=target_countries)
            logger.info(f"  OEKO-TEX: {len(leads)} leads")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"OEKO-TEX harvest failed: {e}")
        
        # 5. GOTS CERTIFIED SUPPLIERS
        try:
            logger.info("🌿 Harvesting GOTS certified suppliers...")
            collector = GotsCertifiedSuppliers()
            leads = collector.harvest(target_iso3=target_countries)
            logger.info(f"  GOTS: {len(leads)} leads")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"GOTS harvest failed: {e}")
        
        # 6. BETTER COTTON MEMBERS
        try:
            logger.info("🌱 Harvesting Better Cotton members...")
            collector = BetterCottonMembers()
            leads = collector.harvest(
                base_url="https://bettercotton.org/find-a-member/",
                max_pages=10,
                use_xlsx=True,
                member_list_url="https://bettercotton.org/wp-content/uploads/2024/01/BCI-Member-List.xlsx"
            )
            logger.info(f"  BetterCotton: {len(leads)} leads")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"BetterCotton harvest failed: {e}")
        
        # 7. TEXBRASIL - BRAZIL COMPANIES
        try:
            logger.info("🇧🇷 Harvesting TexBrasil companies...")
            collector = TexbrasilCompanies()
            leads = collector.harvest()
            logger.info(f"  TexBrasil: {len(leads)} leads")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"TexBrasil harvest failed: {e}")
        
        # Save raw leads
        if all_leads:
            df = pd.DataFrame(all_leads)
            
            # Ensure required columns exist
            required_cols = ["company", "country", "source", "source_type"]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = ""
            
            raw_path = self.project_root / "data" / "staging" / "leads_raw.csv"
            df.to_csv(raw_path, index=False)
            
            self.stats["leads_harvested"] = len(df)
            logger.info(f"Saved {len(df)} raw leads to {raw_path}")
            
            # Summary by source
            logger.info("\nHarvest Summary:")
            logger.info(df["source_type"].value_counts().to_string())
        
        return len(all_leads)
    
    # =========================================================================
    # STAGE 2: ENRICH
    # =========================================================================
    
    def enrich(self):
        """Enrich leads with additional data including website discovery and contact extraction."""
        self.log_stage("ENRICH - Website Discovery + Contact Extraction")
        
        import yaml
        from src.processors.enricher import Enricher
        
        raw_path = self.project_root / "data" / "staging" / "leads_raw.csv"
        enriched_path = self.project_root / "data" / "staging" / "leads_enriched.csv"
        
        if not raw_path.exists():
            logger.error("No raw leads found - run harvest first")
            return 0
        
        # Load configuration files for enrichment
        with open(self.project_root / "config" / "settings.yaml") as f:
            settings = yaml.safe_load(f)
        with open(self.project_root / "config" / "targets.yaml") as f:
            targets = yaml.safe_load(f)
        policies_path = self.project_root / "config" / "policies.yaml"
        policies = {}
        if policies_path.exists():
            with open(policies_path) as f:
                policies = yaml.safe_load(f) or {}
        
        # Inject Brave API key from environment
        if not settings.get("api_keys"):
            settings["api_keys"] = {}
        settings["api_keys"]["brave"] = os.environ.get("BRAVE_API_KEY", "")
        
        # Log enrichment configuration status
        ws_enabled = settings.get("enrichment", {}).get("website_discovery", {}).get("enabled", False)
        ct_enabled = settings.get("enrichment", {}).get("contact", {}).get("enabled", False)
        brave_key = settings["api_keys"].get("brave", "")
        
        logger.info(f"  Website Discovery: {'✓ ENABLED' if ws_enabled else '✗ DISABLED'}")
        logger.info(f"  Contact Enricher: {'✓ ENABLED' if ct_enabled else '✗ DISABLED'}")
        logger.info(f"  Brave API Key: {'✓ SET (' + brave_key[:15] + '...)' if brave_key else '✗ NOT SET'}")
        
        if ws_enabled and not brave_key:
            logger.warning("  ⚠️ Website Discovery enabled but Brave API key not set!")
        
        df = pd.read_csv(raw_path)
        logger.info(f"Enriching {len(df)} leads...")
        
        enricher = Enricher(
            targets_config=targets,
            settings=settings,
            policies=policies
        )
        
        # Convert DataFrame to list of dicts
        leads_list = df.to_dict('records')
        
        # Process all leads
        logger.info("  Processing leads...")
        enriched_leads = enricher.enrich(leads_list)
        
        logger.info(f"  Enriched {len(enriched_leads)} leads")
        
        enriched_df = pd.DataFrame(enriched_leads)
        enriched_df.to_csv(enriched_path, index=False)
        
        self.stats["leads_enriched"] = len(enriched_df)
        logger.info(f"Saved {len(enriched_df)} enriched leads")
        
        return len(enriched_df)
    
    # =========================================================================
    # STAGE 3: DEDUPE
    # =========================================================================
    
    def dedupe(self):
        """Remove duplicate leads."""
        self.log_stage("DEDUPE - Removing Duplicates")
        
        from src.processors.dedupe import LeadDedupe
        
        enriched_path = self.project_root / "data" / "staging" / "leads_enriched.csv"
        master_path = self.project_root / "data" / "processed" / "leads_master.csv"
        
        # Fall back to raw if enriched doesn't exist
        if not enriched_path.exists():
            enriched_path = self.project_root / "data" / "staging" / "leads_raw.csv"
        
        if not enriched_path.exists():
            logger.error("No leads found to dedupe")
            return 0
        
        df = pd.read_csv(enriched_path, on_bad_lines='skip')
        logger.info(f"Deduping {len(df)} leads...")
        
        deduper = LeadDedupe()
        
        # Convert to list of dicts
        leads_list = df.to_dict('records')
        deduped_list, audit = deduper.dedupe(leads_list)
        
        # Save audit log
        if audit:
            audit_df = pd.DataFrame(audit)
            audit_df.to_csv(self.project_root / "outputs" / "dedupe_audit.csv", index=False)
        
        deduped_df = pd.DataFrame(deduped_list)
        
        # Save master
        master_path.parent.mkdir(parents=True, exist_ok=True)
        deduped_df.to_csv(master_path, index=False)
        
        self.stats["leads_deduped"] = len(deduped_df)
        removed = len(df) - len(deduped_df)
        logger.info(f"Removed {removed} duplicates, {len(deduped_df)} unique leads")
        
        return len(deduped_df)
    
    # =========================================================================
    # STAGE 3.5: ENTITY QUALITY GATE (NEW - project_v3 recommendation)
    # =========================================================================
    
    def apply_quality_gate(self):
        """Apply entity quality filtering to reject non-companies."""
        self.log_stage("ENTITY QUALITY GATE V2 - Filtering Non-Companies + Country Normalization")
        
        from src.processors.entity_quality_gate_v2 import EntityQualityGateV2
        
        enriched_path = self.project_root / "data" / "staging" / "leads_enriched.csv"
        
        if not enriched_path.exists():
            enriched_path = self.project_root / "data" / "staging" / "leads_raw.csv"
        
        if not enriched_path.exists():
            logger.error("No leads found for quality gate")
            return 0
        
        df = pd.read_csv(enriched_path, on_bad_lines='skip')
        logger.info(f"Quality checking {len(df)} leads...")
        
        gate = EntityQualityGateV2()
        leads_list = df.to_dict('records')
        
        # Filter leads with V2 quality gate
        filtered = []
        rejected_count = 0
        rejection_reasons = {}
        grade_counts = {'A': 0, 'B': 0, 'C': 0}
        parts_supplier_count = 0
        
        for lead in leads_list:
            # Normalize country first (Turkey/Türkiye → Turkey)
            if lead.get('country'):
                lead['country'] = gate.normalize_country(lead['country'])
            
            grade, reason = gate.grade_entity(lead)
            lead['entity_quality'] = grade
            lead['quality_reason'] = reason
            
            # Check if it's a parts supplier (flag but don't reject)
            if gate._is_parts_supplier(lead):
                lead['is_parts_supplier'] = True
                parts_supplier_count += 1
            else:
                lead['is_parts_supplier'] = False
            
            if grade != 'REJECT':
                filtered.append(lead)
                if grade in grade_counts:
                    grade_counts[grade] += 1
            else:
                rejected_count += 1
                # Track rejection reasons
                reason_key = reason.split(':')[0] if ':' in reason else reason[:30]
                rejection_reasons[reason_key] = rejection_reasons.get(reason_key, 0) + 1
        
        # Save filtered leads back
        filtered_df = pd.DataFrame(filtered)
        filtered_df.to_csv(enriched_path, index=False)
        
        logger.info(f"\nQuality Gate V2 Results:")
        logger.info(f"  Total: {len(leads_list)}, Passed: {len(filtered)}, Rejected: {rejected_count}")
        for grade, count in grade_counts.items():
            pct = (count / len(filtered) * 100) if len(filtered) > 0 else 0
            logger.info(f"  Grade {grade}: {count} ({pct:.1f}%)")
        
        logger.info(f"\n  ⚠️ Parts suppliers flagged: {parts_supplier_count} (not rejected, but flagged)")
        
        logger.info(f"\nTop Rejection Reasons:")
        for reason, count in sorted(rejection_reasons.items(), key=lambda x: -x[1])[:10]:
            logger.info(f"  - {reason}: {count}")
        
        return len(filtered)
    
    # =========================================================================
    # STAGE 4: SCORE
    # =========================================================================
    
    def score(self):
        """Score and rank leads."""
        self.log_stage("SCORE - Ranking Leads")
        
        from src.processors.scorer import Scorer
        
        master_path = self.project_root / "data" / "processed" / "leads_master.csv"
        
        if not master_path.exists():
            logger.error("No master leads found - run dedupe first")
            return 0
        
        df = pd.read_csv(master_path, on_bad_lines='skip')
        logger.info(f"Scoring {len(df)} leads...")
        
        import yaml
        with open(self.project_root / 'config' / 'targets.yaml') as f:
            targets_config = yaml.safe_load(f)
        with open(self.project_root / 'config' / 'scoring.yaml') as f:
            scoring_config = yaml.safe_load(f)
        
        # Load country priority data if available
        country_priority = {}
        priority_path = self.project_root / "data" / "processed" / "country_priority_comtrade.csv"
        if priority_path.exists():
            priority_df = pd.read_csv(priority_path)
            if "iso3" in priority_df.columns and "import_value" in priority_df.columns:
                for _, row in priority_df.iterrows():
                    country_priority[row["iso3"]] = row["import_value"]
        
        scorer = Scorer(targets_config, scoring_config, country_priority)
        
        # Score each lead
        leads_list = df.to_dict('records')
        scored_leads = [scorer.score_lead(lead) for lead in leads_list]
        scored_leads = scorer.rank_leads(scored_leads)
        
        scored_df = pd.DataFrame(scored_leads)
        
        # Save back
        scored_df.to_csv(master_path, index=False)
        
        self.stats["leads_scored"] = len(scored_df)
        
        # Show score distribution
        if "score" in scored_df.columns:
            logger.info("\nScore Distribution:")
            logger.info(f"  High (>70): {len(scored_df[scored_df['score'] > 70])}")
            logger.info(f"  Medium (40-70): {len(scored_df[(scored_df['score'] >= 40) & (scored_df['score'] <= 70)])}")
            logger.info(f"  Low (<40): {len(scored_df[scored_df['score'] < 40])}")
        
        return len(scored_df)
    
    # =========================================================================
    # STAGE 5.5: CUSTOMER QUALIFICATION (NEW)
    # =========================================================================
    
    def qualify_customers(self):
        """Qualify leads as real stenter customers."""
        self.log_stage("CUSTOMER QUALIFICATION - Finding Real Customers")
        
        from src.processors.customer_qualifier import CustomerQualifier
        
        master_path = self.project_root / "data" / "processed" / "leads_master.csv"
        
        if not master_path.exists():
            logger.error("No master leads found")
            return 0
        
        df = pd.read_csv(master_path, on_bad_lines='skip')
        logger.info(f"Qualifying {len(df)} leads...")
        
        qualifier = CustomerQualifier()
        leads_list = df.to_dict('records')
        
        qualified = []
        for lead in leads_list:
            result = qualifier.qualify_lead(lead)
            lead.update(result)
            if result.get('is_qualified'):
                qualified.append(lead)
        
        # Save qualified customers
        output_dir = self.project_root / "outputs" / "crm"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        qualified_df = pd.DataFrame(qualified)
        qualified_df.to_csv(output_dir / "qualified_customers.csv", index=False)
        
        logger.info(f"\nQualification Results:")
        logger.info(f"  Qualified: {len(qualified)} / {len(df)} ({100*len(qualified)/max(1,len(df)):.1f}%)")
        
        # Show by source
        if qualified:
            by_source = qualified_df.groupby('source_type').size().to_dict()
            logger.info(f"\n  By Source:")
            for source, count in sorted(by_source.items(), key=lambda x: -x[1]):
                logger.info(f"    {source}: {count}")
        
        return len(qualified)
    
    # =========================================================================
    # STAGE 5.6: CONTACT VERIFICATION (GPT Fix #4)
    # =========================================================================
    
    def verify_contacts(self):
        """Verify email/phone contacts for sales readiness."""
        self.log_stage("CONTACT VERIFICATION - Validating Sales Readiness")
        
        from src.processors.verifier import ContactVerifier
        
        master_path = self.project_root / "data" / "processed" / "leads_master.csv"
        
        if not master_path.exists():
            logger.error("No master leads found")
            return 0
        
        df = pd.read_csv(master_path, on_bad_lines='skip')
        logger.info(f"Verifying contacts for {len(df)} leads...")
        
        verifier = ContactVerifier()
        leads_list = df.to_dict('records')
        
        verified_leads = [verifier.verify_lead(lead) for lead in leads_list]
        
        # Count by confidence level
        high_conf = sum(1 for l in verified_leads if l.get('contact_confidence') == 'high')
        med_conf = sum(1 for l in verified_leads if l.get('contact_confidence') == 'medium')
        low_conf = sum(1 for l in verified_leads if l.get('contact_confidence') == 'low')
        
        logger.info(f"\nContact Verification Results:")
        logger.info(f"  High confidence: {high_conf} ({100*high_conf/max(1,len(verified_leads)):.1f}%)")
        logger.info(f"  Medium confidence: {med_conf} ({100*med_conf/max(1,len(verified_leads)):.1f}%)")
        logger.info(f"  Low confidence: {low_conf} ({100*low_conf/max(1,len(verified_leads)):.1f}%)")
        
        # Save verified leads
        verified_df = pd.DataFrame(verified_leads)
        verified_df.to_csv(master_path, index=False)
        
        return high_conf + med_conf
    
    # =========================================================================
    # STAGE 5.7: ROLE CLASSIFICATION (GPT Fix #5)
    # =========================================================================
    
    def classify_roles(self):
        """Classify leads as CUSTOMER or INTERMEDIARY."""
        self.log_stage("ROLE CLASSIFICATION - Customer vs Intermediary")
        
        from src.processors.role_classifier import RoleClassifier
        
        master_path = self.project_root / "data" / "processed" / "leads_master.csv"
        
        if not master_path.exists():
            logger.error("No master leads found")
            return 0
        
        df = pd.read_csv(master_path, on_bad_lines='skip')
        logger.info(f"Classifying roles for {len(df)} leads...")
        
        classifier = RoleClassifier()
        leads_list = df.to_dict('records')
        
        customers, intermediaries, unknown = classifier.separate_by_role(leads_list)
        
        logger.info(f"\nRole Classification Results:")
        logger.info(f"  CUSTOMER: {len(customers)} ({100*len(customers)/max(1,len(leads_list)):.1f}%)")
        logger.info(f"  INTERMEDIARY: {len(intermediaries)} ({100*len(intermediaries)/max(1,len(leads_list)):.1f}%)")
        logger.info(f"  UNKNOWN: {len(unknown)} ({100*len(unknown)/max(1,len(leads_list)):.1f}%)")
        
        # Save all leads with role classification
        all_classified = customers + intermediaries + unknown
        classified_df = pd.DataFrame(all_classified)
        classified_df.to_csv(master_path, index=False)
        
        # Save intermediaries separately (for reference, not for sales)
        if intermediaries:
            output_dir = self.project_root / "outputs" / "crm"
            output_dir.mkdir(parents=True, exist_ok=True)
            intermediary_df = pd.DataFrame(intermediaries)
            intermediary_df.to_csv(output_dir / "intermediaries.csv", index=False)
            logger.info(f"  Saved {len(intermediaries)} intermediaries to intermediaries.csv")
        
        return len(customers)
    
    # =========================================================================
    # STAGE 6: EXPORT
    # =========================================================================
    
    def export(self):
        """Export CRM-ready leads."""
        self.log_stage("EXPORT - Creating CRM Files")
        
        from src.processors.exporter import Exporter
        
        master_path = self.project_root / "data" / "processed" / "leads_master.csv"
        output_dir = self.project_root / "outputs" / "crm"
        
        if not master_path.exists():
            logger.error("No master leads found - run score first")
            return 0
        
        df = pd.read_csv(master_path, on_bad_lines='skip')
        logger.info(f"Exporting {len(df)} leads...")
        
        exporter = Exporter(output_dir=str(output_dir))
        
        # Export all formats - convert to list of dicts
        leads_list = df.to_dict('records')
        
        # Main export (this creates targets_master.csv and top100.csv)
        exporter.export_targets(leads_list)
        
        # By source type
        for source_type in df["source_type"].unique():
            if pd.notna(source_type):
                subset = df[df["source_type"] == source_type]
                subset.to_csv(output_dir / f"leads_{source_type}.csv", index=False)
        
        # LinkedIn X-Ray queries
        self._generate_linkedin_queries(df, output_dir)
        
        self.stats["leads_exported"] = len(df)
        logger.info(f"Exported to {output_dir}")
        
        return len(df)
    
    def _generate_linkedin_queries(self, df: pd.DataFrame, output_dir: Path):
        """Generate LinkedIn X-Ray search queries."""
        queries = []
        
        for _, row in df.head(100).iterrows():
            company = row.get("company", "")
            if company and len(company) > 3:
                query = f'site:linkedin.com/in "{company}" (procurement OR purchasing OR buyer OR "supply chain")'
                queries.append({
                    "company": company,
                    "country": row.get("country", ""),
                    "query": query,
                })
        
        if queries:
            queries_df = pd.DataFrame(queries)
            queries_df.to_csv(output_dir / "linkedin_xray_queries.csv", index=False)
            logger.info(f"  Generated {len(queries)} LinkedIn X-Ray queries")
    
    # =========================================================================
    # FULL PIPELINE
    # =========================================================================
    
    def run(self, discover: bool = False):
        """Run full pipeline."""
        logger.info("\n" + "="*70)
        logger.info("LEAD INTEL v2 - FULL AUTONOMOUS PIPELINE")
        logger.info("="*70)
        logger.info(f"Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*70 + "\n")
        
        try:
            # Stage 0: Discovery (optional)
            if discover:
                self.discover_new_sources()
            
            # Stage 1: Harvest
            self.harvest()
            
            # Stage 2: Enrich
            self.enrich()
            
            # Stage 3: Entity Quality Gate (NEW - from project_v3)
            self.apply_quality_gate()
            
            # Stage 4: Dedupe
            self.dedupe()
            
            # Stage 5: Score
            self.score()
            
            # Stage 5.5: Customer Qualification
            self.qualify_customers()
            
            # Stage 5.6: Contact Verification (GPT Fix #4)
            self.verify_contacts()
            
            # Stage 5.7: Role Classification (GPT Fix #5)
            self.classify_roles()
            
            # Stage 6: Export
            self.export()
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        
        # Final report
        self._print_final_report()
        
        return self.stats
    
    def _print_final_report(self):
        """Print final pipeline report."""
        elapsed = (datetime.now() - self.start_time).seconds
        
        logger.info("\n" + "="*70)
        logger.info("PIPELINE COMPLETE")
        logger.info("="*70)
        logger.info(f"Duration: {elapsed} seconds")
        logger.info("")
        logger.info("Statistics:")
        for key, value in self.stats.items():
            logger.info(f"  {key}: {value}")
        logger.info("")
        
        # Load final output for summary
        output_path = self.project_root / "outputs" / "crm" / "targets_master.csv"
        if output_path.exists():
            df = pd.read_csv(output_path)
            
            logger.info("Final Output Summary:")
            logger.info(f"  Total Leads: {len(df)}")
            
            if "email" in df.columns:
                with_email = df["email"].notna().sum()
                logger.info(f"  With Email: {with_email} ({100*with_email/len(df):.1f}%)")
            
            logger.info("")
            logger.info("By Country:")
            logger.info(df["country"].value_counts().head(10).to_string())
            
            logger.info("")
            logger.info("By Source:")
            logger.info(df["source_type"].value_counts().to_string())
            
            if "score" in df.columns:
                logger.info("")
                logger.info("Top 10 Leads:")
                # Use emails (plural) if available, otherwise try email
                email_col = "emails" if "emails" in df.columns else "email" if "email" in df.columns else None
                display_cols = ["company", "country", "score"]
                if email_col:
                    display_cols.append(email_col)
                top10 = df.nlargest(10, "score")[display_cols]
                for _, row in top10.iterrows():
                    email_val = row.get(email_col, "") if email_col else ""
                    email_str = str(email_val)[:30] if pd.notna(email_val) and email_val else ""
                    logger.info(f"  [{row['score']:.0f}] {row['company'][:35]:<35} | {row['country']:<10} | {email_str}")
        
        logger.info("")
        logger.info(f"Output files: {self.project_root / 'outputs' / 'crm'}/")
        logger.info("="*70)
    
    def status(self):
        """Show current pipeline status."""
        logger.info("\n" + "="*70)
        logger.info("LEAD INTEL v2 - STATUS")
        logger.info("="*70 + "\n")
        
        # Check data files
        files = [
            ("Raw Leads", "data/staging/leads_raw.csv"),
            ("Enriched Leads", "data/staging/leads_enriched.csv"),
            ("Master Leads", "data/processed/leads_master.csv"),
            ("CRM Export", "outputs/crm/targets_master.csv"),
            ("Top 100", "outputs/crm/top100.csv"),
        ]
        
        for name, path in files:
            full_path = self.project_root / path
            if full_path.exists():
                try:
                    df = pd.read_csv(full_path, on_bad_lines='skip')
                    mtime = datetime.fromtimestamp(full_path.stat().st_mtime)
                    logger.info(f"✅ {name}: {len(df)} records (updated {mtime.strftime('%H:%M')})")
                except Exception as e:
                    logger.info(f"⚠️ {name}: exists but parse error - {e}")
            else:
                logger.info(f"❌ {name}: not found")
        
        # AutoDiscover status
        logger.info("")
        try:
            from src.autodiscover.engine import AutoDiscoverEngine
            engine = AutoDiscoverEngine()
            status = engine.status()
            logger.info(f"AutoDiscover:")
            logger.info(f"  Sources discovered: {status['discovered']}")
            logger.info(f"  Sites diagnosed: {status['diagnosed']}")
            logger.info(f"  Adapters generated: {status['generated']}")
        except Exception:
            logger.info("AutoDiscover: not initialized")
        
        logger.info("")


def main():
    parser = argparse.ArgumentParser(description="Lead Intel v2 - Autonomous Pipeline")
    parser.add_argument("--discover", action="store_true", help="Include source discovery")
    parser.add_argument("--harvest", action="store_true", help="Only harvest + process")
    parser.add_argument("--status", action="store_true", help="Show current status")
    parser.add_argument("--countries", type=str, help="Target countries (comma-separated)")
    
    args = parser.parse_args()
    
    pipeline = LeadIntelPipeline()
    
    if args.status:
        pipeline.status()
    else:
        pipeline.run(discover=args.discover)


if __name__ == "__main__":
    main()
