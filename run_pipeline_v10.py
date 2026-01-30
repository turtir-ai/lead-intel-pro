#!/usr/bin/env python3
# =============================================================================
# RUN PIPELINE V10 - Lead Intel Pro Master Orchestrator
# =============================================================================
# Purpose: Execute the complete V10 pipeline with all new features:
#   - Multi-source collection from sources_registry.yaml
#   - API hunting for hidden JSON endpoints
#   - Heuristic (LLM-free) scoring
#   - HS code matching
#   - Email guessing
#   - Region quotas
#   - Full dedupe with source priority
# 
# Usage:
#   python run_pipeline_v10.py                    # Full pipeline
#   python run_pipeline_v10.py --mode=collect     # Collection only
#   python run_pipeline_v10.py --mode=score       # Score existing leads
#   python run_pipeline_v10.py --mode=export      # Export only
#   python run_pipeline_v10.py --test             # Test mode (3 sources)
# =============================================================================

import asyncio
import argparse
import sys
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import pipeline components
from src.processors.heuristic_scorer import HeuristicScorer, ScoreResult
from src.processors.dedupe import Deduplicator
from src.processors.enricher import LeadEnricher
from src.processors.scorer import LeadScorer
from src.processors.verifier import ContactVerifier
from src.processors.role_classifier import RoleClassifier
from src.extractors.email_guesser import EmailGuesser, guess_emails_for_leads
from src.utils.logger import get_logger

# Conditional imports for V10 features
try:
    from src.probers.api_hunter import APIHunter, PLAYWRIGHT_AVAILABLE
    from src.probers.safety_guard import SafetyGuard, is_safe_endpoint
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    APIHunter = None
    SafetyGuard = None

logger = get_logger("pipeline_v10")


class PipelineV10:
    """
    V10 Lead Intel Pipeline Orchestrator
    
    Features:
    - Multi-source collection from 20+ configured sources
    - API hunting for hidden JSON endpoints
    - Heuristic LLM-free scoring
    - HS code matching and product relevance
    - Pattern-based email guessing
    - Enhanced dedupe with source priority
    - Region quotas for balanced output
    """
    
    def __init__(self, config_dir: Path = None, data_dir: Path = None):
        self.config_dir = config_dir or PROJECT_ROOT / "config"
        self.data_dir = data_dir or PROJECT_ROOT / "data"
        self.output_dir = PROJECT_ROOT / "outputs"
        
        # Load configurations
        self._load_configs()
        
        # Initialize components
        self.heuristic_scorer = HeuristicScorer(self.config_dir)
        self.deduplicator = Deduplicator()
        self.enricher = LeadEnricher()
        self.verifier = ContactVerifier()
        self.role_classifier = RoleClassifier()
        self.email_guesser = EmailGuesser()
        
        if PLAYWRIGHT_AVAILABLE:
            self.api_hunter = APIHunter(self.data_dir / "api_harvest", self.config_dir)
            self.safety_guard = SafetyGuard()
        else:
            self.api_hunter = None
            self.safety_guard = None
            logger.warning("Playwright not available - API hunting disabled")
            
        # Statistics
        self.stats = {
            "sources_processed": 0,
            "apis_discovered": 0,
            "leads_collected": 0,
            "leads_scored": 0,
            "leads_qualified": 0,
            "leads_deduplicated": 0,
            "leads_exported": 0,
            "errors": []
        }
        
    def _load_configs(self) -> None:
        """Load all configuration files"""
        
        # Sources registry
        sources_file = self.config_dir / "sources_registry.yaml"
        if sources_file.exists():
            with open(sources_file, 'r', encoding='utf-8') as f:
                self.sources_config = yaml.safe_load(f)
        else:
            self.sources_config = {"sources": []}
            logger.warning("sources_registry.yaml not found")
            
        # HS codes
        hs_file = self.config_dir / "hs_codes.yaml"
        if hs_file.exists():
            with open(hs_file, 'r', encoding='utf-8') as f:
                self.hs_config = yaml.safe_load(f)
        else:
            self.hs_config = {}
            
        # Scoring config
        scoring_file = self.config_dir / "scoring.yaml"
        if scoring_file.exists():
            with open(scoring_file, 'r', encoding='utf-8') as f:
                self.scoring_config = yaml.safe_load(f)
        else:
            self.scoring_config = {}
            
    def get_sources_by_priority(self, min_priority: int = 0,
                                 max_sources: int = None) -> List[Dict]:
        """Get sources sorted by priority"""
        sources = self.sources_config.get("sources", [])
        
        # Filter by minimum priority
        sources = [s for s in sources if s.get("priority", 0) >= min_priority]
        
        # Sort by priority (highest first)
        sources.sort(key=lambda x: -x.get("priority", 0))
        
        # Limit if requested
        if max_sources:
            sources = sources[:max_sources]
            
        return sources
        
    async def hunt_apis_for_source(self, source: Dict) -> List[Dict]:
        """
        Use API Hunter to discover hidden JSON endpoints for a source.
        
        Returns list of discovered API endpoints or empty list.
        """
        if not self.api_hunter:
            return []
            
        url = source.get("directory_url") or source.get("homepage_url")
        if not url:
            return []
            
        logger.info(f"🔍 Hunting APIs: {source['name']}")
        
        try:
            result = await self.api_hunter.hunt(url)
            
            if result.success and result.endpoints:
                self.stats["apis_discovered"] += len(result.endpoints)
                logger.info(f"  ✅ Found {len(result.endpoints)} API endpoints")
                
                # Return endpoints for direct fetching
                return [
                    {
                        "url": ep.url,
                        "is_paginated": ep.is_paginated,
                        "pagination_pattern": ep.pagination_pattern
                    }
                    for ep in result.endpoints
                ]
            else:
                logger.info(f"  ℹ️ No APIs found, will use HTML fallback")
                return []
                
        except Exception as e:
            logger.error(f"  ❌ API hunt failed: {e}")
            self.stats["errors"].append(f"API hunt {source['id']}: {str(e)}")
            return []
            
    async def collect_from_api(self, api_endpoint: Dict) -> List[Dict]:
        """Fetch leads directly from discovered API endpoint"""
        if not self.api_hunter:
            return []
            
        try:
            data = await self.api_hunter.fetch_json_directly(api_endpoint["url"])
            
            if data:
                # Handle different response formats
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    # Common patterns: data.results, data.items, data.members
                    return (
                        data.get("results") or 
                        data.get("items") or 
                        data.get("members") or 
                        data.get("data") or 
                        [data]
                    )
            return []
            
        except Exception as e:
            logger.error(f"Failed to fetch API: {e}")
            return []
            
    def score_leads(self, leads: List[Dict]) -> List[Dict]:
        """Score leads using heuristic scorer"""
        scored = []
        
        for lead in leads:
            # Prepare text for scoring
            text = " ".join([
                lead.get("description", ""),
                lead.get("about", ""),
                lead.get("products", ""),
                lead.get("services", ""),
            ])
            
            title = lead.get("company_name", lead.get("name", ""))
            
            metadata = {
                "company_name": title,
                "country": lead.get("country", ""),
                "source": lead.get("source", ""),
            }
            
            # Calculate score
            result = self.heuristic_scorer.calculate_score(text, title, metadata)
            
            # Attach score data to lead
            lead["_heuristic_score"] = result.score
            lead["_confidence"] = result.confidence
            lead["_is_lead"] = result.is_lead
            lead["_matched_hs_codes"] = result.matched_hs_codes
            lead["_product_match"] = result.product_match
            lead["_machine_types"] = result.machine_types
            lead["_evidence"] = result.evidence[:5]
            lead["_warnings"] = result.warnings
            
            if result.is_lead:
                self.stats["leads_qualified"] += 1
                
            scored.append(lead)
            
        self.stats["leads_scored"] += len(leads)
        return scored
        
    def add_email_guesses(self, leads: List[Dict]) -> List[Dict]:
        """Add email guesses to leads"""
        for lead in leads:
            guesses = self.email_guesser.guess_for_lead(lead)
            lead["guessed_emails"] = [g.email for g in guesses[:5]]
            
        return leads
        
    def apply_region_quotas(self, leads: List[Dict]) -> List[Dict]:
        """Apply region quotas from scoring config"""
        quotas = self.scoring_config.get("export", {}).get("region_quotas", {})
        
        if not quotas:
            return leads
            
        # Group leads by region
        by_region = {}
        for lead in leads:
            country = lead.get("country", "").lower()
            region = self._get_region_for_country(country)
            
            if region not in by_region:
                by_region[region] = []
            by_region[region].append(lead)
            
        # Apply quotas
        result = []
        for region, region_leads in by_region.items():
            quota = quotas.get(region, quotas.get("other", 100))
            
            # Sort by score and take top N
            region_leads.sort(key=lambda x: -x.get("_heuristic_score", 0))
            result.extend(region_leads[:quota])
            
            logger.info(f"  {region}: {len(region_leads[:quota])}/{len(region_leads)} (quota: {quota})")
            
        return result
        
    def _get_region_for_country(self, country: str) -> str:
        """Map country to region"""
        regions = self.scoring_config.get("target_regions", {})
        
        for region_name, config in regions.items():
            if country in config.get("countries", []):
                return region_name
                
        return "other"
        
    async def run_collection(self, sources: List[Dict] = None,
                             test_mode: bool = False) -> List[Dict]:
        """Run collection phase"""
        if sources is None:
            sources = self.get_sources_by_priority(min_priority=60)
            
        if test_mode:
            sources = sources[:3]
            logger.info(f"🧪 TEST MODE: Processing {len(sources)} sources only")
            
        all_leads = []
        
        for source in sources:
            logger.info(f"\n📥 Processing: {source['name']} ({source.get('country', 'Global')})")
            
            leads = []
            
            # Strategy 1: Try API hunting first (if JS-heavy or unknown)
            if source.get("js_render") or source.get("format") == "html":
                api_endpoints = await self.hunt_apis_for_source(source)
                
                if api_endpoints:
                    for endpoint in api_endpoints[:2]:  # Limit to 2 endpoints
                        api_leads = await self.collect_from_api(endpoint)
                        leads.extend(api_leads)
                        
            # Strategy 2: Direct download for known formats
            if not leads and source.get("format") in ["xlsx", "csv"]:
                leads = self._collect_spreadsheet(source)
                
            # Strategy 3: PDF extraction
            if not leads and source.get("format") == "pdf":
                leads = self._collect_pdf(source)
                
            # Add source metadata to leads
            for lead in leads:
                lead["source"] = source["id"]
                lead["source_name"] = source["name"]
                lead["source_type"] = source.get("source_type", "unknown")
                lead["source_priority"] = source.get("priority", 50)
                lead["source_url"] = source.get("directory_url") or source.get("homepage_url")
                
            logger.info(f"  → Collected {len(leads)} leads")
            all_leads.extend(leads)
            self.stats["sources_processed"] += 1
            
            # Rate limiting
            await asyncio.sleep(source.get("rate_limit", 1))
            
        self.stats["leads_collected"] = len(all_leads)
        return all_leads
        
    def _collect_spreadsheet(self, source: Dict) -> List[Dict]:
        """Collect leads from XLSX/CSV file"""
        url = source.get("directory_url")
        if not url:
            return []
            
        try:
            import requests
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Save temporarily
            ext = "xlsx" if "xlsx" in url.lower() else "csv"
            temp_file = self.data_dir / "raw" / f"temp_{source['id']}.{ext}"
            temp_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(temp_file, 'wb') as f:
                f.write(response.content)
                
            # Read with pandas
            if ext == "xlsx":
                df = pd.read_excel(temp_file)
            else:
                df = pd.read_csv(temp_file)
                
            # Convert to list of dicts
            leads = df.to_dict('records')
            
            # Cleanup
            temp_file.unlink()
            
            return leads
            
        except Exception as e:
            logger.error(f"Failed to collect spreadsheet: {e}")
            return []
            
    def _collect_pdf(self, source: Dict) -> List[Dict]:
        """Collect leads from PDF catalog"""
        # Placeholder - implement with pdfplumber
        logger.info("  ℹ️ PDF collection not yet implemented")
        return []
        
    def run_full_pipeline(self, test_mode: bool = False) -> pd.DataFrame:
        """Run complete pipeline synchronously"""
        return asyncio.run(self.run_full_pipeline_async(test_mode))
        
    async def run_full_pipeline_async(self, test_mode: bool = False) -> pd.DataFrame:
        """Run complete pipeline"""
        start_time = datetime.now()
        
        logger.info("=" * 70)
        logger.info("🚀 LEAD INTEL V10 PIPELINE")
        logger.info("=" * 70)
        logger.info(f"Start time: {start_time.isoformat()}")
        
        # Phase 1: Collection
        logger.info("\n📥 PHASE 1: COLLECTION")
        logger.info("-" * 50)
        leads = await self.run_collection(test_mode=test_mode)
        
        if not leads:
            logger.warning("No leads collected!")
            return pd.DataFrame()
            
        # Phase 2: Scoring
        logger.info("\n📊 PHASE 2: HEURISTIC SCORING")
        logger.info("-" * 50)
        leads = self.score_leads(leads)
        
        # Filter to qualified leads only
        qualified = [l for l in leads if l.get("_is_lead")]
        logger.info(f"Qualified leads: {len(qualified)}/{len(leads)}")
        
        if not qualified:
            logger.warning("No leads passed scoring threshold!")
            return pd.DataFrame()
            
        # Phase 3: Email Guessing
        logger.info("\n📧 PHASE 3: EMAIL GUESSING")
        logger.info("-" * 50)
        qualified = self.add_email_guesses(qualified)
        
        # Phase 4: Deduplication
        logger.info("\n🔄 PHASE 4: DEDUPLICATION")
        logger.info("-" * 50)
        
        df = pd.DataFrame(qualified)
        df_deduped = self.deduplicator.deduplicate(df)
        self.stats["leads_deduplicated"] = len(df_deduped)
        logger.info(f"After dedupe: {len(df_deduped)} leads")
        
        # Phase 5: Region Quotas
        logger.info("\n🌍 PHASE 5: REGION QUOTAS")
        logger.info("-" * 50)
        leads_list = df_deduped.to_dict('records')
        leads_quota = self.apply_region_quotas(leads_list)
        df_final = pd.DataFrame(leads_quota)
        
        # Phase 6: Export
        logger.info("\n💾 PHASE 6: EXPORT")
        logger.info("-" * 50)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = self.output_dir / "crm" / f"leads_v10_{timestamp}.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        df_final.to_csv(output_file, index=False)
        self.stats["leads_exported"] = len(df_final)
        logger.info(f"Exported: {output_file}")
        
        # Final stats
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("\n" + "=" * 70)
        logger.info("📈 PIPELINE COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Duration: {duration:.1f} seconds")
        logger.info(f"Sources processed: {self.stats['sources_processed']}")
        logger.info(f"APIs discovered: {self.stats['apis_discovered']}")
        logger.info(f"Leads collected: {self.stats['leads_collected']}")
        logger.info(f"Leads qualified: {self.stats['leads_qualified']}")
        logger.info(f"Leads deduplicated: {self.stats['leads_deduplicated']}")
        logger.info(f"Leads exported: {self.stats['leads_exported']}")
        
        if self.stats["errors"]:
            logger.warning(f"Errors: {len(self.stats['errors'])}")
            for err in self.stats["errors"][:5]:
                logger.warning(f"  • {err}")
                
        return df_final


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Lead Intel V10 Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline_v10.py                    # Full pipeline
  python run_pipeline_v10.py --test             # Test mode (3 sources)
  python run_pipeline_v10.py --mode=collect     # Collection only
  python run_pipeline_v10.py --mode=score       # Score existing leads
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["full", "collect", "score", "export"],
        default="full",
        help="Pipeline mode (default: full)"
    )
    
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode - process only 3 sources"
    )
    
    parser.add_argument(
        "--min-priority",
        type=int,
        default=60,
        help="Minimum source priority (default: 60)"
    )
    
    args = parser.parse_args()
    
    pipeline = PipelineV10()
    
    if args.mode == "full":
        df = pipeline.run_full_pipeline(test_mode=args.test)
        print(f"\n✅ Pipeline complete: {len(df)} leads exported")
    else:
        print(f"Mode '{args.mode}' not yet implemented")


if __name__ == "__main__":
    main()
