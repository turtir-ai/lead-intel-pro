"""
AutoDiscover Engine - Main Orchestrator

Autonomous web intelligence system that:
1. Discovers new sources (Brave Search API)
2. Diagnoses sites (Playwright network capture)
3. Analyzes patterns (Heuristics, no LLM)
4. Generates adapters (Python collectors)
5. Integrates with pipeline (lead_intel_v2)

No LLM required. Pure Python automation.

Usage:
    python -m src.autodiscover.engine discover --countries Egypt,Morocco
    python -m src.autodiscover.engine diagnose --url https://example.com
    python -m src.autodiscover.engine process --all
    python -m src.autodiscover.engine run --auto
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime
import argparse

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.autodiscover.discoverer import BraveDiscoverer
from src.autodiscover.diagnoser import SiteDiagnoser
from src.autodiscover.analyzer import PatternAnalyzer
from src.autodiscover.adapter_generator import AdapterGenerator
from src.utils.logger import get_logger

logger = get_logger(__name__)


class AutoDiscoverEngine:
    """
    Main orchestrator for autonomous web intelligence.
    
    Pipeline:
    1. Discovery: Brave Search → candidate URLs
    2. Diagnosis: Playwright → network/DOM capture
    3. Analysis: Pattern detection → field mapping
    4. Generation: Create Python collectors
    5. Integration: Add to pipeline sources
    """
    
    def __init__(self, config_path: str = "config/autodiscover.yaml"):
        self.discoverer = BraveDiscoverer()
        self.diagnoser = SiteDiagnoser()
        self.analyzer = PatternAnalyzer()
        self.generator = AdapterGenerator()
        
        self.results_dir = Path("data/autodiscover")
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # State tracking
        self.state_path = self.results_dir / "engine_state.json"
        self.state = self._load_state()
    
    def _load_state(self) -> Dict:
        """Load engine state."""
        if self.state_path.exists():
            with open(self.state_path, "r") as f:
                return json.load(f)
        return {
            "discovered": {},  # domain -> discovery info
            "diagnosed": {},   # domain -> diagnosis session
            "generated": {},   # domain -> adapter path
            "integrated": {},  # domain -> integration status
        }
    
    def _save_state(self):
        """Save engine state."""
        with open(self.state_path, "w") as f:
            json.dump(self.state, f, indent=2)
    
    # =========================================================================
    # STAGE 1: DISCOVERY
    # =========================================================================
    
    def discover(self, 
                 countries: Optional[List[str]] = None,
                 max_queries: int = 20) -> List[Dict]:
        """
        Stage 1: Discover new potential sources using Brave Search.
        
        Args:
            countries: Target countries to search
            max_queries: Maximum search queries to run
        
        Returns:
            List of discovered source candidates
        """
        logger.info("="*60)
        logger.info("STAGE 1: DISCOVERY")
        logger.info("="*60)
        
        sources = self.discoverer.discover_sources(
            countries=countries,
            max_queries=max_queries
        )
        
        # Update state
        for source in sources:
            domain = source["domain"]
            if domain not in self.state["discovered"]:
                self.state["discovered"][domain] = source
        
        self._save_state()
        self.discoverer.save_discovered_sources(sources)
        
        logger.info(f"Discovered {len(sources)} new sources")
        return sources
    
    # =========================================================================
    # STAGE 2: DIAGNOSIS
    # =========================================================================
    
    async def diagnose_async(self, url: str) -> Dict:
        """
        Stage 2: Diagnose a URL to capture network traffic and patterns.
        
        Args:
            url: URL to diagnose
        
        Returns:
            Diagnosis result dictionary
        """
        logger.info("="*60)
        logger.info(f"STAGE 2: DIAGNOSING {url}")
        logger.info("="*60)
        
        result = await self.diagnoser.diagnose_async(url)
        
        # Update state
        domain = result.get("domain", "")
        if domain:
            self.state["diagnosed"][domain] = {
                "session_dir": result.get("session_dir"),
                "apis_discovered": len(result.get("apis_discovered", [])),
                "diagnosed_at": datetime.utcnow().isoformat(),
            }
            self._save_state()
        
        return result
    
    def diagnose(self, url: str) -> Dict:
        """Sync wrapper for diagnose."""
        return asyncio.run(self.diagnose_async(url))
    
    def diagnose_pending(self, limit: int = 5) -> List[Dict]:
        """Diagnose pending discovered sources."""
        results = []
        
        for domain, info in self.state["discovered"].items():
            if domain in self.state["diagnosed"]:
                continue
            
            if len(results) >= limit:
                break
            
            url = info.get("url", "")
            if url:
                try:
                    result = self.diagnose(url)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Failed to diagnose {url}: {e}")
        
        return results
    
    # =========================================================================
    # STAGE 3: ANALYSIS
    # =========================================================================
    
    def analyze(self, diagnosis_dir: str) -> Dict:
        """
        Stage 3: Analyze diagnosis results to detect patterns.
        
        Args:
            diagnosis_dir: Path to diagnosis session directory
        
        Returns:
            Analysis result with patterns and field mappings
        """
        logger.info("="*60)
        logger.info(f"STAGE 3: ANALYZING {diagnosis_dir}")
        logger.info("="*60)
        
        diag_path = Path(diagnosis_dir)
        result = {
            "patterns": [],
            "recommended_type": None,
            "field_mappings": {},
        }
        
        # Check for API data
        api_data_file = diag_path / "api_data.json"
        if api_data_file.exists():
            with open(api_data_file, "r") as f:
                api_data = json.load(f)
            
            for api in api_data:
                data = api.get("data", {})
                pattern = self.analyzer.detect_list_pattern(data)
                
                if pattern:
                    result["patterns"].append({
                        "type": "api",
                        "url": api.get("url"),
                        "pattern": pattern,
                    })
                    result["recommended_type"] = "api"
        
        # Check HTML patterns
        html_file = diag_path / "page.html"
        if html_file.exists():
            with open(html_file, "r") as f:
                html = f.read()
            
            html_patterns = self.analyzer.analyze_html_for_patterns(html)
            
            if html_patterns.get("tables") or html_patterns.get("cards"):
                result["patterns"].append({
                    "type": "html",
                    "patterns": html_patterns,
                })
                
                if not result["recommended_type"]:
                    result["recommended_type"] = "html"
        
        logger.info(f"Found {len(result['patterns'])} patterns")
        return result
    
    # =========================================================================
    # STAGE 4: GENERATION
    # =========================================================================
    
    def generate(self, diagnosis_dir: str) -> Optional[str]:
        """
        Stage 4: Generate a Python collector adapter.
        
        Args:
            diagnosis_dir: Path to diagnosis session directory
        
        Returns:
            Path to generated adapter module, or None
        """
        logger.info("="*60)
        logger.info(f"STAGE 4: GENERATING ADAPTER from {diagnosis_dir}")
        logger.info("="*60)
        
        adapter_path = self.generator.generate_from_diagnosis(diagnosis_dir)
        
        if adapter_path:
            # Update state
            diag_path = Path(diagnosis_dir)
            domain = diag_path.parent.name
            self.state["generated"][domain] = {
                "adapter_path": adapter_path,
                "generated_at": datetime.utcnow().isoformat(),
            }
            self._save_state()
        
        return adapter_path
    
    # =========================================================================
    # FULL PIPELINE
    # =========================================================================
    
    async def process_url_async(self, url: str) -> Dict:
        """
        Full pipeline: diagnose → analyze → generate for a single URL.
        """
        result = {
            "url": url,
            "success": False,
            "stages": {},
        }
        
        # Stage 2: Diagnose
        try:
            diagnosis = await self.diagnose_async(url)
            result["stages"]["diagnose"] = {
                "success": diagnosis.get("success", False),
                "session_dir": diagnosis.get("session_dir"),
                "apis_found": len(diagnosis.get("apis_discovered", [])),
            }
            
            if not diagnosis.get("success"):
                return result
            
        except Exception as e:
            result["stages"]["diagnose"] = {"success": False, "error": str(e)}
            return result
        
        # Stage 3: Analyze
        try:
            analysis = self.analyze(diagnosis["session_dir"])
            result["stages"]["analyze"] = {
                "success": len(analysis.get("patterns", [])) > 0,
                "patterns_found": len(analysis.get("patterns", [])),
                "recommended_type": analysis.get("recommended_type"),
            }
            
            if not analysis.get("patterns"):
                return result
                
        except Exception as e:
            result["stages"]["analyze"] = {"success": False, "error": str(e)}
            return result
        
        # Stage 4: Generate
        try:
            adapter_path = self.generate(diagnosis["session_dir"])
            result["stages"]["generate"] = {
                "success": adapter_path is not None,
                "adapter_path": adapter_path,
            }
            
            result["success"] = adapter_path is not None
            
        except Exception as e:
            result["stages"]["generate"] = {"success": False, "error": str(e)}
        
        return result
    
    def process_url(self, url: str) -> Dict:
        """Sync wrapper for process_url_async."""
        return asyncio.run(self.process_url_async(url))
    
    def run_auto(self, 
                 countries: Optional[List[str]] = None,
                 max_discoveries: int = 10,
                 max_diagnoses: int = 5) -> Dict:
        """
        Run full autonomous pipeline:
        1. Discover new sources
        2. Diagnose top candidates
        3. Generate adapters
        
        Args:
            countries: Target countries
            max_discoveries: Max sources to discover
            max_diagnoses: Max sources to diagnose
        
        Returns:
            Summary of results
        """
        logger.info("="*60)
        logger.info("AUTODISCOVER ENGINE - FULL AUTO MODE")
        logger.info("="*60)
        
        summary = {
            "discovered": 0,
            "diagnosed": 0,
            "adapters_generated": 0,
            "errors": [],
        }
        
        # Stage 1: Discovery
        try:
            sources = self.discover(countries, max_queries=max_discoveries)
            summary["discovered"] = len(sources)
        except Exception as e:
            summary["errors"].append(f"Discovery failed: {e}")
            logger.error(f"Discovery failed: {e}")
        
        # Stage 2-4: Process top sources
        processed = 0
        for domain, info in list(self.state["discovered"].items())[:max_diagnoses]:
            if domain in self.state["diagnosed"]:
                continue
            
            url = info.get("url", "")
            if not url:
                continue
            
            try:
                result = self.process_url(url)
                
                if result.get("stages", {}).get("diagnose", {}).get("success"):
                    summary["diagnosed"] += 1
                
                if result.get("success"):
                    summary["adapters_generated"] += 1
                    
            except Exception as e:
                summary["errors"].append(f"Failed {url}: {e}")
                logger.error(f"Failed to process {url}: {e}")
            
            processed += 1
            if processed >= max_diagnoses:
                break
        
        # Save final report
        report_path = self.results_dir / f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, "w") as f:
            json.dump(summary, f, indent=2)
        
        logger.info("="*60)
        logger.info(f"AUTODISCOVER COMPLETE")
        logger.info(f"  Discovered: {summary['discovered']}")
        logger.info(f"  Diagnosed: {summary['diagnosed']}")
        logger.info(f"  Adapters Generated: {summary['adapters_generated']}")
        logger.info(f"  Errors: {len(summary['errors'])}")
        logger.info("="*60)
        
        return summary
    
    def status(self) -> Dict:
        """Get current engine status."""
        return {
            "discovered": len(self.state["discovered"]),
            "diagnosed": len(self.state["diagnosed"]),
            "generated": len(self.state["generated"]),
            "integrated": len(self.state["integrated"]),
            "pending_diagnosis": len([
                d for d in self.state["discovered"] 
                if d not in self.state["diagnosed"]
            ]),
            "pending_generation": len([
                d for d in self.state["diagnosed"]
                if d not in self.state["generated"]
            ]),
        }


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="AutoDiscover Engine")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # discover command
    discover_parser = subparsers.add_parser("discover", help="Discover new sources")
    discover_parser.add_argument("--countries", type=str, help="Comma-separated countries")
    discover_parser.add_argument("--max", type=int, default=20, help="Max queries")
    
    # diagnose command
    diagnose_parser = subparsers.add_parser("diagnose", help="Diagnose a URL")
    diagnose_parser.add_argument("--url", type=str, required=True, help="URL to diagnose")
    
    # process command
    process_parser = subparsers.add_parser("process", help="Process a URL (diagnose + analyze + generate)")
    process_parser.add_argument("--url", type=str, help="URL to process")
    process_parser.add_argument("--all", action="store_true", help="Process all pending")
    
    # run command
    run_parser = subparsers.add_parser("run", help="Run full autonomous pipeline")
    run_parser.add_argument("--countries", type=str, help="Comma-separated countries")
    run_parser.add_argument("--max-discover", type=int, default=10)
    run_parser.add_argument("--max-diagnose", type=int, default=5)
    
    # status command
    subparsers.add_parser("status", help="Show engine status")
    
    args = parser.parse_args()
    
    engine = AutoDiscoverEngine()
    
    if args.command == "discover":
        countries = args.countries.split(",") if args.countries else None
        sources = engine.discover(countries, args.max)
        print(f"\nDiscovered {len(sources)} sources")
        
    elif args.command == "diagnose":
        result = engine.diagnose(args.url)
        print(f"\nDiagnosis complete:")
        print(f"  Success: {result.get('success')}")
        print(f"  APIs found: {len(result.get('apis_discovered', []))}")
        print(f"  Output: {result.get('session_dir')}")
        
    elif args.command == "process":
        if args.url:
            result = engine.process_url(args.url)
            print(f"\nProcessing complete:")
            print(json.dumps(result, indent=2))
        elif args.all:
            # Process all pending
            for domain in list(engine.state["discovered"].keys())[:5]:
                if domain not in engine.state["diagnosed"]:
                    url = engine.state["discovered"][domain].get("url")
                    if url:
                        print(f"\nProcessing {url}...")
                        result = engine.process_url(url)
                        print(f"  Success: {result.get('success')}")
        
    elif args.command == "run":
        countries = args.countries.split(",") if args.countries else None
        summary = engine.run_auto(
            countries=countries,
            max_discoveries=args.max_discover,
            max_diagnoses=args.max_diagnose
        )
        print(f"\nRun complete:")
        print(json.dumps(summary, indent=2))
        
    elif args.command == "status":
        status = engine.status()
        print("\nAutoDiscover Engine Status:")
        print(f"  Sources discovered: {status['discovered']}")
        print(f"  Sites diagnosed: {status['diagnosed']}")
        print(f"  Adapters generated: {status['generated']}")
        print(f"  Pending diagnosis: {status['pending_diagnosis']}")
        print(f"  Pending generation: {status['pending_generation']}")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
