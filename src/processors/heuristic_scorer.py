# =============================================================================
# HEURISTIC SCORER - V10 Rule-Based Lead Scoring (LLM-Free)
# =============================================================================
# Purpose: Score leads based on keyword matching, proximity analysis, and
#          product relevance without using expensive LLM APIs
# 
# Benefits:
# - $0 cost (no GPT/Claude API calls)
# - Predictable, explainable scoring
# - Fast execution (regex-based)
# - Customizable rules per product category
# 
# Scoring Logic:
# - Positive keywords: +10 to +30 points
# - Negative keywords: -15 to -40 points
# - HS code relevance: +25 points
# - OEM + component match: +30 points
# - Certification bonus: +15 points
# - Proximity bonus: +15 points
# =============================================================================

import re
import yaml
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from functools import lru_cache


@dataclass
class ScoreResult:
    """Result of lead scoring"""
    score: int
    is_lead: bool
    confidence: str  # high, medium, low
    evidence: List[str]
    matched_hs_codes: List[str]
    product_match: bool
    machine_types: List[str]
    customer_signals: List[str]
    warnings: List[str] = field(default_factory=list)


class HeuristicScorer:
    """
    V10 Rule-Based Lead Scorer
    
    Scores leads based on:
    1. Positive/negative keyword matching
    2. HS code relevance
    3. OEM brand + component proximity
    4. Machine type detection
    5. Customer usage indicators
    6. Certification bonuses
    """
    
    def __init__(self, config_dir: Path = None):
        self.config_dir = config_dir or Path("config")
        self._load_configs()
        
    def _load_configs(self) -> None:
        """Load scoring configurations from YAML files"""
        
        # Load HS codes config
        hs_file = self.config_dir / "hs_codes.yaml"
        if hs_file.exists():
            with open(hs_file, 'r', encoding='utf-8') as f:
                self.hs_config = yaml.safe_load(f)
        else:
            self.hs_config = {"primary_codes": {}, "fallback_codes": {}}
            
        # Load product keywords config
        pk_file = self.config_dir / "product_keywords.yaml"
        if pk_file.exists():
            with open(pk_file, 'r', encoding='utf-8') as f:
                self.product_config = yaml.safe_load(f)
        else:
            self.product_config = {"oem_manufacturers": [], "oem_component_keywords": {}}
            
        # Build keyword sets for fast matching
        self._build_keyword_sets()
        
    def _build_keyword_sets(self) -> None:
        """Pre-compile keyword sets for efficient matching"""
        
        # === POSITIVE KEYWORDS ===
        self.positive_keywords = {
            # Machine types (high value)
            "stenter", "stentering", "ram machine", "sanfor", "sanforizing",
            "calander", "calendering", "dryer", "drying machine",
            "finishing machine", "coating machine", "laminating",
            "heat setting", "shrinking machine", "mercerizing",
            
            # Components/Parts (high value)
            "spare parts", "replacement parts", "wear parts", "components",
            "ersatzteile", "verschleißteile", "piezas de repuesto",
            "peças de reposição", "pièces de rechange",
            
            # Specific components (from hs_codes.yaml)
            "nadelglied", "gleitstein", "gleitleiste", "kluppen",
            "öffner segment", "kette", "clip chain", "pin chain",
            "buchse", "bushing", "bearing", "spindle", "spindel mutter",
            "torlonbuchse", "ball screw", "roller screw",
            
            # Textile operations
            "textile finishing", "fabric finishing", "dyeing", "printing",
            "bleaching", "desizing", "fabric processing",
            
            # Service keywords
            "maintenance", "service", "repair", "overhaul", "refurbishment",
            
            # Certifications
            "oeko-tex", "gots", "better cotton", "wrap certified", "iso 9001",
        }
        
        # === NEGATIVE KEYWORDS ===
        self.negative_keywords = {
            # Non-lead content
            "software", "consulting", "blog", "news article", "magazine",
            "webinar", "conference", "trade show announcement",
            "job opening", "career", "vacancy", "recruitment",
            "fashion show", "runway", "model agency",
            
            # Generic/low value
            "general trading", "import export", "wholesale", "retail",
            "reseller", "distributor only",
            
            # Academic/non-commercial
            "university", "college", "school", "institute", "research only",
            "thesis", "dissertation", "academic paper",
        }
        
        # === OEM MANUFACTURERS (Not customers - reduce score) ===
        self.oem_names = set()
        for oem in self.product_config.get("oem_manufacturers", []):
            self.oem_names.add(oem["name"].lower())
            for alias in oem.get("aliases", []):
                self.oem_names.add(alias.lower())
                
        # === OEM + COMPONENT PATTERNS ===
        self.oem_component_patterns = {}
        for oem, keywords in self.product_config.get("oem_component_keywords", {}).items():
            high_value = keywords.get("high_value", [])
            medium_value = keywords.get("medium_value", [])
            self.oem_component_patterns[oem] = {
                "high": high_value,
                "medium": medium_value
            }
            
        # === HS CODE KEYWORDS ===
        self.hs_keywords = {}
        for hs_code, config in self.hs_config.get("primary_codes", {}).items():
            keywords = config.get("keywords", [])
            weight = config.get("scoring_weight", 25)
            self.hs_keywords[hs_code] = {
                "keywords": set(kw.lower() for kw in keywords),
                "weight": weight
            }
            
        # === MACHINE TYPE PATTERNS ===
        self.machine_patterns = self.product_config.get("machine_type_keywords", {})
        
        # === CUSTOMER DETECTION PATTERNS ===
        self.customer_patterns = self.product_config.get("customer_detection", {})
        
    def calculate_score(self, text: str, title: str = "",
                        metadata: Dict = None) -> ScoreResult:
        """
        Calculate lead score based on heuristic rules.
        
        Args:
            text: Main text content (website, about page, etc.)
            title: Page title or company name
            metadata: Additional metadata (country, source, etc.)
            
        Returns:
            ScoreResult with score, evidence, and classifications
        """
        if metadata is None:
            metadata = {}
            
        # Normalize text for matching
        text_lower = text.lower()
        title_lower = title.lower()
        combined = f"{title_lower} {text_lower}"
        company_name = metadata.get("company_name", title).lower()
        
        score = 0
        evidence = []
        matched_hs = []
        machine_types = []
        customer_signals = []
        warnings = []
        product_match = False
        
        # =====================================================================
        # 1. POSITIVE KEYWORD MATCHING (+10 each, max 100)
        # =====================================================================
        positive_matches = 0
        for keyword in self.positive_keywords:
            if keyword in combined:
                score += 10
                evidence.append(f"+10: keyword '{keyword}'")
                positive_matches += 1
                if positive_matches >= 10:  # Cap at 100 points
                    break
                    
        # =====================================================================
        # 2. NEGATIVE KEYWORD MATCHING (-15 each)
        # =====================================================================
        for keyword in self.negative_keywords:
            if keyword in combined:
                score -= 15
                evidence.append(f"-15: negative '{keyword}'")
                warnings.append(f"Contains negative keyword: {keyword}")
                
        # =====================================================================
        # 3. HS CODE RELEVANCE (+20 to +25 each)
        # =====================================================================
        for hs_code, config in self.hs_keywords.items():
            for keyword in config["keywords"]:
                if keyword in combined:
                    weight = config["weight"]
                    score += weight
                    matched_hs.append(hs_code)
                    evidence.append(f"+{weight}: HS {hs_code} keyword '{keyword}'")
                    break  # One match per HS code
                    
        # =====================================================================
        # 4. OEM + COMPONENT MATCH (+30 high, +15 medium)
        # =====================================================================
        for oem, patterns in self.oem_component_patterns.items():
            if oem in combined:
                # Check high-value components
                for component in patterns.get("high", []):
                    if component in combined:
                        score += 30
                        product_match = True
                        evidence.append(f"+30: Product match '{oem}' + '{component}'")
                        customer_signals.append(f"Uses {oem} {component}")
                        break
                else:
                    # Check medium-value components
                    for component in patterns.get("medium", []):
                        if component in combined:
                            score += 15
                            evidence.append(f"+15: Product match '{oem}' + '{component}'")
                            break
                            
        # =====================================================================
        # 5. OEM DETECTION (NEGATIVE - Not a customer)
        # =====================================================================
        for oem_name in self.oem_names:
            # If company NAME contains OEM name → likely supplier, not customer
            if oem_name in company_name:
                # Check if it's the company itself, not a customer
                name_words = company_name.split()
                if any(oem_name in word for word in name_words[:3]):  # First 3 words
                    score -= 40
                    evidence.append(f"-40: OEM supplier detected '{oem_name}'")
                    warnings.append(f"Possible OEM/supplier: {oem_name}")
                    break
                    
        # =====================================================================
        # 6. MACHINE TYPE DETECTION (+15 per type)
        # =====================================================================
        for machine, keywords in self.machine_patterns.items():
            primary = keywords.get("primary", [])
            for kw in primary:
                if kw in combined:
                    machine_types.append(machine)
                    score += 15
                    evidence.append(f"+15: Machine type '{machine}'")
                    break
                    
        # =====================================================================
        # 7. CUSTOMER USAGE INDICATORS (+20 high, +10 medium)
        # =====================================================================
        usage_indicators = self.customer_patterns.get("usage_indicators", {})
        
        for pattern in usage_indicators.get("high_confidence", []):
            if re.search(pattern, combined, re.IGNORECASE):
                score += 20
                customer_signals.append(f"High confidence: {pattern}")
                evidence.append(f"+20: Customer indicator '{pattern[:30]}...'")
                
        for pattern in usage_indicators.get("medium_confidence", []):
            if pattern in combined:
                score += 10
                customer_signals.append(f"Medium: {pattern}")
                evidence.append(f"+10: Customer indicator '{pattern}'")
                
        # =====================================================================
        # 8. PARTS NEED INDICATORS (+25 high, +15 medium)
        # =====================================================================
        parts_indicators = self.customer_patterns.get("parts_need_indicators", {})
        
        for pattern in parts_indicators.get("high_confidence", []):
            if re.search(pattern, combined, re.IGNORECASE):
                score += 25
                customer_signals.append(f"Parts need: {pattern}")
                evidence.append(f"+25: Parts need '{pattern[:30]}...'")
                
        # =====================================================================
        # 9. CERTIFICATION BONUS (+15 each)
        # =====================================================================
        certs = ["oeko-tex", "gots certified", "better cotton", "wrap certified"]
        for cert in certs:
            if cert in combined:
                score += 15
                evidence.append(f"+15: Certification '{cert}'")
                
        # =====================================================================
        # 10. PROXIMITY BONUS (+15)
        # =====================================================================
        proximity_pairs = [
            ("textile", "finishing"),
            ("spare", "parts"),
            ("stenter", "machine"),
            ("dyeing", "machine"),
        ]
        
        for word1, word2 in proximity_pairs:
            if word1 in combined and word2 in combined:
                pos1 = combined.find(word1)
                pos2 = combined.find(word2)
                if abs(pos1 - pos2) < 50:  # Within 50 characters
                    score += 15
                    evidence.append(f"+15: Proximity '{word1}' + '{word2}'")
                    break
                    
        # =====================================================================
        # 11. REGION BONUS (Target markets)
        # =====================================================================
        country_val = metadata.get("country", "")
        country = str(country_val).lower() if country_val and not (isinstance(country_val, float) and pd.isna(country_val)) else ""
        target_regions = {
            "south_america": ["brazil", "argentina", "colombia", "chile", "ecuador", "peru"],
            "north_africa": ["tunisia", "egypt", "algeria", "morocco"],
            "middle_east": ["turkey", "pakistan", "india", "bangladesh"],
        }
        
        for region, countries in target_regions.items():
            if any(c in country for c in countries):
                score += 10
                evidence.append(f"+10: Target region '{region}'")
                break
                
        # =====================================================================
        # DETERMINE FINAL CLASSIFICATION
        # =====================================================================
        
        # Is it a lead? (threshold: 35 for more coverage, filter later by confidence)
        is_lead = score >= 35
        
        # Confidence level
        if score >= 100:
            confidence = "high"
        elif score >= 70:
            confidence = "medium"
        elif score >= 50:
            confidence = "low"
        elif score >= 35:
            confidence = "marginal"
        else:
            confidence = "reject"
            
        # Limit evidence to top 15
        evidence = sorted(evidence, key=lambda x: -abs(int(x.split(':')[0])))[:15]
        
        return ScoreResult(
            score=score,
            is_lead=is_lead,
            confidence=confidence,
            evidence=evidence,
            matched_hs_codes=list(set(matched_hs)),
            product_match=product_match,
            machine_types=list(set(machine_types)),
            customer_signals=customer_signals[:5],
            warnings=warnings
        )
        
    def batch_score(self, leads: List[Dict]) -> List[Tuple[Dict, ScoreResult]]:
        """
        Score a batch of leads efficiently.
        
        Args:
            leads: List of lead dictionaries with 'text', 'title', and metadata
            
        Returns:
            List of (lead, ScoreResult) tuples
        """
        results = []
        
        for lead in leads:
            text = lead.get("text", lead.get("description", ""))
            title = lead.get("title", lead.get("company_name", ""))
            metadata = {
                "company_name": lead.get("company_name", ""),
                "country": lead.get("country", ""),
                "source": lead.get("source", ""),
            }
            
            result = self.calculate_score(text, title, metadata)
            results.append((lead, result))
            
        return results
        
    def filter_leads(self, leads: List[Dict], 
                     min_score: int = 50,
                     min_confidence: str = "low") -> List[Dict]:
        """
        Filter leads by minimum score and confidence.
        
        Args:
            leads: List of lead dictionaries
            min_score: Minimum score threshold
            min_confidence: Minimum confidence level ('low', 'medium', 'high')
            
        Returns:
            Filtered list of leads with score results attached
        """
        confidence_order = {"reject": 0, "low": 1, "medium": 2, "high": 3}
        min_conf_value = confidence_order.get(min_confidence, 1)
        
        filtered = []
        for lead, result in self.batch_score(leads):
            if result.score >= min_score:
                conf_value = confidence_order.get(result.confidence, 0)
                if conf_value >= min_conf_value:
                    lead["_score"] = result.score
                    lead["_confidence"] = result.confidence
                    lead["_evidence"] = result.evidence[:5]
                    lead["_hs_codes"] = result.matched_hs_codes
                    lead["_product_match"] = result.product_match
                    filtered.append(lead)
                    
        return filtered


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

# Lazy-loaded global scorer
_scorer: Optional[HeuristicScorer] = None


def get_scorer(config_dir: Path = None) -> HeuristicScorer:
    """Get or create global scorer instance"""
    global _scorer
    if _scorer is None:
        _scorer = HeuristicScorer(config_dir)
    return _scorer


def score_lead(text: str, title: str = "", 
               metadata: Dict = None,
               config_dir: Path = None) -> ScoreResult:
    """
    Quick scoring function for single leads.
    
    Usage:
        result = score_lead("Company makes stenter machines...", "ABC Textiles")
        if result.is_lead:
            print(f"Score: {result.score}, Confidence: {result.confidence}")
    """
    scorer = get_scorer(config_dir)
    return scorer.calculate_score(text, title, metadata)


# =============================================================================
# CLI INTERFACE
# =============================================================================

if __name__ == "__main__":
    import sys
    
    # Test scoring
    test_cases = [
        {
            "title": "ABC Textile Finishing Co.",
            "text": "We operate 3 Brückner stenter machines and are looking for spare parts including gleitstein and buchsen. GOTS certified mill in Brazil.",
            "metadata": {"country": "Brazil", "company_name": "ABC Textile Finishing"}
        },
        {
            "title": "XYZ Software Solutions",
            "text": "We provide consulting services for the textile industry. Blog posts about technology trends.",
            "metadata": {"country": "Germany", "company_name": "XYZ Software Solutions"}
        },
        {
            "title": "Brückner Textile Technologies",
            "text": "We manufacture stenter machines and provide original spare parts worldwide.",
            "metadata": {"country": "Germany", "company_name": "Brückner Textile Technologies"}
        },
        {
            "title": "Tunisian Textile Mill",
            "text": "Textile finishing capacity 50,000 meters daily. Monforts ram machine installed 2020.",
            "metadata": {"country": "Tunisia", "company_name": "Tunisian Textile Mill"}
        }
    ]
    
    print("=" * 70)
    print("HEURISTIC SCORER TEST")
    print("=" * 70)
    
    scorer = HeuristicScorer()
    
    for i, case in enumerate(test_cases, 1):
        result = scorer.calculate_score(
            text=case["text"],
            title=case["title"],
            metadata=case["metadata"]
        )
        
        status = "✅ LEAD" if result.is_lead else "❌ REJECT"
        print(f"\n[{i}] {case['title']}")
        print(f"    {status} | Score: {result.score} | Confidence: {result.confidence}")
        
        if result.matched_hs_codes:
            print(f"    HS Codes: {', '.join(result.matched_hs_codes)}")
        if result.machine_types:
            print(f"    Machines: {', '.join(result.machine_types)}")
        if result.product_match:
            print(f"    🎯 PRODUCT MATCH DETECTED")
        if result.warnings:
            print(f"    ⚠️ Warnings: {', '.join(result.warnings)}")
            
        print(f"    Evidence (top 5):")
        for ev in result.evidence[:5]:
            print(f"      • {ev}")
