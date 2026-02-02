
import re
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class HeuristicResult:
    score: float
    confidence: str
    is_lead: bool
    matched_hs_codes: List[str]
    product_match: bool
    machine_types: List[str]
    evidence: List[str]

class HeuristicScorer:
    """
    Scoring engine based on keyword presence, proximity, and negative filtering.
    Designed to identify textile finishing mills and exclude non-relevant entities without LLM.
    """
    
    def __init__(self, config_path=None):
        # 1. POSITIVE SIGNALS (Target: Textile Finishing Mills / Stenter Users)
        # Grouped by language for easier maintenance
        self.positive_keywords = {
            "en": [
                "stenter", "sanfor", "calender", "dyeing machine", "textile finishing",
                "finishing mill", "fabric finishing", "dye house", "dyeing plant",
                "spare parts", "wear parts", "chain", "clip", "pin plate", "needle bar"
            ],
            "es": [
                "estiradora", "sanforizado", "apresto", "tintorería", "acabado textil",
                "recambios", "repuestos", "cadena", "pinza", "maquinaria textil",
                "planta de acabado", "teñido"
            ],
            "pt": [
                "rama", "estiradora", "sanforizadora", "acabamento textil", "tinturaria",
                "peças sobressalentes", "peças de reposição", "corrente", "clipe", "pinça",
                "beneficiamento textil"
            ],
            "tr": [
                "ram makinesi", "stenter", "sanfor", "kumaş boyama", "boyahane",
                "tekstil terbiye", "yedek parça", "kluppen", "iğne", "zincir",
                "kumaş apre"
            ],
            "de": [
                "spannrahmen", "textilveredlung", "färberei", "ersatzteile", "verschleißteile",
                "kluppen", "nadelkette", "gleitstein"
            ]
        }
        
        # Flatten positives for quick lookup
        self.all_positives = [kw for lang in self.positive_keywords.values() for kw in lang]

        # 2. NEGATIVE FILTERS (Reduce False Positives)
        # Entities that might mention terms but are NOT customers
        self.negative_keywords = [
            # Job/Career sites
            "job", "career", "vacancy", "resume", "cv", "empleo", "vacante", "vaga",
            "kariyer", "iş ilanı", "stellenangebot",
            # Information/News/Blog
            "news", "blog", "article", "haber", "noticias", "artigo", "forum",
            "university", "thesis", "academic", "student", "araştırma", "tez",
            # Software/Consulting/Service providers (unless specific)
            "software", "consulting", "logistics", "shipping", "freight"
        ]

    def score_text(self, text: str, title: str = "", url: str = "") -> Dict:
        """
        Scores a text based on heuristic rules.
        
        Returns:
            Dict containing 'score', 'is_lead' (bool), and 'evidence' (list of strings).
        """
        combined_text = f"{title} {text} {url}".lower()
        score = 0
        evidence = []
        
        # A. Basic Keyword Scoring (+10 per match type, max capped)
        matched_positives = set()
        for kw in self.all_positives:
            if kw in combined_text:
                matched_positives.add(kw)
        
        if matched_positives:
            match_count = len(matched_positives)
            # Logarithmic-ish scaling: first few matches matter most
            points = min(match_count * 10, 40) 
            score += points
            evidence.append(f"Found {match_count} keywords: {', '.join(list(matched_positives)[:5])}")

        # B. Context Proximity (+20)
        # Check if "Machine" terms appear near "Parts" terms
        # Regex explanation: (machine_term) ... within 100 chars ... (parts_term)
        machine_terms = r"(stenter|rama|spannrahmen|estiradora|dyeing|tintoreria|boyahane)"
        part_terms = r"(spare|part|recambio|repuesto|peça|parça|chain|clip|pin|kluppen)"
        
        # Forward check: Machine ... Parts
        if re.search(f"{machine_terms}.{{0,100}}{part_terms}", combined_text):
            score += 20
            evidence.append("High Signal: Machine terms found near Part terms")
        # Reverse check: Parts ... Machine
        elif re.search(f"{part_terms}.{{0,100}}{machine_terms}", combined_text):
            score += 20
            evidence.append("High Signal: Part terms found near Machine terms")

        # C. Factory/Mill Signal (+15)
        # Increases confidence that this is a physical facility, not an office
        factory_terms = r"(mill|plant|factory|fábrica|usina|tesis|fabrika|werk|production)"
        if re.search(factory_terms, combined_text):
            score += 15
            evidence.append("Facility Signal: Production/Factory terms present")

        # D. Negative Filtering (Heavy Penalty -50)
        matched_negatives = []
        for neg in self.negative_keywords:
            if neg in combined_text:
                matched_negatives.append(neg)
        
        if matched_negatives:
            penalty = len(matched_negatives) * 20 # -20 per negative term
            score -= penalty
            evidence.append(f"Negative Penalties: {', '.join(matched_negatives)}")

        # E. URL/Domain Specific Logic
        if "linkedin.com/company" in url:
             # Company pages are good, individual profiles are risky
             pass 
        elif "linkedin.com/in/" in url:
            score -= 30
            evidence.append("Penalty: Individual LinkedIn Profile")

        return {
            "score": max(score, 0), # Score shouldn't be negative for display
            "raw_score": score,
            "is_lead": score >= 25, # Lead Threshold
            "evidence": "; ".join(evidence)
        }

    def calculate_score(self, text: str, title: str, metadata: Dict) -> HeuristicResult:
        """
        Compatibility wrapper for run_pipeline.py
        """
        res = self.score_text(text, title=title, url=metadata.get('source', ''))
        
        score = res['raw_score']
        
        # Determine confidence
        confidence = "low"
        if score >= 60: confidence = "high"
        elif score >= 30: confidence = "medium"
        
        return HeuristicResult(
            score=max(0, score),
            confidence=confidence,
            is_lead=res['is_lead'],
            matched_hs_codes=[], # Not yet implemented in V5 Brain
            product_match=score > 10,
            machine_types=[],
            evidence=res['evidence'].split('; ') if res['evidence'] else []
        )

if __name__ == "__main__":
    # Quick Test
    scorer = HeuristicScorer()
    
    test_cases = [
        ("We are a leading textile finishing mill operating Brückner stenters.", "Company A"),
        ("Seeking job as textile engineer, familiar with stenter.", "Job Seeker"),
        ("Global logistics for textile machinery parts.", "Logistics Co"),
        ("Venta de repuestos para ramas Brückner y Monforts. Cadenas y clips.", "Refacciones Textiles SA")
    ]
    
    print("--- Testing Heuristic Scorer ---")
    for text, title in test_cases:
        result = scorer.score_text(text, title=title)
        print(f"\nText: {text[:50]}...")
        print(f"Score: {result['score']} | Is Lead: {result['is_lead']}")
        print(f"Evidence: {result['evidence']}")
