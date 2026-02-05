#!/usr/bin/env python3
"""
FlashText Keyword Processor - CPU-Dostu Çoklu Dil Keyword Eşleştirme

MacBook Pro 2012 (Ivy Bridge, no AVX2) için optimize edilmiş.
Regex yerine Aho-Corasick O(n) algoritması kullanır.

Desteklenen Diller:
- TR (Türkçe)
- EN (English)
- PT (Português)
- ES (Español)
- FR (Français)
- VI (Tiếng Việt)
- RU (Русский)
- AR (العربية)

PRD'den: "FlashText ile O(n) keyword eşleştirme, 100+ kelime için bile <10ms"
"""

import os
import yaml
from typing import Dict, List, Set, Tuple, Optional

try:
    from flashtext import KeywordProcessor
    FLASHTEXT_AVAILABLE = True
except ImportError:
    FLASHTEXT_AVAILABLE = False
    
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Load targets config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../../config/targets.yaml")

LANGUAGE_CODE_MAP = {
    "turkish": "tr",
    "vietnamese": "vi",
    "russian": "ru",
    "arabic": "ar",
    "french": "fr",
    "portuguese": "pt",
    "spanish": "es",
    "english": "en",
}


def _normalize_keywords_config(raw_config: Dict) -> Dict:
    """Normalize multilingual keywords into expected structure."""
    normalized = {
        "finishing": {},
        "decision_makers": {},
    }

    if not isinstance(raw_config, dict):
        return normalized

    for lang_key, payload in raw_config.items():
        if not isinstance(payload, dict):
            continue
        lang_code = LANGUAGE_CODE_MAP.get(lang_key, lang_key)
        finishing = payload.get("finishing_keywords") or payload.get("finishing")
        decision_makers = payload.get("decision_maker_titles") or payload.get("decision_makers")

        if isinstance(finishing, list):
            normalized["finishing"][lang_code] = finishing
        if isinstance(decision_makers, list):
            normalized["decision_makers"][lang_code] = decision_makers

    return normalized


def _load_keywords_config() -> Dict:
    """Load multilingual keywords from targets.yaml."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        raw = config.get("multilingual_keywords", {})
        return _normalize_keywords_config(raw)
    except Exception as e:
        logger.warning(f"Could not load keywords config: {e}")
        return {}


# Fallback keywords if config fails
FALLBACK_FINISHING_KEYWORDS = {
    "tr": ["ramöz", "stenter", "boyahane", "terbiye", "apre", "boyama", "fikse"],
    "en": ["stenter", "finishing", "dyehouse", "dyeing", "heat setting", "mercerizing"],
    "pt": ["rama", "tinturaria", "acabamento", "beneficiamento", "alvejamento"],
    "es": ["rama", "tintorería", "acabado", "teñido", "blanqueo"],
    "fr": ["rame", "teinture", "finition", "blanchiment"],
    "vi": ["nhuộm", "hoàn tất", "sấy"],
    "ru": ["рамоз", "красильная", "отделка"],
}

# OEM brand keywords
OEM_BRAND_KEYWORDS = {
    "tier1": ["brückner", "bruckner", "monforts", "krantz"],
    "tier2": ["artos", "santex", "babcock", "goller", "benninger", "thies"],
}


class MultilingualKeywordProcessor:
    """
    FlashText-based multilingual keyword processor.
    
    Uses Aho-Corasick algorithm for O(n) matching regardless of keyword count.
    CPU-friendly for MacBook Pro 2012 hardware.
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or _load_keywords_config()
        self._init_processors()
        
    def _init_processors(self):
        """Initialize FlashText keyword processors."""
        if not FLASHTEXT_AVAILABLE:
            logger.warning("FlashText not installed. Using fallback regex matching.")
            self.finishing_processor = None
            self.oem_processor = None
            self.decision_maker_processor = None
            return
        
        # Finishing keywords processor
        self.finishing_processor = KeywordProcessor(case_sensitive=False)
        self._load_finishing_keywords()
        
        # OEM brand processor
        self.oem_processor = KeywordProcessor(case_sensitive=False)
        self._load_oem_keywords()
        
        # Decision maker processor
        self.decision_maker_processor = KeywordProcessor(case_sensitive=False)
        self._load_decision_maker_keywords()
        
        logger.info(f"FlashText initialized: {len(self.finishing_processor)} finishing, "
                   f"{len(self.oem_processor)} OEM, {len(self.decision_maker_processor)} decision-maker keywords")
    
    def _load_finishing_keywords(self):
        """Load finishing keywords from config."""
        if not self.finishing_processor:
            return
        
        # Get from config, fallback if empty  
        keywords_by_lang = self.config.get("finishing", {})
        if not keywords_by_lang:
            logger.info("No finishing keywords in config, using defaults")
            keywords_by_lang = FALLBACK_FINISHING_KEYWORDS
        
        for lang, keywords in keywords_by_lang.items():
            if isinstance(keywords, list):
                for kw in keywords:
                    # Add keyword with language tag
                    self.finishing_processor.add_keyword(kw.lower(), f"finishing_{lang}_{kw.lower()}")
    
    def _load_oem_keywords(self):
        """Load OEM brand keywords."""
        if not self.oem_processor:
            return
            
        for tier, brands in OEM_BRAND_KEYWORDS.items():
            for brand in brands:
                # Add brand with tier tag
                self.oem_processor.add_keyword(brand.lower(), f"{tier}_{brand.lower()}")
                
                # Also add common variations
                if brand == "brückner":
                    self.oem_processor.add_keyword("bruckner", "tier1_bruckner")
    
    def _load_decision_maker_keywords(self):
        """Load decision maker title keywords."""
        if not self.decision_maker_processor:
            return
        
        # Default decision maker titles
        default_dm = {
            "tr": ["bakım müdürü", "teknik satınalma müdürü", "üretim müdürü"],
            "en": ["maintenance manager", "purchasing manager", "plant manager"],
            "pt": ["gerente de manutenção", "gerente de compras"],
        }
        
        dm_keywords = self.config.get("decision_makers", {})
        if not dm_keywords:
            logger.info("No decision maker keywords in config, using defaults")
            dm_keywords = default_dm
        
        for lang, titles in dm_keywords.items():
            if isinstance(titles, list):
                for title in titles:
                    self.decision_maker_processor.add_keyword(title.lower(), f"dm_{lang}_{title.lower()}")
    
    def extract_finishing_keywords(self, text: str) -> List[str]:
        """
        Extract finishing keywords from text.
        Returns list of matched keywords with language tags.
        """
        if not text:
            return []
            
        if FLASHTEXT_AVAILABLE and self.finishing_processor:
            return self.finishing_processor.extract_keywords(text.lower())
        
        # Fallback to simple matching
        return self._fallback_match(text, FALLBACK_FINISHING_KEYWORDS)
    
    def extract_oem_brands(self, text: str) -> Tuple[List[str], List[str]]:
        """
        Extract OEM brand mentions from text.
        Returns (tier1_brands, tier2_brands).
        """
        if not text:
            return [], []
            
        tier1 = []
        tier2 = []
        
        if FLASHTEXT_AVAILABLE and self.oem_processor:
            matches = self.oem_processor.extract_keywords(text.lower())
            for m in matches:
                if m.startswith("tier1_"):
                    tier1.append(m.replace("tier1_", ""))
                elif m.startswith("tier2_"):
                    tier2.append(m.replace("tier2_", ""))
        else:
            # Fallback
            text_lower = text.lower()
            for brand in OEM_BRAND_KEYWORDS["tier1"]:
                if brand in text_lower:
                    tier1.append(brand)
            for brand in OEM_BRAND_KEYWORDS["tier2"]:
                if brand in text_lower:
                    tier2.append(brand)
        
        return list(set(tier1)), list(set(tier2))
    
    def extract_decision_makers(self, text: str) -> List[str]:
        """Extract decision maker title keywords."""
        if not text:
            return []
            
        if FLASHTEXT_AVAILABLE and self.decision_maker_processor:
            return self.decision_maker_processor.extract_keywords(text.lower())
        
        return []
    
    def detect_language(self, text: str) -> str:
        """
        Detect language of text based on keyword matches.
        Returns 2-letter language code or 'unknown'.
        """
        if not text:
            return "unknown"
            
        text_lower = text.lower()
        lang_scores = {}
        
        keywords_by_lang = self.config.get("finishing", FALLBACK_FINISHING_KEYWORDS)
        
        for lang, keywords in keywords_by_lang.items():
            if isinstance(keywords, list):
                score = sum(1 for kw in keywords if kw.lower() in text_lower)
                if score > 0:
                    lang_scores[lang] = score
        
        if lang_scores:
            return max(lang_scores, key=lang_scores.get)
        return "unknown"
    
    def score_text_relevance(self, text: str) -> Tuple[float, Dict]:
        """
        Score text relevance based on keyword matches.
        
        Returns:
        - score: 0-100 relevance score
        - details: {finishing_matches, oem_matches, languages_detected}
        """
        if not text:
            return 0.0, {}
            
        finishing = self.extract_finishing_keywords(text)
        tier1, tier2 = self.extract_oem_brands(text)
        lang = self.detect_language(text)
        
        # Calculate score
        score = 0.0
        
        # Finishing keywords (up to 50 points)
        score += min(len(finishing) * 10, 50)
        
        # OEM brands (up to 40 points)
        score += len(tier1) * 15
        score += len(tier2) * 10
        score = min(score, 90)
        
        # Language bonus
        if lang != "unknown":
            score += 10
        
        score = min(100, score)
        
        details = {
            "finishing_matches": finishing,
            "tier1_brands": tier1,
            "tier2_brands": tier2,
            "language": lang,
            "keyword_count": len(finishing) + len(tier1) + len(tier2),
        }
        
        return score, details
    
    def _fallback_match(self, text: str, keywords_dict: Dict) -> List[str]:
        """Fallback regex-free matching when FlashText unavailable."""
        matches = []
        text_lower = text.lower()
        
        for lang, keywords in keywords_dict.items():
            if isinstance(keywords, list):
                for kw in keywords:
                    if kw.lower() in text_lower:
                        matches.append(f"finishing_{lang}_{kw.lower()}")
        
        return matches
    
    def process_lead(self, lead: Dict) -> Dict:
        """
        Process a lead and add keyword analysis fields.
        
        Adds:
        - kw_finishing: List of finishing keywords found
        - kw_oem_tier1: Tier 1 OEM brands found
        - kw_oem_tier2: Tier 2 OEM brands found
        - kw_language: Detected language
        - kw_relevance_score: 0-100 relevance score
        """
        # Get text to analyze
        text_parts = [
            str(lead.get("company", "")),
            str(lead.get("context", "")),
            str(lead.get("segment", "")),
            str(lead.get("description", "")),
        ]
        text = " ".join(text_parts)
        
        # Extract keywords
        finishing = self.extract_finishing_keywords(text)
        tier1, tier2 = self.extract_oem_brands(text)
        lang = self.detect_language(text)
        score, details = self.score_text_relevance(text)
        
        # Update lead
        lead["kw_finishing"] = finishing
        lead["kw_oem_tier1"] = tier1
        lead["kw_oem_tier2"] = tier2
        lead["kw_language"] = lang
        lead["kw_relevance_score"] = round(score, 1)
        lead["kw_keyword_count"] = len(finishing) + len(tier1) + len(tier2)
        
        return lead
    
    def process_batch(self, leads: List[Dict]) -> List[Dict]:
        """Process a batch of leads."""
        logger.info(f"Processing {len(leads)} leads with keyword extraction...")
        
        processed = []
        high_relevance = 0
        
        for lead in leads:
            processed_lead = self.process_lead(lead)
            if processed_lead.get("kw_relevance_score", 0) >= 50:
                high_relevance += 1
            processed.append(processed_lead)
        
        logger.info(f"Keyword processing complete: {high_relevance}/{len(leads)} high relevance")
        return processed


# Convenience function
def extract_keywords(text: str) -> Dict:
    """Quick keyword extraction without creating processor instance."""
    processor = MultilingualKeywordProcessor()
    score, details = processor.score_text_relevance(text)
    return {
        "score": score,
        **details,
    }
