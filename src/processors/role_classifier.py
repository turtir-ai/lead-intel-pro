"""
GPT Fix #5: Role Separation Module

Classifies leads as CUSTOMER (textile producer) or INTERMEDIARY (machinery/service provider).
This prevents selling to competitors/suppliers instead of actual customers.
"""

import re
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Keywords indicating CUSTOMER (textile manufacturer - our target)
CUSTOMER_KEYWORDS = {
    # Textile finishing processes (what Monforts machines do)
    "dyeing", "finishing", "stenter", "stentering", "tenter", "tentering",
    "sanforizing", "mercerizing", "bleaching", "printing", "coating",
    "laminating", "heat setting", "thermosol", "pad steam", "continuous dyeing",
    # Portuguese/Spanish
    "tinturaria", "acabamento", "acabamiento", "teñido", "estampado",
    "blanqueo", "tintura", "rama", "secador", "calandra",
    # Turkish
    "terbiye", "boya", "boyama", "apre", "fikse", "kurutma",
    # German
    "färberei", "ausrüstung", "veredlung", "bleicherei",
    # Production types
    "mill", "fabric", "textile", "woven", "knitted", "denim",
    "cotton", "polyester", "synthetic", "garment", "apparel",
    "weaving", "knitting", "spinning",
    # Company type indicators
    "manufacturer", "producer", "factory", "fabrica", "usine",
    "industrial", "industria", "têxtil", "textil", "confecção",
}

# Keywords indicating INTERMEDIARY (NOT our customer - machinery/service/software)
INTERMEDIARY_KEYWORDS = {
    # Machinery/Equipment (competitors or different segment)
    "machinery", "machine", "equipment", "maschinen", "maquinaria",
    "spare parts", "parts supplier", "components", "ersatzteile",
    "automation", "controls", "systems integrator",
    # Chemicals
    "chemicals", "chemical", "dyes", "dyestuff", "colorant",
    "sizing", "softener", "auxiliaries", "química", "kimya",
    # Software/Services
    "software", "erp", "mes", "plm", "consulting", "consultant",
    "laboratory", "testing", "certification", "inspection",
    # Trading/Distribution
    "trading", "trader", "distributor", "agent", "representative",
    "import", "export", "broker", "wholesale",
    # Other non-customer types
    "association", "federation", "institute", "university",
    "research", "academic", "government", "ministry",
}

# Specific patterns for stronger classification
CUSTOMER_PATTERNS = [
    r'\b(dyeing|dying)\s*(and|&|,)?\s*(finishing|printing)\b',
    r'\b(textile|fabric)\s*(mill|factory|plant)\b',
    r'\b(woven|knit|denim)\s*(fabric|mill)\b',
    r'\btinturaria\b',
    r'\bterbiyehane\b',
    r'\bfärberei\b',
]

INTERMEDIARY_PATTERNS = [
    r'\b(textile|fabric)\s*machinery\b',
    r'\b(spare|machine)\s*parts?\b',
    r'\bchemical\s*(supplier|company|trader)\b',
    r'\b(trading|import.?export)\s*co\b',
    r'\btechnical\s*(service|support)\b',
]


class RoleClassifier:
    """Classifies leads into CUSTOMER or INTERMEDIARY roles."""

    def __init__(self, settings=None):
        self.settings = settings or {}
        self.customer_patterns = [re.compile(p, re.IGNORECASE) for p in CUSTOMER_PATTERNS]
        self.intermediary_patterns = [re.compile(p, re.IGNORECASE) for p in INTERMEDIARY_PATTERNS]

    def _safe_str(self, value):
        """Convert value to string safely, handling NaN/None."""
        import pandas as pd
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        return str(value)
    
    def classify(self, lead):
        """
        Classify a lead's role.
        
        Returns modified lead with:
        - role: 'CUSTOMER', 'INTERMEDIARY', or 'UNKNOWN'
        - role_confidence: 'high', 'medium', 'low'
        - role_signals: list of keywords/patterns that triggered classification
        """
        company = self._safe_str(lead.get("company")).lower()
        context = self._safe_str(lead.get("context")).lower()
        source_type = self._safe_str(lead.get("source_type")).lower()
        
        # Combine text for analysis
        text = f"{company} {context}"
        
        customer_signals = []
        intermediary_signals = []
        
        # Check patterns first (stronger signal)
        for pattern in self.customer_patterns:
            if pattern.search(text):
                customer_signals.append(f"pattern:{pattern.pattern}")
        
        for pattern in self.intermediary_patterns:
            if pattern.search(text):
                intermediary_signals.append(f"pattern:{pattern.pattern}")
        
        # Check keywords
        for kw in CUSTOMER_KEYWORDS:
            if kw in text:
                customer_signals.append(f"keyword:{kw}")
        
        for kw in INTERMEDIARY_KEYWORDS:
            if kw in text:
                intermediary_signals.append(f"keyword:{kw}")
        
        # Source-based hints
        if source_type in {"oekotex", "gots", "better_cotton", "wrap", "known_manufacturer"}:
            customer_signals.append(f"source:{source_type}")
        
        # Calculate scores
        customer_score = len(customer_signals)
        intermediary_score = len(intermediary_signals)
        
        # Pattern matches count more
        customer_pattern_count = sum(1 for s in customer_signals if s.startswith("pattern:"))
        intermediary_pattern_count = sum(1 for s in intermediary_signals if s.startswith("pattern:"))
        
        customer_score += customer_pattern_count * 2
        intermediary_score += intermediary_pattern_count * 2
        
        # Determine role and confidence
        if intermediary_score > customer_score and intermediary_score >= 2:
            role = "INTERMEDIARY"
            confidence = "high" if intermediary_score >= 4 else "medium"
            signals = intermediary_signals
        elif customer_score > intermediary_score and customer_score >= 2:
            role = "CUSTOMER"
            confidence = "high" if customer_score >= 4 else "medium"
            signals = customer_signals
        elif customer_score > 0 or intermediary_score > 0:
            # Weak signal - default to CUSTOMER with low confidence
            role = "CUSTOMER" if customer_score >= intermediary_score else "INTERMEDIARY"
            confidence = "low"
            signals = customer_signals if role == "CUSTOMER" else intermediary_signals
        else:
            role = "UNKNOWN"
            confidence = "low"
            signals = []
        
        lead["role"] = role
        lead["role_confidence"] = confidence
        lead["role_signals"] = signals[:5]  # Top 5 signals
        
        return lead

    def filter_customers_only(self, leads):
        """Filter to only return CUSTOMER leads."""
        classified = [self.classify(lead) for lead in leads]
        return [l for l in classified if l.get("role") != "INTERMEDIARY"]

    def separate_by_role(self, leads):
        """Separate leads into customers and intermediaries."""
        classified = [self.classify(lead) for lead in leads]
        customers = [l for l in classified if l.get("role") == "CUSTOMER"]
        intermediaries = [l for l in classified if l.get("role") == "INTERMEDIARY"]
        unknown = [l for l in classified if l.get("role") == "UNKNOWN"]
        return customers, intermediaries, unknown


def classify_leads(leads, settings=None):
    """Convenience function to classify a list of leads."""
    classifier = RoleClassifier(settings)
    return [classifier.classify(lead) for lead in leads]
