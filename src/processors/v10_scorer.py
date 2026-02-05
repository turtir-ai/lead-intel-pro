#!/usr/bin/env python3
"""
V10 Scorer - 100 Puan Skorlama Modeli

newupgrade.md'den alınan 4 kategorili skorlama sistemi:
1. Faaliyet Alanı Uyumu (30 puan)
2. Makine Kanıtı (25 puan)
3. Firma Profili Kalitesi (25 puan)
4. Satın Alma Sinyalleri (20 puan)

Grade Eşikleri:
- Grade A (Hot): >= 85 puan
- Grade B (Warm): >= 70 puan
- Grade C (Nurturing): >= 50 puan
- Grade D (Cold): < 50 puan
"""

import os
import re
import yaml
from typing import Dict, List, Optional, Tuple
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Load scoring config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../../config/scoring.yaml")


def _load_scoring_config() -> Dict:
    """Load V10 scoring configuration."""
    try:
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f) or {}
        return config.get("v10_scoring_model", {})
    except Exception as e:
        logger.warning(f"Could not load scoring config: {e}")
        return {}


# OEM brand tiers
OEM_TIER1 = {"brückner", "bruckner", "monforts", "krantz"}
OEM_TIER2 = {"artos", "santex", "babcock", "goller", "benninger", "thies"}

# Finishing keywords (multi-language)
FINISHING_KEYWORDS = {
    # Turkish
    "ramöz", "ramoz", "stenter", "boyahane", "terbiye", "apre",
    "boya tesisi", "boyama", "germe makinesi", "kurutma", "fikse",
    # English
    "finishing", "dyeing", "dyehouse", "mercerizing", "sanforizing",
    "calendering", "heat setting", "stentering", "textile mill",
    # Portuguese
    "tinturaria", "acabamento", "beneficiamento", "alvejamento",
    # Spanish
    "tintorería", "acabado", "blanqueo", "teñido",
    # French
    "teinture", "finition", "blanchiment",
    # Vietnamese
    "nhuộm", "hoàn tất", "sấy",
    # Russian
    "красильная", "отделка", "текстиль",
}

# Negative signals (disqualification) - use specific phrases to avoid false positives
NEGATIVE_SIGNALS = {
    # English - machinery/parts dealers
    "machinery supplier", "machine manufacturer", "spare parts dealer",
    "spare parts supplier", "parts distributor", "machine dealer",
    "trading company", "textile machinery", "machinery trading",
    # German
    "maschinenhandel", "ersatzteilhandel",
    # Turkish
    "makine üreticisi", "yedek parça satıcısı", "makine distribütörü",
}


def _is_true(val) -> bool:
    """Check if value is explicitly True (handles NaN as False)."""
    if val is None:
        return False
    if isinstance(val, float) and val != val:  # NaN check
        return False
    if isinstance(val, str):
        return val.lower() in ("true", "1", "yes")
    return bool(val)


class V10Scorer:
    """
    V10 100-point scoring system.
    
    Categories:
    - Activity Fit (30 pts): Is this a dyehouse/finishing plant?
    - Machine Evidence (25 pts): Do they have target brand machines?
    - Company Profile (25 pts): Size, certifications, export capacity
    - Purchase Signals (20 pts): Recent imports, expansion, fair participation
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or _load_scoring_config()
        self.bonus_config = (self.config or {}).get("bonuses", {})
        self.grade_thresholds = (self.config or {}).get("grade_thresholds", {})
        self.negative_signals = (self.config or {}).get("negative_signals", {})
        self.stats = {
            "scored": 0,
            "grade_a": 0,
            "grade_b": 0,
            "grade_c": 0,
            "grade_d": 0,
            "disqualified": 0,
        }
    
    def score_lead(self, lead: Dict) -> Dict:
        """
        Score a single lead using V10 100-point model.
        
        Returns lead with added fields:
        - v10_score: Total score (0-100)
        - v10_grade: A/B/C/D
        - v10_activity_score: Activity fit score
        - v10_machine_score: Machine evidence score
        - v10_profile_score: Company profile score
        - v10_signal_score: Purchase signals score
        - v10_bonuses: List of applied bonuses
        - v10_penalties: List of applied penalties
        """
        # Check for disqualification first
        disqualified, reason = self._check_disqualification(lead)
        if disqualified:
            lead["v10_score"] = 0
            lead["v10_grade"] = "X"
            lead["v10_disqualified"] = True
            lead["v10_disqualification_reason"] = reason
            self.stats["disqualified"] += 1
            return lead
        
        # Score each category
        activity_score, activity_details = self._score_activity_fit(lead)
        machine_score, machine_details = self._score_machine_evidence(lead)
        profile_score, profile_details = self._score_company_profile(lead)
        signal_score, signal_details = self._score_purchase_signals(lead)
        
        # Calculate bonuses
        bonuses, bonus_total = self._calculate_bonuses(lead)
        
        # Total score (capped at 100)
        total = activity_score + machine_score + profile_score + signal_score + bonus_total
        total = min(100, max(0, total))
        
        # Determine grade
        grade = self._determine_grade(total)
        
        # Update lead
        lead["v10_score"] = round(total, 1)
        lead["v10_grade"] = grade
        lead["v10_activity_score"] = activity_score
        lead["v10_machine_score"] = machine_score
        lead["v10_profile_score"] = profile_score
        lead["v10_signal_score"] = signal_score
        lead["v10_bonus_total"] = bonus_total
        lead["v10_bonuses"] = bonuses
        lead["v10_details"] = {
            "activity": activity_details,
            "machine": machine_details,
            "profile": profile_details,
            "signals": signal_details,
        }
        lead["v10_disqualified"] = False
        
        # Update stats
        self.stats["scored"] += 1
        self.stats[f"grade_{grade.lower()}"] += 1
        
        return lead
    
    def _check_disqualification(self, lead: Dict) -> Tuple[bool, str]:
        """Check if lead should be disqualified."""
        # Check flags (NaN-safe)
        if _is_true(lead.get("is_machinery_supplier")):
            return True, "Machinery supplier (competitor)"
        if _is_true(lead.get("is_parts_supplier")):
            return True, "Spare parts supplier (competitor)"
        if _is_true(lead.get("is_spare_parts_reseller")):
            return True, "Spare parts reseller (competitor)"
        if _is_true(lead.get("is_trading_company")):
            return True, "Trading company (not end-user)"
        
        # Check entity type
        entity_type = str(lead.get("entity_type", "")).lower()
        if entity_type in ["supplier", "distributor", "trader", "agent"]:
            return True, f"Entity type: {entity_type}"
        
        # Check context for negative signals
        context = str(lead.get("context", "")).lower()
        company = str(lead.get("company", "")).lower()
        
        for signal in NEGATIVE_SIGNALS:
            if signal in context or signal in company:
                return True, f"Negative signal: {signal}"
        
        return False, ""
    
    def _score_activity_fit(self, lead: Dict) -> Tuple[float, Dict]:
        """
        Score activity fit (max 30 points).
        
        Full score (30): Dyehouse/finishing + target brand machine
        High (25): Integrated mill with finishing
        Medium (15): Textile producer with finishing capacity
        Low (10): Garment/home textile producer
        """
        score = 0
        details = {"signals": [], "reason": ""}
        
        context = str(lead.get("context", "")).lower()
        company = str(lead.get("company", "")).lower()
        role = str(lead.get("role", "")).lower()
        segment = str(lead.get("segment", "")).lower()
        
        # Check for finishing signals
        finishing_found = []
        text_to_check = f"{context} {company} {segment}"
        
        for kw in FINISHING_KEYWORDS:
            if kw in text_to_check:
                finishing_found.append(kw)
        
        # Also check dedicated fields
        if lead.get("has_finishing_context"):
            finishing_found.append("has_finishing_context")
        if lead.get("sce_has_evidence"):
            finishing_found.append("sce_evidence")
        
        # Check finishing_signals field
        finishing_signals = lead.get("finishing_signals", [])
        if isinstance(finishing_signals, str):
            try:
                import ast
                finishing_signals = ast.literal_eval(finishing_signals)
            except:
                finishing_signals = []
        if finishing_signals:
            finishing_found.extend(finishing_signals[:5])
        
        # Determine score based on signals
        if len(finishing_found) >= 3:
            score = 30
            details["reason"] = "Strong finishing evidence (3+ signals)"
        elif len(finishing_found) >= 2:
            score = 25
            details["reason"] = "Good finishing evidence (2 signals)"
        elif len(finishing_found) >= 1:
            score = 15
            details["reason"] = "Some finishing evidence"
        elif role in ["end_user", "customer", "manufacturer"]:
            score = 10
            details["reason"] = "Manufacturer without finishing evidence"
        else:
            score = 5
            details["reason"] = "No finishing evidence"
        
        details["signals"] = list(set(finishing_found))[:10]
        return score, details
    
    def _score_machine_evidence(self, lead: Dict) -> Tuple[float, Dict]:
        """
        Score machine evidence (max 25 points).
        
        Full score (25): Multiple target brands + maintenance signal
        High (20): One target brand detected
        Medium (12): Compatible alternative brand
        Low (8): Machine info missing but finishing confirmed
        """
        score = 0
        details = {"brands": [], "signals": [], "reason": ""}
        
        # Check oem_brands field
        oem_brands = lead.get("oem_brands", [])
        if isinstance(oem_brands, str):
            try:
                import ast
                oem_brands = ast.literal_eval(oem_brands)
            except:
                oem_brands = [oem_brands] if oem_brands.strip() else []
        
        # Check oem_signals field
        oem_signals = lead.get("oem_signals", [])
        if isinstance(oem_signals, str):
            try:
                import ast
                oem_signals = ast.literal_eval(oem_signals)
            except:
                oem_signals = []
        
        # Also check context for brand mentions
        context = str(lead.get("context", "")).lower()
        company = str(lead.get("company", "")).lower()
        text = f"{context} {company}"
        
        tier1_found = []
        tier2_found = []
        
        for brand in OEM_TIER1:
            if brand in text or brand in [b.lower() for b in oem_brands]:
                tier1_found.append(brand)
        
        for brand in OEM_TIER2:
            if brand in text or brand in [b.lower() for b in oem_brands]:
                tier2_found.append(brand)
        
        # Determine score
        has_maintenance_signal = bool(oem_signals) or "maintenance" in text or "bakım" in text
        
        if len(tier1_found) >= 2 or (tier1_found and has_maintenance_signal):
            score = 25
            details["reason"] = "Strong machine evidence (Tier1 + signals)"
        elif tier1_found:
            score = 20
            details["reason"] = f"Tier1 brand: {tier1_found[0]}"
        elif tier2_found:
            score = 12
            details["reason"] = f"Tier2 brand: {tier2_found[0]}"
        elif lead.get("has_finishing_context"):
            score = 8
            details["reason"] = "Finishing confirmed but no brand"
        else:
            score = 0
            details["reason"] = "No machine evidence"
        
        details["brands"] = list(set(tier1_found + tier2_found))
        details["signals"] = oem_signals[:5] if isinstance(oem_signals, list) else []
        
        return score, details
    
    def _score_company_profile(self, lead: Dict) -> Tuple[float, Dict]:
        """
        Score company profile quality (max 25 points).
        
        Full score (25): Large company + international certs + export
        High (20): Medium company + local certs
        Medium (12): SME + regional export
        Low (8): Small company, domestic only
        """
        score = 0
        details = {"signals": [], "reason": ""}
        
        # Check certifications
        certs = []
        source_type = str(lead.get("source_type", "")).lower()
        certification = str(lead.get("certification", "")).lower()
        
        if "gots" in source_type or "gots" in certification:
            certs.append("GOTS")
        if "oekotex" in source_type or "oeko" in certification:
            certs.append("OEKO-TEX")
        if "bettercotton" in source_type or "bci" in certification:
            certs.append("BCI")
        if lead.get("is_premium_fiber"):
            certs.append("Premium Fiber")
        
        # Check size indicators
        context = str(lead.get("context", "")).lower()
        is_large = any(kw in context for kw in ["500", "1000", "group", "holding", "large"])
        is_medium = any(kw in context for kw in ["100", "200", "factory", "plant"])
        
        # Determine score
        if len(certs) >= 2 or (certs and is_large):
            score = 25
            details["reason"] = "Large company with international certs"
        elif certs:
            score = 20
            details["reason"] = f"Certified: {', '.join(certs)}"
        elif is_large:
            score = 15
            details["reason"] = "Large company"
        elif is_medium:
            score = 12
            details["reason"] = "Medium company"
        else:
            score = 8
            details["reason"] = "Basic profile"
        
        details["signals"] = certs
        return score, details
    
    def _score_purchase_signals(self, lead: Dict) -> Tuple[float, Dict]:
        """
        Score purchase signals (max 20 points).
        
        Full score (20): Recent import + expansion announcement
        High (15): Fair participation + capacity increase
        Medium (10): New product launch
        Low (6): No signal detected
        """
        score = 0
        details = {"signals": [], "reason": ""}
        
        signals = []
        
        # Check source type signals
        source_type = str(lead.get("source_type", "")).lower()
        if "fair" in source_type:
            signals.append("fair_participation")
        if "job" in source_type:
            signals.append("job_posting")
        if "trade" in source_type or "import" in source_type:
            signals.append("trade_import")
        
        # Check context for signals
        context = str(lead.get("context", "")).lower()
        if any(kw in context for kw in ["expansion", "genişleme", "new plant", "yeni tesis"]):
            signals.append("expansion")
        if any(kw in context for kw in ["modernization", "retrofit", "yenileme"]):
            signals.append("modernization")
        if any(kw in context for kw in ["investment", "yatırım"]):
            signals.append("investment")
        
        # Check urgency signals
        if lead.get("urgency_signal"):
            signals.append(lead.get("urgency_signal"))
        if lead.get("has_recent_investment"):
            signals.append("recent_investment")
        
        # Determine score
        if len(signals) >= 3:
            score = 20
            details["reason"] = "Multiple purchase signals"
        elif len(signals) >= 2:
            score = 15
            details["reason"] = "Good purchase signals"
        elif signals:
            score = 10
            details["reason"] = f"Signal: {signals[0]}"
        else:
            score = 6
            details["reason"] = "No purchase signals"
        
        details["signals"] = signals
        return score, details
    
    def _calculate_bonuses(self, lead: Dict) -> Tuple[List[str], float]:
        """Calculate bonus points."""
        bonuses = []
        total = 0
        
        # OEM brand bonus
        oem_brand = str(lead.get("oem_brand", "")).lower()
        if oem_brand in OEM_TIER1:
            bonuses.append(f"oem_tier1_{oem_brand}")
            total += self.bonus_config.get("oem_brand_tier1", 5)
        elif oem_brand in OEM_TIER2:
            bonuses.append(f"oem_tier2_{oem_brand}")
            total += self.bonus_config.get("oem_brand_tier2", 3)
        
        # Certification bonus
        if lead.get("source_type") == "gots":
            bonuses.append("gots_certified")
            total += self.bonus_config.get("certification_gots", 3)
        if lead.get("source_type") == "oekotex":
            bonuses.append("oekotex_certified")
            total += self.bonus_config.get("certification_oekotex", 3)
        
        # Golden lead bonus (K1+K2)
        if lead.get("is_golden"):
            bonuses.append("golden_lead")
            total += 5
        
        return bonuses, total
    
    def _determine_grade(self, score: float) -> str:
        """Determine grade based on score."""
        if self.grade_thresholds:
            grade_map = {
                "A": self.grade_thresholds.get("grade_a", {}).get("min_score", 85),
                "B": self.grade_thresholds.get("grade_b", {}).get("min_score", 70),
                "C": self.grade_thresholds.get("grade_c", {}).get("min_score", 50),
                "D": self.grade_thresholds.get("grade_d", {}).get("min_score", 0),
            }
            if score >= grade_map["A"]:
                return "A"
            if score >= grade_map["B"]:
                return "B"
            if score >= grade_map["C"]:
                return "C"
            return "D"

        if score >= 85:
            return "A"
        if score >= 70:
            return "B"
        if score >= 50:
            return "C"
        return "D"
    
    def score_batch(self, leads: List[Dict]) -> List[Dict]:
        """Score a batch of leads."""
        logger.info(f"V10 Scoring {len(leads)} leads...")
        
        scored = []
        for lead in leads:
            scored.append(self.score_lead(lead))
        
        logger.info(f"V10 Scoring complete: A={self.stats['grade_a']}, B={self.stats['grade_b']}, "
                   f"C={self.stats['grade_c']}, D={self.stats['grade_d']}, X={self.stats['disqualified']}")
        
        return scored
    
    def get_stats(self) -> Dict:
        """Get scoring statistics."""
        return self.stats.copy()
