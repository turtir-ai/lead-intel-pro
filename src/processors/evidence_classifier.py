#!/usr/bin/env python3
"""Evidence classifier for K1/K2 dual evidence standard."""

from typing import Dict, List
from urllib.parse import urlparse


def _safe_str(val) -> str:
    """Safely convert value to string, handling NaN."""
    if val is None:
        return ""
    if isinstance(val, float) and val != val:  # NaN check
        return ""
    return str(val).strip()


class EvidenceClassifier:
    """K1 (external) and K2 (internal) evidence classifier."""

    # K1: External/third-party evidence sources
    K1_SOURCE_TYPES = {
        # OEM references
        "oem_reference",
        "oem_customer",
        "known_manufacturer",
        # Fair/exhibition evidence
        "pdf_exhibitor",
        "fair_exhibitor",
        "fair",
        # Certification directories (third-party verification)
        "gots",
        "oekotex",
        "bettercotton",
        "bluesign",
        # Trade/import data
        "trade_import",
        "egypt_tec",
        # Job postings and press
        "job_posting",
        "press_release",
        # Competitor intelligence
        "competitor_customer",
        "competitor",
        # V10.4: Industry directories and associations (verified sources)
        "directory",
        "association_member",
        "amith",
        "abit",
        "regional_collector",
    }

    def classify_lead(self, lead: Dict) -> Dict:
        raw_evidence = lead.get("evidence_sources", []) or []
        
        # V10.4: Handle string-encoded lists from CSV round-trips
        if isinstance(raw_evidence, str):
            try:
                import ast
                raw_evidence = ast.literal_eval(raw_evidence)
            except Exception:
                raw_evidence = []
        
        # Ensure we have a list of dicts, filter out non-dict entries
        evidence_sources: List[Dict] = [
            src for src in (raw_evidence if isinstance(raw_evidence, list) else [])
            if isinstance(src, dict)
        ]

        website = _safe_str(lead.get("website"))
        domain = ""
        try:
            domain = urlparse(website).netloc.lower() if website else ""
        except Exception:
            domain = ""

        # K1: based on source_type
        source_type = _safe_str(lead.get("source_type") or lead.get("source")).lower()
        if source_type in self.K1_SOURCE_TYPES:
            evidence_sources.append({
                "type": "k1_source",
                "source_type": source_type,
                "url": lead.get("source_url", ""),
            })

        # K1: OEM evidence from Brave snippet if URL is external
        raw_details = lead.get("evidence_details", []) or []
        if isinstance(raw_details, str):
            try:
                import ast
                raw_details = ast.literal_eval(raw_details)
            except Exception:
                raw_details = []
        evidence_details = [d for d in (raw_details if isinstance(raw_details, list) else []) if isinstance(d, dict)]
        
        for detail in evidence_details:
            url = detail.get("url", "")
            detail_type = detail.get("type", "")
            is_external = False
            try:
                d = urlparse(url).netloc.lower()
                if d and domain and d != domain:
                    is_external = True
            except Exception:
                pass
            if detail_type == "oem_brand" and is_external:
                evidence_sources.append({
                    "type": "k1_oem",
                    "url": url,
                    "term": detail.get("term", ""),
                })

        # K2: finishing/oem signals from deep validation (internal site content)
        # Parse string lists from CSV if needed
        finishing_signals = lead.get("finishing_signals", []) or []
        oem_signals = lead.get("oem_signals", []) or []
        
        # Handle string-encoded lists from CSV
        if isinstance(finishing_signals, str):
            try:
                import ast
                finishing_signals = ast.literal_eval(finishing_signals)
            except:
                finishing_signals = []
        if isinstance(oem_signals, str):
            try:
                import ast
                oem_signals = ast.literal_eval(oem_signals)
            except:
                oem_signals = []
        
        if finishing_signals or oem_signals:
            evidence_sources.append({
                "type": "k2_site_signals",
                "signals": list(finishing_signals) + list(oem_signals),
                "url": website,
            })

        # K2: Brave evidence details on same domain
        for detail in evidence_details:
            url = detail.get("url", "")
            try:
                d = urlparse(url).netloc.lower()
            except Exception:
                d = ""
            if domain and d == domain:
                evidence_sources.append({
                    "type": "k2_site_snippet",
                    "url": url,
                    "term": detail.get("term", ""),
                })

        # Count K1 / K2
        k1_count = 0
        k2_count = 0
        k1_details = []
        k2_details = []

        for src in evidence_sources:
            src_type = src.get("type", "")
            if src_type.startswith("k1"):
                k1_count += 1
                k1_details.append(src)
            elif src_type.startswith("k2"):
                k2_count += 1
                k2_details.append(src)

        is_golden = k1_count >= 1 and k2_count >= 1

        lead["evidence_sources"] = evidence_sources
        lead["k1_count"] = k1_count
        lead["k2_count"] = k2_count
        lead["k1_details"] = k1_details
        lead["k2_details"] = k2_details
        lead["is_golden"] = is_golden
        lead["evidence_standard"] = "dual" if is_golden else "single"

        return lead
