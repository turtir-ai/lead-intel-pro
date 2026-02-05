#!/usr/bin/env python3
"""Contactability scoring for leads."""

from typing import Dict, Tuple


def _safe_str(val) -> str:
    """Safely convert value to string, handling NaN."""
    if val is None:
        return ""
    if isinstance(val, float) and val != val:  # NaN check
        return ""
    return str(val).strip()


def _safe_list(val) -> list:
    """Safely convert value to list, handling NaN and string-encoded lists."""
    if val is None:
        return []
    if isinstance(val, float) and val != val:  # NaN check
        return []
    if isinstance(val, list):
        return val
    # Handle string-encoded lists from CSV
    if isinstance(val, str):
        val = val.strip()
        if val.startswith('[') and val.endswith(']'):
            try:
                import ast
                parsed = ast.literal_eval(val)
                if isinstance(parsed, list):
                    return parsed
            except:
                pass
        # Could be comma-separated
        if ',' in val:
            return [x.strip() for x in val.split(',') if x.strip()]
        if val:
            return [val]
    return []


class ContactabilityScorer:
    """İletişim kalitesi puanlaması."""

    GENERIC_PREFIXES = ["noreply", "no-reply", "donotreply", "mailer"]
    INFO_PREFIXES = ["info", "contact", "hello", "enquiry", "inquiry"]
    DEPARTMENT_PREFIXES = ["sales", "export", "support", "marketing", "purchase", "procurement"]

    def score_email(self, email: str) -> Tuple[int, str]:
        if not email:
            return 0, "no_email"
        email = _safe_str(email)
        if not email or "@" not in email:
            return 0, "no_email"
        local_part = email.split("@")[0].lower()
        if any(local_part.startswith(p) for p in self.GENERIC_PREFIXES):
            return 0, "generic"
        if any(local_part == p for p in self.INFO_PREFIXES):
            return 10, "info"
        if any(local_part.startswith(p) for p in self.DEPARTMENT_PREFIXES):
            return 30, "department"
        if "." in local_part or "_" in local_part:
            return 60, "personal"
        return 20, "unknown"

    def score_lead(self, lead: Dict) -> Dict:
        score = 0
        details = []

        emails = _safe_list(lead.get("emails_extracted"))
        best_email_score = 0
        best_email_type = "no_email"
        for email in emails:
            email_score, email_type = self.score_email(email)
            if email_score > best_email_score:
                best_email_score = email_score
                best_email_type = email_type
        if best_email_score > 0:
            details.append(f"email:{best_email_type}:{best_email_score}")
        score += best_email_score

        phones = _safe_list(lead.get("phones_extracted"))
        if phones:
            score += 20
            details.append("phone:direct:20")

        linkedin_xray = _safe_str(lead.get("linkedin_xray"))
        if linkedin_xray:
            score += 15
            details.append("linkedin:xray:15")

        contact_person = _safe_str(lead.get("contact_person"))
        contact_role = _safe_str(lead.get("contact_role"))
        if contact_person and contact_role:
            score += 25
            details.append("decision_maker:named:25")

        lead["contactability_score"] = min(score, 100)
        lead["contactability_details"] = details
        return lead
