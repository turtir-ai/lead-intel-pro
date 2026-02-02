from pathlib import Path
import yaml


class HSMapper:
    """Map text to HS codes using config/hs_rules.yaml decision tree."""

    def __init__(self, rules_path=None, default_primary="845190", default_fallback=None):
        self.rules_path = rules_path or (
            Path(__file__).parent.parent.parent / "config" / "hs_rules.yaml"
        )
        self.rules = self._load_rules()
        self.decision_tree = self.rules.get("decision_tree", [])
        self.default_primary = default_primary
        self.default_fallback = default_fallback or ["392690"]

    def _load_rules(self):
        if self.rules_path and Path(self.rules_path).exists():
            try:
                with open(self.rules_path, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
            except Exception:
                return {}
        return {}

    def map_text(self, text):
        text_l = (text or "").lower()
        for rule in self.decision_tree:
            keywords = [k.lower() for k in (rule.get("if_any_keyword") or [])]
            if keywords and any(k in text_l for k in keywords):
                return {
                    "hs_primary": rule.get("primary", ""),
                    "hs_secondary": rule.get("secondary", ""),
                    "hs_fallback": rule.get("fallback", []) or [],
                    "hs_reason": rule.get("reason", "keyword_match"),
                    "hs_matched_keywords": [k for k in keywords if k in text_l],
                }

        # Default mapping for stenter spare parts if no explicit keyword hit
        return {
            "hs_primary": self.default_primary,
            "hs_secondary": "",
            "hs_fallback": self.default_fallback,
            "hs_reason": "default_stenter_parts",
            "hs_matched_keywords": [],
        }
