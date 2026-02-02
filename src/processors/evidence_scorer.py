import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from unidecode import unidecode

from src.utils.logger import get_logger

logger = get_logger(__name__)


class EvidenceScorer:
    """
    Evidence scorer builds a small, sales-ready evidence object from page text.
    Uses config/keyword_signals.yml when available, with a safe fallback.
    """

    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or (Path(__file__).parent.parent.parent / "config" / "keyword_signals.yml")
        self.signals: List[Dict] = []
        self.negative_signals: List[str] = []
        self._load_config()

    def _load_config(self) -> None:
        if self.config_path.exists():
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f) or {}
                self.negative_signals = [str(x).lower() for x in cfg.get("negative_signals", [])]
                for name, payload in (cfg.get("signals") or {}).items():
                    weight = float(payload.get("weight", 1))
                    keywords = []
                    for _, values in (payload.get("keywords") or {}).items():
                        if isinstance(values, list):
                            keywords.extend([str(v).strip() for v in values if str(v).strip()])
                    self.signals.append({
                        "name": str(name),
                        "weight": weight,
                        "keywords": keywords,
                    })
                return
            except Exception as e:
                logger.warning(f"Failed to load keyword_signals.yml: {e}")

        # Fallback: minimal signal list
        self.signals = [
            {"name": "finishing", "weight": 2, "keywords": ["textile finishing", "dyeing", "färberei", "boyahane"]},
            {"name": "stenter", "weight": 3, "keywords": ["stenter", "rama", "spannrahmen", "ram makinesi"]},
            {"name": "parts", "weight": 2, "keywords": ["spare parts", "yedek parça", "ersatzteile"]},
        ]
        self.negative_signals = ["job", "career", "news", "blog"]

    def _find_match(self, text: str, keyword: str) -> Optional[re.Match]:
        try:
            return re.search(re.escape(keyword), text, flags=re.IGNORECASE)
        except Exception:
            return None

    def _best_snippet(self, original_text: str, keyword: str) -> str:
        match = self._find_match(original_text, keyword)
        if match:
            return self._build_snippet(original_text, match)
        return original_text[:280].strip()

    def _build_snippet(self, text: str, match: re.Match, window: int = 140) -> str:
        start = max(match.start() - window, 0)
        end = min(match.end() + window, len(text))
        snippet = text[start:end].strip()
        if start > 0:
            snippet = "…" + snippet
        if end < len(text):
            snippet = snippet + "…"
        return snippet

    def score(
        self,
        text: str,
        url: Optional[str] = None,
        retrieved_at: Optional[str] = None,
    ) -> Dict[str, object]:
        """
        Return evidence object with snippet + signals + confidence.
        """
        text = text or ""
        trimmed = text[:10000]
        normalized = unidecode(trimmed).lower()
        matches: List[Tuple[Dict, str, re.Match]] = []

        for signal in self.signals:
            for kw in signal.get("keywords", []):
                if not kw:
                    continue
                match = self._find_match(normalized, kw.lower())
                if match:
                    matches.append((signal, kw, match))
                    break

        if not matches:
            return {
                "snippet": "",
                "signals": [],
                "matched_keywords": [],
                "confidence": "low",
                "url": url or "",
                "retrieved_at": retrieved_at or "",
            }

        # Pick best match by weight, then earliest position
        matches.sort(key=lambda item: (-float(item[0].get("weight", 1)), item[2].start()))
        best_signal, best_kw, _ = matches[0]

        matched_signal_names = [m[0].get("name") for m in matches]
        matched_keywords = [m[1] for m in matches]

        score = sum(float(m[0].get("weight", 1)) for m in matches)
        negative_hits = [neg for neg in self.negative_signals if neg in normalized]
        if negative_hits:
            score = max(0, score - 1.5)

        if score >= 6:
            confidence = "high"
        elif score >= 3:
            confidence = "medium"
        else:
            confidence = "low"

        return {
            "snippet": self._best_snippet(trimmed, best_kw),
            "signals": matched_signal_names,
            "matched_keywords": matched_keywords,
            "confidence": confidence,
            "url": url or "",
            "retrieved_at": retrieved_at or "",
        }
