#!/usr/bin/env python3
"""Source ROI tracker."""

from typing import Dict


class SourceTracker:
    def __init__(self):
        self.stats: Dict[str, Dict[str, int]] = {}

    def record_lead(self, source_id: str, tier: int) -> None:
        if not source_id:
            source_id = "unknown"
        if source_id not in self.stats:
            self.stats[source_id] = {"total": 0, "tier1": 0, "tier2": 0, "tier3": 0}
        self.stats[source_id]["total"] += 1
        tier_key = f"tier{tier}" if tier in (1, 2, 3) else "tier3"
        self.stats[source_id][tier_key] += 1

    def get_tier1_rate(self, source_id: str) -> float:
        s = self.stats.get(source_id, {})
        total = s.get("total", 0)
        tier1 = s.get("tier1", 0)
        return tier1 / total if total > 0 else 0.0

    def should_kill_source(self, source_id: str, threshold: float = 0.10) -> bool:
        s = self.stats.get(source_id, {})
        if s.get("total", 0) < 50:
            return False
        return self.get_tier1_rate(source_id) < threshold

    def to_rows(self):
        rows = []
        for source_id, s in self.stats.items():
            total = s.get("total", 0)
            tier1 = s.get("tier1", 0)
            tier2 = s.get("tier2", 0)
            tier3 = s.get("tier3", 0)
            rows.append({
                "source_id": source_id,
                "total": total,
                "tier1": tier1,
                "tier2": tier2,
                "tier3": tier3,
                "yield_tier1": (tier1 / total) if total else 0.0,
            })
        return rows
