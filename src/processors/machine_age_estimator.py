#!/usr/bin/env python3
"""Machine age estimator and urgency boost."""

from typing import Dict


class MachineAgeEstimator:
    PRIORITY_YEARS = range(2016, 2022)
    WARRANTY_YEARS = range(2023, 2027)

    def estimate_age(self, lead: Dict) -> Dict:
        year = lead.get("estimated_installation_year")
        if isinstance(year, str) and year.isdigit():
            year = int(year)
        if isinstance(year, int):
            if year in self.PRIORITY_YEARS:
                lead["machine_age_priority"] = "high"
                lead["urgency_boost"] = 20
            elif year in self.WARRANTY_YEARS:
                lead["machine_age_priority"] = "low"
                lead["urgency_boost"] = -10
            else:
                lead["machine_age_priority"] = "medium"
                lead["urgency_boost"] = 0
        else:
            lead.setdefault("machine_age_priority", "unknown")
            lead.setdefault("urgency_boost", 0)
        return lead
