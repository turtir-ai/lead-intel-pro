#!/usr/bin/env python3
"""Sales outcome tracking (lightweight)."""

from typing import Dict


class SalesTracker:
    STATUSES = [
        "not_started",
        "contacted",
        "replied",
        "meeting_scheduled",
        "meeting_done",
        "proposal_sent",
        "won",
        "lost",
        "disqualified",
    ]

    LOSS_REASONS = [
        "wrong_person",
        "no_stenter",
        "no_reply",
        "no_budget",
        "competitor",
        "price",
        "timing",
        "other",
    ]

    def update_lead_status(self, lead: Dict, status: str, reason: str = None, notes: str = None) -> Dict:
        if status in self.STATUSES:
            lead["outreach_status"] = status
        if reason in self.LOSS_REASONS:
            lead["reason_lost"] = reason
        if notes:
            lead["outreach_notes"] = notes
        return lead
