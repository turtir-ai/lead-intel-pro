#!/usr/bin/env python3
"""
Event Trigger Module - Otomatik Lead Skor Güncellemeleri

newupgrade.md PRD'den:
- High Priority: Large import (20+ ton), new facility announcement, fair participation
- Medium Priority: Association membership update, website machine list change, job posting

Trigger'lar ek puan ve CRM otomasyon aksiyonları üretir.
"""

import os
import re
import yaml
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Load scoring config for triggers
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../../config/scoring.yaml")


def _normalize_trigger_config(raw_config: Dict) -> Dict:
    """Normalize event trigger config into a dict by priority and trigger name."""
    normalized = {"high_priority": {}, "medium_priority": {}}

    if not raw_config:
        return normalized

    # Already in dict form
    if isinstance(raw_config, dict):
        for priority_key in ("high_priority", "medium_priority"):
            bucket = raw_config.get(priority_key)
            if isinstance(bucket, dict):
                normalized[priority_key].update(bucket)
            elif isinstance(bucket, list):
                for item in bucket:
                    trigger_key = (item or {}).get("trigger")
                    if not trigger_key:
                        continue
                    normalized[priority_key][trigger_key] = {
                        "description": item.get("description"),
                        "score_bonus": item.get("score_bonus"),
                        "crm_action": item.get("crm_action") or item.get("action"),
                        "urgency": item.get("urgency") or ("high" if priority_key == "high_priority" else "medium"),
                    }
        return normalized

    return normalized


def _load_trigger_config() -> Dict:
    """Load event trigger configuration."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
        raw = config.get("event_triggers", {})
        return _normalize_trigger_config(raw)
    except Exception as e:
        logger.warning(f"Could not load trigger config: {e}")
        return {}


# Default trigger definitions
DEFAULT_TRIGGERS = {
    "high_priority": {
        "large_import": {
            "description": "Import > 20 ton HS 8451.90",
            "score_bonus": 15,
            "crm_action": "notify_sales_team",
            "urgency": "high",
        },
        "new_facility": {
            "description": "Yeni tesis / kapasite artışı duyurusu",
            "score_bonus": 12,
            "crm_action": "create_opportunity",
            "urgency": "high",
        },
        "fair_participation": {
            "description": "ITMA / ITM / Techtextil katılımı",
            "score_bonus": 10,
            "crm_action": "schedule_visit",
            "urgency": "medium",
        },
    },
    "medium_priority": {
        "association_update": {
            "description": "Dernek üyelik güncellemesi",
            "score_bonus": 5,
            "crm_action": "update_record",
            "urgency": "low",
        },
        "website_change": {
            "description": "Web sitesi makine listesi değişikliği",
            "score_bonus": 5,
            "crm_action": "flag_for_review",
            "urgency": "low",
        },
        "job_posting": {
            "description": "Teknik personel ilanı",
            "score_bonus": 8,
            "crm_action": "add_to_nurture",
            "urgency": "medium",
        },
    },
}


class EventTriggerProcessor:
    """
    Event-based trigger processor for lead scoring automation.
    
    Detects events from:
    1. Trade data signals (large imports)
    2. Website changes (new machine mentions)
    3. Fair/exhibition data
    4. Job postings
    5. Association updates
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or _load_trigger_config()
        if not self.config:
            self.config = DEFAULT_TRIGGERS
        else:
            # Ensure normalized structure even if caller passed raw config
            self.config = _normalize_trigger_config(self.config)
            
        self.stats = {
            "leads_processed": 0,
            "triggers_fired": 0,
            "high_priority": 0,
            "medium_priority": 0,
            "total_bonus_applied": 0,
        }
        
        # Pattern matchers for event detection
        self._init_patterns()
    
    def _init_patterns(self):
        """Initialize regex patterns for event detection."""
        # Import volume patterns
        self.import_patterns = [
            r"(\d+)\s*(ton|tonne|mt|kg)",
            r"import(?:ed|s)?\s*[\$€]?([\d,]+)",
        ]
        
        # New facility patterns (multi-language)
        self.facility_keywords = [
            # Turkish
            "yeni tesis", "yeni fabrika", "kapasite artışı", "genişleme",
            "modernizasyon", "yatırım",
            # English
            "new plant", "new facility", "capacity expansion", "expansion",
            "modernization", "investment", "upgrade", "retrofit",
            # Portuguese
            "nova fábrica", "expansão", "investimento",
            # Spanish
            "nueva planta", "expansión", "inversión",
        ]
        
        # Fair patterns
        self.fair_keywords = [
            "itma", "itm", "techtextil", "fimi", "index",
            "heimtextil", "intertextile", "texworld",
            # Turkish
            "fuarı", "sergi",
        ]
        
        # Job posting patterns
        self.job_keywords = [
            # Turkish
            "iş ilanı", "personel alımı", "teknik müdür", "bakım mühendisi",
            "makine operatörü",
            # English
            "job posting", "hiring", "vacancy", "engineer", "technician",
            "maintenance manager", "production manager",
        ]
    
    def detect_triggers(self, lead: Dict) -> List[Dict]:
        """
        Detect all applicable triggers for a lead.
        
        Returns list of triggered events with bonus and action info.
        """
        triggers = []
        
        # Get text content to analyze
        context = str(lead.get("context", "")).lower()
        source_type = str(lead.get("source_type", "")).lower()
        company = str(lead.get("company", "")).lower()
        text = f"{context} {company}"
        
        # Check high priority triggers
        triggers.extend(self._check_large_import(lead))
        triggers.extend(self._check_new_facility(text))
        triggers.extend(self._check_fair_participation(lead, text))
        
        # Check medium priority triggers
        triggers.extend(self._check_job_posting(text))
        triggers.extend(self._check_association_update(lead))
        triggers.extend(self._check_website_change(lead))
        
        return triggers
    
    def _check_large_import(self, lead: Dict) -> List[Dict]:
        """Check for large import trigger."""
        triggers = []
        
        # Check trade data fields
        import_volume = lead.get("import_volume_tons", 0)
        if isinstance(import_volume, str):
            try:
                import_volume = float(import_volume.replace(",", ""))
            except:
                import_volume = 0
        
        if import_volume >= 20:
            trigger_config = self.config.get("high_priority", {}).get("large_import", 
                                           DEFAULT_TRIGGERS["high_priority"]["large_import"])
            triggers.append({
                "trigger_type": "large_import",
                "priority": "high",
                "description": f"Import volume: {import_volume} tons",
                "score_bonus": trigger_config.get("score_bonus", 15),
                "crm_action": trigger_config.get("crm_action", "notify_sales_team"),
                "urgency": trigger_config.get("urgency", "high"),
            })
        
        return triggers
    
    def _check_new_facility(self, text: str) -> List[Dict]:
        """Check for new facility / expansion trigger."""
        triggers = []
        
        for keyword in self.facility_keywords:
            if keyword in text:
                trigger_config = self.config.get("high_priority", {}).get("new_facility",
                                               DEFAULT_TRIGGERS["high_priority"]["new_facility"])
                triggers.append({
                    "trigger_type": "new_facility",
                    "priority": "high",
                    "description": f"Facility signal: {keyword}",
                    "score_bonus": trigger_config.get("score_bonus", 12),
                    "crm_action": trigger_config.get("crm_action", "create_opportunity"),
                    "urgency": trigger_config.get("urgency", "high"),
                })
                break  # Only one trigger per type
        
        return triggers
    
    def _check_fair_participation(self, lead: Dict, text: str) -> List[Dict]:
        """Check for fair/exhibition participation trigger."""
        triggers = []
        
        # Check source type
        source_type = str(lead.get("source_type", "")).lower()
        is_fair_source = "fair" in source_type or "exhibitor" in source_type
        
        # Check text for fair keywords
        fair_found = None
        for keyword in self.fair_keywords:
            if keyword in text:
                fair_found = keyword
                break
        
        if is_fair_source or fair_found:
            trigger_config = self.config.get("high_priority", {}).get("fair_participation",
                                           DEFAULT_TRIGGERS["high_priority"]["fair_participation"])
            triggers.append({
                "trigger_type": "fair_participation",
                "priority": "high",
                "description": f"Fair: {fair_found or source_type}",
                "score_bonus": trigger_config.get("score_bonus", 10),
                "crm_action": trigger_config.get("crm_action", "schedule_visit"),
                "urgency": trigger_config.get("urgency", "medium"),
            })
        
        return triggers
    
    def _check_job_posting(self, text: str) -> List[Dict]:
        """Check for technical job posting trigger."""
        triggers = []
        
        for keyword in self.job_keywords:
            if keyword in text:
                trigger_config = self.config.get("medium_priority", {}).get("job_posting",
                                               DEFAULT_TRIGGERS["medium_priority"]["job_posting"])
                triggers.append({
                    "trigger_type": "job_posting",
                    "priority": "medium",
                    "description": f"Job signal: {keyword}",
                    "score_bonus": trigger_config.get("score_bonus", 8),
                    "crm_action": trigger_config.get("crm_action", "add_to_nurture"),
                    "urgency": trigger_config.get("urgency", "medium"),
                })
                break
        
        return triggers
    
    def _check_association_update(self, lead: Dict) -> List[Dict]:
        """Check for association membership update trigger."""
        triggers = []
        
        # Check if from association source with recent update
        source_type = str(lead.get("source_type", "")).lower()
        is_association = any(kw in source_type for kw in ["gots", "bci", "oekotex", "association"])
        
        # Check for recent update indicator
        last_updated = lead.get("last_updated") or lead.get("updated_at")
        is_recent = False
        
        if last_updated:
            try:
                if isinstance(last_updated, str):
                    update_date = datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                else:
                    update_date = last_updated
                is_recent = (datetime.now(update_date.tzinfo) - update_date) < timedelta(days=90)
            except:
                is_recent = False
        
        if is_association and is_recent:
            trigger_config = self.config.get("medium_priority", {}).get("association_update",
                                           DEFAULT_TRIGGERS["medium_priority"]["association_update"])
            triggers.append({
                "trigger_type": "association_update",
                "priority": "medium",
                "description": f"Recent {source_type} update",
                "score_bonus": trigger_config.get("score_bonus", 5),
                "crm_action": trigger_config.get("crm_action", "update_record"),
                "urgency": trigger_config.get("urgency", "low"),
            })
        
        return triggers
    
    def _check_website_change(self, lead: Dict) -> List[Dict]:
        """Check for website machine list change trigger."""
        triggers = []
        
        # This would be used with a website monitoring system
        # For now, check if there's a website_changed flag
        if lead.get("website_changed") or lead.get("new_machine_detected"):
            trigger_config = self.config.get("medium_priority", {}).get("website_change",
                                           DEFAULT_TRIGGERS["medium_priority"]["website_change"])
            triggers.append({
                "trigger_type": "website_change",
                "priority": "medium",
                "description": "Website machine list updated",
                "score_bonus": trigger_config.get("score_bonus", 5),
                "crm_action": trigger_config.get("crm_action", "flag_for_review"),
                "urgency": trigger_config.get("urgency", "low"),
            })
        
        return triggers
    
    def process_lead(self, lead: Dict) -> Dict:
        """
        Process a lead and apply event triggers.
        
        Adds:
        - triggers_detected: List of triggered events
        - trigger_bonus_total: Sum of bonus points
        - trigger_priority: Highest priority level
        - trigger_crm_actions: List of CRM actions to take
        """
        triggers = self.detect_triggers(lead)
        
        if triggers:
            # Calculate totals
            bonus_total = sum(t.get("score_bonus", 0) for t in triggers)
            
            # Get highest priority
            priorities = [t.get("priority", "low") for t in triggers]
            if "high" in priorities:
                highest = "high"
            elif "medium" in priorities:
                highest = "medium"
            else:
                highest = "low"
            
            # Collect CRM actions
            crm_actions = list(set(t.get("crm_action") for t in triggers if t.get("crm_action")))
            
            # Update lead
            lead["triggers_detected"] = triggers
            lead["trigger_count"] = len(triggers)
            lead["trigger_bonus_total"] = bonus_total
            lead["trigger_priority"] = highest
            lead["trigger_crm_actions"] = crm_actions
            lead["has_high_priority_trigger"] = highest == "high"
            
            # Update stats
            self.stats["triggers_fired"] += len(triggers)
            self.stats["total_bonus_applied"] += bonus_total
            if highest == "high":
                self.stats["high_priority"] += 1
            elif highest == "medium":
                self.stats["medium_priority"] += 1
        else:
            lead["triggers_detected"] = []
            lead["trigger_count"] = 0
            lead["trigger_bonus_total"] = 0
            lead["trigger_priority"] = None
            lead["trigger_crm_actions"] = []
            lead["has_high_priority_trigger"] = False
        
        self.stats["leads_processed"] += 1
        return lead
    
    def process_batch(self, leads: List[Dict]) -> List[Dict]:
        """Process a batch of leads."""
        logger.info(f"Processing {len(leads)} leads for event triggers...")
        
        processed = []
        for lead in leads:
            processed.append(self.process_lead(lead))
        
        logger.info(f"Event trigger processing complete: "
                   f"{self.stats['triggers_fired']} triggers fired, "
                   f"{self.stats['high_priority']} high priority, "
                   f"{self.stats['medium_priority']} medium priority")
        
        return processed
    
    def get_stats(self) -> Dict:
        """Get trigger processing statistics."""
        return self.stats.copy()
    
    def get_leads_requiring_action(self, leads: List[Dict], action_type: str = None) -> List[Dict]:
        """
        Get leads that require a specific CRM action.
        
        action_type: "notify_sales_team", "create_opportunity", "schedule_visit", etc.
        """
        result = []
        for lead in leads:
            actions = lead.get("trigger_crm_actions", [])
            if action_type:
                if action_type in actions:
                    result.append(lead)
            elif actions:
                result.append(lead)
        return result


# Convenience function
def detect_events(lead: Dict) -> List[Dict]:
    """Quick event detection without creating processor instance."""
    processor = EventTriggerProcessor()
    return processor.detect_triggers(lead)
