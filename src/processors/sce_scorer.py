#!/usr/bin/env python3
"""
SCE (Stenter Customer Evidence) Scoring
GPT önerisi: 3 katmanlı "kanıt" skoru

E1 (Kesin): stenter / tenter frame / Montex / Brückner / Krantz / Santex / Artos
E2 (Güçlü): dyeing / printing / finishing / tenter / mercerizing
E3 (Destek): woven/knit fabric manufacturing + finishing plant / dyehouse

Kural: Outbound satış listesi = E1 veya (E2 + E3)
"""

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class SCEResult:
    """SCE scoring result."""
    lead_id: str
    company: str
    e1_score: float  # Kesin kanıt (0-1)
    e2_score: float  # Güçlü kanıt (0-1)
    e3_score: float  # Destek kanıt (0-1)
    total_score: float  # Combined score
    e1_signals: List[str]
    e2_signals: List[str]
    e3_signals: List[str]
    is_sales_ready: bool
    confidence: str  # "high", "medium", "low"


class SCEScorer:
    """
    Stenter Customer Evidence (SCE) Scorer
    
    Her lead'e kanıt skoru verir ve satış hazırlığını belirler.
    """
    
    def __init__(self):
        # E1: Kesin kanıtlar - stenter/ram frame veya OEM referansları
        self.e1_keywords = {
            # Stenter/ram frame keywords
            'stenter', 'stentor', 'tenter', 'tenter frame', 'ram frame',
            'heat setting', 'heat-setting', 'thermofixation', 'termo-fixação',
            'termofijado', 'termofissaggio', 'wärmefixierung',
            # OEM brands
            'brückner', 'bruckner', 'brueckner', 
            'monforts', 'montex', 'monfortex',
            'krantz', 'krantz stenter',
            'santex', 'santex rimar',
            'artos', 'artos textile',
            'babcock', 'babcock-textil',
            'benninger', 'benninger küsters',
            'goller', 'goller group',
            'thies', 'thies group',
            # Parts keywords
            'chain rail', 'rail chain', 'pin chain', 'clip chain',
            'stenter chain', 'tenter pin', 'stenter clip',
            'spindle nut', 'slide block', 'bearing bush',
        }
        
        # E2: Güçlü kanıtlar - finishing/dyeing işlemleri
        self.e2_keywords = {
            # English
            'dyeing', 'printing', 'finishing', 'bleaching', 'mercerizing',
            'sanforizing', 'coating', 'laminating', 'calendering',
            'singeing', 'desizing', 'scouring', 'shrink proofing',
            'dyehouse', 'dye house', 'finishing plant', 'finishing line',
            'continuous dyeing', 'jet dyeing', 'pad dyeing',
            # Turkish
            'terbiye', 'boya', 'boyama', 'boyahane', 'baskı', 'apre',
            'ağartma', 'merserizasyon', 'sanfor', 'kaplama', 'laminasyon',
            'ram makinesi', 'ramöz', 'şardonlama', 'zımparalama',
            # Portuguese
            'tinturaria', 'tingimento', 'acabamento', 'estamparia',
            'alvejamento', 'mercerização', 'sanforização',
            'termofixação', 'calandragem', 'chamuscagem',
            # Spanish
            'tintorería', 'teñido', 'acabados', 'estampado',
            'blanqueo', 'mercerizado', 'sanforizado',
            'planta de acabado', 'línea de acabado',
            # German
            'färberei', 'druckerei', 'ausrüstung', 'veredlung',
            'bleicherei', 'appretur', 'kontinue-färbung',
            # Italian
            'tintoria', 'stamperia', 'finissaggio', 'nobilitazione',
            'candeggio', 'mercerizzazione',
        }
        
        # E3: Destek kanıtlar - tekstil üretimi + fabrika göstergeleri
        self.e3_keywords = {
            # Manufacturing types
            'textile manufacturing', 'fabric manufacturing',
            'woven fabric', 'knitted fabric', 'knit fabric',
            'weaving mill', 'knitting mill', 'spinning mill',
            'textile mill', 'textile plant', 'textile factory',
            # Turkish
            'tekstil üretimi', 'kumaş üretimi', 'dokuma fabrikası',
            'örme fabrikası', 'iplik fabrikası', 'tekstil fabrikası',
            'entegre tesis', 'dikey entegrasyon',
            # Portuguese
            'tecelagem', 'malharia', 'fiação', 'tecelaria',
            'fábrica têxtil', 'indústria têxtil',
            'produção têxtil', 'fabricação de tecidos',
            # Spanish
            'tejeduría', 'hilandería', 'fábrica textil',
            'planta textil', 'producción textil',
            'manufactura textil', 'confección',
            # Investment/expansion indicators
            'new line', 'expansion', 'investment', 'capacity',
            'nova linha', 'expansão', 'investimento',
            'nueva línea', 'expansión', 'inversión',
            'modernization', 'modernização', 'modernización',
        }
        
        # Machinery/supplier keywords (negative signal)
        self.machinery_keywords = {
            'machinery', 'equipment', 'spare parts', 'components',
            'máquina', 'maquinaria', 'repuestos', 'partes',
            'máquinas', 'equipamentos', 'peças',
            'supplier', 'dealer', 'distributor', 'agent',
            'trading', 'import', 'export company',
        }
    
    def score(self, lead: Dict) -> SCEResult:
        """
        Score a lead for Stenter Customer Evidence.
        
        Args:
            lead: Lead dictionary with company, context, website, etc.
            
        Returns:
            SCEResult with scores and signals
        """
        # Build searchable text
        company = str(lead.get("company", "")).lower()
        context = str(lead.get("context", "")).lower()
        website_content = str(lead.get("website_content", "")).lower()
        source_name = str(lead.get("source_name", "")).lower()
        
        # Phase 2: Include Brave evidence if available
        sce_evidence = str(lead.get("sce_evidence_text", "")).lower()
        sce_evidence_type = str(lead.get("sce_evidence_type", "")).lower()
        
        text = f"{company} {context} {website_content} {source_name} {sce_evidence} {sce_evidence_type}"
        
        # Score E1
        e1_signals = []
        e1_count = 0
        for kw in self.e1_keywords:
            if kw in text:
                e1_signals.append(kw)
                e1_count += 1
        
        # Phase 2: Boost E1 if Brave found strong evidence
        if lead.get('sce_has_evidence') and lead.get('sce_confidence') == 'strong':
            e1_count += 2  # Strong boost
            e1_signals.append(f"brave_evidence:{lead.get('sce_evidence_type', '')}")
        
        e1_score = min(1.0, e1_count * 0.4)  # Cap at 1.0
        
        # Score E2
        e2_signals = []
        e2_count = 0
        for kw in self.e2_keywords:
            if kw in text:
                e2_signals.append(kw)
                e2_count += 1
        
        # Phase 2: Boost E2 if Brave found medium evidence
        if lead.get('sce_has_evidence') and lead.get('sce_confidence') == 'medium':
            e2_count += 1  # Medium boost
            e2_signals.append(f"brave_evidence:{lead.get('sce_evidence_type', '')}")
        
        e2_score = min(1.0, e2_count * 0.25)
        
        # Score E3
        e3_signals = []
        e3_count = 0
        for kw in self.e3_keywords:
            if kw in text:
                e3_signals.append(kw)
                e3_count += 1
        e3_score = min(1.0, e3_count * 0.2)
        
        # Check for machinery/supplier (negative)
        is_machinery = any(kw in text for kw in self.machinery_keywords)
        if is_machinery:
            e1_score *= 0.3
            e2_score *= 0.3
            e3_score *= 0.3
        
        # Calculate total score
        # E1 alone is strong enough
        # E2 + E3 together is also good
        total_score = max(
            e1_score,
            (e2_score * 0.6) + (e3_score * 0.4),
            (e1_score * 0.5) + (e2_score * 0.3) + (e3_score * 0.2)
        )
        
        # Determine sales readiness
        # Rule: E1 >= 0.4 OR (E2 >= 0.4 AND E3 >= 0.3)
        is_sales_ready = (
            e1_score >= 0.4 or 
            (e2_score >= 0.4 and e3_score >= 0.3) or
            total_score >= 0.5
        ) and not is_machinery
        
        # Confidence level
        if e1_score >= 0.6 or total_score >= 0.7:
            confidence = "high"
        elif e1_score >= 0.3 or total_score >= 0.4:
            confidence = "medium"
        else:
            confidence = "low"
        
        return SCEResult(
            lead_id=str(lead.get("id", "")),
            company=lead.get("company", ""),
            e1_score=round(e1_score, 3),
            e2_score=round(e2_score, 3),
            e3_score=round(e3_score, 3),
            total_score=round(total_score, 3),
            e1_signals=e1_signals[:5],  # Top 5
            e2_signals=e2_signals[:5],
            e3_signals=e3_signals[:5],
            is_sales_ready=is_sales_ready,
            confidence=confidence
        )
    
    def score_batch(self, leads: List[Dict]) -> Tuple[List[Dict], Dict]:
        """
        Score all leads and return enriched leads + summary stats.
        
        Returns:
            Tuple of (leads_with_scores, summary_stats)
        """
        logger.info("=" * 60)
        logger.info("📊 SCE (Stenter Customer Evidence) SCORING")
        logger.info("=" * 60)
        
        scored_leads = []
        stats = {
            "total": len(leads),
            "sales_ready": 0,
            "high_confidence": 0,
            "medium_confidence": 0,
            "low_confidence": 0,
            "e1_positive": 0,
            "e2_positive": 0,
            "e3_positive": 0,
        }
        
        for lead in leads:
            result = self.score(lead)
            
            # Add scores to lead
            lead["sce_e1"] = result.e1_score
            lead["sce_e2"] = result.e2_score
            lead["sce_e3"] = result.e3_score
            lead["sce_total"] = result.total_score
            lead["sce_signals"] = "; ".join(result.e1_signals + result.e2_signals + result.e3_signals)
            lead["sce_sales_ready"] = result.is_sales_ready
            lead["sce_confidence"] = result.confidence
            
            # Update stats
            if result.is_sales_ready:
                stats["sales_ready"] += 1
            
            if result.confidence == "high":
                stats["high_confidence"] += 1
            elif result.confidence == "medium":
                stats["medium_confidence"] += 1
            else:
                stats["low_confidence"] += 1
            
            if result.e1_score > 0:
                stats["e1_positive"] += 1
            if result.e2_score > 0:
                stats["e2_positive"] += 1
            if result.e3_score > 0:
                stats["e3_positive"] += 1
            
            scored_leads.append(lead)
        
        # Log summary
        logger.info(f"\n📊 SCE Scoring Summary:")
        logger.info(f"  Total leads: {stats['total']}")
        logger.info(f"  Sales ready: {stats['sales_ready']} ({100*stats['sales_ready']/max(1, stats['total']):.1f}%)")
        logger.info(f"  High confidence: {stats['high_confidence']}")
        logger.info(f"  Medium confidence: {stats['medium_confidence']}")
        logger.info(f"  Low confidence: {stats['low_confidence']}")
        logger.info(f"  E1 positive: {stats['e1_positive']}")
        logger.info(f"  E2 positive: {stats['e2_positive']}")
        logger.info(f"  E3 positive: {stats['e3_positive']}")
        
        return scored_leads, stats
    
    def filter_sales_ready(self, leads: List[Dict], min_confidence: str = "medium") -> List[Dict]:
        """
        Filter leads to only include sales-ready ones.
        
        Args:
            leads: List of leads (already scored with score_batch)
            min_confidence: Minimum confidence level ("high", "medium", "low")
            
        Returns:
            Filtered list of sales-ready leads
        """
        confidence_order = {"high": 3, "medium": 2, "low": 1}
        min_level = confidence_order.get(min_confidence, 1)
        
        sales_ready = []
        for lead in leads:
            if lead.get("sce_sales_ready"):
                conf = lead.get("sce_confidence", "low")
                if confidence_order.get(conf, 0) >= min_level:
                    sales_ready.append(lead)
        
        return sales_ready


# Test
if __name__ == "__main__":
    scorer = SCEScorer()
    
    test_leads = [
        {
            "company": "Döhler Textil",
            "context": "textile manufacturing with Monforts Montex stenter finishing line",
            "country": "Brazil"
        },
        {
            "company": "Santana Textiles",
            "context": "denim production with dyeing and finishing facilities",
            "country": "Brazil"
        },
        {
            "company": "ANJ Máquinas",
            "context": "textile machinery dealer and equipment supplier",
            "country": "Brazil"
        },
        {
            "company": "Random Company",
            "context": "general business services",
            "country": "Brazil"
        }
    ]
    
    scored, stats = scorer.score_batch(test_leads)
    
    print(f"\n{'='*60}")
    print("Scored Leads:")
    for lead in scored:
        print(f"\n{lead['company']}:")
        print(f"  E1: {lead['sce_e1']}, E2: {lead['sce_e2']}, E3: {lead['sce_e3']}")
        print(f"  Total: {lead['sce_total']}, Sales Ready: {lead['sce_sales_ready']}")
        print(f"  Confidence: {lead['sce_confidence']}")
        print(f"  Signals: {lead['sce_signals']}")
