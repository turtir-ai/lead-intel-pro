#!/usr/bin/env python3
"""
Customer Qualifier - Sadece gerçek stenter/finishing müşterilerini filtrele
Sizin ürünlerinizi (Gleitstein, Gleitleiste, Kluppen, Buchse, Spindel) 
satın alabilecek şirketleri tespit eder.
"""

import os
import re
import logging
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CustomerQualifier:
    """
    Lead'leri sizin ürünleriniz için uygun müşteri olup olmadığına göre filtreler.
    
    Sizin Ürünleriniz (Plastik Enjeksiyon Stenter Parçaları):
    - Brückner: Nadelglied, Gleitleiste, Kluppen Gleitstein, Öffner Segment, Vertikal Kette
    - Monforts: Spindel Mutter, Torlonbuchse, Ketten Gleitstücke
    - Krantz: Gleitstein
    - Artos/Santex: Buchsen
    
    Hedef Müşteri Profili:
    - Stenter makinesi kullanan tekstil finishing fabrikaları
    - Dyeing & finishing operasyonları olan şirketler
    - Ram makinesi kullanan terbiye tesisleri
    """
    
    def __init__(self):
        self.base_path = Path(__file__).parent.parent.parent
        
        # Sizin müşteriniz olabilecek şirketlerin işaretleri
        self.qualifying_keywords = {
            # Makine türleri (sizin parçalarınızı kullanan)
            "machinery": [
                "stenter", "tenter", "ram", "ramöz", "spannrahmen",
                "montex", "power-frame", "power frame",
                "heat setting", "heat-setting", "thermofixierung"
            ],
            
            # Operasyon türleri (finishing yapanlar)
            "operations": [
                "finishing", "terbiye", "acabado", "finissage", "finition",
                "dyeing", "boyama", "teñido", "teinture", "tingimento",
                "bleaching", "ağartma", "blanqueo",
                "mercerizing", "merserize",
                "heat setting", "thermofixierung", "termofiksaj",
                "coating", "kaplama", "beschichtung",
                "sanforizing", "sanfor",
                "drying", "kurutma", "secado"
            ],
            
            # OEM markaları (sizin parçalarınızın uyumlu olduğu)
            "oem_brands": [
                "brückner", "bruckner", "brueckner",
                "monforts", "montex",
                "krantz", "artos", "santex",
                "babcock", "strahm"
            ],
            
            # Ürün kategorileri (finishing yapanlar)
            "product_categories": [
                "woven fabric", "knitted fabric", "dokuma", "örme",
                "denim", "nonwoven", "technical textile",
                "home textile", "ev tekstili",
                "upholstery", "döşemelik",
                "automotive textile", "otomotiv tekstil"
            ]
        }
        
        # Kesinlikle müşteri DEĞİL olan işaretler
        self.disqualifying_keywords = [
            # Sadece iplik üretenler (stenter kullanmaz)
            "spinning only", "sadece iplik", "yarn manufacturer",
            
            # Sadece konfeksiyon (finishing yapmaz)
            "garment only", "sadece konfeksiyon", "apparel manufacturer",
            "clothing factory", "giyim fabrikası",
            
            # Makine üreticileri (rakip, müşteri değil)
            "machinery manufacturer", "makine üreticisi",
            "textile machinery", "tekstil makinesi",
            
            # Distribütörler (son kullanıcı değil)
            "distributor", "trading company", "ticaret şirketi"
        ]
        
        # Yüksek güvenilirlik kaynakları (doğrudan müşteri)
        self.high_confidence_sources = [
            "known_manufacturer",
            "oem_customer", 
            "precision_search"
        ]
    
    def qualify_lead(self, lead: Dict) -> Dict:
        """
        Tek bir lead'i değerlendir.
        
        Returns:
            Lead dict with added fields:
            - is_qualified: bool - Gerçek müşteri mi?
            - qualification_score: int (0-100)
            - qualification_reason: str
        """
        company = str(lead.get("company", "")).lower()
        context = str(lead.get("context", "")).lower()
        source_type = str(lead.get("source_type", "")).lower()
        
        # Combine all text for analysis
        all_text = f"{company} {context}"
        
        # Start with base score
        score = 0
        reasons = []
        
        # 1. High confidence sources get automatic qualification
        if source_type in self.high_confidence_sources:
            score += 60
            reasons.append(f"Yüksek güvenilirlik kaynağı: {source_type}")
        
        # 2. Check for qualifying keywords
        for category, keywords in self.qualifying_keywords.items():
            for keyword in keywords:
                if keyword in all_text:
                    if category == "oem_brands":
                        score += 25
                        reasons.append(f"OEM marka: {keyword}")
                    elif category == "machinery":
                        score += 20
                        reasons.append(f"Makine referansı: {keyword}")
                    elif category == "operations":
                        score += 15
                        reasons.append(f"Finishing operasyonu: {keyword}")
                    elif category == "product_categories":
                        score += 10
                        reasons.append(f"Ürün kategorisi: {keyword}")
                    break  # Only count once per category
        
        # 3. Check for disqualifying keywords
        for keyword in self.disqualifying_keywords:
            if keyword in all_text:
                score -= 30
                reasons.append(f"Diskalifiye: {keyword}")
        
        # 4. Country bonus (priority markets)
        country = str(lead.get("country", "")).lower()
        priority_countries = ["turkey", "türkiye", "egypt", "brazil", "argentina", "pakistan", "india"]
        if any(c in country for c in priority_countries):
            score += 10
            reasons.append(f"Öncelikli pazar: {country}")
        
        # 5. Cap score
        score = max(0, min(100, score))
        
        # 6. Determine qualification
        is_qualified = score >= 50
        
        lead["is_qualified"] = is_qualified
        lead["qualification_score"] = score
        lead["qualification_reason"] = "; ".join(reasons[:3]) if reasons else "Yetersiz veri"
        
        return lead
    
    def qualify_all(self, leads_df: pd.DataFrame) -> pd.DataFrame:
        """Tüm lead'leri değerlendir."""
        logger.info(f"🎯 Qualifying {len(leads_df)} leads...")
        
        qualified_leads = []
        
        for _, row in leads_df.iterrows():
            lead = row.to_dict()
            qualified = self.qualify_lead(lead)
            qualified_leads.append(qualified)
        
        result_df = pd.DataFrame(qualified_leads)
        
        # Stats
        qualified_count = result_df["is_qualified"].sum()
        logger.info(f"✅ Qualified leads: {qualified_count} / {len(result_df)}")
        
        return result_df
    
    def filter_real_customers(self, input_path: str = None, output_path: str = None) -> pd.DataFrame:
        """
        Sadece gerçek müşterileri filtrele ve kaydet.
        """
        if input_path is None:
            input_path = self.base_path / "outputs" / "crm" / "targets_master.csv"
        if output_path is None:
            output_path = self.base_path / "outputs" / "crm" / "qualified_customers.csv"
        
        logger.info("=" * 60)
        logger.info("🎯 CUSTOMER QUALIFICATION - Gerçek Müşterileri Filtreleme")
        logger.info("=" * 60)
        
        # Load leads
        df = pd.read_csv(input_path)
        logger.info(f"Loaded {len(df)} leads from {input_path}")
        
        # Qualify all
        qualified_df = self.qualify_all(df)
        
        # Filter only qualified
        real_customers = qualified_df[qualified_df["is_qualified"] == True].copy()
        
        # Sort by qualification score
        real_customers = real_customers.sort_values("qualification_score", ascending=False)
        
        # Save
        real_customers.to_csv(output_path, index=False)
        logger.info(f"💾 Saved {len(real_customers)} qualified customers to {output_path}")
        
        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("📊 QUALIFICATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total leads: {len(df)}")
        logger.info(f"Qualified customers: {len(real_customers)} ({len(real_customers)/len(df)*100:.1f}%)")
        
        logger.info("\nBy Source Type:")
        for source in real_customers["source_type"].value_counts().head(10).items():
            logger.info(f"  {source[0]}: {source[1]}")
        
        logger.info("\nBy Country:")
        for country in real_customers["country"].value_counts().head(10).items():
            logger.info(f"  {country[0]}: {country[1]}")
        
        logger.info("\nTop 20 Qualified Customers:")
        for _, row in real_customers.head(20).iterrows():
            company = str(row.get("company", ""))[:40]
            country = str(row.get("country", ""))
            score = row.get("qualification_score", 0)
            reason = str(row.get("qualification_reason", ""))[:50]
            logger.info(f"  [{score:3.0f}] {company:40} | {country:12} | {reason}")
        
        return real_customers


def main():
    """Run customer qualification."""
    qualifier = CustomerQualifier()
    qualified = qualifier.filter_real_customers()
    
    print(f"\n✅ {len(qualified)} gerçek müşteri tespit edildi!")
    print(f"📁 Kaydedildi: outputs/crm/qualified_customers.csv")


if __name__ == "__main__":
    main()
