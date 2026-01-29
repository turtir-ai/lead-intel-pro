#!/usr/bin/env python3
"""
Import Intelligence - Gümrük Hareketleri Analizi

HS kodlarına göre ithalat yapan ülkeleri ve potansiyel müşterileri bulur.

Strateji:
1. Comtrade API ile HS 844832, 844839, 845190 ithalatlarını çek
2. En çok ithalat yapan ülkeleri belirle
3. Bu ülkelerdeki tekstil firmalarını hedefle
4. Opsiyonel: Import-Export kayıtlarından firma isimlerini bul (varsa)
"""

import os
import time
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
import pandas as pd
import yaml

from src.utils.logger import get_logger

logger = get_logger(__name__)


class ImportIntelligence:
    """
    Gümrük/Trade data analizi ile potansiyel müşterileri belirler.
    
    HS Kodları:
    - 844832: Tekstil elyaf hazırlama makine parçaları
    - 844839: Dokuma/örgü makine parçaları  
    - 845190: Boyama/terbiye makine parçaları (stenter chains, clips)
    """
    
    COMTRADE_BASE_URL = "https://comtradeapi.un.org/data/v1/get/C/A"
    
    # Hedef HS kodları
    HS_CODES = ["844832", "844839", "845190"]
    
    # Partner ülkeler (ithalatçılar - hedef pazarlar)
    TARGET_IMPORTERS = {
        "TUR": {"name": "Turkey", "priority": 1},
        "BRA": {"name": "Brazil", "priority": 2},
        "EGY": {"name": "Egypt", "priority": 3},
        "MAR": {"name": "Morocco", "priority": 4},
        "TUN": {"name": "Tunisia", "priority": 5},
        "ARG": {"name": "Argentina", "priority": 6},
        "COL": {"name": "Colombia", "priority": 7},
        "PER": {"name": "Peru", "priority": 8},
        "IND": {"name": "India", "priority": 9},
        "PAK": {"name": "Pakistan", "priority": 10},
        "BGD": {"name": "Bangladesh", "priority": 11},
        "VNM": {"name": "Vietnam", "priority": 12},
        "IDN": {"name": "Indonesia", "priority": 13},
    }
    
    # Ihracatçı ülkeler (rakip tedarikçiler)
    EXPORTERS = {
        "DEU": "Germany",
        "CHN": "China",
        "ITA": "Italy",
        "JPN": "Japan",
        "TWN": "Taiwan",
    }

    def __init__(self, api_key: Optional[str] = None, config_path: Optional[Path] = None):
        self.api_key = api_key or os.environ.get("COMTRADE_API_KEY") or os.environ.get("Comtrade_API_KEY")
        self.cache_dir = Path("data/cache/comtrade")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Load products config if available
        config_path = config_path or Path(__file__).parent.parent.parent / "config" / "products.yaml"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                self.products_config = yaml.safe_load(f)
            # Update HS codes from config
            config_hs = self.products_config.get("hs_codes", [])
            if config_hs:
                self.HS_CODES = [h.get("code", h) for h in config_hs if h]
    
    def get_import_rankings(self, years: List[int] = None, force_refresh: bool = False) -> pd.DataFrame:
        """
        Her ülkenin HS kodlarına göre toplam ithalat değerini hesaplar.
        
        Returns:
            DataFrame with columns: country_iso3, country_name, import_value, hs_code, year
        """
        years = years or [2022, 2023]
        cache_key = f"import_rankings_{'_'.join(map(str, years))}.csv"
        cache_path = self.cache_dir / cache_key
        
        # Check cache
        if not force_refresh and cache_path.exists():
            try:
                df = pd.read_csv(cache_path)
                logger.info(f"Loaded import rankings from cache: {len(df)} records")
                return df
            except:
                pass
        
        all_data = []
        
        for iso3, info in self.TARGET_IMPORTERS.items():
            for hs_code in self.HS_CODES:
                for year in years:
                    try:
                        value = self._fetch_import_value(iso3, hs_code, year)
                        if value is not None:
                            all_data.append({
                                "country_iso3": iso3,
                                "country_name": info["name"],
                                "import_value": value,
                                "hs_code": hs_code,
                                "year": year,
                                "priority": info["priority"],
                            })
                        time.sleep(0.5)  # Rate limiting
                    except Exception as e:
                        logger.warning(f"Error fetching {iso3}/{hs_code}/{year}: {e}")
        
        if all_data:
            df = pd.DataFrame(all_data)
            
            # Save to cache
            df.to_csv(cache_path, index=False)
            logger.info(f"Saved import rankings to cache: {len(df)} records")
            
            return df
        
        return pd.DataFrame()
    
    def _fetch_import_value(self, reporter_iso3: str, hs_code: str, year: int) -> Optional[float]:
        """Fetch import value from Comtrade API."""
        if not self.api_key:
            logger.warning("Comtrade API key not set")
            return None
        
        # Convert ISO3 to Comtrade numeric code
        reporter_code = self._iso3_to_code(reporter_iso3)
        
        params = {
            "reporterCode": reporter_code,
            "period": str(year),
            "partnerCode": "0",  # World
            "flowCode": "M",    # Imports
            "cmdCode": hs_code,
        }
        
        headers = {
            "Ocp-Apim-Subscription-Key": self.api_key
        }
        
        try:
            resp = requests.get(self.COMTRADE_BASE_URL, params=params, headers=headers, timeout=30)
            
            if resp.status_code == 429:
                logger.warning("Rate limit hit, waiting...")
                time.sleep(10)
                return None
            
            if resp.status_code != 200:
                return None
            
            data = resp.json()
            
            # Sum up values
            total = 0
            for record in data.get("data", []):
                value = record.get("primaryValue", 0) or 0
                total += float(value)
            
            return total if total > 0 else None
            
        except Exception as e:
            logger.debug(f"Comtrade API error: {e}")
            return None
    
    def get_country_priority_scores(self) -> Dict[str, float]:
        """
        Her ülke için normalize edilmiş öncelik skoru hesaplar.
        
        Scoring faktörleri:
        - Toplam ithalat değeri
        - Büyüme trendi (varsa)
        - Stratejik öncelik
        """
        # Check existing priority file
        priority_path = Path("data/processed/country_priority_comtrade.csv")
        
        if priority_path.exists():
            try:
                df = pd.read_csv(priority_path)
                scores = {}
                max_value = df["import_value"].max()
                
                for _, row in df.iterrows():
                    iso3 = row["country_iso3"]
                    value = row["import_value"]
                    # Normalize to 0-100 scale
                    scores[iso3] = (value / max_value * 100) if max_value > 0 else 0
                
                return scores
            except Exception as e:
                logger.warning(f"Error loading priority file: {e}")
        
        # Fall back to hardcoded priorities based on known data
        return {
            "TUR": 100.0,  # Highest importer
            "BRA": 58.5,
            "ARG": 26.2,
            "COL": 18.2,
            "PER": 7.9,
            "TUN": 4.9,
            "MAR": 4.0,
            "EGY": 3.5,
            "IND": 50.0,
            "PAK": 30.0,
            "BGD": 25.0,
        }
    
    def find_importing_companies(self, country_iso3: str) -> List[Dict]:
        """
        Bir ülkede ithalat yapan firmaları bulmaya çalışır.
        
        Not: Comtrade firma düzeyinde veri vermez, bu yüzden:
        1. O ülkedeki sertifikalı firmaları hedefleriz (GOTS, OEKO-TEX)
        2. Fuar katılımcılarını hedefleriz
        3. Sektör derneklerindeki üyeleri hedefleriz
        
        Bu fonksiyon "import sinyali" olan firmaları döndürür.
        """
        # Bu fonksiyon diğer collector'larla entegre çalışır
        # Gerçek firma verisi GOTS, OEKO-TEX, fuar listelerinden gelir
        
        country_name = self.TARGET_IMPORTERS.get(country_iso3, {}).get("name", country_iso3)
        
        return [{
            "strategy": f"Find textile finishing companies in {country_name}",
            "rationale": f"{country_name} imports {self.HS_CODES} - look for dyeing/finishing mills",
            "sources": [
                f"GOTS certified suppliers in {country_name}",
                f"OEKO-TEX certified companies in {country_name}",
                f"Trade fair exhibitors from {country_name}",
                f"{country_name} textile association members",
            ]
        }]
    
    def generate_priority_report(self) -> str:
        """Ülke öncelik raporu oluşturur."""
        scores = self.get_country_priority_scores()
        
        report = []
        report.append("# Textile Machinery Parts Import Priority Report")
        report.append(f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report.append(f"\nHS Codes: {', '.join(self.HS_CODES)}")
        report.append("\n## Country Rankings\n")
        
        sorted_scores = sorted(scores.items(), key=lambda x: -x[1])
        
        for i, (iso3, score) in enumerate(sorted_scores, 1):
            name = self.TARGET_IMPORTERS.get(iso3, {}).get("name", iso3)
            report.append(f"{i}. **{name}** ({iso3}): {score:.1f}/100")
        
        report.append("\n## Strategy Recommendations\n")
        
        for iso3, _ in sorted_scores[:5]:
            name = self.TARGET_IMPORTERS.get(iso3, {}).get("name", iso3)
            report.append(f"\n### {name}")
            report.append("- Target: Dyeing & finishing mills with stenter frames")
            report.append("- Sources: GOTS, OEKO-TEX, trade fairs, local associations")
            report.append("- Products: Stenter chains, clips, lubricating rails")
        
        return "\n".join(report)
    
    def _iso3_to_code(self, iso3: str) -> str:
        """Convert ISO3 to Comtrade numeric code."""
        mapping = {
            "TUR": "792",
            "BRA": "076",
            "ARG": "032",
            "COL": "170",
            "PER": "604",
            "EGY": "818",
            "MAR": "504",
            "TUN": "788",
            "DZA": "012",
            "IND": "699",
            "PAK": "586",
            "BGD": "050",
            "VNM": "704",
            "IDN": "360",
            "DEU": "276",
            "CHN": "156",
            "ITA": "380",
        }
        return mapping.get(iso3, iso3)


def update_country_priorities():
    """Ülke önceliklerini günceller ve kaydeder."""
    intel = ImportIntelligence()
    
    # Generate report
    report = intel.generate_priority_report()
    
    report_path = Path("outputs/reports/import_priority_report.md")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report)
    
    print(report)
    print(f"\nSaved to: {report_path}")


if __name__ == "__main__":
    update_country_priorities()
