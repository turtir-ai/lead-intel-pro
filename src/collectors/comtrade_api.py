#!/usr/bin/env python3
"""
UN Comtrade API Integration

Fetches textile machinery spare parts import data by country.
Uses HS codes: 844832, 844839, 845190

This helps prioritize countries by their import volume.
"""

import os
import time
import json
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)


class ComtradeAPI:
    """
    UN Comtrade API client for trade statistics.
    
    API docs: https://comtradeapi.un.org/
    Free tier: 500 requests/day
    """
    
    BASE_URL = "https://comtradeapi.un.org/data/v1/get/C/A"
    
    # Textile machinery spare parts HS codes
    HS_CODES = ["844832", "844839", "845190"]
    
    # Target countries (ISO3)
    TARGET_COUNTRIES = [
        "TUR",  # Turkey
        "BRA",  # Brazil
        "ARG",  # Argentina
        "COL",  # Colombia
        "PER",  # Peru
        "EGY",  # Egypt
        "MAR",  # Morocco
        "TUN",  # Tunisia
        "DZA",  # Algeria
        "IND",  # India (bonus)
        "PAK",  # Pakistan (bonus)
        "BGD",  # Bangladesh (bonus)
    ]
    
    def __init__(self, api_key=None):
        self.api_key = api_key or os.environ.get("COMTRADE_API_KEY") or os.environ.get("Comtrade_API_KEY")
        self.cache_path = Path("data/processed/country_priority_comtrade.csv")
        
    def get_country_priorities(self, force_refresh=False):
        """
        Get country priorities based on import values.
        Returns dict: {ISO3: import_value}
        """
        # Check cache first
        if not force_refresh and self.cache_path.exists():
            try:
                df = pd.read_csv(self.cache_path)
                priorities = dict(zip(df["country_iso3"], df["import_value"]))
                logger.info(f"Comtrade: loaded {len(priorities)} countries from cache")
                return priorities
            except Exception as e:
                logger.warning(f"Cache read error: {e}")
        
        # Fetch from API
        if not self.api_key:
            logger.warning("Comtrade API key not set, using cached data")
            return self._get_cached_priorities()
        
        priorities = self._fetch_from_api()
        
        # Save to cache
        if priorities:
            self._save_cache(priorities)
        
        return priorities
    
    def _fetch_from_api(self):
        """Fetch import data from Comtrade API."""
        priorities = {}
        
        for country in self.TARGET_COUNTRIES:
            try:
                # Build query
                params = {
                    "reporterCode": self._iso3_to_comtrade(country),
                    "period": "2023,2022",  # Last 2 years
                    "partnerCode": "0",  # World
                    "flowCode": "M",  # Imports
                    "cmdCode": ",".join(self.HS_CODES),
                }
                
                headers = {
                    "Ocp-Apim-Subscription-Key": self.api_key
                }
                
                resp = requests.get(self.BASE_URL, params=params, headers=headers, timeout=30)
                
                if resp.status_code == 429:
                    logger.warning("Comtrade rate limit, waiting...")
                    time.sleep(10)
                    continue
                
                if resp.status_code != 200:
                    logger.warning(f"Comtrade API error for {country}: {resp.status_code}")
                    continue
                
                data = resp.json()
                
                # Sum up import values
                total_value = 0
                for record in data.get("data", []):
                    value = record.get("primaryValue", 0) or 0
                    total_value += float(value)
                
                priorities[country] = total_value
                logger.debug(f"{country}: ${total_value:,.0f}")
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.warning(f"Error fetching {country}: {e}")
                continue
        
        return priorities
    
    def _get_cached_priorities(self):
        """Get priorities from existing cache."""
        if self.cache_path.exists():
            try:
                df = pd.read_csv(self.cache_path)
                return dict(zip(df["country_iso3"], df["import_value"]))
            except:
                pass
        
        # Default priorities if no cache
        return {
            "TUR": 89500000,
            "BRA": 52393540,
            "ARG": 23446139,
            "COL": 16248487,
            "PER": 7072511,
            "TUN": 4404275,
            "MAR": 3611485,
            "EGY": 3123395,
            "DZA": 0,
        }
    
    def _save_cache(self, priorities):
        """Save priorities to cache."""
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            
            rows = []
            for iso3, value in sorted(priorities.items(), key=lambda x: -x[1]):
                rows.append({
                    "country_iso3": iso3,
                    "import_value": value,
                    "period": "2022",
                    "flow": "Import",
                    "partner": "World",
                    "cmd_codes": ",".join(self.HS_CODES),
                })
            
            df = pd.DataFrame(rows)
            df.to_csv(self.cache_path, index=False)
            logger.info(f"Saved {len(rows)} country priorities to {self.cache_path}")
            
        except Exception as e:
            logger.warning(f"Error saving cache: {e}")
    
    def _iso3_to_comtrade(self, iso3):
        """Convert ISO3 to Comtrade country code."""
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
        }
        return mapping.get(iso3, iso3)


def update_priorities():
    """Standalone function to update country priorities."""
    api = ComtradeAPI()
    priorities = api.get_country_priorities(force_refresh=True)
    
    print("\nCountry Import Priorities (Textile Machinery Parts):")
    print("=" * 50)
    for country, value in sorted(priorities.items(), key=lambda x: -x[1]):
        print(f"  {country}: ${value:,.0f}")
    
    return priorities


if __name__ == "__main__":
    update_priorities()
