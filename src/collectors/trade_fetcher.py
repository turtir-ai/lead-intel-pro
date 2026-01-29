import os
import time

import pandas as pd
import requests

from src.utils.cache import load_json_cache, save_json_cache
from src.utils.logger import get_logger

logger = get_logger(__name__)


COMTRADE_BASE = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
COUNTRY_REF_URL = "https://comtradeapi.un.org/files/v1/app/reference/country_area_code_iso.json"


class TradeFetcher:
    def __init__(self, settings=None):
        settings = settings or {}
        self.api_keys = settings.get("api_keys", {})
        self.trade_cfg = settings.get("trade", {})
        self.timeout = self.trade_cfg.get("timeout", 30)
        self.rate_delay = float(self.trade_cfg.get("rate_delay", 1.0))
        self.session = requests.Session()
        self._country_map = None

    def fetch_comtrade_data(self, hs_codes, regions, local_path="data/inputs/trade_comtrade.csv"):
        logger.info(f"Fetching Comtrade data for HS codes: {hs_codes}")
        if os.path.exists(local_path):
            logger.info(f"Using local trade file: {local_path}")
            df = pd.read_csv(local_path)
            return self._rank_from_local(df, hs_codes)

        api_key = self.api_keys.get("un_comtrade") or self.api_keys.get("comtrade")
        if not api_key:
            logger.info("Comtrade API key missing; skipping API fetch.")
            return {"rankings": []}

        period = str(self.trade_cfg.get("period", "2022"))
        country_codes = self._target_country_codes(regions)
        if not country_codes:
            logger.info("No target country codes available for trade ranking.")
            return {"rankings": []}

        rankings = {}
        hs_list = [self._hs6(code) for code in hs_codes if self._hs6(code)]
        for iso3, reporter_code in country_codes.items():
            total_value = 0
            for hs in hs_list:
                cache_key = f"comtrade:{iso3}:{hs}:{period}"
                data = load_json_cache(cache_key)
                if data is None:
                    params = {
                        "reportercode": reporter_code,
                        "partnerCode": 0,
                        "flowCode": "M",
                        "period": period,
                        "cmdCode": hs,
                        "maxRecords": 500,
                        "format": "JSON",
                        "includeDesc": True,
                        "subscription-key": api_key,
                    }
                    data = self._request_comtrade(params)
                    if not data:
                        continue
                    save_json_cache(cache_key, data)
                    time.sleep(self.rate_delay)

                for row in data.get("data", []) or []:
                    value = row.get("primaryValue") or row.get("cifvalue") or row.get("fobvalue") or 0
                    try:
                        total_value += float(value)
                    except Exception:
                        continue

            rankings[iso3] = {
                "country_iso3": iso3,
                "import_value": round(total_value, 2),
            }

        ranked = sorted(rankings.values(), key=lambda x: x["import_value"], reverse=True)
        return {"rankings": ranked}

    def fetch_eurostat_data(self, hs_codes, local_path="data/inputs/trade_comext.csv"):
        logger.info(f"Fetching Eurostat data for HS codes: {hs_codes}")
        if os.path.exists(local_path):
            logger.info(f"Using local trade file: {local_path}")
            df = pd.read_csv(local_path)
            return self._rank_from_local(df, hs_codes)
        logger.info("No local trade file found; API integration not configured.")
        return {"rankings": []}

    def _rank_from_local(self, df, hs_codes):
        if df.empty:
            return {"rankings": []}
        if "hs_code" in df.columns:
            df = df[df["hs_code"].astype(str).isin([str(code) for code in hs_codes])]
        if "import_value" in df.columns and "country" in df.columns:
            grouped = df.groupby("country")["import_value"].sum().reset_index()
            grouped = grouped.sort_values("import_value", ascending=False)
            return {"rankings": grouped.to_dict(orient="records")}
        return {"rankings": []}

    def _hs6(self, code):
        if not code:
            return ""
        digits = "".join([c for c in str(code) if c.isdigit()])
        return digits[:6] if len(digits) >= 6 else digits

    def _target_country_codes(self, regions):
        if self._country_map is None:
            self._country_map = self._load_country_map()
        country_codes = {}
        for _, data in (regions or {}).items():
            for iso3 in data.get("countries", []):
                if iso3 in self._country_map:
                    country_codes[iso3] = self._country_map[iso3]
        return country_codes

    def _load_country_map(self):
        cache_key = "comtrade:country_codes"
        data = load_json_cache(cache_key)
        if data is None:
            try:
                resp = self.session.get(COUNTRY_REF_URL, timeout=self.timeout)
                if resp.status_code != 200:
                    logger.error(f"Comtrade country reference error {resp.status_code}")
                    return {}
                data = resp.json()
                save_json_cache(cache_key, data)
            except Exception as exc:
                logger.error(f"Comtrade country reference fetch failed: {exc}")
                return {}
        mapping = {}
        for row in data.get("results", []):
            iso3 = row.get("iso3")
            code = row.get("country_area_code")
            if iso3 and code:
                mapping[iso3] = int(code)
        return mapping

    def _request_comtrade(self, params, retries=3):
        for _ in range(retries):
            try:
                resp = self.session.get(COMTRADE_BASE, params=params, timeout=self.timeout)
            except Exception as exc:
                logger.error(f"Comtrade request failed: {exc}")
                return None
            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception:
                    logger.error("Comtrade response JSON parse failed.")
                    return None
            if resp.status_code == 429:
                wait = self._retry_after_seconds(resp.text)
                logger.warning(f"Comtrade rate limit hit; sleeping {wait} seconds.")
                time.sleep(wait)
                continue
            logger.error(f"Comtrade API error {resp.status_code}: {resp.text[:200]}")
            return None
        return None

    def _retry_after_seconds(self, text):
        try:
            # message: "Try again in 27 seconds."
            for token in text.split():
                if token.isdigit():
                    return int(token)
        except Exception:
            pass
        return 30
