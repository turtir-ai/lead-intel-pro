#!/usr/bin/env python3
"""Golden record exporter (sales-friendly output)."""

from typing import Dict


class GoldenExporter:
    def export_golden_record(self, lead: Dict) -> Dict:
        return {
            "company": lead.get("company"),
            "country": lead.get("country"),
            "website": lead.get("website"),
            "why_customer": self._generate_why_customer(lead),
            "evidence_url": (lead.get("k1_details") or [{}])[0].get("url", ""),
            "evidence_snippet": (lead.get("evidence_snippets") or [""])[0][:200],
            "target_product": lead.get("target_product", ""),
            "hs_code": "8451.90",
            "best_email": self._get_best_email(lead),
            "phone": (lead.get("phones_extracted") or [""])[0],
            "linkedin_xray": lead.get("linkedin_xray"),
            "contact_person": lead.get("contact_person"),
            "contact_role": lead.get("contact_role"),
            "sales_angle": self._suggest_sales_angle(lead),
            "final_score": lead.get("final_score"),
            "evidence_score": lead.get("evidence_score"),
            "contactability_score": lead.get("contactability_score"),
            "tier": lead.get("tier"),
            "is_golden": lead.get("is_golden"),
            "source": lead.get("source"),
        }

    def _generate_why_customer(self, lead: Dict) -> str:
        oem = lead.get("oem_brand", "")
        if oem:
            return f"{oem} referans listesinde görülüyor, stenter kullanımı doğrulanmış."
        if lead.get("evidence_type") == "job_posting":
            return "Stenter operatörü arıyor, aktif üretim tesisi."
        signals = lead.get("stenter_signals", []) or lead.get("finishing_signals", [])
        if signals:
            return f"Web sitesinde {', '.join(signals[:2])} terimleri geçiyor."
        return "Tekstil terbiye tesisi olarak tespit edildi."

    def _suggest_sales_angle(self, lead: Dict) -> str:
        oem = lead.get("oem_brand", "")
        if oem == "Monforts":
            return "Monforts zincir baklası ve gleitstein uyumluluğu vurgula"
        if oem == "Brückner":
            return "Brückner kluppen ve segment parçaları için teklif hazırla"
        # Guard against NaN values from pandas
        country_raw = lead.get("country", "")
        country = str(country_raw).lower() if country_raw and not (isinstance(country_raw, float) and country_raw != country_raw) else ""
        if country in ["brazil", "argentina"]:
            return "Hızlı teslimat ve yerel stok avantajı vurgula"
        if country in ["turkey", "türkiye"]:
            return "Türkçe destek ve hızlı servis vurgula"
        return "Orijinal kalitede, rekabetçi fiyat ve hızlı teslimat"

    def _get_best_email(self, lead: Dict) -> str:
        emails = lead.get("emails_extracted", []) or []
        return emails[0] if emails else ""
