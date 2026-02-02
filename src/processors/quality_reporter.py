#!/usr/bin/env python3
"""
Quality Report Generator
GPT önerisi: Her run sonunda kalite raporu

Her pipeline run sonunda:
1. 50 rastgele lead seç
2. E1/E2/E3 kanıt skorlarını raporla
3. precision_estimate üret (CUSTOMER oranı)
"""

import csv
import random
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from src.utils.logger import get_logger

logger = get_logger(__name__)


class QualityReporter:
    """
    Pipeline kalite raporu üretici.
    
    Her run sonunda örneklem bazlı kalite analizi yapar.
    """
    
    def __init__(self, output_dir: str = "outputs/reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_report(
        self, 
        leads: List[Dict], 
        sample_size: int = 50,
        run_name: str = None
    ) -> Dict:
        """
        Generate quality report for leads.
        
        Args:
            leads: List of lead dictionaries (with SCE scores if available)
            sample_size: Number of leads to sample for analysis
            run_name: Optional name for this run
            
        Returns:
            Quality report dictionary
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_name = run_name or f"run_{timestamp}"
        
        logger.info("=" * 60)
        logger.info(f"📋 QUALITY REPORT: {run_name}")
        logger.info("=" * 60)
        
        # Basic stats
        total_leads = len(leads)
        
        # Sample for detailed analysis
        sample = random.sample(leads, min(sample_size, total_leads))
        
        # Calculate metrics
        report = {
            "run_name": run_name,
            "timestamp": timestamp,
            "total_leads": total_leads,
            "sample_size": len(sample),
            "metrics": {},
            "distributions": {},
            "sample_analysis": [],
            "recommendations": []
        }
        
        # Website coverage
        with_website = [l for l in leads if self._has_value(l.get("website"))]
        report["metrics"]["website_coverage"] = len(with_website) / max(1, total_leads)
        
        # Email coverage
        with_email = [l for l in leads if self._has_value(l.get("emails"))]
        report["metrics"]["email_coverage"] = len(with_email) / max(1, total_leads)
        
        # Phone coverage
        with_phone = [l for l in leads if self._has_value(l.get("phones"))]
        report["metrics"]["phone_coverage"] = len(with_phone) / max(1, total_leads)
        
        # Role distribution (if available)
        role_dist = {}
        for lead in leads:
            role = lead.get("role", "UNKNOWN")
            role_dist[role] = role_dist.get(role, 0) + 1
        report["distributions"]["role"] = role_dist
        
        # Calculate precision estimate (CUSTOMER ratio)
        customer_count = role_dist.get("CUSTOMER", 0)
        report["metrics"]["precision_estimate"] = customer_count / max(1, total_leads)
        
        # SCE metrics (if available)
        sce_leads = [l for l in leads if "sce_total" in l]
        if sce_leads:
            sales_ready = [l for l in sce_leads if l.get("sce_sales_ready")]
            report["metrics"]["sce_sales_ready_ratio"] = len(sales_ready) / max(1, len(sce_leads))
            
            avg_e1 = sum(l.get("sce_e1", 0) for l in sce_leads) / len(sce_leads)
            avg_e2 = sum(l.get("sce_e2", 0) for l in sce_leads) / len(sce_leads)
            avg_e3 = sum(l.get("sce_e3", 0) for l in sce_leads) / len(sce_leads)
            
            report["metrics"]["avg_e1_score"] = round(avg_e1, 3)
            report["metrics"]["avg_e2_score"] = round(avg_e2, 3)
            report["metrics"]["avg_e3_score"] = round(avg_e3, 3)
            
            # Confidence distribution
            conf_dist = {}
            for lead in sce_leads:
                conf = lead.get("sce_confidence", "unknown")
                conf_dist[conf] = conf_dist.get(conf, 0) + 1
            report["distributions"]["sce_confidence"] = conf_dist
        
        # Country distribution
        country_dist = {}
        for lead in leads:
            country = lead.get("country", "Unknown")
            country_dist[country] = country_dist.get(country, 0) + 1
        report["distributions"]["country"] = dict(sorted(
            country_dist.items(), 
            key=lambda x: -x[1]
        )[:20])  # Top 20
        
        # Source distribution
        source_dist = {}
        for lead in leads:
            source = lead.get("source_name", "Unknown")
            source_dist[source] = source_dist.get(source, 0) + 1
        report["distributions"]["source"] = dict(sorted(
            source_dist.items(),
            key=lambda x: -x[1]
        )[:20])
        
        # Sample analysis
        for lead in sample[:10]:  # Detailed analysis for first 10
            analysis = {
                "company": lead.get("company"),
                "country": lead.get("country"),
                "role": lead.get("role", "UNKNOWN"),
                "has_website": bool(self._has_value(lead.get("website"))),
                "has_email": bool(self._has_value(lead.get("emails"))),
                "sce_score": lead.get("sce_total", 0),
                "sce_sales_ready": lead.get("sce_sales_ready", False),
                "source": lead.get("source_name"),
            }
            report["sample_analysis"].append(analysis)
        
        # Generate recommendations
        report["recommendations"] = self._generate_recommendations(report)
        
        # Save report
        report_path = self.output_dir / f"quality_report_{timestamp}.txt"
        self._save_report(report, report_path)
        
        # Save sample CSV
        sample_path = self.output_dir / f"quality_sample_{timestamp}.csv"
        self._save_sample_csv(sample, sample_path)
        
        # Log summary
        self._log_summary(report)
        
        return report
    
    def _has_value(self, value) -> bool:
        """Check if value is non-empty."""
        if value is None:
            return False
        if isinstance(value, str):
            return value.lower() not in {"", "nan", "none", "[]", "null"}
        if isinstance(value, list):
            return len(value) > 0
        return bool(value)
    
    def _generate_recommendations(self, report: Dict) -> List[str]:
        """Generate recommendations based on metrics."""
        recs = []
        metrics = report["metrics"]
        
        # Website coverage
        if metrics.get("website_coverage", 0) < 0.7:
            recs.append(f"⚠️ Website coverage is low ({metrics['website_coverage']*100:.1f}%). Consider running enrichment queue.")
        
        # Email coverage
        if metrics.get("email_coverage", 0) < 0.5:
            recs.append(f"⚠️ Email coverage is low ({metrics['email_coverage']*100:.1f}%). Enable contact enrichment.")
        
        # Precision
        if metrics.get("precision_estimate", 0) < 0.5:
            recs.append(f"⚠️ Customer precision is low ({metrics['precision_estimate']*100:.1f}%). Review role classifier keywords.")
        
        # SCE sales ready
        if metrics.get("sce_sales_ready_ratio", 0) < 0.3:
            recs.append(f"⚠️ Sales-ready ratio is low ({metrics.get('sce_sales_ready_ratio', 0)*100:.1f}%). Focus on finishing/dyeing sources.")
        
        # Country coverage
        country_dist = report["distributions"].get("country", {})
        sa_countries = ["Brazil", "Argentina", "Colombia", "Peru", "Ecuador", "Chile"]
        sa_total = sum(country_dist.get(c, 0) for c in sa_countries)
        if sa_total < 100:
            recs.append(f"⚠️ South America coverage is low ({sa_total} leads). Run SA collectors.")
        
        # Source diversity
        source_dist = report["distributions"].get("source", {})
        if len(source_dist) < 5:
            recs.append("⚠️ Limited source diversity. Add more collectors.")
        
        if not recs:
            recs.append("✅ All metrics look healthy!")
        
        return recs
    
    def _save_report(self, report: Dict, path: Path):
        """Save text report."""
        with open(path, "w") as f:
            f.write(f"QUALITY REPORT: {report['run_name']}\n")
            f.write(f"Generated: {report['timestamp']}\n")
            f.write("=" * 60 + "\n\n")
            
            f.write("METRICS\n")
            f.write("-" * 40 + "\n")
            for key, value in report["metrics"].items():
                if isinstance(value, float):
                    f.write(f"  {key}: {value*100:.1f}%\n")
                else:
                    f.write(f"  {key}: {value}\n")
            f.write("\n")
            
            f.write("ROLE DISTRIBUTION\n")
            f.write("-" * 40 + "\n")
            for role, count in report["distributions"].get("role", {}).items():
                pct = 100 * count / max(1, report["total_leads"])
                f.write(f"  {role}: {count} ({pct:.1f}%)\n")
            f.write("\n")
            
            f.write("TOP COUNTRIES\n")
            f.write("-" * 40 + "\n")
            for country, count in list(report["distributions"].get("country", {}).items())[:10]:
                f.write(f"  {country}: {count}\n")
            f.write("\n")
            
            f.write("TOP SOURCES\n")
            f.write("-" * 40 + "\n")
            for source, count in list(report["distributions"].get("source", {}).items())[:10]:
                f.write(f"  {source}: {count}\n")
            f.write("\n")
            
            f.write("SAMPLE ANALYSIS\n")
            f.write("-" * 40 + "\n")
            for item in report["sample_analysis"]:
                f.write(f"  {item['company']} ({item['country']})\n")
                f.write(f"    Role: {item['role']}, SCE: {item['sce_score']:.2f}\n")
            f.write("\n")
            
            f.write("RECOMMENDATIONS\n")
            f.write("-" * 40 + "\n")
            for rec in report["recommendations"]:
                f.write(f"  {rec}\n")
        
        logger.info(f"📝 Report saved: {path}")
    
    def _save_sample_csv(self, sample: List[Dict], path: Path):
        """Save sample leads to CSV for manual review."""
        if not sample:
            return
        
        # Select fields for CSV
        fields = [
            "company", "country", "website", "emails", "phones",
            "role", "role_confidence", "sce_total", "sce_sales_ready",
            "sce_confidence", "source_name", "context"
        ]
        
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for lead in sample:
                row = {k: lead.get(k, "") for k in fields}
                # Convert lists to strings
                if isinstance(row.get("emails"), list):
                    row["emails"] = "; ".join(row["emails"])
                if isinstance(row.get("phones"), list):
                    row["phones"] = "; ".join(row["phones"])
                writer.writerow(row)
        
        logger.info(f"📊 Sample CSV saved: {path}")
    
    def _log_summary(self, report: Dict):
        """Log report summary to console."""
        m = report["metrics"]
        
        logger.info(f"\n📊 Quality Summary:")
        logger.info(f"  Total Leads: {report['total_leads']}")
        logger.info(f"  Website Coverage: {m.get('website_coverage', 0)*100:.1f}%")
        logger.info(f"  Email Coverage: {m.get('email_coverage', 0)*100:.1f}%")
        logger.info(f"  Precision Estimate: {m.get('precision_estimate', 0)*100:.1f}%")
        
        if "sce_sales_ready_ratio" in m:
            logger.info(f"  SCE Sales Ready: {m['sce_sales_ready_ratio']*100:.1f}%")
            logger.info(f"  Avg E1: {m.get('avg_e1_score', 0):.3f}")
            logger.info(f"  Avg E2: {m.get('avg_e2_score', 0):.3f}")
            logger.info(f"  Avg E3: {m.get('avg_e3_score', 0):.3f}")
        
        logger.info(f"\n📌 Recommendations:")
        for rec in report["recommendations"]:
            logger.info(f"  {rec}")


# Test
if __name__ == "__main__":
    reporter = QualityReporter()
    
    # Sample leads for testing
    test_leads = [
        {
            "company": "Döhler Textil",
            "country": "Brazil",
            "website": "https://dohler.com.br",
            "emails": ["contato@dohler.com.br"],
            "phones": ["+55 47 3451-0000"],
            "role": "CUSTOMER",
            "role_confidence": 0.85,
            "sce_total": 0.75,
            "sce_e1": 0.6,
            "sce_e2": 0.8,
            "sce_e3": 0.5,
            "sce_sales_ready": True,
            "sce_confidence": "high",
            "source_name": "GOTS Directory"
        },
        {
            "company": "ANJ Máquinas",
            "country": "Brazil",
            "website": "https://anjmaquinas.com.br",
            "emails": [],
            "phones": [],
            "role": "INTERMEDIARY",
            "role_confidence": 0.9,
            "sce_total": 0.2,
            "sce_e1": 0.0,
            "sce_e2": 0.3,
            "sce_e3": 0.1,
            "sce_sales_ready": False,
            "sce_confidence": "low",
            "source_name": "Febratex"
        }
    ] * 25  # Duplicate to get 50 leads
    
    report = reporter.generate_report(test_leads, sample_size=50, run_name="test_run")
    print(f"\nReport generated with {len(report['recommendations'])} recommendations")
