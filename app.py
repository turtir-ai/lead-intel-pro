import argparse
import os
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv

from src.collectors.bettercotton_members import BetterCottonMembers
from src.collectors.competitor_harvester import CompetitorHarvester
from src.collectors.competitor_websearch import CompetitorWebSearch
from src.collectors.egypt_textile_export_council import EgyptTextileExportCouncil
from src.collectors.exhibitor_list import ExhibitorListCollector
from src.collectors.fairs_harvester import FairsHarvester
from src.collectors.gots_directory import GotsCertifiedSuppliers
from src.collectors.texbrasil_companies import TexbrasilCompanies
from src.collectors.trade_fetcher import TradeFetcher
from src.collectors.oekotex_directory import OekoTexDirectory
from src.collectors.bluesign_partners import BluesignPartners
from src.collectors.amith_directory import AmithDirectory
from src.collectors.abit_directory import AbitDirectory
from src.collectors.discovery.brave_search import BraveSearchClient, SourceDiscovery
from src.processors.dedupe import LeadDedupe
from src.processors.enricher import Enricher
from src.processors.entity_extractor import EntityExtractor
from src.processors.exporter import Exporter
from src.processors.pdf_processor import PdfProcessor
from src.processors.scorer import Scorer
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_config(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

def ensure_dirs():
    for path in [
        "data/staging",
        "data/processed",
        "outputs",
        "outputs/crm",
        "outputs/reports",
        "outputs/evidence",
    ]:
        os.makedirs(path, exist_ok=True)

def _target_country_labels(targets):
    labels = []
    for _, data in (targets or {}).get("target_regions", {}).items():
        labels.extend(data.get("labels", []))
    return labels

def _target_country_iso3(targets):
    codes = []
    for _, data in (targets or {}).get("target_regions", {}).items():
        codes.extend(data.get("countries", []))
    return codes

def apply_env(settings):
    api_keys = settings.setdefault("api_keys", {})
    if os.getenv("Brave_API_KEY"):
        api_keys["brave"] = os.getenv("Brave_API_KEY")
    if os.getenv("Comtrade_API_KEY"):
        api_keys["un_comtrade"] = os.getenv("Comtrade_API_KEY")
    return settings

def merge_discovered_sources(sources):
    discovered_path = "data/staging/discovered_sources.yaml"
    if not os.path.exists(discovered_path):
        return sources
    discovered = load_config(discovered_path)
    for key in ["fairs", "directories"]:
        for item in discovered.get(key, []):
            if not any(src.get("url") == item.get("url") for src in sources.get(key, [])):
                sources.setdefault(key, []).append(item)
    return sources

def discover_sources(settings, sources):
    discovery_cfg = sources.get("discovery", {})
    if not discovery_cfg.get("enabled", False):
        logger.info("Discovery disabled.")
        return
    client = BraveSearchClient(settings.get("api_keys", {}).get("brave"), discovery_cfg)
    discoverer = SourceDiscovery(client, disallow_domains=discovery_cfg.get("disallow_domains"))
    results = discoverer.discover(discovery_cfg.get("queries", []), max_results=discovery_cfg.get("max_results", 10))
    os.makedirs("data/staging", exist_ok=True)
    with open("data/staging/discovered_sources.yaml", "w") as f:
        yaml.safe_dump(results, f)
    logger.info("Discovery complete. Saved to data/staging/discovered_sources.yaml")

def harvest(targets, competitors, settings, policies, sources):
    logger.info("Stage: lead-harvest")
    ensure_dirs()
    sources = merge_discovered_sources(sources)
    harvester = CompetitorHarvester(settings=settings, policies=policies)
    fairs_harvester = FairsHarvester(settings=settings, policies=policies)
    pdf_processor = PdfProcessor()
    extractor = EntityExtractor()
    bettercotton = BetterCottonMembers(settings=settings, policies=policies)
    gots = GotsCertifiedSuppliers(settings=sources.get("gots", {}))
    websearch = CompetitorWebSearch(
        settings.get("api_keys", {}).get("brave"), settings=sources.get("discovery", {}), policies=policies
    )
    egypt_tec = EgyptTextileExportCouncil(
        settings.get("api_keys", {}).get("brave"), settings=sources.get("discovery", {}), policies=policies
    )
    exhibitor_lists = ExhibitorListCollector(settings=settings, policies=policies)
    texbrasil = TexbrasilCompanies(settings=settings, policies=policies)

    all_leads = []

    for comp in competitors.get("competitors", []):
        logger.info(f"Harvesting competitor: {comp.get('name', 'unknown')}")
        contents = harvester.harvest_competitor(comp)
        if not comp.get("customer_source", True):
            continue
        for entry in contents:
            companies = extractor.extract_companies(entry.get("content", ""), strict=True)
            for company in companies:
                all_leads.append(
                    {
                        "company": company,
                        "source": entry.get("url"),
                        "context": entry.get("content", "")[:2000],
                        "snippet": entry.get("snippet", ""),
                        "source_type": "competitor",
                        "source_name": comp.get("name", ""),
                        "competitor": comp.get("name", ""),
                    }
                )

    search_cfg = sources.get("competitor_websearch", {})
    web_leads = websearch.harvest(competitors.get("competitors", []), search_cfg)
    all_leads.extend(web_leads)

    fair_leads = fairs_harvester.harvest_sources(sources, targets)
    for lead in fair_leads:
        lead["context"] = (lead.get("context") or "")[:2000]
        all_leads.append(lead)

    bc_cfg = sources.get("bettercotton", {})
    if bc_cfg.get("enabled"):
        bc_countries = _target_country_labels(targets)
        bc_leads = bettercotton.harvest(
            bc_cfg.get("url", "https://bettercotton.org/membership/find-members/"),
            max_pages=bc_cfg.get("max_pages", 10),
            country_filter=bc_countries,
            include_categories=bc_cfg.get("include_categories", []),
            use_xlsx=bc_cfg.get("use_xlsx", False),
            member_list_url=bc_cfg.get("member_list_url"),
        )
        all_leads.extend(bc_leads)

    gots_cfg = sources.get("gots", {})
    if gots_cfg.get("enabled"):
        gots_leads = gots.harvest(_target_country_iso3(targets))
        all_leads.extend(gots_leads)

    tec_cfg = sources.get("egypt_tec", {})
    if tec_cfg.get("enabled"):
        tec_leads = egypt_tec.harvest(
            tec_cfg.get("search_query"),
            max_results=tec_cfg.get("max_results", 30),
        )
        all_leads.extend(tec_leads)

    brazil_cfg = sources.get("brazil", {})
    for key in ("febratex", "febratextil"):
        cfg = brazil_cfg.get(key, {}) if isinstance(brazil_cfg, dict) else {}
        if cfg.get("enabled"):
            leads = exhibitor_lists.harvest(
                cfg.get("url"),
                source_name=cfg.get("name", key),
                country=cfg.get("country", "Brazil"),
            )
            all_leads.extend(leads)

    tex_cfg = brazil_cfg.get("texbrasil", {}) if isinstance(brazil_cfg, dict) else {}
    if tex_cfg.get("enabled"):
        leads = texbrasil.harvest(
            base_url=tex_cfg.get("base_url", "https://texbrasil.com.br"),
            sitemap_url=tex_cfg.get("sitemap_url"),
            max_pages=int(tex_cfg.get("max_pages", 200)),
            country=tex_cfg.get("country", "Brazil"),
        )
        all_leads.extend(leads)

    # OEKO-TEX Directory
    oekotex_cfg = sources.get("oekotex", {})
    if oekotex_cfg.get("enabled"):
        oekotex = OekoTexDirectory(settings=oekotex_cfg)
        oekotex_leads = oekotex.harvest(_target_country_iso3(targets))
        all_leads.extend(oekotex_leads)

    # bluesign Partners
    bluesign_cfg = sources.get("bluesign", {})
    if bluesign_cfg.get("enabled"):
        bluesign = BluesignPartners(settings=bluesign_cfg, policies=policies)
        bluesign_leads = bluesign.harvest(_target_country_iso3(targets))
        all_leads.extend(bluesign_leads)

    # AMITH Morocco
    amith_cfg = sources.get("amith", {})
    if amith_cfg.get("enabled"):
        amith = AmithDirectory(settings=amith_cfg, policies=policies)
        amith_leads = amith.harvest()
        all_leads.extend(amith_leads)

    # ABIT Brazil
    abit_cfg = sources.get("abit", {})
    if abit_cfg.get("enabled"):
        abit = AbitDirectory(settings=abit_cfg, policies=policies)
        abit_leads = abit.harvest()
        all_leads.extend(abit_leads)

    pdf_results = pdf_processor.process_all_pdfs()
    for source, content in pdf_results.items():
        companies = extractor.extract_companies(content, strict=False)
        for company in companies:
            all_leads.append(
                {
                    "company": company,
                    "source": source,
                    "context": content[:2000],
                    "snippet": content[:200],
                    "source_type": "pdf",
                    "source_name": source,
                    "competitor": "PDF_Catalog",
                }
            )

    df = pd.DataFrame(all_leads)
    if not df.empty:
        df = df.drop_duplicates(subset=["company", "source"])
        df.to_csv("data/staging/leads_raw.csv", index=False)
        logger.info(f"Harvested {len(df)} raw leads.")
    else:
        logger.info("No leads harvested.")

def enrich(targets, settings, sources, policies):
    logger.info("Stage: enrich")
    if not os.path.exists("data/staging/leads_raw.csv"):
        logger.warning("No raw leads found.")
        return
    df = pd.read_csv("data/staging/leads_raw.csv")
    leads = df.to_dict(orient="records")
    enricher = Enricher(targets_config=targets, settings=settings, sources=sources, policies=policies)

    enrich_cfg = (settings or {}).get("enrichment", {})
    checkpoint_every = int(enrich_cfg.get("checkpoint_every", 50))
    resume = bool(enrich_cfg.get("resume", False))
    output_path = "data/staging/leads_enriched.csv"

    existing_keys = set()
    if resume and os.path.exists(output_path):
        try:
            existing = pd.read_csv(output_path)
            for _, row in existing.iterrows():
                key = f"{row.get('company','')}|{row.get('source','')}"
                existing_keys.add(key)
            logger.info(f"Resuming enrichment; {len(existing_keys)} leads already processed.")
        except Exception:
            existing_keys = set()
    else:
        if os.path.exists(output_path):
            os.remove(output_path)

    buffer = []
    processed = 0
    for lead in leads:
        key = f"{lead.get('company','')}|{lead.get('source','')}"
        if key in existing_keys:
            continue
        enriched = enricher.enrich_one(lead)
        buffer.append(enriched)
        processed += 1
        if len(buffer) >= checkpoint_every:
            pd.DataFrame(buffer).to_csv(output_path, mode="a", header=not os.path.exists(output_path), index=False)
            buffer = []
            logger.info(f"Enriched {processed} new leads (checkpoint).")

    if buffer:
        pd.DataFrame(buffer).to_csv(output_path, mode="a", header=not os.path.exists(output_path), index=False)

    total = len(existing_keys) + processed
    logger.info(f"Enriched {processed} new leads. Total enriched: {total}.")

def dedupe():
    logger.info("Stage: dedupe")
    if not os.path.exists("data/staging/leads_enriched.csv"):
        logger.warning("No enriched leads found.")
        return
    df = pd.read_csv("data/staging/leads_enriched.csv")
    leads = df.to_dict(orient="records")
    deduper = LeadDedupe()
    merged, audit = deduper.dedupe(leads)
    pd.DataFrame(merged).to_csv("data/processed/leads_master.csv", index=False)
    pd.DataFrame(audit).to_csv("outputs/dedupe_audit.csv", index=False)
    logger.info(f"Dedupe complete: {len(merged)} leads retained.")

def score_and_export(targets, scoring):
    logger.info("Stage: score + export")
    if not os.path.exists("data/processed/leads_master.csv"):
        logger.warning("No master leads found.")
        return
    df = pd.read_csv("data/processed/leads_master.csv")
    trade_path = scoring.get("trade_priority_path", "data/processed/country_priority_comtrade.csv")
    country_priority = {}
    if os.path.exists(trade_path):
        try:
            trade_df = pd.read_csv(trade_path)
            for _, row in trade_df.iterrows():
                country_priority[str(row.get("country_iso3", "")).upper()] = float(
                    row.get("import_value", 0)
                )
        except Exception:
            country_priority = {}

    scorer = Scorer(targets, scoring, country_priority=country_priority)
    scored = [scorer.score_lead(lead) for lead in df.to_dict(orient="records")]

    export_cfg = scoring.get("export", {}) if isinstance(scoring, dict) else {}
    allowed_sources = set(export_cfg.get("allowed_source_types", []) or [])
    min_score = export_cfg.get("min_score")
    exclude_name_keywords = [k.lower() for k in (export_cfg.get("exclude_name_keywords") or [])]
    require_reachability = bool(export_cfg.get("require_reachability", False))
    reachability_level = str(export_cfg.get("require_reachability_level", "any")).lower()

    filtered = scored
    if allowed_sources:
        filtered = [lead for lead in filtered if lead.get("source_type") in allowed_sources]
    if min_score is not None:
        filtered = [lead for lead in filtered if lead.get("score", 0) >= float(min_score)]
    if exclude_name_keywords:
        filtered = [
            lead
            for lead in filtered
            if not any(
                kw in str(lead.get("company", "")).lower() for kw in exclude_name_keywords
            )
        ]

    exporter = Exporter()
    if filtered != scored:
        exporter.export_targets(scored, tag="_all")

    sales_ready = filtered
    needs_enrichment = []
    if require_reachability:
        def _has_reachability(lead):
            has_contact = bool(lead.get("emails") or lead.get("phones"))
            has_website = bool(lead.get("website") or lead.get("websites"))
            if reachability_level == "contact":
                return has_contact
            if reachability_level == "website":
                return has_website
            return has_contact or has_website

        sales_ready = [lead for lead in filtered if _has_reachability(lead)]
        needs_enrichment = [lead for lead in filtered if not _has_reachability(lead)]

    exporter.export_targets(sales_ready)
    if needs_enrichment:
        exporter.export_targets(needs_enrichment, tag="_needs_enrichment")

    # Competitor customers export
    competitor_customers = [
        lead for lead in scored if lead.get("source_type") in ("competitor", "competitor_search")
    ]
    if competitor_customers:
        exporter.export_targets(competitor_customers, tag="_competitor_customers")

    # Lookalike customers based on trade priority countries
    lookalike_cfg = export_cfg
    top_n = int(lookalike_cfg.get("lookalike_top_countries", 5))
    top_countries = [
        row[0]
        for row in sorted(country_priority.items(), key=lambda x: x[1], reverse=True)[:top_n]
    ]
    def _country_iso3_from_lead(lead):
        country = str(lead.get("country", "")).strip().upper()
        if country in top_countries:
            return country
        # try map from labels
        for _, data in targets.get("target_regions", {}).items():
            for iso3, label in zip(data.get("countries", []), data.get("labels", [])):
                if str(label).strip().lower() == str(lead.get("country", "")).strip().lower():
                    return iso3
        return ""

    lookalikes = [
        lead
        for lead in scored
        if lead.get("source_type") in ("gots", "bettercotton")
        and _country_iso3_from_lead(lead) in top_countries
    ]
    if lookalikes:
        exporter.export_targets(lookalikes, tag="_lookalikes")

def trade_rank(targets, settings):
    logger.info("Stage: trade-prioritize")
    fetcher = TradeFetcher(settings)
    hs_codes = [item["code"] for item in targets.get("hs_codes", [])]
    comtrade = fetcher.fetch_comtrade_data(hs_codes, targets.get("target_regions", {}))
    eurostat = fetcher.fetch_eurostat_data(hs_codes)
    if comtrade.get("rankings"):
        df = pd.DataFrame(comtrade["rankings"])
        trade_cfg = settings.get("trade", {})
        df["period"] = trade_cfg.get("period", "2022")
        df["flow"] = "Import"
        df["partner"] = "World"
        df["cmd_codes"] = ",".join([str(code) for code in hs_codes])
        df.to_csv("data/processed/country_priority_comtrade.csv", index=False)
    if eurostat.get("rankings"):
        pd.DataFrame(eurostat["rankings"]).to_csv(
            "data/processed/country_priority_comext.csv", index=False
        )

def main():
    parser = argparse.ArgumentParser(description="Lead Intelligence Pipeline v2 - Skill Based")
    parser.add_argument(
        "stage",
        choices=["discover", "harvest", "enrich", "dedupe", "score", "trade-rank", "ui", "all"],
        help="Pipeline stage to run",
    )
    
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    targets = load_config("config/targets.yaml")
    competitors = load_config("config/competitors.yaml")
    settings = load_config("config/settings.yaml")
    policies = load_config("config/policies.yaml")
    sources = load_config("config/sources.yaml")
    scoring = load_config("config/scoring.yaml")
    settings = apply_env(settings)
    sources = merge_discovered_sources(sources)
    ensure_dirs()

    if args.stage in ("discover", "all"):
        discover_sources(settings, sources)
    if args.stage in ("harvest", "all"):
        harvest(targets, competitors, settings, policies, sources)
    if args.stage in ("enrich", "all"):
        enrich(targets, settings, sources, policies)
    if args.stage in ("dedupe", "all"):
        dedupe()
    if args.stage in ("score", "all"):
        score_and_export(targets, scoring)
    if args.stage in ("trade-rank", "all"):
        trade_rank(targets, settings)
    if args.stage == "ui":
        logger.info("Launching Review UI...")
        os.system("streamlit run src/ui/app_streamlit.py")

if __name__ == "__main__":
    main()
