# Skill: dedupe-entity
## Goal
Merge duplicate companies across sources and name variants.

## Inputs
- staging/leads_enriched.parquet

## Outputs
- outputs/leads_master.csv
- outputs/dedupe_audit.csv (why merged)

## Procedure
1) Normalize names (legal suffix stripping)
2) Blocking keys: country + website_domain OR normalized_name_prefix
3) Splink model, then human review for low-confidence merges
