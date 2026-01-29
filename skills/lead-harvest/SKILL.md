# Skill: lead-harvest
## Goal
Harvest raw lead candidates from competitor references + industry directories without violating ToS.

## Inputs
- config/competitors.yaml
- config/sources.yaml
- policies.yaml

## Outputs
- staging/leads_raw.parquet
- logs/harvest.log

## Procedure
1) For each competitor domain:
   - discover pages with keywords: kunden|referenzen|case study|success story
   - crawl depth=2 (configurable)
2) Extract candidate entities:
   - company name, location, website (if present), evidence_url, snippet
3) For each directory/fair source:
   - run the corresponding spider / extractor
4) Store each lead with:
   - evidence_url, fetched_at, content_hash
