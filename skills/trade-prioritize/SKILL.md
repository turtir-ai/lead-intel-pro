# Skill: trade-prioritize
## Goal
Rank target countries for HS 8448xx / 845190 using UN Comtrade + Eurostat/Comext signals.

## Inputs
- config/targets.yaml (HS codes, target regions, time window)
- Optional: MCP endpoint for eurostat-mcp

## Outputs
- outputs/country_rank.csv
- outputs/country_rank.md (explain ranking + key stats)
- logs/trade_prio.log

## Hard Constraints
- No paid data sources unless explicitly enabled.
- Record all query parameters for reproducibility.

## Procedure
1) Pull Comtrade imports for target HS into target countries.
2) If Eurostat available: pull Comext mirror for EU exports into same countries.
3) Compute:
   - import_value_latest, CAGR, volatility
   - supplier_count, HHI concentration
4) Score formula from config/scoring.yaml
5) Export CSV + a short narrative summary.

## Verification Checklist
- Missing data handled (nulls)
- Same period window applied across sources
- Top10 countries have evidence rows with query params
