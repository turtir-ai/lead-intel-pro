# Skill: global-customer-finder

## Goal
Find REAL customers for stenter spare parts globally by combining all intelligence sources with precision filtering.

## Problem Statement
We manufacture plastic injection spare parts for stenter/ramöz machines:
- Gleitstein (Guide Block)
- Gleitleiste (Guide Rail)  
- Kluppen (Clips)
- Buchse (Bushing)
- Spindel Mutter (Spindle Nut)
- Öffner Segment, Nadelleiste, etc.

Our customers are **textile finishing mills** that operate:
- Brückner (Power-Frame, SUPRA)
- Monforts (Montex)
- Krantz, Artos, Santex

**NOT our customers:**
- Yarn spinners (no stenters)
- Garment factories (no stenters)
- Machinery manufacturers (competitors)
- Trading companies (not end users)

## Intelligence Sources (Priority Order)

### Tier 1: High Trust (Direct Customer Evidence)
| Source | Confidence | Method |
|--------|------------|--------|
| OEM References | 95% | Scrape Brückner/Monforts news pages |
| Known Manufacturers | 100% | Curated list in targets.yaml |
| Association Members | 90% | Official textile association directories |
| Facility Databases | 85% | Open Supply Hub API verification |

### Tier 2: Medium Trust (Needs Verification)
| Source | Confidence | Method |
|--------|------------|--------|
| Trade Fair Exhibitors | 70% | Filter by "finishing" category |
| GOTS/OEKO-TEX | 60% | Filter by process scope |
| Brave Search | 50% | Precision queries + validation |

### Tier 3: Low Trust (Heavy Filtering Required)
| Source | Confidence | Method |
|--------|------------|--------|
| General Web Search | 30% | Multiple validation steps |
| PDF Extraction | 40% | Entity quality gate |

## Qualification Pipeline

```
Raw Leads (any source)
       │
       ▼
┌──────────────────────┐
│ Entity Quality Gate  │  ← Reject articles, marketplaces, generic terms
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Customer Qualifier   │  ← Check for finishing/stenter keywords
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Facility Verification│  ← Optional: Cross-check with OS Hub
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Contact Enrichment   │  ← Find website, email, phone
└──────────┬───────────┘
           │
           ▼
    Qualified Customers
```

## Target Markets (Comtrade Priority)

### Priority 1: High Import Volume + Growth
- Brazil (24 leads)
- Turkey (14 leads)
- Egypt (14 leads)
- Pakistan (13 leads)

### Priority 2: Growing Markets
- India, Argentina, Peru, Colombia, Morocco, Bangladesh

### Priority 3: Established Markets
- Germany, Italy, Spain, Portugal, USA

## Output Requirements

Each qualified customer must have:
1. **Company name** - Real entity, validated
2. **Country** - Verified
3. **Entity quality grade** - A/B/C
4. **Evidence** - URL + snippet proving finishing operation
5. **Equipment signal** - OEM brand mentioned (if available)
6. **Contact info** - At least website OR email

## Key Metrics

| Metric | Target |
|--------|--------|
| Precision (real customers) | > 80% |
| Coverage (target countries) | 25+ |
| High-confidence leads | > 200 |
| Contact rate | > 60% |

## Implementation Files

- `src/collectors/global_customer_finder.py` - Main orchestration
- `src/processors/entity_quality_gate.py` - Quality filtering
- `src/processors/customer_qualifier.py` - Business logic filtering
- `src/collectors/oem_reference_extractor.py` - OEM intel
- `config/sources.yaml` - All source configurations

## Verification Checklist
- [ ] No marketplace links in output
- [ ] No article headlines as company names
- [ ] Each lead has evidence_url
- [ ] Entity quality distribution: A > 30%, B > 40%, REJECT < 20%
- [ ] Geographic coverage matches target markets
- [ ] OEM customers clearly flagged
