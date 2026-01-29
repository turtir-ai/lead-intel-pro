# Skill: entity-quality-gate

## Goal
Filter out non-company entities (article titles, process names, marketplace listings) before they enter CRM.

## Problem Solved
Precision search and web scraping often extract:
- Article/news headlines ("Sustainable Heat-setting Process")
- Marketplace listings ("Monforts Stenter Machine Product on Alibaba")
- Generic terms ("Manufacturer", "Textile Finishing")
- Academic papers ("Research on Dyeing Methods")

These pollute the lead database and inflate metrics.

## Inputs
- Raw leads from any collector
- `config/entity_blacklist.yaml` - disallowed patterns/domains

## Outputs
- Filtered leads with `entity_quality` field (A/B/C/REJECT)
- Rejected entities logged to `logs/entity_rejected.log`

## Quality Levels
| Grade | Criteria | Action |
|-------|----------|--------|
| A | Company suffix (GmbH/Ltd/SA/Inc) + Website found | CRM ready |
| B | 2+ words + Domain match OR directory source | Needs verification |
| C | No suffix, no website, but from fair/GOTS | Low confidence |
| REJECT | Single word, generic term, marketplace, academic | Drop |

## Implementation

### Rejection Rules (immediate drop)
1. **Single word + generic term**: "Manufacturer", "Textile", "Finishing", "Stand"
2. **Marketplace domains**: alibaba.com, indiamart.com, made-in-china.com, globalsources.com
3. **Academic domains**: sciencedirect.com, researchgate.net, academia.edu, springer.com
4. **News/blog patterns**: "... announces", "... reveals", "How to...", "What is..."
5. **Process/technology phrases**: Contains "process", "method", "technology" without company name

### Upgrade Rules (boost quality)
1. Company suffix present → Grade A candidate
2. Official website found → +1 grade
3. From official directory (GOTS/OEKO-TEX/association) → +1 grade
4. Has contact info (email/phone) → +1 grade

## Verification Checklist
- [ ] No rejected entities in final CRM output
- [ ] Entity quality distribution logged
- [ ] Rejection reasons stored for audit

## Dependencies
- `src/processors/entity_quality_gate.py` - Main implementation
- `config/entity_blacklist.yaml` - Patterns and domains to reject
