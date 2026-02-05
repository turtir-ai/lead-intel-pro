---
name: leadintel-osint
description: LeadIntel Pro customer validation, evidence collection, and Tier classification per V9 Sniper standard
---

# LeadIntel OSINT Skill

## Purpose
Validate B2B leads as textile finishing mill prospects using dual evidence (K1 external + K2 internal) and assign actionable tier classification.

## Core Rules

### 1. Mill Definition
**A "Prospect Mill" must:**
- Operate finishing/dyeing/processing equipment (NOT just trade/distribute)
- Serve the apparel or technical textile industry
- Have operational capacity (not startup-stage only)

**Machines in scope:** Stenter frames, tenter frames, ramöz, dyeing vats, mercerizing lines, coating applicators, heat-setting ovens

### 2. Evidence Standards (K1 + K2)

| Evidence Type | Source | Strength | Example |
|---|---|---|---|
| K1: OEM Reference | Monforts/Brückner/Krantz customer page | ⭐⭐⭐ Highest | "Acme Mills - Turkey - stenter reference" |
| K1: PDF Exhibitor | Fair catalogs (ITMA, Texfair, Colombiatex) | ⭐⭐⭐ High | PDF: "ITMA 2022 Exhibitors: Acme Textiles - Dyeing" |
| K1: Job Posting | LinkedIn/Indeed "Stenter Operator" | ⭐⭐⭐ High | "Hiring: Stenter Machine Operator - Acme Mills" |
| K1: Trade Import | HS 8451.90 import records (Comtrade) | ⭐⭐ Medium | "Importer: Acme Mills Ltd. - Parts value $200K" |
| K2: Website - Production | /capabilities, /technology, /production | ⭐⭐⭐ High | "Our Monforts stenter line..." on official site |
| K2: Website - Keywords | Mentions stenter, heat-setting, finishing | ⭐⭐ Medium | "Finishing services for synthetic fabrics" in homepage |
| K1: Press Release | News: factory opening, new equipment | ⭐⭐ Medium | "Acme opens €2M Brückner dyehouse" |

**Tier-1 Golden:** BOTH K1 (≥1) AND K2 (≥1)  
**Tier-2 Promising:** K1 only OR K2 only  
**Tier-3 Research:** No evidence yet, or contradictory signals

### 3. Negative Filters (Auto-Reject)
- Company name contains: Brand, Retail, Trading, Distribution, Group, Association, Event
- Website is generic listing (*.org.br, global-trace-base.org, linkedin.com only)
- Contact role is sales/marketing for unrelated product
- No operational mill found after 20-min research

### 4. Classification Logic

```python
def classify_lead(k1_count, k2_count, negative_signal, oem_brand):
    if negative_signal:
        return "❌ REJECT"
    if k1_count >= 1 and k2_count >= 1:
        return "✅ TIER-1 GOLDEN"
    if k1_count >= 1 or k2_count >= 1:
        return "⚠️ TIER-2 PROMISING"
    return "🔍 TIER-3 RESEARCH"
```

## Skill Application Examples

### Example 1: Clear Fit
**Input:** Acme Textile Mills, Turkey, no website provided

**Process:**
1. Search: "Acme Textile Mills Turkey dyeing" → Find acme-textile.com.tr
2. Check Monforts reference list → Found "Acme Mills"
3. Visit acme-textile.com.tr/capabilities → "Monforts stenter, heat-setting, 50 tons/day"
4. Search LinkedIn → Job post: "Stenter Operator – Acme Mills (posted 2 months ago)"

**Output:**
```json
{
  "classification": "✅ TIER-1 GOLDEN",
  "k1_evidence": ["Monforts reference list", "LinkedIn job posting"],
  "k2_evidence": ["Website /capabilities page"],
  "hs_code": "8451.90",
  "sales_angle": "Monforts chain parts + spindle nuts for 50-ton stenter",
  "next_action": "Prioritize outreach; reference Monforts customer base"
}
```

### Example 2: Uncertain Fit
**Input:** TechFab Solutions, Pakistan

**Process:**
1. Website: techfab.pk → "Textile solutions provider"
2. No stenter/finishing keywords visible
3. LinkedIn: "B2B platform for textile buyers and sellers"
4. NOT found in OEM reference lists
5. No job postings for stenter operators

**Output:**
```json
{
  "classification": "⚠️ TIER-2 PROMISING (Low)",
  "reason": "Website vague; no K1 evidence yet",
  "next_action": "Request more details: Do you operate finishing equipment? What brands?",
  "confidence": "MEDIUM"
}
```

### Example 3: Clear Reject
**Input:** Istanbul Fashion Group

**Process:**
1. Website: "Fashion brand showcasing Turkish textiles"
2. No production capability mentioned
3. Retail/distribution focus
4. NOT in OEM reference lists

**Output:**
```json
{
  "classification": "❌ REJECT",
  "reason": "Retail brand, not mill operator",
  "confidence": "HIGH"
}
```

## When Applying This Skill

Use this skill when:
- ✅ Validating a CSV of new leads from fairs/trade data
- ✅ Double-checking a lead's "Tier" before CRM entry
- ✅ Scoring HS code fit (8451.90 vs fallback)
- ✅ Generating "Why this customer?" narratives for sales

Do NOT use if:
- ❌ Lead has clear legal/compliance issues (IP disputes, sanctions)
- ❌ User is asking about proprietary competitor data
- ❌ Scraping LinkedIn (ToS violation)

## Output Checklist
- [ ] Company name + country confirmed
- [ ] Website verified (or flagged as missing)
- [ ] K1 evidence cited with URL
- [ ] K2 evidence cited with excerpt + keyword list
- [ ] Tier classification assigned (✅ / ⚠️ / ❌)
- [ ] HS code prediction (8451.90 or fallback reasoning)
- [ ] Next sales action stated
- [ ] Confidence level (HIGH/MEDIUM/LOW)

---
**Standard:** LeadIntel Pro V9 Sniper (K1+K2 Dual Evidence)  
**Skill Version:** 1.0  
**Last Updated:** 2026-02-05
