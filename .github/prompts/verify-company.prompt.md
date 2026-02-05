---
name: verify-company
description: Verify if a company is a finishing/dyeing mill, likely stenter user, and collect evidence per LeadIntel standards
agent: ask
argument-hint: "company=<name> country=<optional> website=<optional>"
---

# Verify Company as Sales-Ready Lead

## Task
Validate company against LeadIntel Pro Tier-1 criteria and generate actionable go/no-go decision.

## Input
- **company:** Company name or partial
- **country:** Country (optional, for region-specific context)
- **website:** Known website (optional; will search if missing)

## Validation Checklist

### 1. Identity Confirmation
- [ ] Confirm legal name + common aliases
- [ ] Verify domain ownership (About/Contact pages, LinkedIn if public)
- [ ] Check: Is this a real operational mill or a shell/distributor?

### 2. Evidence Collection (K1 + K2)
Search for BOTH categories:

**K1 - External Proof (one required for Tier-2, two for Tier-1):**
- OEM customer list (Monforts, Brückner, Krantz reference pages)
- PDF exhibitor at textile fair (ITMA, Texfair, Colombiatex)
- Job posting mentioning "stenter operator", "finishing engineer"
- Press release / news about textile expansion
- Trade import data (HS 8451.90 buyer history)

**K2 - Internal Proof (required for Tier-1):**
- Official production/technology page describing finishing process
- Visible mention: stenter, tenter frame, ramöz, heat-setting, dyeing, sanforizing
- Equipment list or capabilities page
- Certifications (GOTS, Bluesign, OEKO-TEX) → implies finishing

### 3. Classification

**✅ SALES-READY (Tier-1):**
- Finishing/dyeing/processing activity is clear (K2 on site)
- At least one external proof (K1) confirms this
- NO negative signals (see below)

**⚠️ SUSPECT (Tier-2):**
- Site suggests textile operations but K2 evidence weak
- OR: Only K1 evidence, no K2 confirmation
- Recommendation: low-touch outreach or monitor

**❌ NOT A FIT (Tier-3 or Reject):**
- Brand/retail only (sells textiles, doesn't process)
- Supplier/distributor (sells parts, not services)
- Association/event/news portal (no operations)
- No evidence of finishing/dyeing/heat-setting activity

### 4. Negative Signals (Auto-Reject if present)
- Website is *.org.br generic listing, or global-trace-base, or LinkedIn-only
- Company name contains: "Brand", "Retail", "Trading", "Import-Export Group", "Association", "Event", "News"
- Contact info goes to a dernek/association, not company HQ

## Output Format

```json
{
  "company": "Acme Textile Mills Ltd.",
  "country": "Turkey",
  "classification": "✅ SALES-READY",
  "confidence": "HIGH",
  
  "website": "https://www.acme-textile.com.tr",
  "evidence_k1": [
    {
      "type": "oem_reference",
      "source": "https://www.monforts.de/en/references/",
      "snippet": "Acme Textile Mills - Turkey"
    }
  ],
  "evidence_k2": [
    {
      "type": "production_page",
      "url": "https://www.acme-textile.com.tr/capabilities",
      "keywords": ["stenter frame", "heat setting", "finishing"],
      "excerpt": "Our Monforts stenter line handles polyester blend fabrics..."
    }
  ],
  
  "hs_code_primary": "8451.90",
  "hs_code_reasoning": "Stenter parts (chain links, clamps) for Monforts equipment",
  
  "next_action": "Sales: Lookup Monforts service intervals for Acme; propose spindle nut inventory.",
  "red_flags": "None"
}
```

## Follow-up Questions for Tier-2 / Uncertain Cases
1. Can you find a job posting for a stenter operator or finishing engineer at this company?
2. Does the company appear in any OEM customer lists (Brückner, Monforts, Krantz)?
3. Is there a LinkedIn company page showing "Textile Finishing" or "Dyeing" in the industry field?

---
**Standard:** LeadIntel Pro V9 - K1+K2 Dual Evidence
**Updated:** 2026-02-05
