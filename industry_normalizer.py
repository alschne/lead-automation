"""
industry_normalizer.py

Converts raw industry strings (LinkedIn taxonomy, scraped text, or free-form input)
into plain conversational lowercase labels — the same format used in the leads sheet.

Examples:
    "Information Technology and Services" → "it services"
    "Computer & Network Security"         → "cybersecurity"
    "Graphic Design"                      → "graphic design"
    "Public Relations and Communications" → "public relations"

Priority order:
    1. Exact match (case-insensitive)
    2. Substring / keyword match
    3. Gemini fallback for anything unrecognized
    4. Return raw lowercased input if Gemini also fails
"""

import re
import json
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Normalization map
# Key   = canonical output label (what goes in the sheet)
# Value = list of strings that should map to this label
#         (checked as case-insensitive substrings of the input)
# ---------------------------------------------------------------------------

INDUSTRY_MAP: dict[str, list[str]] = {
    "it services": [
        "information technology and services",
        "information technology & services",
        "it services",
        "managed services",
        "managed it",
        "it consulting",
        "technology services",
        "tech services",
    ],
    "cybersecurity": [
        "cybersecurity",
        "cyber security",
        "computer & network security",
        "computer and network security",
        "network security",
        "information security",
        "infosec",
    ],
    "software": [
        "computer software",
        "software",
        "saas",
        "internet",
        "software development",
    ],
    "hardware manufacturing": [
        "hardware manufacturing",
        "computer hardware",
        "hardware",
        "semiconductors",
        "electronic manufacturing",
        "electronics manufacturing",
        "electrical and electronic manufacturing",
        "electrical & electronic manufacturing",
    ],
    "electronics manufacturing": [
        "electronics",
        "consumer electronics",
    ],
    "advertising": [
        "advertising",
        "marketing and advertising",
        "marketing & advertising",
        "digital marketing",
        "media and advertising",
    ],
    "public relations": [
        "public relations",
        "public relations and communications",
        "pr agency",
        "communications",
    ],
    "graphic design": [
        "graphic design",
        "design services",
        "visual design",
        "branding",
        "brand design",
    ],
    "photography": [
        "photography",
        "photo",
    ],
    "legal services": [
        "legal services",
        "law practice",
        "law firm",
        "legal",
        "attorney",
    ],
    "consulting": [
        "management consulting",
        "consulting",
        "business consulting",
        "strategy consulting",
        "professional services",
    ],
    "research services": [
        "research",
        "market research",
        "research services",
        "think tank",
    ],
    "staffing": [
        "staffing and recruiting",
        "staffing & recruiting",
        "staffing",
        "recruiting",
        "executive search",
        "human resources",
    ],
    "financial services": [
        "financial services",
        "accounting",
        "banking",
        "investment",
        "venture capital",
        "private equity",
        "insurance",
        "fintech",
    ],
    "healthcare": [
        "hospital & health care",
        "hospital and health care",
        "healthcare",
        "health care",
        "medical",
        "biotech",
        "pharmaceuticals",
        "health tech",
    ],
    "education": [
        "education management",
        "e-learning",
        "higher education",
        "education",
        "edtech",
    ],
    "real estate": [
        "real estate",
        "commercial real estate",
        "property management",
    ],
    "construction": [
        "construction",
        "civil engineering",
        "architecture & planning",
        "architecture and planning",
    ],
    "retail": [
        "retail",
        "consumer goods",
        "e-commerce",
        "ecommerce",
    ],
    "logistics": [
        "logistics and supply chain",
        "logistics & supply chain",
        "transportation",
        "supply chain",
        "warehousing",
    ],
    "media": [
        "media production",
        "broadcast media",
        "publishing",
        "entertainment",
        "online media",
    ],
    "nonprofit": [
        "non-profit organization management",
        "nonprofit",
        "non-profit",
        "charity",
        "ngo",
    ],
}


def normalize_industry(raw: str, gemini_client=None) -> str:
    """
    Returns a clean lowercase conversational industry label.

    Args:
        raw:           Raw industry string from any source.
        gemini_client: Optional Gemini client for fallback on unrecognized inputs.

    Returns:
        Normalized lowercase industry string.
        Falls back to Gemini, then to lowercased raw input.
    """
    if not raw or not raw.strip():
        return "unknown"

    normalized_input = raw.lower().strip()

    # 1. Exact match against all alias values
    for canonical, aliases in INDUSTRY_MAP.items():
        if normalized_input in [a.lower() for a in aliases]:
            logger.debug("industry exact match: %r → %r", raw, canonical)
            return canonical

    # 2. Substring match — input contains an alias
    for canonical, aliases in INDUSTRY_MAP.items():
        for alias in aliases:
            if alias.lower() in normalized_input:
                logger.debug("industry substring match: %r contains %r → %r", raw, alias, canonical)
                return canonical

    # 3. Gemini fallback
    if gemini_client:
        result = _gemini_normalize(raw, gemini_client)
        if result:
            logger.debug("industry Gemini match: %r → %r", raw, result)
            return result

    # 4. Last resort — clean up the raw string
    fallback = re.sub(r"\s+", " ", normalized_input).strip()
    logger.warning("industry unrecognized, using raw: %r", fallback)
    return fallback


# ---------------------------------------------------------------------------
# Gemini fallback
# ---------------------------------------------------------------------------

_GEMINI_PROMPT = """\
You are normalizing an industry label for a B2B sales pipeline.

Raw industry string: "{raw}"

Convert it to a short, plain English, lowercase label that a human would say in conversation.
Examples: "it services", "cybersecurity", "graphic design", "legal services", "consulting"

Rules:
- Lowercase only
- 1–4 words maximum
- No jargon, no LinkedIn taxonomy
- Return only the label — no explanation, no quotes, no punctuation

Label:"""


def _gemini_normalize(raw: str, gemini_client) -> str | None:
    """
    Ask Gemini to normalize an unrecognized industry string.
    Returns a clean string or None on failure.
    """
    try:
        prompt = _GEMINI_PROMPT.format(raw=raw)
        response = gemini_client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        result = response.text.strip().lower()
        # Sanity check: reject if Gemini returns something too long or weird
        if result and len(result.split()) <= 5 and len(result) < 60:
            return result
        logger.warning("Gemini industry response looks off: %r", result)
        return None
    except Exception as e:
        logger.warning("Gemini industry normalization failed for %r: %s", raw, e)
        return None


# ---------------------------------------------------------------------------
# Smoke test — python3 industry_normalizer.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)

    test_cases = [
        # LinkedIn taxonomy → expected output
        ("Information Technology and Services", "it services"),
        ("Computer & Network Security",         "cybersecurity"),
        ("Marketing and Advertising",           "advertising"),
        ("Public Relations and Communications", "public relations"),
        ("Graphic Design",                      "graphic design"),
        ("Law Practice",                        "legal services"),
        ("Management Consulting",               "consulting"),
        ("Staffing and Recruiting",             "staffing"),
        ("Computer Software",                   "software"),
        ("Photography",                         "photography"),
        # Already clean
        ("it services",                         "it services"),
        ("cybersecurity",                       "cybersecurity"),
        # Free-form / scraped
        ("We are a SaaS company",               "software"),
        ("digital marketing agency",            "advertising"),
        ("managed IT provider",                 "it services"),
        # Unrecognized — will show as-is without Gemini
        ("Quantum Nanotechnology Fabrication",  None),
    ]

    print(f"\n{'Raw Input':<45} {'Expected':<25} {'Got':<25} {'Pass?'}")
    print("-" * 105)
    passed = 0
    for raw, expected in test_cases:
        got = normalize_industry(raw)
        ok = "✅" if (expected is None or got == expected) else "❌"
        if ok == "✅":
            passed += 1
        print(f"{raw:<45} {str(expected):<25} {got:<25} {ok}")

    print(f"\n{passed}/{len(test_cases)} passed (unrecognized inputs without Gemini expected to vary)")