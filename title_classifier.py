"""
title_classifier.py

Deterministic rule-based classifier for role_level and role_context.
Falls back to Gemini only for genuinely ambiguous titles.

role_level values:   ceo_founder | hr_leader | needs_review
role_context values: founders and CEOs | leadership teams | HR and people leaders | HR teams | needs_review
"""

import re
import os
import json
import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rule tables — order matters, first match wins
# ---------------------------------------------------------------------------

# (pattern, role_level)
ROLE_LEVEL_RULES: list[tuple[str, str]] = [
    # CEO / Founder / Owner — must check before generic "chief" rule
    (r"\bceo\b", "ceo_founder"),
    (r"\bchief\s+executive\s+officer\b", "ceo_founder"),
    (r"\bfounder\b", "ceo_founder"),
    (r"\bco[\s\-]?founder\b", "ceo_founder"),
    (r"\bowner\b", "ceo_founder"),
    # Negative lookbehind: "President" but NOT "Vice President"
    (r"(?<!vice\s)\bpresident\b", "ceo_founder"),

    # Any other C-suite → hr_leader (CPO, CHRO, Chief People, Chief HR, etc.)
    (r"\bchief\b", "hr_leader"),

    # Explicit HR / People signals
    (r"\b(hr|h\.r\.)\b", "hr_leader"),
    (r"\bhrbp\b", "hr_leader"),
    (r"\bhuman\s+resources\b", "hr_leader"),
    (r"\bpeople\b", "hr_leader"),
    (r"\btalent\b", "hr_leader"),
    (r"\bcompensation\b", "hr_leader"),
    (r"\btotal\s+rewards\b", "hr_leader"),
    (r"\brecruiting\b", "hr_leader"),
    (r"\brecruitment\b", "hr_leader"),
    (r"\bbenefits\b", "hr_leader"),
    (r"\bworkforce\b", "hr_leader"),
    (r"\bdiversity\b", "hr_leader"),
    (r"\bdei\b", "hr_leader"),
    (r"\binclusion\b", "hr_leader"),

    # Edge cases — managing director, partner, principal
    (r"\bmanaging\s+director\b", "hr_leader"),
    (r"\bpartner\b", "hr_leader"),
    (r"\bprincipal\b", "hr_leader"),

    # Broad seniority signals — any department (per spec: VP/Director/Manager all classify)
    (r"\bvice\s+president\b", "hr_leader"),
    (r"\bvp\b", "hr_leader"),
    (r"\bdirector\b", "hr_leader"),
    (r"\bmanager\b", "hr_leader"),
    (r"\bhead\s+of\b", "hr_leader"),
    (r"\blead\b", "hr_leader"),
]

# (pattern, role_context)
ROLE_CONTEXT_RULES: list[tuple[str, str]] = [
    # CEO / Founder → founders and CEOs
    (r"\bceo\b", "founders and CEOs"),
    (r"\bchief\s+executive\s+officer\b", "founders and CEOs"),
    (r"\bfounder\b", "founders and CEOs"),
    (r"\bco[\s\-]?founder\b", "founders and CEOs"),
    (r"\bowner\b", "founders and CEOs"),
    # Negative lookbehind: "President" but NOT "Vice President"
    (r"(?<!vice\s)\bpresident\b", "founders and CEOs"),

    # C-suite (non-CEO) + VP + MD + Partner + Principal → leadership teams
    (r"\bchief\b", "leadership teams"),
    (r"\bvice\s+president\b", "leadership teams"),
    (r"\bvp\b", "leadership teams"),
    (r"\bmanaging\s+director\b", "leadership teams"),
    (r"\bpartner\b", "leadership teams"),
    (r"\bprincipal\b", "leadership teams"),

    # Director-level → HR and people leaders
    # Per spec: Director = high up, leading the company → leadership teams
    (r"\bdirector\b", "leadership teams"),
    (r"\bhead\s+of\b", "HR and people leaders"),

    # Manager / Lead / Individual contributor → HR teams
    (r"\bmanager\b", "HR and people leaders"),
    (r"\blead\b", "HR teams"),
    (r"\bspecialist\b", "HR teams"),
    (r"\bcoordinator\b", "HR teams"),
    (r"\bgeneralist\b", "HR teams"),
    (r"\bbusiness\s+partner\b", "HR teams"),
    (r"\bhrbp\b", "HR teams"),
]


def _normalize(title: str) -> str:
    """Lowercase and collapse whitespace for consistent matching."""
    return re.sub(r"\s+", " ", title.lower().strip())


def classify_role_level(title: str) -> str:
    """
    Returns role_level for a given title string.
    Returns 'needs_review' if no rule matches and Gemini is unavailable.
    """
    normalized = _normalize(title)
    for pattern, role_level in ROLE_LEVEL_RULES:
        if re.search(pattern, normalized):
            logger.debug("role_level match: pattern=%r title=%r → %s", pattern, title, role_level)
            return role_level
    return "needs_review"


def classify_role_context(title: str) -> str:
    """
    Returns role_context for a given title string.
    Returns 'needs_review' if no rule matches.
    """
    normalized = _normalize(title)
    for pattern, role_context in ROLE_CONTEXT_RULES:
        if re.search(pattern, normalized):
            logger.debug("role_context match: pattern=%r title=%r → %s", pattern, title, role_context)
            return role_context
    return "needs_review"


def classify_title(title: str, gemini_client=None) -> dict:
    """
    Main entry point. Returns a dict with:
        role_level:       str
        role_context:     str
        confidence:       'high' | 'low'
        needs_review:     bool

    High confidence = both fields resolved by deterministic rules.
    Low confidence  = at least one field fell through to Gemini or 'needs_review'.
    """
    role_level = classify_role_level(title)
    role_context = classify_role_context(title)

    # Attempt Gemini fallback for any unresolved field
    if (role_level == "needs_review" or role_context == "needs_review") and gemini_client:
        gemini_result = _gemini_classify(title, gemini_client)
        if role_level == "needs_review":
            role_level = gemini_result.get("role_level", "needs_review")
        if role_context == "needs_review":
            role_context = gemini_result.get("role_context", "needs_review")

    still_unresolved = role_level == "needs_review" or role_context == "needs_review"
    confidence = "low" if still_unresolved else "high"

    return {
        "role_level": role_level,
        "role_context": role_context,
        "confidence": confidence,
        "needs_review": still_unresolved,
    }


# ---------------------------------------------------------------------------
# Gemini fallback
# ---------------------------------------------------------------------------

_GEMINI_PROMPT = """\
You are classifying a job title for a B2B sales pipeline.

Job title: "{title}"

Respond ONLY with a JSON object — no preamble, no markdown, no explanation.

Choose role_level from exactly one of:
  - "ceo_founder"  (CEO, Founder, Co-Founder, Owner, President)
  - "hr_leader"    (any HR, People, Talent, Compensation, or non-CEO C-suite role)
  - "needs_review" (genuinely ambiguous or unrelated)

Choose role_context from exactly one of:
  - "founders and CEOs"
  - "leadership teams"
  - "HR and people leaders"
  - "HR teams"
  - "needs_review"

Return format:
{{"role_level": "...", "role_context": "..."}}
"""


def _gemini_classify(title: str, gemini_client) -> dict:
    """
    Call Gemini Flash to classify a title that didn't match any rule.
    Returns a dict with role_level and role_context keys.
    Falls back to needs_review on any error.
    """
    try:
        prompt = _GEMINI_PROMPT.format(title=title)
        model    = gemini_client.GenerativeModel("gemini-flash-latest")
        response = model.generate_content(prompt)
        raw = response.text.strip()
        # Strip markdown fences if present
        raw = re.sub(r"^```json\s*|```$", "", raw, flags=re.MULTILINE).strip()
        result = json.loads(raw)

        valid_role_levels = {"ceo_founder", "hr_leader", "needs_review"}
        valid_role_contexts = {"founders and CEOs", "leadership teams", "HR and people leaders", "HR teams", "needs_review"}

        return {
            "role_level": result.get("role_level", "needs_review") if result.get("role_level") in valid_role_levels else "needs_review",
            "role_context": result.get("role_context", "needs_review") if result.get("role_context") in valid_role_contexts else "needs_review",
        }
    except Exception as e:
        logger.warning("Gemini classification failed for title=%r: %s", title, e)
        return {"role_level": "needs_review", "role_context": "needs_review"}


# ---------------------------------------------------------------------------
# Quick smoke test — run directly: python3 title_classifier.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)

    test_titles = [
        # --- ceo_founder ---
        "CEO",
        "Co-Founder & CEO",
        "Founder",
        "President",
        "Owner",
        # --- hr_leader: C-suite (non-CEO) ---
        "Chief People Officer",
        "Chief Human Resources Officer",
        "Chief Talent Officer",
        # --- hr_leader: explicit HR signals ---
        "VP of People",
        "VP HR",
        "Head of Total Rewards",
        "Director of Compensation",
        "HR Manager",
        "People Operations Lead",
        "HRBP",                           # bug fix: was needs_review for role_level
        "Talent Acquisition Specialist",
        # --- edge cases ---
        "Managing Director",
        "Partner",
        "Principal",
        # --- bug fixes: these were misfiring ---
        "Vice President of Sales",        # was ceo_founder, should be hr_leader + leadership teams
        "Vice President of Engineering",  # same fix
        "Vice President",                 # bare VP
        # --- department-agnostic per spec ---
        "Director of Engineering",        # hr_leader + leadership teams
        "Operations Manager",             # hr_leader + HR and people leaders
        "Director of Finance",            # hr_leader + leadership teams
        "Manager of IT",                  # hr_leader + HR and people leaders
        # --- still expects needs_review (no seniority signal) ---
        "Software Engineer",
        "Account Executive",
        "Sales Representative",
    ]

    print(f"{'Title':<45} {'role_level':<15} {'role_context':<25} {'confidence'}")
    print("-" * 110)
    for t in test_titles:
        result = classify_title(t)  # no Gemini client — pure rules
        print(f"{t:<45} {result['role_level']:<15} {result['role_context']:<25} {result['confidence']}")