"""
confidence_gate.py

Assigns final sheet status to a lead based on classification confidence.

Rules:
    HIGH confidence = role_level + role_context both resolved, confidence = 'high'
        → status: 'ready_to_send'
    LOW confidence  = any field unresolved or confidence = 'low'
        → status: 'needs_review'

Also stamps:
    - date_added:   today's date (YYYY-MM-DD)
    - lead_source:  source tag from discovery engine
"""

from datetime import date


# Fields that must be resolved (not needs_review) for high confidence
REQUIRED_FIELDS = ["role_level", "role_context"]
UNRESOLVED      = "needs_review"


def assign_status(lead: dict) -> dict:
    """
    Takes a fully classified lead dict and assigns status + metadata.

    Expects lead to have at minimum:
        role_level, role_context, confidence (from title_classifier)
        source (from discovery engine)

    Returns the same dict with added fields:
        status, date_added, lead_source
    """
    confidence   = lead.get("confidence", "low")
    role_level   = lead.get("role_level",   UNRESOLVED)
    role_context = lead.get("role_context", UNRESOLVED)

    # Gate logic — all conditions must pass for ready_to_send
    is_high_confidence = (
        confidence   == "high" and
        role_level   != UNRESOLVED and
        role_context != UNRESOLVED
    )

    lead["status"]      = "ready_to_send" if is_high_confidence else "needs_review"
    lead["date_added"]  = date.today().isoformat()
    lead["lead_source"] = lead.get("source", "unknown")

    return lead


def gate_leads(leads: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Process a list of leads through the confidence gate.

    Returns:
        (ready, needs_review) — two lists split by status
    """
    ready        = []
    needs_review = []

    for lead in leads:
        lead = assign_status(lead)
        if lead["status"] == "ready_to_send":
            ready.append(lead)
        else:
            needs_review.append(lead)

    return ready, needs_review


# ---------------------------------------------------------------------------
# Smoke test — python3 confidence_gate.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    test_leads = [
        # Should be ready_to_send
        {
            "first_name":   "Lori",
            "last_name":    "Joint",
            "title":        "President & CEO",
            "company":      "MBA USA",
            "domain":       "mbausa.org",
            "role_level":   "ceo_founder",
            "role_context": "founders and CEOs",
            "confidence":   "high",
            "source":       "hackernews",
        },
        # Should be needs_review — low confidence
        {
            "first_name":   "John",
            "last_name":    "Smith",
            "title":        "Operations Manager",
            "company":      "Acme Corp",
            "domain":       "acme.com",
            "role_level":   "hr_leader",
            "role_context": "needs_review",
            "confidence":   "low",
            "source":       "hackernews",
        },
        # Should be needs_review — role_level unresolved
        {
            "first_name":   "Jane",
            "last_name":    "Doe",
            "title":        "Software Engineer",
            "company":      "Tech Co",
            "domain":       "techco.com",
            "role_level":   "needs_review",
            "role_context": "needs_review",
            "confidence":   "low",
            "source":       "scraper",
        },
    ]

    ready, review = gate_leads(test_leads)

    print(f"{'Name':<25} {'Title':<30} {'Status':<15} {'Confidence'}")
    print("-" * 85)
    for lead in test_leads:
        name = f"{lead['first_name']} {lead['last_name']}"
        print(f"{name:<25} {lead['title']:<30} {lead['status']:<15} {lead['confidence']}")

    print(f"\nready_to_send: {len(ready)}")
    print(f"needs_review:  {len(review)}")
