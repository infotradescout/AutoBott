from pathlib import Path


def test_purpose_lock_exists() -> None:
    purpose_lock = Path("docs/AUTOBOTT_V2_PURPOSE_LOCK.md")
    assert purpose_lock.exists(), "Purpose lock document must exist."


def test_purpose_lock_contains_required_sections() -> None:
    text = Path("docs/AUTOBOTT_V2_PURPOSE_LOCK.md").read_text(encoding="utf-8").lower()

    required_markers = [
        "one-sentence purpose",
        "1. primary purpose",
        "2. primary user",
        "3. market scope",
        "4. instrument scope",
        "5. time horizon",
        "6. automation level",
        "7. decision authority model",
        "8. signal/edge source",
        "9. profit/risk objective",
        "10. kpis",
        "11. forbidden scope",
        "12. build implications for p2/p3/p4",
    ]

    for marker in required_markers:
        assert marker in text, f"Purpose lock missing required section: {marker}"
