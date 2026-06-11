from pathlib import Path


def test_doctrine_forbids_live_capabilities() -> None:
    doctrine = Path("docs/DOCTRINE.md").read_text(encoding="utf-8").lower()

    required_forbidden_phrases = [
        "live trading",
        "live broker execution",
        "real orders",
        "broker credential requirements",
        "external connector dependencies",
    ]

    for phrase in required_forbidden_phrases:
        assert phrase in doctrine, f"Doctrine must explicitly forbid: {phrase}"
