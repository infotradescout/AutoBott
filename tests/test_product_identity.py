from pathlib import Path


def test_readme_declares_execution_product_identity() -> None:
    text = Path("README.md").read_text(encoding="utf-8").lower()

    required_phrases = [
        "automated options trading system",
        "historical analysis are crucial support",
        "the product is the trading bot itself",
    ]

    for phrase in required_phrases:
        assert phrase in text, f"README missing product identity phrase: {phrase}"


def test_product_architecture_distinguishes_product_and_support_layers() -> None:
    text = Path("docs/PRODUCT_ARCHITECTURE.md").read_text(encoding="utf-8").lower()

    required_phrases = [
        "autobott is an automated options trading bot",
        "supporting",
        "execution",
        "positions",
        "the repository is strongest in supporting domains",
    ]

    for phrase in required_phrases:
        assert phrase in text, f"Product architecture doc missing phrase: {phrase}"
