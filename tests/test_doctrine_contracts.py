from pathlib import Path


def test_doctrine_centers_the_trading_bot_as_the_product() -> None:
    doctrine = Path("docs/DOCTRINE.md").read_text(encoding="utf-8").lower()

    required_phrases = [
        "automated options trading system",
        "the product is the bot",
        "research stack, replay stack, and historical stack",
        "support systems",
        "operator-visible safety status",
    ]

    for phrase in required_phrases:
        assert phrase in doctrine, f"Doctrine must explicitly contain: {phrase}"
