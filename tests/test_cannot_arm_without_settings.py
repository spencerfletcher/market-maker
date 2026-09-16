"""Missing private settings cannot enable live trading in this tree.

Three seams gate money: `config.DRY_RUN` (the only gate on order placement), the Kalshi shim's
`--i-understand-real-money` flag (which must AGREE with DRY_RUN or `main()` refuses), and the
Polymarket US maker's `real` switch (off by default; the launch shim that sets it is withheld).
"""
from pathlib import Path

from bot.core import config
from bot.kalshi import maker as kalshi_maker
from bot.poly_us.maker import PolyMaker

ROOT = Path(__file__).resolve().parents[1]


def test_a_clean_environment_is_dry_with_no_venue_credentials():
    assert config.DRY_RUN is True
    for name in ("KALSHI_API_KEY", "KALSHI_PRIVATE_KEY_PATH", "POLYMARKET_US_KEY_ID", "POLYMARKET_US_SECRET_KEY"):
        assert getattr(config, name, "") == "", f"{name} is set in the test environment"


def test_the_env_example_ships_dry_with_placeholder_credentials():
    text = (ROOT / ".env.example").read_text(encoding="utf-8")
    values = dict(line.split("=", 1) for line in text.splitlines()
                  if line and not line.startswith("#") and "=" in line)
    assert values["DRY_RUN"].strip().lower() == "true"
    assert values["POLYMARKET_US_KEY_ID"].startswith("your-") and values["KALSHI_API_KEY"].startswith("your-")


def test_the_kalshi_shim_refuses_dry_run_off_without_its_flag():
    # DRY_RUN=false from the environment alone is a hard stop, never a silent live run.
    assert kalshi_maker._real_money_refusal(dry_run=False, flagged=False)
    assert kalshi_maker._real_money_refusal(dry_run=True, flagged=False) is None


def test_the_poly_maker_is_not_real_by_default():
    m = PolyMaker(client=object(), slugs=[])
    assert m.real is False and m.shadow is True
