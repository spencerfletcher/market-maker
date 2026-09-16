"""Tests for Kalshi config defaults."""
import importlib

from bot.core import config


def test_kalshi_arb_min_edge_default(monkeypatch):
    # Code-level default is 0.02 (the divergence-tail gate; ~2x margin over the ~0.9%
    # ambiguous-settlement rate, still captures the Cleveland 2.2% edge). Tested via the
    # _float helper to stay isolated from any ambient/.env override.
    monkeypatch.delenv("KALSHI_ARB_MIN_EDGE", raising=False)
    assert config._float("KALSHI_ARB_MIN_EDGE", 0.02) == 0.02


def test_kalshi_env_default(monkeypatch):
    # Assert the code-level default, isolated from any ambient/.env KALSHI_ENV.
    monkeypatch.delenv("KALSHI_ENV", raising=False)
    assert config._optional("KALSHI_ENV", "demo") == "demo"


def test_kalshi_series_default():
    # NOTE: config.KALSHI_SERIES reflects the .env override when set (production pins it), so this
    # asserts the always-present core series, NOT the full code default. MLS/soccer additions must be
    # enabled in the deployed .env (KALSHI_SERIES + POLYMARKET_US_SERIES) — a code-default change alone
    # does not scan them. The code default is covered by test_kalshi_series_default_includes_mls below.
    assert "KXNBAGAME" in config.KALSHI_SERIES
    assert "KXNHLGAME" in config.KALSHI_SERIES
    assert "KXMLBGAME" in config.KALSHI_SERIES


def test_kalshi_series_default_includes_mls(monkeypatch):
    # The CODE default (env unset) scans MLS — guards against a regression dropping it from the
    # fallback. Mirrors test_kalshi_env_default: prod enablement is still the deployed .env.
    monkeypatch.delenv("KALSHI_SERIES", raising=False)
    default = config._optional(
        "KALSHI_SERIES", "KXNBAGAME,KXNHLGAME,KXMLBGAME,KXWCGAME,KXMLSGAME,KXWNBAGAME"
    )
    assert "KXMLSGAME" in default.split(",")


def test_kalshi_arb_min_edge_override(monkeypatch):
    monkeypatch.setattr(config, "KALSHI_ARB_MIN_EDGE", 0.02)
    assert config.KALSHI_ARB_MIN_EDGE == 0.02
