"""Tests for Polymarket US config additions."""
import importlib
import os


def _reload_config(monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import bot.core.config as config
    return importlib.reload(config)


def test_poly_venue_defaults_to_global(monkeypatch):
    # Assert the code-level default, isolated from any ambient/.env POLY_VENUE
    # (reload re-runs load_dotenv, which would re-inject a .env value).
    import bot.core.config as config
    monkeypatch.delenv("POLY_VENUE", raising=False)
    assert config._optional("POLY_VENUE", "global").lower() == "global"


def test_poly_venue_reads_us(monkeypatch):
    config = _reload_config(monkeypatch, POLY_VENUE="us")
    assert config.POLY_VENUE == "us"


def test_poly_us_credentials_read_from_env(monkeypatch):
    config = _reload_config(
        monkeypatch,
        POLYMARKET_US_KEY_ID="kid-123",
        POLYMARKET_US_SECRET_KEY="sec-456",
    )
    assert config.POLYMARKET_US_KEY_ID == "kid-123"
    assert config.POLYMARKET_US_SECRET_KEY == "sec-456"


def test_poly_us_series_ids_default(monkeypatch):
    monkeypatch.delenv("POLYMARKET_US_SERIES", raising=False)
    config = _reload_config(monkeypatch)
    # World Cup (69) must be present — user's primary market
    assert "69" in config.POLYMARKET_US_SERIES


def test_poly_us_series_ids_override(monkeypatch):
    config = _reload_config(monkeypatch, POLYMARKET_US_SERIES="69,15")
    assert config.POLYMARKET_US_SERIES == ["69", "15"]
