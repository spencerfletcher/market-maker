"""Sandbox every operational rail for the public suite.

The module defaults are ABSOLUTE paths under the repo root (a relative operational path silently
reads a different file under another cwd), so a test's `chdir` does not sandbox them. Redirect them
per test, and blank every webhook so a developer's local `.env` can never post from a test.
"""
from pathlib import Path

import pytest

from bot.core import alert_health, config, heartbeat, maker_state, venue_backoff, venue_budget


@pytest.fixture(autouse=True)
def _sandbox_operational_files(tmp_path_factory, monkeypatch):
    root = tmp_path_factory.mktemp("ops")
    monkeypatch.chdir(root)      # cwd-relative defaults (`logs/...`) land here, never in the checkout
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[1]))   # subprocess tests still find `bot`
    ah, hb, ms = str(root / "alert_health.json"), str(root / "heartbeat"), str(root / "maker_state.json")
    monkeypatch.setattr(config, "ALERT_HEALTH_FILE", ah, raising=False)
    monkeypatch.setattr(config, "HEARTBEAT_DIR", hb, raising=False)
    monkeypatch.setattr(config, "MAKER_STATE_FILE", ms, raising=False)
    monkeypatch.setattr(config, "KILL_SWITCH_FILE", str(root / "pause.json"), raising=False)
    monkeypatch.setattr(alert_health, "DEFAULT_PATH", ah)
    monkeypatch.setattr(heartbeat, "DEFAULT_DIR", hb)
    monkeypatch.setattr(maker_state, "DEFAULT_PATH", ms)
    monkeypatch.setattr(venue_budget, "STATE_DIR", str(root))
    monkeypatch.setattr(venue_backoff, "LATCH_PATH", str(root / "venue_backoff.json"))
    for name in dir(config):
        if name.startswith("DISCORD_") and name.endswith("_WEBHOOK_URL"):
            monkeypatch.setattr(config, name, "", raising=False)
