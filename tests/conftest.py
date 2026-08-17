"""Shared test guards."""
import pytest

from bot.core import alert_health, config, heartbeat, maker_state


@pytest.fixture(autouse=True)
def _no_live_side_effects(monkeypatch):
    """Neutralize live Discord posts in EVERY test.

    A developer's local .env may set DISCORD_WEBHOOK_URL / DISCORD_EDGE_WEBHOOK_URL /
    DISCORD_SIGNIFICANT_EDGE_WEBHOOK_URL (and DRY_RUN=false). The send_* alert helpers post
    only when a URL is set, so a test that exercises a code path which alerts (e.g.
    _log_would_fire) would fire a real webhook. Blank the URLs for all tests; a test that needs
    to assert posting re-sets them locally (and mocks the webhook transport)."""
    monkeypatch.setattr(config, "DISCORD_WEBHOOK_URL", "", raising=False)
    monkeypatch.setattr(config, "DISCORD_EDGE_WEBHOOK_URL", "", raising=False)
    monkeypatch.setattr(config, "DISCORD_SIGNIFICANT_EDGE_WEBHOOK_URL", "", raising=False)
    monkeypatch.setattr(config, "DISCORD_FILLS_WEBHOOK_URL", "", raising=False)
    monkeypatch.setattr(config, "DISCORD_REPORTS_WEBHOOK_URL", "", raising=False)


@pytest.fixture(autouse=True)
def _sandbox_operational_files(tmp_path_factory, monkeypatch):
    """Redirect EVERY operational-rail file to a per-test tmp directory.

    These defaults are deliberately ABSOLUTE (they resolve against the repo root, because a
    relative operational default is how both loss caps became permanently inert under any other
    cwd). The consequence is that a test's `chdir` into
    `tmp_path` does NOT sandbox them, and without this fixture the suite writes into the live
    repo's `logs/`. It did: a `main()`-driven maker test left a real `logs/maker_state.json`
    describing a fake run, and the alert tests drove the live delivery-failure counter.

    A stray `maker_state.json` is not cosmetic — it is the file the next real maker start consults
    to decide whether to refuse, so a test artefact there could block a real run or, worse, make a
    genuine crash record look resolved.

    Tests that need to READ one of these files re-point it themselves; this fixture runs first, so
    a local `monkeypatch.setattr` still wins.
    """
    root = tmp_path_factory.mktemp("ops")
    ah, hb, ms = (str(root / "alert_health.json"), str(root / "heartbeat"),
                  str(root / "maker_state.json"))
    # Both spellings: some callers read the config knob, others the module default.
    monkeypatch.setattr(config, "ALERT_HEALTH_FILE", ah, raising=False)
    monkeypatch.setattr(config, "HEARTBEAT_DIR", hb, raising=False)
    monkeypatch.setattr(config, "MAKER_STATE_FILE", ms, raising=False)
    monkeypatch.setattr(alert_health, "DEFAULT_PATH", ah)
    monkeypatch.setattr(heartbeat, "DEFAULT_DIR", hb)
    monkeypatch.setattr(maker_state, "DEFAULT_PATH", ms)
    # ⛔ THE KILL SWITCH TOO. It was the one operational rail this fixture missed, so the suite
    # read the LIVE `pause.json`: the first time the position guard tripped for real and wrote
    # one, dozens of tests went red — a genuine safety artefact protecting an open position turned
    # into a broken suite, with the obvious "fix" being to delete the operator's kill switch. Tests
    # that exercise pausing set `maker.is_paused` or this path themselves; this only stops the
    # suite from depending on the state of the real repo.
    monkeypatch.setattr(config, "KILL_SWITCH_FILE", str(root / "pause.json"), raising=False)
    # ⛔ THE POLY TAPE CSVS TOO. Their defaults are module-level ABSOLUTE paths (repo_path), so
    # a test that constructs PolyMaker without explicit tape paths writes the REAL
    # logs/poly_live_mm_*.csv. It happened more than once: fabricated fills with FakeClient order
    # ids (slug `mkt-a`) landed in the live fills tape, both times from transient work-in-progress
    # test states (the committed suite is clean), which is exactly why the guard must be
    # structural, not per-test. Real tape rows are money evidence; a fixture row there poisons
    # every P&L replay downstream.
    from bot.poly_us import maker as poly_maker
    monkeypatch.setattr(poly_maker, "DEFAULT_QUOTE_CSV", str(root / "poly_quotes.csv"))
    monkeypatch.setattr(poly_maker, "DEFAULT_CYCLE_CSV", str(root / "poly_cycles.csv"))
    monkeypatch.setattr(poly_maker, "DEFAULT_FILL_CSV", str(root / "poly_fills.csv"))
