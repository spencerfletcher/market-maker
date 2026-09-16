"""The per-venue state-path split (TODO ) and its adoption migration.

Both makers shared one MAKER_STATE_FILE; the 2026-07-31 guards made the mixed state
safe-but-noisy, and four recorded edges rode on the real fix: a Kalshi start resetting a Poly
record's orders, the Poly preflight certifying a KALSHI record from a Poly venue read (the
symmetric hole), a record with NO names sailing past every name-shape guard, and the dust-sign
asymmetry. The split closes the first three STRUCTURALLY — venue is decided by which FILE a
store opens, not by heuristics over the record's contents — and the fourth is fixed here
because any real position with NO record is unknown by definition, whatever its sign.
"""
from __future__ import annotations

import json
import os
from decimal import Decimal

import pytest

from bot.core import maker_state


def _legacy(tmp_path, run_id: str, *, pid: int = 1, clean: bool = True,
            realized: str = "1.1840") -> str:
    base = str(tmp_path / "maker_state.json")
    with open(base, "w") as f:
        json.dump({"run_id": run_id, "pid": pid, "mode": "real", "started_ts": 1.0,
                   "clean_exit": clean, "tickers": ["x"], "orders": {},
                   "inventory": {}, "realized_pnl": realized, "loss_cap": "4.00"}, f)
    return base


# ── path + classification ────────────────────────────────────────────────────

def test_venue_path_derives_a_sibling_file_per_venue():
    assert maker_state.venue_path("logs/maker_state.json", "poly") == (
        "logs/maker_state_poly.json")
    assert maker_state.venue_path("logs/maker_state.json", "kalshi") == (
        "logs/maker_state_kalshi.json")


def test_run_id_prefixes_classify_the_venue():
    """polymm- is the Poly maker's prefix (old `polymm-<ts>-<hex>` AND the new mode-stamped
    `polymm-real-<ts>-<hex>`); real-/dry- are the Kalshi maker's documented mode stamps."""
    assert maker_state.venue_of_run_id("polymm-1785512559-2e1783") == "poly"
    assert maker_state.venue_of_run_id("polymm-real-example-run") == "poly"
    assert maker_state.venue_of_run_id("real-1753900000") == "kalshi"
    assert maker_state.venue_of_run_id("dry-1753900000") == "kalshi"
    assert maker_state.venue_of_run_id("mystery-123") is None
    assert maker_state.venue_of_run_id("") is None


# ── adoption ─────────────────────────────────────────────────────────────────

def test_adoption_moves_a_matching_legacy_record_and_carries_the_ledger(tmp_path):
    """The realized_pnl ratchet is the durable loss-cap input — losing it in the migration
    would reset a lifetime budget."""
    base = _legacy(tmp_path, "polymm-1785512559-2e1783", realized="1.1840")
    store = maker_state.store_for_venue("poly", base_path=base)
    assert store.path == maker_state.venue_path(base, "poly")
    snap = store.snapshot()
    assert snap.run_id == "polymm-1785512559-2e1783"
    assert snap.realized_pnl == Decimal("1.1840")
    assert not os.path.exists(base), "legacy must not remain as a readable stale copy"
    migrated = [n for n in os.listdir(tmp_path) if n.startswith("maker_state.json.migrated-")]
    assert migrated, "the legacy content is preserved as evidence, renamed not deleted"


def test_adoption_leaves_the_OTHER_venues_legacy_record_alone(tmp_path):
    base = _legacy(tmp_path, "polymm-1785512559-2e1783")
    store = maker_state.store_for_venue("kalshi", base_path=base)
    assert store.snapshot() is None or store.snapshot().run_id == "", (
        "a Kalshi store must start FRESH, never adopt a Poly record")
    assert os.path.exists(base), "the Poly record stays for the Poly store to adopt"


def test_an_unclassifiable_legacy_run_id_refuses_rather_than_guesses(tmp_path):
    base = _legacy(tmp_path, "mystery-123")
    with pytest.raises(maker_state.LegacyStateUnclassifiable):
        maker_state.store_for_venue("poly", base_path=base)
    assert os.path.exists(base), "refusal must not consume the record"


def test_adoption_refuses_while_the_legacy_records_pid_is_alive(tmp_path):
    """The live maker writes the legacy path until it exits — migrating underneath it splits
    one run's writes across two files."""
    base = _legacy(tmp_path, "polymm-1785512559-2e1783", pid=os.getpid(), clean=False)
    with pytest.raises(maker_state.LegacyStateLive):
        maker_state.store_for_venue("poly", base_path=base)
    assert os.path.exists(base)


def test_no_legacy_file_means_a_plain_fresh_store(tmp_path):
    base = str(tmp_path / "maker_state.json")
    store = maker_state.store_for_venue("poly", base_path=base)
    assert store.path == maker_state.venue_path(base, "poly")


def test_an_existing_venue_file_wins_and_the_legacy_is_not_touched(tmp_path):
    """Adoption is one-time: once the venue file exists, a lingering legacy (e.g. the OTHER
    venue's un-adopted record) must never overwrite it."""
    base = _legacy(tmp_path, "polymm-old-run")
    vpath = maker_state.venue_path(base, "poly")
    with open(vpath, "w") as f:
        json.dump({"run_id": "polymm-current", "pid": 1, "mode": "real", "started_ts": 2.0,
                   "clean_exit": True, "tickers": [], "orders": {}, "inventory": {},
                   "realized_pnl": "2.00", "loss_cap": "4.00"}, f)
    store = maker_state.store_for_venue("poly", base_path=base)
    assert store.snapshot().run_id == "polymm-current"
    assert os.path.exists(base), "legacy untouched once the venue file exists"


def test_an_interrupted_adoption_is_finished_not_left_double_readable(tmp_path):
    """mm-review Q1: a crash between the venue-file write and the legacy rename leaves BOTH
    files readable with the same record — opswatch then double-reports it and the recovery
    tool's legacy fallback re-sees a migrated record. The next store construction finishes
    the interrupted rename."""
    import shutil
    base = _legacy(tmp_path, "polymm-crashwin")
    shutil.copy(base, maker_state.venue_path(base, "poly"))   # the crash window
    store = maker_state.store_for_venue("poly", base_path=base)
    assert not os.path.exists(base), "the interrupted rename must be finished"
    assert store.snapshot().run_id == "polymm-crashwin"


# ── the dust-sign asymmetry () ────────────────────────────────────

def test_a_real_position_with_NO_record_is_unknown_whatever_its_sign():
    """Pre-fix: at exactly |qty| == _DUST with no record, the magnitude clause did not fire
    and the sign clause decided asymmetrically — negative dust was planned for AUTO-FLATTEN
    (moving money against a basis we do not own), positive refused. A position with no record
    is unknown BY DEFINITION; the sign of the venue's number is not evidence of ownership."""
    dust = maker_state._DUST
    for qty in (dust, -dust, Decimal("5"), Decimal("-5")):
        plan = maker_state.assess_recovery(None, [], {"t": qty})
        assert plan.action == "refuse" and plan.reason == "unknown_position", qty


def test_recorded_positions_keep_the_shrink_exemption_and_sign_check():
    state = maker_state._to_state({
        "run_id": "polymm-x", "pid": 1, "mode": "real", "started_ts": 1.0,
        "clean_exit": False, "tickers": ["t"], "orders": {},
        "inventory": {"t": "12"}, "realized_pnl": "0", "loss_cap": "0"})
    shrink = maker_state.assess_recovery(state, [], {"t": Decimal("5")})
    assert shrink.reason == "live_venue_state", "same-sign shrink stays attributable"
    flipped = maker_state.assess_recovery(state, [], {"t": Decimal("-5")})
    assert flipped.reason == "unknown_position", "the sign flip stays unknown"
