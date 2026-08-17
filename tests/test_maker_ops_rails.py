"""The operational rails, exercised through the REAL `main()` — not just unit-tested in isolation.

WHY THIS FILE EXISTS SEPARATELY FROM the per-module tests. This repo's signature defect is a
correct mechanism with no live caller: `KalshiClient.get_positions()` sat with ZERO callers, then
returned `[]` for a month once it had one; `pause.json` had exactly one caller, in the process that
has never traded; both loss caps have never once executed. A rail nothing calls is not a rail. So
every rail wired into the maker gets an end-to-end assertion here, driven by the same in-memory
venue `tests/test_maker_main_e2e.py` uses.

Covers: the heartbeat (and its clean-exit marker), the memory guard's clean halt, the durable
crash state (run record, order intents written BEFORE the create, clean-exit flag), and the
refusal to start on top of an unresolved prior crash.
"""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from bot.core import durable, maker_state, memguard
from bot.kalshi import maker as mm

# The e2e harness (fakes for venue/book/clock/scanner) and its helpers. Imported rather than
# duplicated so both files exercise one wiring.
from tests.test_maker_main_e2e import harness, _run, _rows, _events, _argv  # noqa: F401


def _heartbeat(tmp_path):
    return json.loads((tmp_path / "logs" / "heartbeat" / "kalshi_maker.json").read_text())


@pytest.fixture(autouse=True)
def _sandbox_ops_paths(tmp_path, monkeypatch):
    """Point every operational file at the test's own tmp tree.

    ⚠️ The harness chdirs into `tmp_path`, but these defaults are ABSOLUTE (they resolve against
    the repo root on purpose — a relative operational default is how both loss caps became
    permanently inert). So a chdir does NOT sandbox them and they must be redirected explicitly,
    or a test run would write into the live repo's logs/.
    """
    monkeypatch.setattr(mm.config, "HEARTBEAT_DIR", str(tmp_path / "logs" / "heartbeat"))
    monkeypatch.setattr(mm.config, "MAKER_STATE_FILE", str(tmp_path / "logs" / "maker_state.json"))


# ── heartbeat ────────────────────────────────────────────────────────────────────────────────

def test_the_maker_beats_a_heartbeat_while_it_runs(harness, monkeypatch, tmp_path):
    """The deadman's whole premise: a live process asserts life continuously, because a SIGKILLed
    one cannot report its own death."""
    _run(monkeypatch)
    hb = _heartbeat(tmp_path)
    assert hb["seq"] >= 1
    assert hb["pid"] > 0


def test_the_heartbeat_carries_markets_inventory_and_pnl(harness, monkeypatch, tmp_path):
    """'still alive, N markets quoted, inventory X, P&L Y' — the operator-facing ask."""
    _run(monkeypatch)
    hb = _heartbeat(tmp_path)
    assert "markets_quoted" in hb and hb["markets_quoted"] >= 1
    assert "inventory" in hb
    assert "marked_pnl" in hb, "the P&L field must be present and labelled as MARKED, not realized"


def test_the_heartbeat_records_a_clean_exit_so_a_planned_stop_is_not_a_deadman_trip(
        harness, monkeypatch, tmp_path):
    _run(monkeypatch)
    assert _heartbeat(tmp_path)["exit_status"] == "clean"


def test_a_halted_run_records_the_halt_reason_in_the_heartbeat(harness, monkeypatch, tmp_path):
    """A guard-triggered stop is an exit an operator must see — it must not read as `clean`."""
    (tmp_path / "pause.json").write_text("{}")
    monkeypatch.setattr(mm.config, "KILL_SWITCH_FILE", str(tmp_path / "pause.json"))
    _run(monkeypatch)
    assert _heartbeat(tmp_path)["exit_status"] == "halted:kill_switch"


def test_a_heartbeat_write_failure_never_stops_the_run(harness, monkeypatch, tmp_path):
    """Instrumentation must not be able to halt trading.

    Scoped to the HEARTBEAT's writer only — patching `bot.core.durable` itself would also break
    the durable maker state, whose write failure is fail-CLOSED on purpose, and the test would
    then pass for the wrong reason."""
    class _BrokenDurable:
        StateCorrupt = mm.heartbeat.durable.StateCorrupt

        @staticmethod
        def write_json_durable(*a, **k):
            raise OSError("disk full")

    monkeypatch.setattr(mm.heartbeat, "durable", _BrokenDurable)
    _run(monkeypatch)
    assert "end" in _events(harness["csv"])


# ── memory guard ─────────────────────────────────────────────────────────────────────────────

def test_memory_pressure_HALTS_the_run_cleanly(harness, monkeypatch, tmp_path):
    """A clean halt with orders cancelled beats a SIGKILL with orders resting. The OOM killer runs
    no teardown; this does."""
    monkeypatch.setattr(mm.memguard, "check",
                        lambda *a, **k: memguard.MemStatus("halt", "rss 9000MB >= halt 700MB"))
    _run(monkeypatch)
    halts = [r for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "halt"]
    assert [r[2] for r in halts] == ["memory"], f"halted for the wrong reason(s): {halts}"


def test_a_memory_halt_still_runs_the_full_teardown(harness, monkeypatch, tmp_path):
    """The halt is only worth taking if it cancels — otherwise it is a slower SIGKILL."""
    monkeypatch.setattr(mm.memguard, "check",
                        lambda *a, **k: memguard.MemStatus("halt", "rss 9000MB >= halt 700MB"))
    _run(monkeypatch)
    assert harness["client"].resting_ids() == []


def test_memory_pressure_short_of_the_halt_threshold_does_NOT_stop_the_run(
        harness, monkeypatch, tmp_path):
    monkeypatch.setattr(mm.memguard, "check",
                        lambda *a, **k: memguard.MemStatus("warn", "rss 450MB >= warn 400MB"))
    _run(monkeypatch)
    halts = [r for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "halt"]
    assert halts == [], f"a WARNING halted the run: {halts}"


def test_a_raising_memory_reader_does_not_kill_the_run(harness, monkeypatch, tmp_path):
    """Fail-OPEN, deliberately: an unreadable /proc carries no information about memory pressure,
    so it must not become the outage. (The opposite of the cannot-verify rule for POSITIONS, which
    is missing evidence about money.)"""
    monkeypatch.setattr(mm.memguard, "check",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("procfs gone")))
    _run(monkeypatch)
    assert "end" in _events(harness["csv"])


# ── durable crash state ──────────────────────────────────────────────────────────────────────

def test_the_run_is_recorded_in_the_durable_state(harness, monkeypatch, tmp_path):
    _run(monkeypatch)
    # Post-split the Kalshi maker writes its VENUE file (maker_state_kalshi.json).
    state = maker_state.load_state_for_venue(
        "kalshi", base_path=str(tmp_path / "logs" / "maker_state.json"))
    assert state is not None
    assert state.mode == "real"
    assert state.tickers


def test_an_order_intent_is_durable_BEFORE_the_create_reaches_the_venue(
        harness, monkeypatch, tmp_path):
    """The uncoverable window is 'sent, then died before the response'. Recording only after the
    venue answers leaves that window with no trace at all."""
    seen: list[tuple[str, int]] = []
    path = str(tmp_path / "logs" / "maker_state.json")
    real_intent = maker_state.MakerStateStore.record_intent

    def spy(self, intent_id, **kw):
        seen.append(("intent", len(harness["client"].calls_of("create"))))
        return real_intent(self, intent_id, **kw)

    monkeypatch.setattr(maker_state.MakerStateStore, "record_intent", spy)
    _run(monkeypatch)
    assert seen, "no order intent was ever recorded"
    # The first intent is written when zero creates have happened yet.
    assert seen[0][1] == 0, f"the intent was recorded AFTER the create: {seen[0]}"


def test_a_state_write_failure_SKIPS_the_order_rather_than_placing_it_unrecorded(
        harness, monkeypatch, tmp_path):
    """Safe-direction, matching every other gate in this codebase: skip, do not fire. An order we
    cannot record is an order nothing can ever cancel."""
    def boom(self, intent_id, **kw):
        raise OSError("disk full")
    monkeypatch.setattr(maker_state.MakerStateStore, "record_intent", boom)
    _run(monkeypatch)
    assert harness["client"].calls_of("create") == [], \
        "an order was placed that could not be durably recorded"


def test_a_completed_run_leaves_no_maybe_live_orders_in_the_durable_state(
        harness, monkeypatch, tmp_path):
    """Every cancelled order must be forgotten, or the next start refuses on a phantom."""
    _run(monkeypatch)
    state = maker_state.load_state_for_venue(
        "kalshi", base_path=str(tmp_path / "logs" / "maker_state.json"))
    assert state.maybe_live_orders == 0, state.orders


def test_a_completed_run_records_a_clean_exit(harness, monkeypatch, tmp_path):
    _run(monkeypatch)
    assert maker_state.load_state_for_venue(
        "kalshi", base_path=str(tmp_path / "logs" / "maker_state.json")).clean_exit is True


def test_a_venue_sweep_that_CANNOT_CONFIRM_leaves_the_crash_record_OPEN(
        harness, monkeypatch, tmp_path):
    """The teardown may only write `clean_exit` on the VENUE'S evidence, never on our own belief.

    ⛔ THIS TEST EXISTS BECAUSE MUTATION TESTING FOUND THE GATE UNPINNED: replacing
    `if swept_clean:` with `if True:` survived the entire suite. That mutant is the exact
    historical bug shape — a position reconciler reporting "confirmed flat" from a read it never
    actually parsed — and here it is worse than usual, because this flag is precisely what tells
    the recovery tool there is nothing to recover. A run that could
    not confirm the venue is empty, recorded as clean, is a silent strand.
    """
    client = harness["client"]
    real_list = client.get_resting_orders
    calls = {"n": 0}

    async def flaky(tickers=None):
        calls["n"] += 1
        if calls["n"] > 1:                 # the preflight listing succeeds; the teardown sweep does not
            raise RuntimeError("shape we cannot read — refusing to report nothing-resting")
        return await real_list(tickers)

    monkeypatch.setattr(client, "get_resting_orders", flaky)
    _run(monkeypatch)
    state = maker_state.load_state_for_venue(
        "kalshi", base_path=str(tmp_path / "logs" / "maker_state.json"))
    assert state.clean_exit is False, "a run that could not confirm the venue recorded a CLEAN exit"
    assert state.exit_status is None


def test_the_recovery_path_reads_a_completed_run_as_SAFE_TO_START(
        harness, monkeypatch, tmp_path):
    """The rails have to compose: what the maker writes is what recovery reads."""
    _run(monkeypatch)
    # Reads the VENUE file — pointing this at the legacy base would pass vacuously (no state
    # at that path assesses as a fresh "start"), pinning nothing about composition.
    plan = maker_state.assess_recovery_from_disk(
        maker_state.venue_path(str(tmp_path / "logs" / "maker_state.json"), "kalshi"),
        venue_orders=[], venue_positions={})
    assert plan.action == "start"


def test_the_maker_REFUSES_to_start_on_top_of_an_unresolved_prior_crash(
        harness, monkeypatch, tmp_path):
    """Starting would reset the order list and erase the only record of what is resting."""
    path = tmp_path / "logs" / "maker_state.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    durable.write_json_durable(str(path), {
        "run_id": "crashed", "pid": 1, "mode": "real", "started_ts": 1.0, "clean_exit": False,
        "exit_status": None, "tickers": ["KXA"],
        "orders": {"i1": {"order_id": "oid-ghost", "ticker": "KXA", "side": "yes",
                          "price": "0.40", "count": "1"}},
        "inventory": {}, "realized_pnl": "0", "loss_cap": "5",
    })
    _run(monkeypatch)
    assert harness["client"].calls_of("create") == [], \
        "the maker traded on top of an unresolved crash"
    refusals = [r for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "refused"]
    assert any("prior_run_unresolved" in r[2] for r in refusals), refusals


def test_the_durable_state_is_NOT_written_by_a_DRY_preview(harness, monkeypatch, tmp_path):
    """A DRY run places nothing. Letting it call `begin_run` would reset a crashed REAL run's
    order list — a preview destroying the evidence of a crash."""
    path = tmp_path / "logs" / "maker_state.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    durable.write_json_durable(str(path), {
        "run_id": "crashed", "pid": 1, "mode": "real", "started_ts": 1.0, "clean_exit": False,
        "exit_status": None, "tickers": ["KXA"],
        "orders": {"i1": {"order_id": "oid-ghost", "ticker": "KXA", "side": "yes",
                          "price": "0.40", "count": "1"}},
        "inventory": {}, "realized_pnl": "0", "loss_cap": "5",
    })
    monkeypatch.setattr(mm.config, "DRY_RUN", True)
    import sys
    argv = [a for a in _argv() if a != "--i-understand-real-money"]
    monkeypatch.setattr(sys, "argv", argv)
    import asyncio
    asyncio.run(mm.main())
    after = maker_state.load_state(str(path))
    assert after.run_id == "crashed", "a DRY preview overwrote a crashed real run's record"
    assert after.maybe_live_orders == 1
