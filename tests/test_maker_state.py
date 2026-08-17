"""Crash-durable maker state — the half of the safety story that has to work when nothing runs.

THE FAILURE MODE THIS IS DESIGNED AGAINST is SIGKILL, not a clean shutdown. `bot/kalshi/maker.py`
names SIGKILL as the one death that always strands live orders, and a memory-constrained host
OOM-kills often enough that this is routine. SIGKILL runs no `finally`, no `atexit`, no signal
handler — so anything that would be written *during* cleanup is, by definition, not written.

That has one consequence and it drives every test here: **the record must already be on disk
before it is needed.** In particular an order intent must be durable BEFORE the order is sent,
because the uncoverable window is exactly "we sent it and died before seeing the response" — the
same window a position reconciler exists to catch after the fact.

The second half is the loss cap. A cap held in a local variable dies with the process, so a
crash-restart loop resets the budget to zero every time. Here it accumulates on disk.
⚠️ SCOPE: this makes the cap SURVIVE. It does not make the cap's INPUT correct — whatever number
the process computes is what persists.
"""
from __future__ import annotations

import json
from decimal import Decimal

import pytest

from bot.core import durable, maker_state


def _store(tmp_path):
    return maker_state.MakerStateStore(path=str(tmp_path / "maker_state.json"))


# ── durability ───────────────────────────────────────────────────────────────────────────────

def test_begin_run_marks_the_run_as_NOT_cleanly_exited(tmp_path):
    """The flag is written pessimistically at the start and only cleared at a real exit. A crash
    therefore leaves `clean_exit=False` on disk by DOING NOTHING, which is the only thing SIGKILL
    reliably permits."""
    s = _store(tmp_path)
    s.begin_run("run-1", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    assert s.snapshot().clean_exit is False


def test_end_run_records_a_clean_exit(tmp_path):
    s = _store(tmp_path)
    s.begin_run("run-1", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.end_run("clean")
    assert s.snapshot().clean_exit is True


def test_every_mutation_is_written_durably(tmp_path, monkeypatch):
    """Not `open().write()`. A record that is only in the page cache when the box resets is not a
    record — and the reset is the case this whole module is for."""
    calls: list[str] = []
    real = durable.write_json_durable
    monkeypatch.setattr(maker_state.durable, "write_json_durable",
                        lambda p, o, **k: (calls.append(p), real(p, o, **k))[1])
    s = _store(tmp_path)
    s.begin_run("run-1", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.record_intent("i1", ticker="KXA", side="yes", price=Decimal("0.40"), count=1)
    s.set_inventory({"KXA": Decimal("2")})
    s.add_realized(Decimal("-0.25"))
    assert len(calls) == 4, calls


def test_an_intent_is_durable_BEFORE_the_order_is_sent(tmp_path):
    """The uncoverable window is 'sent, then died before the response'. Recording only after the
    venue answers means that window leaves NO trace at all — a live order nobody knows about."""
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.record_intent("i1", ticker="KXA", side="yes", price=Decimal("0.40"), count=1)
    # schema v2: the record lives under lanes.main (the durability property is unchanged)
    on_disk = json.loads(open(s.path).read())["lanes"]["main"]
    assert "i1" in on_disk["orders"]
    assert on_disk["orders"]["i1"]["order_id"] is None      # not yet confirmed by the venue


def test_an_unresolved_intent_counts_as_MAYBE_LIVE(tmp_path):
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.record_intent("i1", ticker="KXA", side="yes", price=Decimal("0.40"), count=1)
    assert s.snapshot().maybe_live_orders == 1


def test_recording_the_venue_order_id_keeps_it_live_and_attaches_the_id(tmp_path):
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.record_intent("i1", ticker="KXA", side="yes", price=Decimal("0.40"), count=1)
    s.record_placed("i1", "oid-9")
    snap = s.snapshot()
    assert snap.order_ids == ("oid-9",)
    assert snap.maybe_live_orders == 1


def test_clearing_an_order_removes_it(tmp_path):
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.record_intent("i1", ticker="KXA", side="yes", price=Decimal("0.40"), count=1)
    s.record_placed("i1", "oid-9")
    s.clear_order("oid-9")
    assert s.snapshot().maybe_live_orders == 0


def test_an_order_can_be_cleared_by_its_intent_id_too(tmp_path):
    """A create that returns 2xx with NO order_id is a real, recorded case. It must
    still be clearable, or the state file grows a permanent phantom."""
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.record_intent("i1", ticker="KXA", side="yes", price=Decimal("0.40"), count=1)
    s.clear_order("i1")
    assert s.snapshot().maybe_live_orders == 0


def test_inventory_survives_a_reload_from_disk(tmp_path):
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.set_inventory({"KXA": Decimal("-3"), "KXB": Decimal("0")})
    reloaded = maker_state.load_state(s.path)
    assert reloaded.inventory["KXA"] == Decimal("-3")
    assert "KXB" not in reloaded.inventory, "a zero position is not a position"


# ── the loss cap that survives process death ─────────────────────────────────────────────────

def test_realized_loss_accumulates_across_processes(tmp_path):
    """Today the cap is a local variable, so a crash-restart loop resets the budget every time.
    Two separate stores over one file must see one running total."""
    p = str(tmp_path / "maker_state.json")
    a = maker_state.MakerStateStore(path=p)
    a.begin_run("r1", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    a.add_realized(Decimal("-2.00"))
    b = maker_state.MakerStateStore(path=p)       # a fresh process
    b.add_realized(Decimal("-1.50"))
    assert b.snapshot().realized_pnl == Decimal("-3.50")
    assert b.snapshot().loss_to_date == Decimal("3.50")


def test_a_profit_reduces_the_loss_to_date_but_never_below_zero(tmp_path):
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.add_realized(Decimal("2.00"))
    assert s.snapshot().loss_to_date == Decimal("0")


def test_money_is_Decimal_all_the_way_to_disk(tmp_path):
    """House rule: prices, quantities and money are Decimal, serialised from their string form.
    A float round-trip through JSON would launder binary error into the cap's input."""
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.add_realized(Decimal("-0.07"))
    raw = json.loads(open(s.path).read())["lanes"]["main"]
    assert isinstance(raw["realized_pnl"], str)
    assert raw["realized_pnl"] == "-0.07"


def test_the_cap_is_breached_once_the_durable_loss_exceeds_it(tmp_path):
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.add_realized(Decimal("-5.01"))
    assert s.snapshot().cap_breached is True


def test_the_cap_is_not_breached_below_it(tmp_path):
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    s.add_realized(Decimal("-4.99"))
    assert s.snapshot().cap_breached is False


def test_a_zero_cap_means_disabled(tmp_path):
    s = _store(tmp_path)
    s.begin_run("r", mode="real", loss_cap=Decimal("0"), tickers=["KXA"], inventory={})
    s.add_realized(Decimal("-500"))
    assert s.snapshot().cap_breached is False


# ── reading it back ──────────────────────────────────────────────────────────────────────────

def test_load_state_of_a_missing_file_is_None(tmp_path):
    assert maker_state.load_state(str(tmp_path / "nope.json")) is None


def test_begin_run_REFUSES_to_clobber_an_unresolved_prior_crash(tmp_path):
    """`begin_run` resets the order list. If the previous run died holding orders, doing that
    silently destroys the only record of what is resting on the venue — turning a recoverable
    crash into an invisible one, which is strictly worse than the crash."""
    p = str(tmp_path / "maker_state.json")
    a = maker_state.MakerStateStore(path=p)
    a.begin_run("r1", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    a.record_intent("i1", ticker="KXA", side="yes", price=Decimal("0.4"), count=1)

    b = maker_state.MakerStateStore(path=p)
    with pytest.raises(maker_state.PriorRunUnresolved) as e:
        b.begin_run("r2", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    # ⛔ "maker_recover in msg" alone was VACUOUS: the Poly branch also contains that token
    # ("Do NOT use scripts.maker_recover"), so `is_poly = True` survived the whole suite and a
    # Kalshi operator would have been sent AWAY from the only tool that cancels their stray.
    # Pin the KALSHI branch by its invocation and by the Poly remedy's absence.
    msg = str(e.value)
    assert "-m scripts.maker_recover --execute" in msg, "the Kalshi remedy must be the tool"
    assert "poly_us_orders" not in msg, "a Kalshi record must not get the Poly remedy"


def test_a_POLY_records_refusal_names_the_poly_remedy_not_the_kalshi_tool(tmp_path):
    """The venue branch of the same refusal: a Poly-shaped record must point at the Poly
    procedure and must NOT instruct the Kalshi-only tool."""
    p = str(tmp_path / "maker_state.json")
    a = maker_state.MakerStateStore(path=p)
    a.begin_run("polymm-1", mode="real", loss_cap=Decimal("5"),
                tickers=["example-election-alpha-2026-11-03-yes"], inventory={})
    a.record_intent("i1", ticker="example-election-alpha-2026-11-03-yes", side="bid",
                    price=Decimal("0.84"), count=22)
    with pytest.raises(maker_state.PriorRunUnresolved) as e:
        maker_state.MakerStateStore(path=p).begin_run(
            "polymm-2", mode="real", loss_cap=Decimal("5"),
            tickers=["example-election-alpha-2026-11-03-yes"], inventory={})
    msg = str(e.value)
    assert "-m scripts.maker_recover --execute" not in msg
    assert "discards the durable loss ledger" in msg, "the delete advice needs the caveat"
    # This message has NO refused-count context (the state file does not record WHY the
    # sweep failed), so unlike the teardown's gated copy it may offer terminal-but-listed
    # only as a POSSIBILITY — never as a diagnosis, and never with the unconditional
    # "not a wrong-account signal" claim (an unattributable order — listed, no readable
    # id — leaves the SAME record, and the preflight's id-extraction silently drops
    # exactly those, so "the preflight already confirmed flat" cannot carry it).
    # Confirmation points at the API dump, same credentials, never the UI alone.
    # Pinned by INSTRUCTION, not just label.
    assert "poly_us_orders" in msg, "confirmation must point at the API dump"
    assert "Polymarket UI" not in msg, "the UI is the wrong-account-blind observation"
    assert "terminal-but-listed" in msg, "the nothing-resting case needs naming"
    assert "possibility to confirm, not a diagnosis" in msg, (
        "without refused-count context the ghost may only be offered, never asserted")
    assert "Do NOT use scripts.maker_recover" in msg, "the Kalshi-tool warning must survive"


def test_poly_names_separates_the_venues_by_name_shape():
    """The ONE copy of the venue heuristic (three operator remedies branch on it)."""
    kalshi = maker_state._to_state({
        "run_id": "real-1", "tickers": ["KXMLBGAME-26JUL31NYYBOS-NYY"],
        "orders": {"i1": {"order_id": "K1", "ticker": "KXWCGAME-X"}}, "inventory": {}})
    assert maker_state.poly_names(kalshi) == ()
    mixed = maker_state._to_state({
        "run_id": "real-2", "tickers": ["KXA"],
        "orders": {}, "inventory": {"example-election-beta-2026-11-03-no": "-12"}})
    assert maker_state.poly_names(mixed) == ("example-election-beta-2026-11-03-no",)


def test_the_refusal_can_be_overridden_deliberately(tmp_path):
    p = str(tmp_path / "maker_state.json")
    a = maker_state.MakerStateStore(path=p)
    a.begin_run("r1", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    a.record_intent("i1", ticker="KXA", side="yes", price=Decimal("0.4"), count=1)
    maker_state.MakerStateStore(path=p).begin_run(
        "r2", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], allow_unresolved=True, inventory={})


def test_a_RESOLVED_prior_crash_does_not_block_the_next_run(tmp_path):
    """After `maker_recover` marks it recovered, the next run must start without ceremony —
    otherwise the guard wedges the maker and gets deleted."""
    p = str(tmp_path / "maker_state.json")
    a = maker_state.MakerStateStore(path=p)
    a.begin_run("r1", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    a.record_intent("i1", ticker="KXA", side="yes", price=Decimal("0.4"), count=1)
    a.clear_order("i1")
    a.end_run("recovered")
    maker_state.MakerStateStore(path=p).begin_run(
        "r2", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})


def test_a_clean_prior_run_with_no_orders_does_not_block(tmp_path):
    p = str(tmp_path / "maker_state.json")
    a = maker_state.MakerStateStore(path=p)
    a.begin_run("r1", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})
    a.end_run("clean")
    maker_state.MakerStateStore(path=p).begin_run(
        "r2", mode="real", loss_cap=Decimal("5"), tickers=["KXA"], inventory={})


def test_a_CORRUPT_state_file_raises_rather_than_looking_like_a_fresh_start(tmp_path):
    """A truncated state file is the SIGKILL-mid-write artefact. Reading it as 'no prior run' is
    the single most dangerous possible interpretation — it says 'nothing to recover' at exactly
    the moment there is something to recover."""
    p = tmp_path / "maker_state.json"
    p.write_text("{trunca")
    with pytest.raises(durable.StateCorrupt):
        maker_state.load_state(str(p))


# ── lanes: one shared per-venue file, multiple concurrent runs ───────────────────────────────

def test_two_lanes_do_not_clobber_each_other(tmp_path):
    """The property the shared ledger exists for: a park probe's store writes must preserve the
    main run's record byte-for-byte, and vice versa — through interleaved mutations."""
    p = str(tmp_path / "maker_state.json")
    main = maker_state.MakerStateStore(path=p, lane="main")
    park = maker_state.MakerStateStore(path=p, lane="park")
    main.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("5"),
                   tickers=["a"], inventory={})
    park.begin_run("polymm-real-2-bb", mode="real", loss_cap=Decimal("2"),
                   tickers=["b"], inventory={})
    main.record_intent("i1", ticker="a", side="yes", price=Decimal("0.40"), count=1)
    park.add_realized(Decimal("-0.50"))
    main.add_realized(Decimal("-1.25"))
    park.end_run("clean")
    # each lane's record is intact on disk, unaffected by the other's writes
    m = maker_state.load_state(p, lane="main")
    k = maker_state.load_state(p, lane="park")
    assert m is not None and m.run_id == "polymm-real-1-aa"
    assert m.maybe_live_orders == 1 and m.realized_pnl == Decimal("-1.25")
    assert not m.clean_exit
    assert k is not None and k.run_id == "polymm-real-2-bb"
    assert k.clean_exit and k.realized_pnl == Decimal("-0.50")


def test_legacy_flat_record_reads_as_the_main_lane_and_migrates_on_write(tmp_path):
    p = str(tmp_path / "maker_state.json")
    legacy = {"run_id": "polymm-real-9-ff", "pid": 1, "mode": "real", "started_ts": 1.0,
              "clean_exit": True, "tickers": ["x"], "orders": {},
              "inventory": {}, "realized_pnl": "-2.00", "loss_cap": "5"}
    (tmp_path / "maker_state.json").write_text(json.dumps(legacy))
    # reads: the flat record IS the main lane; other lanes see nothing
    m = maker_state.load_state(p, lane="main")
    assert m is not None and m.realized_pnl == Decimal("-2.00")
    assert maker_state.load_state(p, lane="park") is None
    # a park-lane write migrates the file to v2 WITHOUT touching main's record
    park = maker_state.MakerStateStore(path=p, lane="park")
    park.begin_run("polymm-real-3-cc", mode="real", loss_cap=Decimal("2"),
                   tickers=["b"], inventory={})
    doc = json.loads(open(p).read())
    assert doc["schema"] == 2
    assert doc["lanes"]["main"]["realized_pnl"] == "-2.00"
    assert doc["lanes"]["park"]["run_id"] == "polymm-real-3-cc"


def test_account_carried_sums_across_lanes(tmp_path):
    """One account, one ratchet: the budget prices off Σ lanes, so a lane cannot escape another
    lane's losses by being a separate process."""
    p = str(tmp_path / "maker_state.json")
    a = maker_state.MakerStateStore(path=p, lane="main")
    a.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("5"), tickers=["a"],
                inventory={})
    a.add_realized(Decimal("-1.00"))
    b = maker_state.MakerStateStore(path=p, lane="park")
    b.begin_run("polymm-real-2-bb", mode="real", loss_cap=Decimal("5"), tickers=["b"],
                inventory={})
    b.add_realized(Decimal("-0.75"))
    assert maker_state.account_carried(p) == Decimal("-1.75")
    assert set(maker_state.load_all_lanes(p)) == {"main", "park"}
    # missing file → empty, zero (a fresh account has no history, not an error)
    assert maker_state.load_all_lanes(str(tmp_path / "nope.json")) == {}
    assert maker_state.account_carried(str(tmp_path / "nope.json")) == Decimal("0")


def test_write_rereads_the_file_so_a_sibling_write_between_ours_survives(tmp_path):
    """The ctor's view of other lanes goes stale the moment a sibling writes; _write must merge
    against the CURRENT file, not the ctor snapshot."""
    p = str(tmp_path / "maker_state.json")
    main = maker_state.MakerStateStore(path=p, lane="main")
    main.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("5"), tickers=["a"],
                   inventory={})
    # park store constructed AFTER main's begin_run, then main writes AGAIN after park's write
    park = maker_state.MakerStateStore(path=p, lane="park")
    park.begin_run("polymm-real-2-bb", mode="real", loss_cap=Decimal("2"), tickers=["b"],
                   inventory={})
    main.add_realized(Decimal("-0.10"))      # merges against the file that now holds park
    assert maker_state.load_state(p, lane="park").run_id == "polymm-real-2-bb"
    assert maker_state.load_state(p, lane="main").realized_pnl == Decimal("-0.10")


# ── slate-scoped recovery: the prerequisite for running isolated lanes side by side ──────────

def _lane_state(run_id="polymm-real-9-zz", *, clean_exit=True, tickers=(), orders=None,
                inventory=None, realized="-0.00", loss_cap="5"):
    rec = {"run_id": run_id, "pid": 1, "mode": "real", "started_ts": 1.0,
           "clean_exit": clean_exit, "tickers": list(tickers), "orders": orders or {},
           "inventory": inventory or {}, "realized_pnl": realized, "loss_cap": loss_cap}
    return maker_state._to_state(rec)


def test_scoped_cancel_plan_never_touches_a_sibling_order(tmp_path):
    """The money-path point of scoping: 'recovery' for lane B must not plan to cancel lane A's
    LIVE order. Foreign and slug-unreadable orders are reported, never cancelled."""
    # Real Poly open-order shape: the id key is `id` (maker.py pins a prior bug where a fixture
    # written from the wrong key passed the whole suite) [review nit 2].
    orders = [{"id": "mine", "marketSlug": "a"},
              {"id": "theirs", "marketSlug": "z"},
              {"id": "noslug"}]
    plan = maker_state.assess_recovery(None, orders, {}, slate={"a"}, other_lanes={})
    assert plan.cancel_order_ids == ("mine",)
    assert "NOT ours to cancel" in plan.detail
    # unscoped (legacy) still cancels everything — the pin that slate=None is unchanged
    legacy = maker_state.assess_recovery(None, orders, {})
    assert set(legacy.cancel_order_ids) == {"mine", "theirs", "noslug"}


def test_slate_collision_refuses(tmp_path):
    """Two processes must never quote one book: a sibling lane whose record claims one of OUR
    slugs refuses outright — inventory claim, order claim, or (crashed lane) whole-slate claim."""
    inv = _lane_state(inventory={"a": Decimal("3")})
    plan = maker_state.assess_recovery(None, [], {}, slate={"a", "b"},
                                       other_lanes={"main": inv})
    assert (plan.action, plan.reason) == ("refuse", "slate_collision")
    crashed = _lane_state(clean_exit=False, tickers=["b"])
    plan2 = maker_state.assess_recovery(None, [], {}, slate={"a", "b"},
                                        other_lanes={"main": crashed})
    assert (plan2.action, plan2.reason) == ("refuse", "slate_collision")
    # a CLEANLY exited flat sibling that merely QUOTED b once does not collide
    old = _lane_state(clean_exit=True, tickers=["b"])
    assert maker_state.assess_recovery(None, [], {}, slate={"a", "b"},
                                       other_lanes={"main": old}).action == "start"


def test_foreign_position_is_reported_never_refused(tmp_path):
    """A foreign-slug position attributed to a sibling lane is info; one NO lane explains is an
    ORPHAN — loud, but with scoping it must not block an unrelated start."""
    sib = _lane_state(inventory={"z": Decimal("5")})
    plan = maker_state.assess_recovery(
        None, [], {"z": "5", "ghost": "2"}, slate={"a"}, other_lanes={"park": sib})
    assert plan.action == "start"
    assert "attributed to sibling lanes" in plan.detail
    assert "ORPHAN" in plan.detail and "ghost" in plan.detail
    # same venue state UNscoped refuses on unknown_position — the legacy pin
    legacy = maker_state.assess_recovery(None, [], {"z": "5", "ghost": "2"})
    assert (legacy.action, legacy.reason) == ("refuse", "unknown_position")


def test_a_position_on_MY_slate_keeps_the_unknown_refusal(tmp_path):
    plan = maker_state.assess_recovery(None, [], {"a": "4"}, slate={"a"}, other_lanes={})
    assert (plan.action, plan.reason) == ("refuse", "unknown_position")


def test_sibling_cap_breach_refuses_account_wide(tmp_path):
    """One account, one ratchet: lane A's breached cap halts lane B's start too."""
    breached = _lane_state(realized="-6.00", loss_cap="5")
    plan = maker_state.assess_recovery(None, [], {}, slate={"b"},
                                       other_lanes={"main": breached})
    assert (plan.action, plan.reason) == ("refuse", "loss_cap")
    # the detail now states its true scope (scoped starts only; the engine's live account
    # read is the always-on rail)
    assert "SCOPE" in plan.detail


# ── the account-wide ratchet: pins for the ways one lane can borrow another lane's budget ────

def test_account_loss_never_nets_profit_against_loss(tmp_path):
    """A lane up $4 must NOT buy another lane $4 of loss budget: the ratchet sums
    per-lane LOSS (floored at zero), never signed realized."""
    p = str(tmp_path / "maker_state.json")
    a = maker_state.MakerStateStore(path=p, lane="main")
    a.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("5"), tickers=["a"],
                inventory={})
    a.add_realized(Decimal("4.00"))                  # profitable lane
    b = maker_state.MakerStateStore(path=p, lane="park")
    b.begin_run("polymm-real-2-bb", mode="real", loss_cap=Decimal("5"), tickers=["b"],
                inventory={})
    b.add_realized(Decimal("-2.50"))                 # losing lane
    assert maker_state.account_carried(p) == Decimal("1.50")     # signed (reporting)
    assert maker_state.account_loss(p) == Decimal("2.50")        # ratchet: losses only


def test_live_sibling_lanes_ignores_dead_records(tmp_path):
    """Lane records are never deleted, so scoping must key on a LIVE sibling (unclean
    exit, orders, or inventory) — a cleanly-exited flat lane is history, not a sibling."""
    p = str(tmp_path / "maker_state.json")
    dead = maker_state.MakerStateStore(path=p, lane="park")
    dead.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("2"), tickers=["b"],
                   inventory={})
    dead.end_run("clean")
    assert maker_state.live_sibling_lanes(p, "main") == {}
    # …but a clean exit that still RECORDS inventory is live (a carried anchor)
    held = maker_state.MakerStateStore(path=p, lane="anchor")
    held.begin_run("polymm-real-2-bb", mode="real", loss_cap=Decimal("2"), tickers=["c"],
                   inventory={"c": Decimal("7")})
    held.end_run("clean")
    assert set(maker_state.live_sibling_lanes(p, "main")) == {"anchor"}


def test_corrupt_lane_value_raises_never_vanishes(tmp_path):
    """A v2 document with a torn lane must raise StateCorrupt — filtering the bad lane
    out would silently delete its carried loss, slate claim, and crash record."""
    p = tmp_path / "maker_state.json"
    p.write_text(json.dumps({"schema": 2, "lanes": {"main": {"run_id": "polymm-real-1-aa"},
                                                    "park": "TRUNCATED"}}))
    with pytest.raises(durable.StateCorrupt):
        maker_state.load_all_lanes(str(p))
    p.write_text(json.dumps({"schema": 2, "lanes": None}))
    with pytest.raises(durable.StateCorrupt):
        maker_state.load_all_lanes(str(p))


def test_begin_run_refuses_a_live_same_lane_sibling(tmp_path, monkeypatch):
    """An open record whose pid is ALIVE (and not us) is a running maker — a second
    same-lane writer would last-writer-win the ledger. No override."""
    p = str(tmp_path / "maker_state.json")
    a = maker_state.MakerStateStore(path=p, lane="main")
    a.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("5"), tickers=["a"],
                inventory={})
    monkeypatch.setattr(maker_state, "_pid_alive", lambda pid: True)
    monkeypatch.setattr(maker_state.os, "getpid", lambda: 999999)   # we are NOT that pid
    b = maker_state.MakerStateStore(path=p, lane="main")
    with pytest.raises(maker_state.PriorRunUnresolved, match="STILL RUNNING"):
        b.begin_run("polymm-real-2-bb", mode="real", loss_cap=Decimal("5"), tickers=["a"],
                    inventory={}, allow_unresolved=True)   # even the override must not bypass


def test_own_off_slate_position_refuses_not_orphans(tmp_path):
    """OUR lane's recorded position off this run's slate would be left unmanaged —
    refuse (like the adopt gate), never demote to an orphan note."""
    own = _lane_state(inventory={"y": Decimal("20")})
    plan = maker_state.assess_recovery(own, [], {"y": "20"}, slate={"x"}, other_lanes={})
    assert (plan.action, plan.reason) == ("refuse", "own_position_off_slate")


def test_apply_carry_preserves_scoped_notes(tmp_path):
    """Carries are the NORMAL start mode; converting recover→start must not delete the
    orphan/foreign warnings the scoped assessment attached to the plan."""
    sib = _lane_state(inventory={"z": Decimal("5")})
    own = _lane_state(run_id="polymm-real-8-aa", inventory={"x": Decimal("10")})
    plan = maker_state.assess_recovery(
        own, [], {"x": "10", "ghost": "2"}, slate={"x"}, other_lanes={"park": sib})
    assert plan.action == "recover" and "ORPHAN" in plan.detail
    out = maker_state.apply_carry(plan, {"x": Decimal("10")}, {"x": "10", "ghost": "2"})
    assert out.action == "start"
    assert "ORPHAN" in out.detail       # the warning survived the conversion


def test_lock_times_out_instead_of_blocking_forever(tmp_path, monkeypatch):
    """A HUNG sibling holding the flock must fail this mutation loudly, never freeze
    the event loop (no quotes, no kill-switch checks) behind an unbounded LOCK_EX."""
    import fcntl
    p = str(tmp_path / "maker_state.json")
    monkeypatch.setattr(maker_state._FileLock, "_TIMEOUT_S", 0.2)
    holder = open(f"{p}.lock", "w")
    fcntl.flock(holder.fileno(), fcntl.LOCK_EX)     # simulate the hung sibling
    s = maker_state.MakerStateStore(path=p, lane="main")
    with pytest.raises(maker_state.StateLocked):
        s.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("5"), tickers=["a"],
                    inventory={})
    fcntl.flock(holder.fileno(), fcntl.LOCK_UN)
    holder.close()


def test_engine_lifetime_axis_sees_a_sibling_lanes_live_loss(tmp_path):
    """N lanes must not mean N times the cap. Two lanes on one ledger: lane B's loss must tighten lane
    A's lifetime axis WITHIN the run (live read), not only at the next start."""
    p = str(tmp_path / "maker_state.json")
    a = maker_state.MakerStateStore(path=p, lane="main")
    a.begin_run("polymm-real-1-aa", mode="real", loss_cap=Decimal("3"), tickers=["a"],
                inventory={})
    b = maker_state.MakerStateStore(path=p, lane="park")
    b.begin_run("polymm-real-2-bb", mode="real", loss_cap=Decimal("3"), tickers=["b"],
                inventory={})
    b.add_realized(Decimal("-2.00"))     # sibling loses DURING our run
    a.add_realized(Decimal("-1.50"))     # own loss
    # the engine's read: max(own snapshot, account Σ) = max(1.50, 3.50) = 3.50 > cap 3
    assert maker_state.account_loss(p) == Decimal("3.50")
