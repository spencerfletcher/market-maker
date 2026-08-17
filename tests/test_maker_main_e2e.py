"""`main()` driven end-to-end against an in-memory Kalshi — the coverage that did not exist.

WHY THIS FILE. `main()` is ~490 lines and had **zero executable coverage**. That is not a gap in
passing: it is where the defects have actually lived. The `inv` rebinding regression lived in
`main()`, its fix lives in `main()`, and until now the only thing pinning that fix was an AST check
over `main()`'s own source — which cannot tell whether the code RUNS, only that it is written.

Everything here executes the real `main()`. The venue, the book, the scanner, the clock and the CSV
are fakes; the orchestration under test is not.

⚠️ These run with `real=True` deliberately. DRY is not a rehearsal — `create_order` returns no
`order_id` in DRY, so tracking, cancellation, fill attribution and markout never execute, and a DRY
preview always reports `quotes=0 fills=0`. Exercising those paths outside real money is the entire
point of the fakes.
"""
import asyncio
import sys

import pytest

from bot.kalshi import maker as mm
from tests.mm_fakes import (FakeBook, FakeKalshiClient, FakeMarket, FakeScanner,
                            FakeTradeFeed)

T = "KXNEXTTEAMNBA-26LJAM-MIA"
T2 = "KXNEXTTEAMNBA-26JKUMINGA0-CLE"


class _Clock:
    """Replaces the `time` module inside `maker` (it only ever calls `time.time()`).

    The loop is `while time.time() - t0 < args.seconds`, so on a real clock the cycle count depends
    on machine speed and every CSV timestamp differs run to run. Advancing only when the patched
    `asyncio.sleep` fires makes both deterministic."""

    def __init__(self, start=1_700_000_000.0):
        self.now = start

    def time(self):
        return self.now


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """Wire `main()` to fakes and hand back the pieces a test needs to assert on."""
    clock = _Clock()
    client = FakeKalshiClient(clock=clock)
    book = FakeBook({T: (0.40, 0.45, 120.0, 35.0), T2: (0.23, 0.31, 5.0, 35.0)})

    real_sleep = asyncio.sleep          # captured BEFORE the patch, or _sleep recurses into itself
    pending: list[list[float]] = []     # one cell per sleeping task: its virtual wake time
    # Hooks run on every virtual sleep — the only place a test can observe the run MID-FLIGHT
    # (e.g. to check the CSV is flushed rather than buffered until close).
    on_sleep: list = []
    hook_errors: list = []              # surfaced by the fixture so a broken hook cannot pass silently

    async def _sleep(seconds, *a, **kw):
        """Virtual time. Sleeps do not wait — the clock jumps to the EARLIEST pending deadline.

        `main()` runs three concurrent sleepers (the quote loop, the markout ticker, the book task),
        so the naive version — each sleeper adding its own duration to a shared clock — let whichever
        task woke first drag time forward for everybody, and the run ended before it quoted. Jumping
        to the minimum deadline is the least-aggressive advance and is what a real event loop does:
        the next task to wake is the one with the nearest deadline.

        ⚠️ LOAD-BEARING PRECONDITION: `pending` sees only tasks *currently inside* `_sleep`. A task
        that is runnable but not yet parked is invisible to the `all(...)` check, so the clock can
        jump past its deadline. That is safe here ONLY because no fake await ever yields to the
        event loop — every fake coroutine returns without awaiting, so a single `real_sleep(0)` is
        enough for `markout_ticker` to run all the way to its own sleep and register. Introduce any
        genuinely yielding await (an `asyncio.Queue`, a mocked `httpx`, `asyncio.to_thread`) and the
        first jump goes straight to the requote deadline, silently degrading the markout ticker from
        1 Hz to once per requote interval — which is the very defect commit 8fffb3f fixed. If you
        add a yielding fake, replace this with a real virtual-time scheduler."""
        for _hook in on_sleep:
            try:
                _hook()          # a raising hook would silently kill the task it fires inside
            except Exception as _e:
                hook_errors.append(_e)
        cell = [clock.now + float(seconds)]
        pending.append(cell)
        try:
            while clock.now < cell[0]:
                await real_sleep(0)                     # let every other task make progress
                if pending and all(c[0] > clock.now for c in pending):
                    clock.now = min(c[0] for c in pending)   # everyone is parked: skip ahead
        finally:
            pending.remove(cell)

    monkeypatch.setattr(mm, "time", clock)
    monkeypatch.setattr(asyncio, "sleep", _sleep)
    monkeypatch.setattr(mm, "KalshiClient", lambda *a, **k: client)
    # ⚠️ Expiry must be set and must be AFTER the fake clock (1_700_000_000 = 2023-11-14), or
    # `_phase` returns "unknown" and `eligible_tickers` fail-closes — correct behaviour, and it
    # silently produced a run that selected nothing. Far future ⇒ "long-dated", the clean lane.
    exp = "2024-02-12T22:13:20Z"
    monkeypatch.setattr(mm, "KalshiScanner",
                        FakeScanner([FakeMarket(T, volume_24h=1_700_000.0,
                                                expected_expiration_time=exp),
                                     FakeMarket(T2, volume_24h=1_700.0,
                                                expected_expiration_time=exp)]))
    monkeypatch.setattr(mm, "KalshiOrderBookCache", lambda *a, **k: book)
    # The trade tape is a second live socket. Faked for the same reason the book
    # is — and see FakeTradeFeed's docstring: the REAL one's 3s reconnect sleep drags the virtual
    # clock, which manufactures a failure in the fill-poll-ordering invariant that production does
    # not have. `tapes` is a list because a test asserts the maker built exactly one.
    tapes: list = []

    def _make_tape(*a, **k):
        t = FakeTradeFeed(*a, **k)
        tapes.append(t)
        return t

    monkeypatch.setattr(mm, "KalshiTradeFeed", _make_tape)
    # The PROD guard and the real-money refusal are genuine gates; satisfy them honestly rather than
    # bypassing them, so the test exercises the same path an operator would.
    monkeypatch.setattr(mm.config, "DRY_RUN", False)
    monkeypatch.setattr(mm.config, "KALSHI_ENV", "prod")
    monkeypatch.setattr(mm, "_BOOK_WARMUP_S", 0.0)
    monkeypatch.setattr(mm, "_POS_BACKOFF_S", 0.0)
    # `logs/kalshi_live_mm.csv` is a RELATIVE path, so chdir sandboxes it.
    (tmp_path / "logs").mkdir()
    monkeypatch.chdir(tmp_path)
    return {"clock": clock, "client": client, "book": book, "on_sleep": on_sleep,
            "hook_errors": hook_errors, "tapes": tapes,
            "csv": tmp_path / "logs" / "kalshi_live_mm.csv"}


def _argv(**over):
    a = {"--seconds": "6", "--markets": "1", "--size": "1", "--inv-cap": "3",
         "--loss-cap": "3", "--requote-s": "6", "--improve-ticks": "1", "--inv-coef": "0"}
    a.update({k: str(v) for k, v in over.items()})
    out = ["kalshi_live_mm"]
    for k, v in a.items():
        out += [k, v]
    return out + ["--series", "KXNEXTTEAMNBA", "--maker-free-only",
                  "--confirm", "--i-understand-real-money"]


def _run(monkeypatch, **over):
    monkeypatch.setattr(sys, "argv", _argv(**over))
    asyncio.run(mm.main())


def _rows(csv_path):
    if not csv_path.exists():
        return []
    import csv as _csv
    with open(csv_path, newline="") as fh:
        return [r for r in _csv.reader(fh) if r and r[0] != "ts"]


def _events(csv_path):
    return [r[1] for r in _rows(csv_path) if len(r) > 1]


# ── the run happens at all ───────────────────────────────────────────────────────────────────────

def test_a_full_run_quotes_and_writes_its_ledger(harness, monkeypatch):
    _run(monkeypatch)
    ev = _events(harness["csv"])
    assert "balance_start" in ev and "end" in ev
    assert "quote_ctx" in ev, "the replay row must be written for every quote cycle"
    assert harness["client"].calls_of("create"), "main() must actually place orders in real mode"


def test_every_order_is_cancelled_and_the_venue_is_swept(harness, monkeypatch):
    """The tool must not leave orders resting. Teardown cancels tracked ids, THEN asks the
    venue what is still resting and cancels that too.

    ⚠️ THE SWEEP MUST BE ASSERTED *AFTER* THE LAST CANCEL. An earlier version asserted only
    `"get_resting_orders" in c.calls`, which the PREFLIGHT listing satisfies on its own — so
    deleting `await _sweep_venue_strays()` from the teardown left the whole suite green. Worse,
    `resting_ids() == []` could not catch it either, because `_cancel_everything` cancels the same
    ids the sweep would: each mechanism was 'covered' only by the other, and only deleting BOTH
    went red. The sweep is the one that reaches a stray whose create-response was LOST — the only
    kind the tracked cancel cannot see — so it needs its own discriminator."""
    _run(monkeypatch)
    c = harness["client"]
    assert c.resting_ids() == [], "no order may remain resting after main() returns"
    cancels = [i for i, x in enumerate(c.calls) if x.startswith("cancel:")]
    lists = [i for i, x in enumerate(c.calls) if x.startswith("get_resting_orders")]
    assert cancels, "expected the run to have placed and cancelled orders"
    assert any(i > cancels[-1] for i in lists), (
        "the venue stray sweep must LIST after the last tracked cancel — a listing that only "
        "happens at preflight does not reach a lost-create-response stray")


def test_the_stray_sweep_does_not_touch_an_unrelated_market(harness, monkeypatch):
    """The sweep cancels every resting order in the TARGET markets — that is by design and is a
    documented footgun — but it must not reach beyond them. An unscoped
    `get_resting_orders()` in the teardown would cancel a position the operator is running in a
    market this process never quoted. The scoping argument is the only thing preventing that."""
    c = harness["client"]
    c.orders["oid-external"] = {"ticker": "KXUNRELATED-MARKET", "action": "buy",
                                "price": 0.30, "count": 1, "resting": True}
    _run(monkeypatch)
    assert "oid-external" in c.resting_ids(), (
        "the teardown sweep cancelled an order in a market this run never targeted")
    assert "cancel:oid-external" not in c.calls


# ── the ordering invariant that several review rounds were about ─────────────────────────────────

def test_every_order_is_fill_polled_between_its_create_and_its_cancel(harness, monkeypatch):
    """`quote()` starts by cancelling, and the filled/unfilled decision reads a set only the fill
    poll populates. An order cancelled with no poll since it was PLACED is written
    `cancelled_unfilled` even if it filled — the defect that mislabelled ~95% of fills.

    ⚠️ TWO WEAKER VERSIONS OF THIS TEST BOTH PASSED AGAINST THE DEFECT, so the assertion below is
    about TIME, not order. (1) `calls.index("get_fills") < first cancel` over a ONE-cycle run is
    vacuous: the first in-loop `_cancel_all` has nothing to cancel, so the only cancels are at
    teardown, after a poll that happens anyway. (2) "some poll lies between this order's create and
    its cancel" ALSO passes with the poll moved after `_quote` — cycle 1 still polls after placing,
    just a full requote interval before the cancel. Neither expresses the real invariant.

    The invariant is that **no unpolled fill window may precede a cancel**: a fill landing between
    the last poll and the cancel is invisible to `filled_oids`. Every call within one cycle shares a
    virtual timestamp, so requiring the most recent poll to be in the SAME cycle as the cancel is
    exactly that statement — and it goes red when the poll moves after `_quote`."""
    _run(monkeypatch, **{"--seconds": 8})           # ≥2 cycles, so cycle 1's orders reach a cancel
    c = harness["client"]
    calls = c.calls
    polls = [i for i, x in enumerate(calls) if x == "get_fills"]
    cancels = [i for i, x in enumerate(calls) if x.startswith("cancel:")]
    assert len(cancels) >= 2, "expected orders from more than one cycle to be cancelled"
    assert polls, "the run never polled fills at all"
    for idx in cancels:
        prior = [p for p in polls if p < idx]
        assert prior, f"cancel at call {idx} had no preceding fill poll"
        gap = c.at_call(idx) - c.at_call(prior[-1])
        assert gap == 0.0, (
            f"{calls[idx]} was issued {gap:g}s after the last fill poll — a fill landing in that "
            f"window is absent from filled_oids and gets written cancelled_unfilled. The poll must "
            f"run in the same cycle as the cancel, before it.")


# ── THE REGRESSION: inv must reach the session, or the caps are inert ────────────────────────────

def test_the_inventory_cap_binds_END_TO_END(harness, monkeypatch):
    """THE `inv` REBINDING REGRESSION, caught by execution rather than by reading source.

    `_mark_step` returns a FRESH dict each cycle, so `main()` must write it back into the session or
    `quote()` reads zeros forever and `--inv-cap` never stops anything.

    ⚠️ The cap must be reached BY A FILL, not by pre-setting the venue position: `inv[t] = c -
    base[t]`, so a position that already exists at startup is baseline and reads as flat — by
    design, "only OUR fills from here count". An earlier version of this test set the position up
    front, measured zero inventory, and would have passed with the write-back deleted.

    So: cycle 1 quotes both sides and its BUY fills for the full cap. Cycle 2 must quote the sell
    side only. With `sess.sync_inventory(inv)` removed, cycle 2 quotes a second buy straight
    through the cap and this fails."""
    c = harness["client"]
    real_create = c.create_order

    async def create_and_fill(ticker, side, action, count, price, **kw):
        res = await real_create(ticker, side, action, count, price, **kw)
        if action == "buy" and c.position_of(ticker) < 3.0:
            c.fill(res["order_id"], count=3.0, ts=harness["clock"].now)   # straight to the cap
        return res

    c.create_order = create_and_fill
    _run(monkeypatch, **{"--seconds": 8})           # 8s @ 6s requote = exactly two cycles
    creates = c.calls_of("create")
    buys = [x for x in creates if ":buy@" in x]
    sells = [x for x in creates if ":sell@" in x]
    assert len(sells) >= 2, "expected at least two quote cycles"
    assert len(buys) == 1, (
        f"the cap must block the second cycle's BUY, got {len(buys)} buys — if >1, sess.inv never "
        f"received the venue's position and --inv-cap/--inv-coef are inert")


def test_below_the_cap_both_sides_are_quoted(harness, monkeypatch):
    """The control for the test above: the cap must not be blocking everything for some other
    reason. Flat inventory ⇒ both sides."""
    _run(monkeypatch)
    creates = harness["client"].calls_of("create")
    assert any(":buy@" in c for c in creates) and any(":sell@" in c for c in creates)


# ── the only automatic money rail ────────────────────────────────────────────────────────────────

def test_the_loss_cap_HALTS_the_run_when_the_market_moves_against_us(harness, monkeypatch):
    """The loss kill-switch, executed. Mutating `if pnl < -args.loss_cap` to `if False` previously
    survived the ENTIRE suite — the tool's one automatic money rail was pinned by nothing.

    Timing matters and is the reason this test looks fussy. `_mark_step` books a newly-seen fill at
    the CURRENT mid, so P&L is 0 at the moment the fill is first marked and accrues only on a LATER
    adverse move. So: cycle 1 fills, cycle 2 books it at 0.425, and only from cycle 3 — with the
    book moved hard against the long — does the mark go negative."""
    c, book = harness["client"], harness["book"]
    real_create, seen = c.create_order, {"positions": 0}
    real_positions = c.get_positions

    async def create_and_fill(ticker, side, action, count, price, **kw):
        res = await real_create(ticker, side, action, count, price, **kw)
        if action == "buy" and c.position_of(ticker) == 0:
            c.fill(res["order_id"], count=3.0, ts=harness["clock"].now)
        return res

    async def positions_then_move_the_market():
        out = await real_positions()
        seen["positions"] += 1
        if seen["positions"] >= 3:          # after the fill has been booked at its own mid
            book.set(T, 0.08, 0.12, 120.0, 35.0)
        return out

    c.create_order, c.get_positions = create_and_fill, positions_then_move_the_market
    _run(monkeypatch, **{"--seconds": 14, "--loss-cap": "0.5"})    # 3 cycles @ 6s
    rows = _rows(harness["csv"])
    halts = [r for r in rows if len(r) > 2 and r[1] == "halt"]
    assert halts, "a long marked far below its entry must trip the loss cap"
    assert halts[0][2] == "loss_cap", f"halted for the wrong reason: {halts[0][2]}"
    assert c.resting_ids() == [], "a loss-cap halt must still cancel everything"


def test_the_final_headline_never_reports_an_unmarkable_SHORT_as_a_profit(harness, monkeypatch):
    """The teardown headline had the same valuation defect as the loss cap, one screen apart.

    `final_pnl` EXCLUDED unmarkable inventory while keeping its cash, so a short whose book went
    one-sided printed its sale proceeds as pure profit. Because the in-loop cap was fixed first,
    the two numbers were briefly built on opposite conventions and could print with OPPOSITE SIGNS
    in one run: a kill-switch halt on a loss, immediately followed by a `DONE. marked P&L=+…`
    headline claiming a profit of similar magnitude.

    ⚠️ THE FILL MUST BE BOOKED BEFORE THE BOOK BREAKS, or this test proves nothing. If the book
    goes one-sided in the same cycle as the fill, the sale is never booked, cash stays 0, and the
    headline is 0 under BOTH the correct and the defective code — an earlier version asserted only
    `<= 0` and passed with the defect restored. So: fill in cycle 1, let cycle 2 book it at a real
    mid, and only then break the book. Cash is then genuinely +$1.275 and the sign is decisive."""
    c, book = harness["client"], harness["book"]
    real_create, real_positions = c.create_order, c.get_positions
    seen = {"positions": 0}

    async def create_and_fill(ticker, side, action, count, price, **kw):
        res = await real_create(ticker, side, action, count, price, **kw)
        if action == "sell" and c.position_of(ticker) == 0:
            c.fill(res["order_id"], count=3.0, ts=harness["clock"].now)
        return res

    async def positions_then_break_the_book():
        out = await real_positions()
        seen["positions"] += 1
        if seen["positions"] >= 3:             # only AFTER the sale has been booked at a real mid
            book.one_sided(T, keep="bid")      # NO ladder empties → yes_ask fabricated to 1.0
        return out

    c.create_order, c.get_positions = create_and_fill, positions_then_break_the_book
    _run(monkeypatch, **{"--seconds": 14, "--loss-cap": "99"})   # cap high: this tests the HEADLINE
    end = [r for r in _rows(harness["csv"]) if len(r) > 5 and r[1] == "end"]
    assert end, "expected an end row"
    assert float(end[0][5]) < 0.0, (
        f"a short held in a one-sided book reported P&L {end[0][5]} — excluding it from the mark "
        f"while keeping its +$1.275 of sale proceeds prints those proceeds as pure profit")


def test_a_FAILED_teardown_position_read_stamps_the_end_row(harness, monkeypatch):
    """When the FINAL venue read fails, the marked-P&L headline is computed against stale local
    belief (last known) and the UNBOOKED check cannot run through a failed read. The `end` row must be
    stamped VENUE_READ=FAILED so an analysis never mistakes it for a venue-backed number."""
    c = harness["client"]
    # The baseline reads BEFORE any sleep, so it succeeds; break the venue read once the run is under
    # way (first sleep), so both the next cycle's read and the teardown read fail.
    harness["on_sleep"].append(lambda: setattr(c, "fail_positions", RuntimeError("Kalshi 429")))
    _run(monkeypatch)
    end = [r for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "end"]
    assert end, "the run must still write an end row through the finally"
    assert "VENUE_READ=FAILED" in end[-1][2], "a failed teardown read must stamp the end row"


def test_each_cycle_logs_its_OWN_positions_not_an_accumulating_list(harness, monkeypatch):
    """⚠️ PINS THE CALL SITE, not the function. `_positions(raw_out=…)` is unit-tested, but the value
    of the capture lives in the wiring — and two mutations of that wiring survived the ENTIRE suite:
    hoisting `_cycle_raw = []` out of the loop, and dropping the `if real` gate.

    The hoist is the same shape as the original `sess.inv` regression. Its consequence: cycle 40's
    row would carry 40 copies of the same record, and a reconciliation reading cost basis per
    cycle would see a monotonically growing list — most naturally summing it, catastrophically."""
    c = harness["client"]
    c.set_position(T, 1.0, exposure=0.44)
    _run(monkeypatch, **{"--seconds": 14})           # 3 cycles
    rows = [r for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "positions_raw_cycle"]
    assert len(rows) >= 2, "every cycle must log its own row"
    import json as _json
    for r in rows:
        recs = _json.loads(r[2])
        assert len(recs) <= 1, (
            f"a cycle logged {len(recs)} records for 1 target market — the list is accumulating "
            f"across cycles instead of being rebuilt per cycle")
    assert any("market_exposure_dollars" in _json.loads(r[2])[0] for r in rows if _json.loads(r[2])), (
        "the venue's cost basis must be present — it is the reconciliation input")


def test_rows_are_READABLE_WHILE_THE_RUN_IS_STILL_GOING(harness, monkeypatch):
    """⚠️ PROVES THE FLUSH, by reading the CSV mid-run rather than after it.

    Without a per-row flush the file is block-buffered (~8 KB), so a 90-minute real-money run is
    invisible while it runs and a hard kill discards the tail. Asserting on the file AFTER `main()`
    returns cannot detect this at all — `fh.close()` flushes on the way out, so a fully-buffered
    writer passes such a test. The only honest check is to look while the handle is still open,
    which the patched clock lets us do from inside a sleep."""
    snapshots: list[int] = []
    csv_path = harness["csv"]

    def _peek():
        if csv_path.exists():
            with open(csv_path) as fh:
                snapshots.append(sum(1 for _ in fh))

    harness["on_sleep"].append(_peek)
    _run(monkeypatch, **{"--seconds": 14})
    assert not harness["hook_errors"], f"the mid-run hook raised: {harness['hook_errors']}"
    assert snapshots and max(snapshots) > 0, (
        "no CSV row was readable while the run was still going — the writer is buffering, so a live "
        "run cannot be monitored from its own ledger and a hard kill loses the tail")
    # ⚠️ GROWTH, not merely non-empty. `> 0` alone would stay green if only ONE event type flushed
    # (say `balance_start`), which is not a monitorable ledger. Requiring the count to increase
    # across snapshots pins that rows keep arriving as the run proceeds.
    assert max(snapshots) > min(snapshots), (
        f"row count never grew mid-run ({min(snapshots)}→{max(snapshots)}) — something flushed once "
        f"but the ledger is not keeping up with the run")


def test_the_PRE_TRADE_baseline_is_captured_before_any_order(harness, monkeypatch):
    """`E`/`R`/`F` (cost basis, realized, fees) are LIFETIME per market, so the reconciliation
    `pnl = (R − R_base) − (F − F_base) + Σ[mark − (E − E_base)]` is unanchored without a PRE-TRADE
    snapshot. The first in-loop row is written after the first quote round, so it is not a baseline.

    Pins both halves: that the row exists, and that it precedes every order."""
    c = harness["client"]
    c.set_position(T, 1.0, exposure=0.44)
    _run(monkeypatch)
    rows = _rows(harness["csv"])
    base_idx = [i for i, r in enumerate(rows) if len(r) > 1 and r[1] == "positions_raw_base"]
    assert base_idx, "a pre-trade E/R/F baseline row must be written"
    import json as _json
    recs = _json.loads(rows[base_idx[0]][2])
    assert recs and "market_exposure_dollars" in recs[0], "the baseline must carry the cost basis"
    # and it must precede the first order: no create may appear before the baseline row exists
    first_cycle = [i for i, r in enumerate(rows) if len(r) > 1 and r[1] == "positions_raw_cycle"]
    if first_cycle:
        assert base_idx[0] < first_cycle[0], "the baseline must precede the first in-loop capture"


def test_a_flat_account_still_logs_its_baseline_row(harness, monkeypatch):
    """The capture must emit on a NEVER-TRADED target, or the run cannot read its own E/R/F baseline
    — which is the entire purpose. Gating on 'the list is non-empty' silently suppressed exactly the
    case the reconciliation starts from."""
    _run(monkeypatch)                                 # no positions seeded at all
    rows = [r for r in _rows(harness["csv"]) if len(r) > 1 and r[1] == "positions_raw_cycle"]
    assert rows, "a flat account must still produce a baseline row, not silence"


def test_quotes_are_post_only_and_sized_from_the_size_argument(harness, monkeypatch):
    """Two mutants that survived the whole suite because the fake's call log recorded neither:
    dropping `post_only=True` (rail #1 — the maker silently becomes a taker and pays taker fees on
    a strategy whose entire margin is sub-cent) and `args.size * 10`."""
    _run(monkeypatch, **{"--size": 2})
    creates = harness["client"].calls_of("create")
    assert creates, "expected the run to quote"
    for x in creates:
        assert "po=True" in x, f"every maker quote must be post_only, got: {x}"
        assert "x2" in x, f"order size must come from --size, got: {x}"


# ── real-money gating, executed rather than string-matched ───────────────────────────────────────

def test_a_DRY_RUN_false_config_without_the_flag_refuses_to_start(harness, monkeypatch):
    """Orders are gated ONLY by config.DRY_RUN, so this combination would place real quotes while
    reporting a dry preview. It must be a hard stop, and nothing may be placed."""
    argv = [a for a in _argv() if a != "--i-understand-real-money"]
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as e:
        asyncio.run(mm.main())
    assert "REFUSING" in str(e.value)
    assert harness["client"].calls_of("create") == [], "nothing may be placed on the refusal path"


def test_a_rate_limit_profile_that_429d_a_real_run_is_refused(harness, monkeypatch):
    """markets=3 @ 6s is the configuration that actually 429'd and left orders resting."""
    with pytest.raises(SystemExit) as e:
        _run(monkeypatch, **{"--markets": 3, "--requote-s": 6})
    assert "REFUSING" in str(e.value)


# ── fail-closed on an unreadable position ────────────────────────────────────────────────────────

def test_an_unreadable_position_halts_before_placing_anything(harness, monkeypatch):
    """CANNOT-VERIFY must never read as flat: the baseline read fails, so the run must not quote."""
    c = harness["client"]
    c.fail_positions = RuntimeError("Kalshi 429")
    _run(monkeypatch)
    assert c.calls_of("create") == [], "nothing may be placed without a verified baseline"


def test_an_unreadable_position_still_sweeps_the_venue_for_strays(harness, monkeypatch):
    """Found by this file on its very first run, and fixed.

    `main()`'s book refusal is deliberately inside the `try`, with the comment "a refusal must never
    be the reason a prior run's resting order survives unswept and unmentioned". The
    baseline-position refusal one screen earlier was a bare `return` ahead of that `try`, so the
    exact scenario that comment guards against was live on the earlier path: nothing this run placed
    is at risk, but a PRIOR run's strays are, and a 429 on `get_positions` right after a run that
    left something resting is a plausible way to arrive here.

    ⚠️ Recorded first as a strict xfail, then fixed — because a strict xfail does not pin the
    REASON. Mutating `_positions` to return `{}` instead of `None` (fail-OPEN, the opposite of a
    fix) also turned it green, and any unrelated exception on that path would have kept it xfailing
    silently forever.

    ⚠️ A REAL STRAY IS SEEDED, and that is the point. An earlier version asserted only that a
    listing happened — but with no resting orders the preflight's `if allrest:` body never ran, so
    neutering the CANCEL loop (the single line this whole fix's safety argument rests on) left all
    1392 tests green. Asserting the stray is GONE pins the cancel, not just the listing.

    ⚠️ The bare-string `"get_resting_orders"` below discriminates the PREFLIGHT listing from the
    teardown sweep only because `mm_fakes` logs the scoped call as `get_resting_orders:scoped`.
    Unify those labels and this assertion silently weakens to "some listing happened"."""
    c = harness["client"]
    c.orders["oid-stray"] = {"ticker": T, "action": "buy", "price": 0.30,
                             "count": 1, "resting": True}      # left by a PRIOR run
    c.fail_positions = RuntimeError("Kalshi 429")
    _run(monkeypatch)
    assert c.calls_of("create") == [], "nothing may be placed without a verified baseline"
    assert "get_resting_orders" in c.calls, "a refusal must still LIST the venue for strays"
    assert c.resting_ids() == [], (
        "a prior run's stray must be CANCELLED before we refuse — listing it is not enough, and "
        "this refusal path never reaches the teardown sweep")


# ── queue attribution: did the queue ahead of us TRADE away or CANCEL away? ──────────────────────
#
# Run at `--improve-ticks 0` so the maker quotes AT the touch and `queue_ahead` is the real visible
# level (120 on the bid). Improving makes `ahead` 0 by construction, which would make every verdict
# TRADE_THROUGH trivially and test nothing.


def _qd(csv_path):
    """The queue_dynamics detail strings as dicts. The first two fields are positional (ticker,
    action) and the rest are `k=v`."""
    out = []
    for r in _rows(csv_path):
        if len(r) > 2 and r[1] == "queue_dynamics":
            parts = r[2].split()
            d = dict(p.split("=", 1) for p in parts if "=" in p)
            d["ticker"], d["action"] = parts[0], parts[1]
            out.append(d)
    return out


def _fill_with_queue_activity(harness, *, prints, cancels, before=None):
    """Once an order is RESTING, make `prints` contracts trade and `cancels` cancel at its price,
    then fill it. Both feeds are driven — the tape through the callback the maker registered on the
    trade feed, the level deltas through the observer it registered on the book — so this exercises
    the real wiring, not the tracker in isolation.

    ⚠️ DRIVEN FROM THE SLEEP HOOK, NOT FROM `create_order`. The maker learns an order's id from the
    create RESPONSE and only then starts tracking it, so anything fed while still inside
    `create_order` lands before the tracker exists and is silently dropped. That blind window is
    real — it is what `track_lag_s` on the row measures — but a fixture that sat inside it would
    show every counter reading zero and prove nothing."""
    from decimal import Decimal
    c = harness["client"]
    done: list = []

    def _hook():
        observe = harness["book"].level_observer
        if done or not harness["tapes"] or observe is None:
            return
        resting = [(oid, o) for oid, o in c.orders.items()
                   if o["resting"] and o["action"] == "buy"]
        if not resting:
            return
        done.append(True)
        oid, o = resting[0]
        tk, px = o["ticker"], o["price"]
        tape = harness["tapes"][0]
        if before is not None:
            before(tape)

        def _lvl(delta):
            observe({"kind": "delta", "ticker": tk, "side": "yes", "price": Decimal(str(px)),
                     "delta": Decimal(str(delta)), "qty_after": Decimal("0")})

        if prints:
            tape.feed(tk, px, prints, ts=harness["clock"].now)
            _lvl(-prints)
        if cancels:
            _lvl(-cancels)                                    # pulled, never printed
        tape.feed(tk, px, 1, ts=harness["clock"].now)         # our own fill prints too
        _lvl(-1)
        c.fill(oid, count=1.0, ts=harness["clock"].now)

    harness["on_sleep"].append(_hook)
    return c


def test_a_queue_that_trades_through_is_recorded_as_TRADE_THROUGH(harness, monkeypatch):
    _fill_with_queue_activity(harness, prints=120, cancels=0)
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 8})

    rows = [r for r in _qd(harness["csv"]) if r["outcome"] == "fill"]
    assert len(rows) == 1, f"expected exactly one fill attribution, got {rows}"
    r = rows[0]
    assert r["verdict"] == "TRADE_THROUGH"
    assert r["ahead"] == "120"
    assert r["traded_ahead"] == "120"       # 121 printed, our own 1 excluded
    assert r["cancelled_at_level"] == "0"
    assert r["cancelled_ahead_implied"] == "0"


def test_a_queue_that_pulls_before_we_fill_is_recorded_as_NOT_TRADE_THROUGH(harness, monkeypatch):
    """The adverse-selection signature the whole feature exists to detect: 120 ahead of us, only 2
    of them ever trade, 118 evaporate, and THEN we fill."""
    _fill_with_queue_activity(harness, prints=2, cancels=118)
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 8})

    r = [x for x in _qd(harness["csv"]) if x["outcome"] == "fill"][0]
    assert r["verdict"] == "NOT_TRADE_THROUGH"
    assert r["traded_ahead"] == "2"
    # The FIFO-implied quantity (we filled, 2 of the 120 ahead traded) and the independent L2
    # corroboration that removals at the level really do cover it.
    assert r["cancelled_ahead_implied"] == "118"
    assert r["cancelled_at_level"] == "118"


def test_an_improved_quote_does_not_score_TRADE_THROUGH_by_construction(harness, monkeypatch):
    """At the level that matters: `--improve-ticks 1` is the policy intended for the wide
    maker-free lane, and it makes `queue_ahead` 0 for every order. If `ahead == 0` scored
    TRADE_THROUGH, a real run would come back ~100% TRADE_THROUGH — a restatement of the flag, not
    a measurement of anything."""
    _fill_with_queue_activity(harness, prints=0, cancels=0)
    _run(monkeypatch, **{"--improve-ticks": 1, "--seconds": 8})

    r = [x for x in _qd(harness["csv"]) if x["outcome"] == "fill"][0]
    assert r["ahead"] == "0", "improving is supposed to put us first — fixture check"
    assert r["verdict"] == "ALONE_AT_PRICE"


def test_a_sell_side_fill_is_attributed_on_the_NO_ladder(harness, monkeypatch):
    """A yes sell at 0.45 rests as a NO bid at 0.55. If the ladder mapping were inverted this would
    accumulate the opposite side of the book and still look like data — so drive the real observer
    with `no`-ladder deltas and require the verdict to come out of them."""
    from decimal import Decimal
    c = harness["client"]
    done: list = []

    def _hook():
        observe = harness["book"].level_observer
        if done or not harness["tapes"] or observe is None:
            return
        sells = [(oid, o) for oid, o in c.orders.items()
                 if o["resting"] and o["action"] == "sell"]
        if not sells:
            return
        done.append(True)
        oid, o = sells[0]
        tape = harness["tapes"][0]

        def _lvl(delta):                       # NO ladder, at the complement of our yes price
            observe({"kind": "delta", "ticker": o["ticker"], "side": "no",
                     "price": Decimal(str(1.0 - o["price"])), "delta": Decimal(str(delta)),
                     "qty_after": Decimal("0")})

        tape.feed(o["ticker"], o["price"], 35, ts=harness["clock"].now)   # the whole queue trades
        _lvl(-35)
        tape.feed(o["ticker"], o["price"], 1, ts=harness["clock"].now)    # then us
        _lvl(-1)
        c.fill(oid, count=1.0, ts=harness["clock"].now)

    harness["on_sleep"].append(_hook)
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 8})

    r = [x for x in _qd(harness["csv"]) if x["outcome"] == "fill" and x["action"] == "sell"][0]
    assert r["ahead"] == "35", "the yes-ASK depth, i.e. the NO bid queue we joined"
    assert r["traded_ahead"] == "35"
    assert r["verdict"] == "TRADE_THROUGH"


def test_our_own_resting_size_is_not_counted_as_queue_ahead_of_us(harness, monkeypatch):
    """`depth_b` is read BEFORE `_cancel_all`, so on every cycle after the first it includes our own
    previous order. Left in, `ahead` overstates the external queue and a fill gets blamed on a
    queue that pulled when it was really our own requote churn.

    The fake book's depth is static, which is exactly what a real book that still carries our
    resting order looks like at this point in the cycle — so cycle 1 must report the raw 120 and
    every later cycle 119."""
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 14})
    rows = _qd(harness["csv"])
    buys = [r["ahead"] for r in rows if r["action"] == "buy"]
    sells = [r["ahead"] for r in rows if r["action"] == "sell"]
    assert len(buys) >= 2 and len(sells) >= 2, "need more than one cycle to see the correction"
    assert buys[0] == "120" and sells[0] == "35", "cycle 1 has nothing of ours resting yet"
    # BOTH sides. The sell side is the one that goes through the NO-ladder complement, where an
    # error would be least obvious — and asserting only the buy side leaves half the fix unpinned.
    assert set(buys[1:]) == {"119"}, f"later cycles must net out our own 1 lot, got {buys}"
    assert set(sells[1:]) == {"34"}, f"same on the sell side, got {sells}"


def test_a_FILLED_order_is_not_still_subtracted_as_if_it_were_resting(harness, monkeypatch):
    """B-1. `resting[t]` is cleared only by `cancel_all`, and fills are polled BEFORE quoting, so an
    order that filled this cycle is still named there — while being gone from the venue book and so
    absent from `depth`. Subtracting it anyway understates `ahead` on exactly the cycles that follow
    a fill, toward TRADE_THROUGH: under-detecting adverse selection."""
    _fill_with_queue_activity(harness, prints=120, cancels=0)
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 14})

    rows = _qd(harness["csv"])
    filled = [r for r in rows if r["outcome"] == "fill"]
    assert filled, "fixture must produce a fill for this to test anything"
    after = [r for r in rows if r["action"] == "buy" and r["outcome"] != "fill"]
    assert after, "need a buy quote on a cycle AFTER the fill"
    assert after[0]["ahead"] == "120", (
        f"the filled order is gone from the book — nothing of ours rests at 0.40, so `ahead` is the "
        f"raw 120, got {after[0]['ahead']}")


def test_track_lag_spans_the_whole_blind_window_not_just_the_create(harness, monkeypatch):
    """`ahead` is measured at the depth read, but tracking cannot start until `create_order` returns
    an id — and `_cancel_all`'s round trip per resting order sits in between. Reporting only the
    create's RTT would understate the window it exists to bound."""
    clock = harness["clock"]
    c = harness["client"]
    real_cancel, real_create = c.cancel_order, c.create_order

    async def slow_cancel(oid):
        clock.now += 0.5
        return await real_cancel(oid)

    async def slow_create(*a, **kw):
        clock.now += 0.25
        return await real_create(*a, **kw)

    c.cancel_order, c.create_order = slow_cancel, slow_create
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 14})

    lags = [float(r["track_lag_s"]) for r in _qd(harness["csv"])]
    assert lags, "no rows written"
    assert max(lags) >= 0.7, (
        f"the widest window must include the cancel round trips, not only the create's 0.25s — "
        f"got {max(lags)}")


def test_multiple_partials_in_one_poll_do_not_inflate_traded_ahead(harness, monkeypatch):
    """The venue partial-fills below one contract, so one order can appear as several fill records
    in a single poll — and the tape carried a print for each. Subtracting only the FIRST partial
    leaves the rest counted as queue that traded ahead of us."""
    from decimal import Decimal
    c = harness["client"]
    done: list = []

    def _hook():
        observe = harness["book"].level_observer
        if done or not harness["tapes"] or observe is None:
            return
        resting = [(oid, o) for oid, o in c.orders.items()
                   if o["resting"] and o["action"] == "buy"]
        if not resting:
            return
        done.append(True)
        oid, o = resting[0]
        tape = harness["tapes"][0]
        # THREE partials summing to exactly 1.00 in Decimal but to 1.0000000000000002 in float —
        # i.e. our own filled count reads as MORE than the tape's exact total, which is precisely
        # what trips the `traded < filled` guard. Two-way splits all sum exactly in float (checked:
        # 0 of 99), so a two-partial fixture cannot tell a Decimal accumulator from a float one;
        # of the 204 three-way splits that break, only 6 break in this direction.
        for part in ("0.33", "0.56", "0.11"):
            tape.feed(o["ticker"], o["price"], part, ts=harness["clock"].now)
            observe({"kind": "delta", "ticker": o["ticker"], "side": "yes",
                     "price": Decimal(str(o["price"])), "delta": -Decimal(part),
                     "qty_after": Decimal("0")})
            c.fill(oid, count=float(part), ts=harness["clock"].now)

    harness["on_sleep"].append(_hook)
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 8})

    r = [x for x in _qd(harness["csv"]) if x["outcome"] == "fill"][0]
    assert r["traded_ahead"] == "0", (
        f"every partial was ours; none of it traded ahead of us — got {r['traded_ahead']}")
    assert r["verdict"] != "UNKNOWN_TAPE_INCOMPLETE", (
        "0.33 + 0.56 + 0.11 must sum EXACTLY to the tape's 1.00 — accumulated in float it is "
        "1.0000000000000002, which trips a guard whose docstring calls it ARITHMETIC PROOF that a "
        "print was dropped, on a perfectly healthy row")


def test_a_tape_that_never_acked_cannot_manufacture_an_adverse_verdict(harness, monkeypatch):
    """A rejected subscription yields `traded = 0`, which is arithmetically identical to a queue
    that pulled — so an unacked tape would report the alarming verdict on 100% of fills. This repo
    has already lost two real-money runs to a feed that logged healthy while dropping everything."""
    _fill_with_queue_activity(harness, prints=0, cancels=120,
                              before=lambda tape: setattr(tape, "subscribed", False))
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 8})

    r = [x for x in _qd(harness["csv"]) if x["outcome"] == "fill"][0]
    assert r["verdict"] == "UNKNOWN_TAPE_DOWN"
    assert r["tape"] == "DOWN"


def test_a_tape_unacked_at_startup_is_flagged_before_the_run_trades(harness, monkeypatch):
    """Per-row `tape=DOWN` keeps the DATA honest, but an operator watching a size-1 run needs to
    know up front that it is collecting nothing — the run's whole purpose."""
    def _unacked(*a, **k):
        t = FakeTradeFeed(*a, **k)
        t.subscribed = False
        t.last_error = "subscribe rejected"
        harness["tapes"].append(t)
        return t

    monkeypatch.setattr(mm, "KalshiTradeFeed", _unacked)
    _run(monkeypatch)

    status = [r[2] for r in _rows(harness["csv"]) if len(r) > 1 and r[1] == "tape_status"]
    assert status, "the run must record whether its tape acked"
    assert "NOT_ACKED" in status[0] and "subscribe rejected" in status[0]
    assert harness["client"].calls_of("create"), (
        "an unacked TAPE must not stop trading — it is instrumentation, and letting an "
        "observability fault veto a money decision is its own defect")


def test_an_unfilled_order_reports_what_the_queue_did_while_we_waited(harness, monkeypatch):
    """The survivorship half. A fills-only log cannot say "we sat behind 120 and 2 traded", which is
    the observation that separates a thin market from a policy that never reaches the front."""
    _run(monkeypatch, **{"--improve-ticks": 0})
    rows = _qd(harness["csv"])
    assert rows, "cancelled orders must be attributed too, not just fills"
    assert all(r["outcome"] != "fill" for r in rows)
    assert rows[0]["ahead"] in ("120", "35"), "the real visible level, not a placeholder"
    assert rows[0]["verdict"] == "NA", "an unfilled order has no fill to explain"


def test_an_unrecognised_control_frame_is_not_treated_as_a_lost_print(harness, monkeypatch):
    """The maker must feed the NARROW counter into the gap trigger. The ack message types are a
    GUESS — the venue does not document them — so a venue emitting any periodic control frame drives the wide
    `n_dropped` up forever — marking every live order on every 1 Hz poll and returning a run that is
    100% UNKNOWN_TAPE_GAP. A measurement that dies quietly is the failure this module exists to
    prevent, and it would be indistinguishable from a genuinely broken tape."""
    _fill_with_queue_activity(
        harness, prints=120, cancels=0,
        # A heartbeat lands while we rest: unrecognised, but no print was lost.
        before=lambda tape: setattr(tape, "n_dropped", tape.n_dropped + 5))
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 8})

    r = [x for x in _qd(harness["csv"]) if x["outcome"] == "fill"][0]
    assert r["tape_gap"] == "N", "unparsed control frames are not lost prints"
    assert r["verdict"] == "TRADE_THROUGH"


def test_a_level_observer_fault_reaches_the_tracker_from_the_MAKER(harness, monkeypatch):
    """The unit test proves `note_observer_errors` works; this proves the maker ever calls it. "A
    fault counter nothing reads is not a guard" applies to the reading, not just the counter."""
    book = harness["book"]
    monkeypatch.setattr(type(book), "level_observer_errors",
                        property(lambda self: {"ValueError: boom": 3}), raising=False)
    _fill_with_queue_activity(harness, prints=120, cancels=0)
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 8})

    rows = [r for r in _qd(harness["csv"]) if r["outcome"] == "fill"]
    assert rows, "fixture must produce a fill"
    assert rows[0]["book_gap"] == "Y", (
        "lost L2 events must flag the row's level-derived columns — the maker has to poll the "
        "observer's fault counter for that to happen at all")


def test_a_raising_trade_callback_reaches_the_gap_trigger(harness, monkeypatch):
    """`KalshiTradeFeed.errors` counts prints the tape delivered and the tracker then dropped — lost
    prints on the half that now solely decides the verdict. Left unrouted it is the same
    counted-but-unread pattern, on the side where it actually changes a finding."""
    _fill_with_queue_activity(
        harness, prints=120, cancels=0,
        before=lambda tape: setattr(tape, "n_callback_errors", 2))
    _run(monkeypatch, **{"--improve-ticks": 0, "--seconds": 8})

    r = [x for x in _qd(harness["csv"]) if x["outcome"] == "fill"][0]
    assert r["tape_gap"] == "Y" and r["verdict"] == "UNKNOWN_TAPE_GAP"


def test_a_run_without_a_level_feed_says_so_instead_of_collecting_nothing(harness, monkeypatch):
    """Outside orderbook mode `get_depth` returns None, the quote path's `or 0.0` turns "depth
    unobserved" into "empty queue", and every fill scores ALONE_AT_PRICE with no level events at
    all — a run that looks like it worked and collected nothing."""
    monkeypatch.setattr(mm.config, "KALSHI_PRICE_SOURCE", "ticker")
    _run(monkeypatch)

    status = [r[2] for r in _rows(harness["csv"]) if len(r) > 1 and r[1] == "tape_status"]
    assert status and "NO_L2" in status[0], (
        f"a run with no level feed must announce it, got {status}")


def test_the_tape_watches_exactly_the_markets_we_quote(harness, monkeypatch):
    _run(monkeypatch)
    tapes = harness["tapes"]
    assert len(tapes) == 1, f"expected one trade feed for the run, got {len(tapes)}"
    assert tapes[0].tickers == [T], "the tape must cover the targets, or it observes nothing"
    assert tapes[0].ran, "the feed was built but never started"


def test_the_level_observer_is_detached_at_teardown(harness, monkeypatch):
    """Attached to a live feed, an observer outliving its consumer is a leak — and a delta in flight
    could resurrect a tracker after the run has already reported on it."""
    _run(monkeypatch)
    assert harness["book"].level_observer is None


def test_the_run_reports_whether_the_attribution_was_actually_wired(harness, monkeypatch):
    """`matched=0` across a run with fills means broken plumbing, not a quiet market — and the two
    are indistinguishable without the counter."""
    _fill_with_queue_activity(harness, prints=5, cancels=0)
    _run(monkeypatch, **{"--improve-ticks": 0})
    health = [r[2] for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "tape_health"]
    assert health, "the run must report its tape health"
    assert "matched_prints=2" in health[0] and "acked=True" in health[0]
    assert "lvl_obs_errors=0" in health[0]


def test_the_end_row_and_closing_line_report_MAKER_fills_only(harness, monkeypatch, capsys):
    """⛔ THE PIN THREE SUCCESSIVE VERSIONS FAILED TO PROVIDE. The reported fill count was guarded by
    an AST assertion in `test_live_mm_guards.py`, and review walked a mutant through each of its
    three successive versions: a decoy that calls the extracted function and discards the result, a
    rebind of `note` after the pinned call, and a SECOND print of the inflated number beside the
    pinned one. Every one kept the asserted substring present in `ast.unparse(main)` while reporting
    the wrong number. ⚠️ Against the assertion that ships TODAY the decoy is red — but the rebind and
    the second print still pass it, because a source assertion cannot see an INSERT. Those two are
    caught here and nowhere else.

    The reason those weak pins were written was a belief — still stated in `test_maker_session.py`'s
    `test_main_calls_sync_inventory`, and until recently in `maker.py` too — that `main()` has no
    executable coverage. This file has driven
    `asyncio.run(main())` since it was created. That belief cost real coverage on a number the run
    plan compares against a FIFO model's prediction: a count inflated by our own forced exit argues
    the model UNDER-predicts, which is exactly the reading that would justify committing more
    capital.

    So: drive the real `main()` through a run with ONE maker fill and ONE teardown-flatten fill, and
    assert on what is actually reported — the `end` CSV row and the operator's closing line."""
    c = harness["client"]
    real_create = c.create_order
    seen = {"maker": False}

    async def create_and_fill(ticker, side, action, count, price, **kw):
        res = await real_create(ticker, side, action, count, price, **kw)
        n = float(count)
        # ⚠️ DISTINGUISH THE FLATTEN FROM AN ORDINARY QUOTE BY SIZE. Quotes are `--size 1`; the
        # teardown flatten sizes off the whole net position (`floor(abs(net))`), so a sell of >1 is
        # the flatten. A first version filled any sell and the run ended flat from ORDINARY fills —
        # the flatten never fired and the test proved nothing about it.
        if action == "buy" and not seen["maker"]:
            seen["maker"] = True
            # Fill 2 contracts on the single quoted buy so the position is 2 and the teardown
            # flatten consolidates it into ONE sell of 2 — distinguishable from a `--size 1` quote.
            c.fill(res["order_id"], count=2.0, ts=harness["clock"].now)
        elif action == "sell" and n > 1:
            c.fill(res["order_id"], count=n, ts=harness["clock"].now)     # the teardown flatten
        return res

    c.create_order = create_and_fill
    monkeypatch.setattr(sys, "argv", _argv() + ["--flatten-on-exit", "--flatten-wait-s", "0"])
    asyncio.run(mm.main())

    end = [r for r in _rows(harness["csv"]) if len(r) > 1 and r[1] == "end"][-1][2]
    assert "flatten_fills=" in end, (
        f"the flatten's own fills must be reported SEPARATELY, not folded into the maker count. "
        f"end row: {end!r}")
    maker_n = int(end.split("fills=")[1].split()[0])
    flat_n = int(end.split("flatten_fills=")[1].split()[0])
    # ⛔ ASSERT THE LITERAL THE FIXTURE CONSTRUCTS. The first version read
    #     assert maker_n == len(harness["session"].seen_fills) - flat_n if harness.get("session") else True
    # which is DEAD twice over: `assert X if C else True` binds as `assert (X if C else True)`, and
    # the harness has no "session" key at all — so it was permanently `assert True`. Without a value
    # assertion the rest of this test only checks that the CSV row and the stdout line AGREE with
    # each other, which both do while both being inflated. Verified: reverting `fill_counts` to the
    # defect left all 37 e2e tests green.
    # The fixture places one maker BUY that fills 2 contracts (ONE fill record) and one teardown
    # flatten SELL of 2 (one more), so: 2 fills seen, 1 of them the flatten.
    assert (maker_n, flat_n) == (1, 1), (
        f"expected exactly 1 maker fill and 1 flatten fill; got maker={maker_n} flatten={flat_n}. "
        f"If the flatten's fill is being counted as maker edge, maker_n reads 2.")

    out = capsys.readouterr().out
    lines = [ln for ln in out.splitlines() if "real fills=" in ln]
    assert len(lines) == 1, (
        f"exactly ONE closing line may report the count — a second, inflated one is how the decoy "
        f"mutant reported the wrong number while keeping the pinned string present. Got: {lines}")
    assert f"real fills={maker_n}" in lines[0], (
        f"the operator's line and the CSV row must agree; a disagreement between them is worse than "
        f"either being wrong alone. line={lines[0]!r} end={end!r}")
    assert "NOT maker edge" in lines[0], "the flatten count must be labelled, not silently added"


# ── the documented kill switch, which did not reach this process ─────────────────────────────────
#
# ⛔ The runbook presented `touch pause.json` as THE kill switch, unscoped. For a long time
# `is_paused()` had exactly ONE caller, inside a program that was deliberately stopped and had
# never traded; the maker and its launcher contained zero references to it. So the one program
# that actually placed real orders ignored the control an operator was documented to reach for,
# and `touch pause.json` returned no error while doing nothing. That is the worst shape a control
# can have: it reports the same thing whether or not it did anything.


def test_the_documented_kill_switch_HALTS_a_running_maker(harness, monkeypatch, tmp_path):
    """Armed mid-run, the pause file must stop the maker within one cycle and take the proven
    teardown — the same `finally` a loss-cap halt takes, so resting orders are cancelled and the
    venue is swept. Asserting only "a halt row exists" would pass with the orders left resting."""
    pause = tmp_path / "pause.json"
    monkeypatch.setattr(mm.config, "KILL_SWITCH_FILE", str(pause))

    def _arm_once_the_run_has_quoted():
        # ⚠️ NOT on the first sleep. The book warmup and the markout ticker both park before cycle 1
        # quotes, so arming there would halt the run before it ever placed an order — which passes a
        # naive "a halt row exists" assertion while proving nothing about stopping a RUNNING maker.
        if harness["client"].calls_of("create"):
            pause.write_text("{}")

    harness["on_sleep"].append(_arm_once_the_run_has_quoted)
    _run(monkeypatch, **{"--seconds": 20})                          # ~4 cycles if never halted

    halts = [r for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "halt"]
    assert halts, "an armed kill switch must halt the maker — it is the documented operator control"
    # ⚠️ The WHOLE list, not `halts[0]`. Indexing [0] passes with a SECOND halt row inserted beside
    # the pinned one — verbatim the "a second print beside the pinned one" insert shape that
    # defeated a pin in this codebase three times running.
    assert [r[2] for r in halts] == ["kill_switch"], f"halted for the wrong reason(s): {halts}"
    assert harness["client"].resting_ids() == [], (
        "a kill-switch halt must still cancel every resting order — a pause that leaves live quotes "
        "on the venue with the loss cap dead is worse than no pause at all")
    # It must stop QUOTING, not merely record that it noticed. One cycle = one buy + one sell.
    assert len(harness["client"].calls_of("create")) == 2, (
        f"quoting continued after the pause was armed: "
        f"{harness['client'].calls_of('create')}")


def test_a_maker_STARTED_while_paused_places_nothing_at_all(harness, monkeypatch, tmp_path):
    """The check has to be inside the cycle, not a one-off before it — but it must also cover the
    first cycle, which is the case an operator hits when they pause and then restart by mistake."""
    pause = tmp_path / "pause.json"
    pause.write_text("{}")
    monkeypatch.setattr(mm.config, "KILL_SWITCH_FILE", str(pause))
    _run(monkeypatch)

    assert harness["client"].calls_of("create") == [], (
        "nothing may be quoted while the kill switch is armed")
    halts = [r for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "halt"]
    assert [r[2] for r in halts] == ["kill_switch"], f"expected exactly one kill_switch halt: {halts}"


def test_the_kill_switch_does_not_halt_a_run_when_it_is_NOT_armed(harness, monkeypatch, tmp_path):
    """The control. A check wired to a path that always exists (or read with the wrong polarity)
    would halt every run instantly and read as "the pause works"."""
    monkeypatch.setattr(mm.config, "KILL_SWITCH_FILE", str(tmp_path / "no-such-pause.json"))
    _run(monkeypatch)

    halts = [r for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "halt"]
    assert not halts, f"an unarmed kill switch must not halt anything, got {halts}"
    assert harness["client"].calls_of("create"), "the run must still quote normally"


def test_the_kill_switch_HALTS_AND_FLATTENS_a_maker_that_is_HOLDING_INVENTORY(harness, monkeypatch,
                                                                              tmp_path):
    """⛔ THE CASE AN OPERATOR ACTUALLY REACHES FOR THE SWITCH, and the three tests above all miss it.

    Review drove ten insert/rebind mutants at the new guard. Eight were killed; two survived the
    ENTIRE suite, and both survive for the same reason — no test above ever scripts a fill, so
    `sess.inv` is all-zeros for their whole run:

      (a) `if is_paused() and not any(sess.inv.values()):` — a kill switch that works while FLAT and
          silently does nothing once the maker holds a position. That is an inverted kill switch: it
          is disabled in exactly the state that makes an operator reach for it.
      (b) re-adding the `not halted` gate around the `_should_flatten` call site — i.e. halting
          but walking away from the position, leaving it unattended with the loss cap dead. That
          gate was removed deliberately; extracting `_should_flatten` moved the pin onto the pure
          function and left the CALL SITE naked, which its own docstring predicted.

    This test closes both: the maker fills a real position, THEN the switch is armed, and the
    teardown must both halt and post the passive reducing order."""
    pause = tmp_path / "pause.json"
    monkeypatch.setattr(mm.config, "KILL_SWITCH_FILE", str(pause))
    c = harness["client"]
    real_create = c.create_order
    seen = {"maker": False}

    async def create_and_fill(ticker, side, action, count, price, **kw):
        res = await real_create(ticker, side, action, count, price, **kw)
        # Size discriminates the teardown flatten (sized off the whole net) from a --size 1 quote.
        if action == "buy" and not seen["maker"]:
            seen["maker"] = True
            c.fill(res["order_id"], count=2.0, ts=harness["clock"].now)
            pause.write_text("{}")          # armed only once we genuinely hold something
        return res

    c.create_order = create_and_fill
    monkeypatch.setattr(sys, "argv",
                        _argv(**{"--seconds": 20}) + ["--flatten-on-exit", "--flatten-wait-s", "0"])
    asyncio.run(mm.main())

    rows = _rows(harness["csv"])
    halts = [r for r in rows if len(r) > 2 and r[1] == "halt"]
    assert [r[2] for r in halts] == ["kill_switch"], (
        f"the switch must halt a maker that is HOLDING inventory, not only a flat one: {halts}")

    sells = [x for x in c.calls_of("create") if ":sell@" in x]
    assert any("x2" in x for x in sells), (
        f"the teardown must still post the passive reducing order for the 2 lots we hold — "
        f"declining to flatten on a halt walks away from the position with the loss cap dead. "
        f"Sell orders placed: {sells}")
    assert c.resting_ids() == [], "and the sweep must run AFTER the flatten, leaving nothing resting"


def test_the_run_ANNOUNCES_which_kill_switch_file_it_is_watching(harness, monkeypatch, tmp_path,
                                                                 capsys):
    """Wiring the switch in is only half a fix if the operator cannot see which path THIS run
    watches. `KILL_SWITCH_FILE` is a RELATIVE default and the maker is launched by hand, so a run
    started from ~ watches ~/pause.json while `scripts/show_config` — run from the repo — prints the
    repo one. An operator touching that file gets no error and no effect: the exact pre-fix
    behaviour, reintroduced through the path rather than the wiring."""
    monkeypatch.setattr(mm.config, "KILL_SWITCH_FILE", "pause.json")
    _run(monkeypatch)
    out = capsys.readouterr().out
    assert "kill switch: watching" in out
    assert str(tmp_path / "pause.json") in out, (
        "the ABSOLUTE path this run resolves must be printed — a bare 'pause.json' is the "
        "ambiguity that makes the switch unreachable")
    rows = [r for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "kill_switch_path"]
    assert rows and rows[0][2] == str(tmp_path / "pause.json"), (
        "and it must be in the ledger, so a post-hoc read can tell which file the run watched")


def test_a_DISABLED_kill_switch_is_announced_LOUDLY_rather_than_silently(harness, monkeypatch,
                                                                        capsys):
    """An empty `KILL_SWITCH_FILE` makes `is_paused()` a constant `False` with no warning anywhere,
    so the disabled state is byte-indistinguishable from the armed one. This is the only place a
    real-money run says so."""
    monkeypatch.setattr(mm.config, "KILL_SWITCH_FILE", "")
    _run(monkeypatch)
    out = capsys.readouterr().out
    assert "kill switch DISABLED" in out and "NO way to halt" in out
    rows = [r for r in _rows(harness["csv"]) if len(r) > 2 and r[1] == "kill_switch_path"]
    assert rows and rows[0][2] == "DISABLED"
