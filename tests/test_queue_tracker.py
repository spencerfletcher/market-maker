"""Behavioural tests for the traded-vs-cancelled queue attribution.

Every test drives real event sequences through `QueueTracker` and asserts on the verdict it
produces. None of them assert a property of their own fixture — that pattern is what this suite
exists to stamp out, because a test that re-derives its own setup passes against any
implementation.
"""
from decimal import Decimal

import pytest

from bot.kalshi.queue_tracker import (
    ALONE_AT_PRICE,
    NOT_TRADE_THROUGH,
    TRADE_THROUGH,
    UNKNOWN_FILL_AT_CANCEL,
    UNKNOWN_FILL_SECOND,
    UNKNOWN_NO_FILL_TS,
    UNKNOWN_NO_QUEUE,
    UNKNOWN_TAPE_DOWN,
    UNKNOWN_TAPE_GAP,
    UNKNOWN_TAPE_INCOMPLETE,
    QueueTracker,
    _ladder_of,
)

T = "KXKBOGAME-26JUL24LOTSAM-LOT"


def _trade(px, qty, ticker=T, ts=None):
    return {"ticker": ticker, "yes_price": Decimal(px), "qty": Decimal(qty), "ts": ts}


def _delta(side, price, delta, ticker=T, before=None):
    """A level event. `qty_before`/`qty_after` are what the feed really sends; when `before` is
    given they are consistent with `delta`, so a test can also exercise the case where they are NOT
    (a level popped below the resolution floor)."""
    ev = {"kind": "delta", "ticker": ticker, "side": side,
          "price": Decimal(price), "delta": Decimal(delta)}
    if before is not None:
        ev["qty_before"] = Decimal(before)
        ev["qty_after"] = Decimal(before) + Decimal(delta)
    return ev


def _track_buy(qt, *, px="0.40", size=1, ahead=10, oid="o1"):
    assert qt.track(oid, T, "buy", px, size, ahead, t_placed=100.0)
    return oid


def _summarize(qt, oid, **kw):
    q = qt.close(oid)
    assert q is not None
    kw.setdefault("outcome", "fill")
    kw.setdefault("tape_ok", True)
    kw.setdefault("now", 200.0)
    return qt.summarize(q, **kw)


# ── the ladder mapping (the inversion that would silently read the wrong side) ──

def test_a_yes_sell_rests_on_the_no_ladder_at_the_complement():
    """A yes sell at 0.60 is a NO bid at 0.40. Mapping it to the yes ladder would accumulate the
    opposite side of the book and still look like data."""
    assert _ladder_of("buy", Decimal("0.60")) == ("yes", Decimal("0.60"))
    assert _ladder_of("sell", Decimal("0.60")) == ("no", Decimal("0.40"))


def test_level_deltas_reach_a_sell_order_only_on_its_own_ladder():
    qt = QueueTracker()
    qt.track("s1", T, "sell", "0.60", 1, 8, t_placed=100.0)

    qt.on_level(_delta("yes", "0.60", "-5"))   # the YES ladder at 0.60 — NOT where we rest
    assert qt.get("s1").removed == Decimal("0")

    qt.on_level(_delta("no", "0.40", "-5"))    # our actual level
    assert qt.get("s1").removed == Decimal("5")


# ── the two findings ──

def test_flow_through_the_whole_queue_reads_as_trade_through():
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    # 10 contracts ahead of us trade away, then our own 1 fills. All 11 print at our price.
    # Timestamps are REALISTIC: the queue trades strictly before the venue's whole-second fill_ts,
    # our own print lands fractionally after it.
    qt.on_trade(_trade("0.40", "10", ts=999.5))
    qt.on_level(_delta("yes", "0.40", "-10"))
    qt.on_trade(_trade("0.40", "1", ts=1000.035))
    qt.on_level(_delta("yes", "0.40", "-1"))

    row = _summarize(qt, "o1", filled_qty=1, fill_ts=1000.0)
    assert row["verdict"] == TRADE_THROUGH
    assert row["traded_ahead"] == Decimal("10")     # our own fill excluded
    assert row["cancelled_at_level"] == Decimal("0")   # removed 11 - traded 11


def test_a_queue_that_pulls_before_we_fill_reads_as_cancel_driven():
    """The adverse-selection signature: 10 ahead, only 2 of them ever trade, the other 8 vanish."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "2"))
    qt.on_level(_delta("yes", "0.40", "-2"))
    qt.on_level(_delta("yes", "0.40", "-8"))    # cancels: removed, never printed
    qt.on_trade(_trade("0.40", "1"))            # now flow reaches us
    qt.on_level(_delta("yes", "0.40", "-1"))

    row = _summarize(qt, "o1", filled_qty=1)
    assert row["verdict"] == NOT_TRADE_THROUGH
    assert row["traded_ahead"] == Decimal("2")
    assert row["cancelled_at_level"] == Decimal("8")


def test_our_own_fill_cancels_out_of_the_removed_minus_traded_identity():
    """Our fill removes size from the level AND prints on the tape, so it must not inflate the
    cancelled residual. Regression on the correction that is easy to apply twice or not at all."""
    qt = QueueTracker()
    _track_buy(qt, ahead=0, size=3)
    qt.on_trade(_trade("0.40", "3"))
    qt.on_level(_delta("yes", "0.40", "-3"))

    row = _summarize(qt, "o1", filled_qty=3)
    assert row["cancelled_at_level"] == Decimal("0")
    assert row["traded_ahead"] == Decimal("0")


def test_the_fill_verdict_rests_on_the_TAPE_alone_not_on_the_level_stream():
    """The L2 half is a diagnostic on the fill path, not a gate. Under FIFO, cancelled-ahead is
    identically `ahead − traded_ahead` for a filled order, so `cancelled_at_level` — which is
    level-wide and un-timestamped — carries no information the tape does not already give, and an
    earlier version that made it a corroboration threshold demoted textbook adverse fills to
    AMBIGUOUS."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "2"))            # only 2 of the 10 ahead ever traded
    qt.on_level(_delta("yes", "0.40", "-2"))
    qt.on_level(_delta("yes", "0.40", "-1"))    # the level stream saw only 1 of the 8 that left
    qt.on_trade(_trade("0.40", "1"))
    qt.on_level(_delta("yes", "0.40", "-1"))

    row = _summarize(qt, "o1", filled_qty=1)
    assert row["verdict"] == NOT_TRADE_THROUGH, (
        "we filled having seen only 2 of the 10 ahead trade — under FIFO the other 8 left without "
        "trading, and a thin level stream cannot argue with that")
    assert row["cancelled_ahead_implied"] == Decimal("8")   # the FIFO quantity
    assert row["cancelled_at_level"] == Decimal("1")        # the diagnostic, reported not used


def test_gone_at_cancel_is_a_FILL_and_must_not_be_corrected_as_a_cancel():
    """The venue's 404 on cancel means the order filled between the poll and the cancel. Its size
    left via a print, which already nets out of `removed - traded` — subtracting again would
    under-count the cancelled population, the very population this row exists to describe."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10, size=1)
    qt.on_trade(_trade("0.40", "1"))            # our own fill printed
    qt.on_level(_delta("yes", "0.40", "-1"))    # and left the level

    row = _summarize(qt, "o1", outcome="gone_at_cancel")
    assert row["cancelled_at_level"] == Decimal("0")


def test_our_own_cancel_is_not_counted_as_someone_elses_cancel():
    """A cancel removes our size from the level with no print, so without the correction every
    unfilled order reports `size` phantom cancelled contracts."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10, size=1)
    qt.on_level(_delta("yes", "0.40", "-1"))     # us, cancelling

    row = _summarize(qt, "o1", outcome="cancelled_unfilled")
    assert row["cancelled_at_level"] == Decimal("0")


def test_an_unfilled_order_still_reports_its_queue_evidence():
    """The survivorship half: a fills-only log cannot say 'we sat behind 35 and 2 traded'."""
    qt = QueueTracker()
    _track_buy(qt, ahead=35)
    qt.on_trade(_trade("0.40", "2"))
    qt.on_level(_delta("yes", "0.40", "-2"))

    row = _summarize(qt, "o1", outcome="cancelled_unfilled")
    assert row["verdict"] == ""                 # no fill to explain
    assert row["traded_ahead"] == Decimal("2")
    assert row["ahead"] == Decimal("35")


# ── the guards that must beat both findings ──

def test_a_dead_tape_never_manufactures_cancel_driven():
    """`traded = 0` from an unsubscribed socket is arithmetically identical to a queue that pulled.
    The most dangerous failure mode of the whole feature."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_level(_delta("yes", "0.40", "-10"))
    qt.on_level(_delta("yes", "0.40", "-1"))

    row = _summarize(qt, "o1", filled_qty=1, tape_ok=False)
    assert row["verdict"] == UNKNOWN_TAPE_DOWN


def test_a_book_snapshot_flags_the_L2_columns_without_touching_the_fill_verdict():
    """A resnapshot puts a hole in `removed`, and `removed` no longer decides a fill verdict — so
    the row must carry the flag (the diagnostic columns and the unfilled population still depend on
    it) while the tape-derived verdict stands on its own."""
    qt = QueueTracker()
    _track_buy(qt, oid="gap", ahead=10)
    qt.on_trade(_trade("0.40", "2"))
    qt.on_level(_delta("yes", "0.40", "-2"))
    qt.on_level({"kind": "snapshot", "ticker": T})
    qt.on_level(_delta("yes", "0.40", "-8"))
    row = _summarize(qt, "gap", filled_qty=1)
    assert row["book_gap"] is True
    assert row["verdict"] == NOT_TRADE_THROUGH

    qt2 = QueueTracker()
    _track_buy(qt2, oid="gap2", ahead=10)
    qt2.on_trade(_trade("0.40", "10", ts=999.5))    # tape alone already clears the queue
    qt2.on_level({"kind": "snapshot", "ticker": T})
    qt2.on_trade(_trade("0.40", "1", ts=1000.035))
    assert _summarize(qt2, "gap2", filled_qty=1, fill_ts=1000.0)["verdict"] == TRADE_THROUGH


def test_a_level_observer_fault_is_flagged_like_a_book_gap():
    """Observer exceptions are lost L2 events. A fault counter nothing reads is not a guard."""
    qt = QueueTracker()
    _track_buy(qt, oid="live")
    qt.note_observer_errors(0)
    qt.note_observer_errors(3)
    assert qt.get("live").book_gap is True


def test_a_snapshot_does_not_taint_orders_placed_after_it():
    qt = QueueTracker()
    _track_buy(qt, oid="before", ahead=10)
    qt.on_level({"kind": "snapshot", "ticker": T})
    _track_buy(qt, oid="after", ahead=10)
    assert qt.get("before").book_gap is True
    assert qt.get("after").book_gap is False


def test_an_unobservable_queue_is_not_reported_as_an_empty_one():
    """`ahead=None` means we quoted outside the touch, where depth is unobserved. Treating it as 0
    would grant priority we never had and flatter TRADE_THROUGH."""
    qt = QueueTracker()
    qt.track("o1", T, "buy", "0.40", 1, None, t_placed=100.0)
    qt.on_trade(_trade("0.40", "1"))
    assert _summarize(qt, "o1", filled_qty=1)["verdict"] == UNKNOWN_NO_QUEUE


def test_cancel_driven_states_the_claim_it_can_actually_support():
    """⚠️ NOT_TRADE_THROUGH conflates 'the queue ahead of us pulled' with '`ahead` was already stale when
    our order landed'. Both are cases where we did NOT get through a queue by trading, which is what
    the verdict asserts — and nothing available can separate them (L2 reports a level, not a queue
    position). `cancelled_at_level` is on the row for anyone who wants to try."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "1"))            # only our own fill ever prints
    qt.on_level(_delta("yes", "0.40", "-1"))    # and the level stream saw no cancels at all
    row = _summarize(qt, "o1", filled_qty=1)
    assert row["verdict"] == NOT_TRADE_THROUGH
    assert row["cancelled_at_level"] == Decimal("0"), (
        "zero cancels observed at the level — the reader can tell this apart from a queue that "
        "visibly pulled, which is why the column stays on the row")


def test_a_fill_discovered_at_cancel_time_is_labelled_not_silently_dropped():
    """`gone_at_cancel` IS a fill, but we hold neither its size nor its timestamp, and closing the
    tracker here means the later fill poll writes nothing. Left blank it would silently shrink the
    finding population by the late-in-cycle fills — the ones closest to a price move."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    assert _summarize(qt, "o1", outcome="gone_at_cancel")["verdict"] == UNKNOWN_FILL_AT_CANCEL


# ── the bound direction: under-detecting adverse selection is the dangerous error ──

def test_the_verdict_uses_the_TIGHTER_bound_not_the_looser_one():
    """Two upper bounds on `traded_ahead` exist (detection-lag vs venue-ts) and the verdict must use
    the TIGHTER. Post-fill prints are not neutral noise: once we are gone the ask can fall to our
    price, so those prints can be opposite-side flow, and that happens precisely on the ADVERSE
    fills. Counting them would convert adverse fills into TRADE_THROUGH — under-detecting adverse
    selection, which is the error that green-lights committing more capital.

    ⚠️ THIS TEST EXISTS BECAUSE ITS PREDECESSOR DID NOT PIN THE CLAIM. `traded_ahead_by_ts` sums a
    strict SUBSET of the same prints, so it can never exceed `traded_ahead`; the old `max` was
    therefore a no-op, and swapping it for `min` left every test in this module green. The
    fixture below is built so the two bounds STRADDLE `ahead` — that is what makes the choice
    observable at all."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "4", ts=1000.0))     # before the fill: 4 of the 10 ahead traded
    qt.on_level(_delta("yes", "0.40", "-4"))
    qt.on_level(_delta("yes", "0.40", "-6"))        # the other 6 pulled
    qt.on_trade(_trade("0.40", "1", ts=1000.0))     # our own fill
    qt.on_level(_delta("yes", "0.40", "-1"))
    qt.on_trade(_trade("0.40", "9", ts=1005.0))     # AFTER — poll-lag, possibly opposite-side flow
    qt.on_level(_delta("yes", "0.40", "-9"))

    row = _summarize(qt, "o1", filled_qty=1, fill_ts=1000.0)
    assert row["traded_ahead"] == Decimal("13")        # loose bound: would clear ahead=10
    assert row["traded_ahead_by_ts"] == Decimal("4")   # tight bound: falls short of it
    assert row["verdict"] == NOT_TRADE_THROUGH, (
        "the loose bound would have said TRADE_THROUGH — using it hides adverse selection")
    assert row["cancelled_ahead_implied"] == Decimal("6")   # 10 ahead - 4 that traded


# ── the guards that must beat both findings ──

def test_quoting_alone_at_a_price_is_not_a_trade_through_finding():
    """`_queue_ahead` returns exactly 0 for any quote INSIDE the touch, so `best_traded >= 0` is
    vacuously true and every improved fill would score TRADE_THROUGH having shown nothing — the
    verdict would just be restating `--improve-ticks`. Being alone at a price IS being the last
    resting order in front of whatever arrives, i.e. the NOT_TRADE_THROUGH hypothesis, not its
    refutation."""
    qt = QueueTracker()
    _track_buy(qt, ahead=0)                          # improved: inside the touch
    qt.on_trade(_trade("0.40", "1"))                 # only our own fill ever prints
    qt.on_level(_delta("yes", "0.40", "-1"))
    assert _summarize(qt, "o1", filled_qty=1)["verdict"] == ALONE_AT_PRICE


def test_a_tape_reconnect_while_we_rest_invalidates_the_row():
    """A reconnect that SUCCEEDS leaves `subscribed` back at True, so `tape_ok` alone cannot see
    it — but every print during the outage is gone, which reads as a queue that pulled. One 3-second
    blip would otherwise turn every order resting across it into NOT_TRADE_THROUGH."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.note_tape_state(n_reconnects=0, n_dropped=0)   # baseline
    qt.note_tape_state(n_reconnects=1, n_dropped=0)   # the socket dropped and came back
    qt.on_level(_delta("yes", "0.40", "-10"))
    qt.on_level(_delta("yes", "0.40", "-1"))
    assert _summarize(qt, "o1", filled_qty=1, tape_ok=True)["verdict"] == UNKNOWN_TAPE_GAP


def test_dropped_prints_also_mark_a_tape_gap_not_only_reconnects():
    """The reconnect counter is not the only way prints go missing — a frame that WAS a trade and
    could not be parsed is a lost print too."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.note_tape_state(n_reconnects=0, n_dropped=0)
    qt.note_tape_state(n_reconnects=0, n_dropped=2)
    assert qt.get("o1").tape_gap is True


def test_a_tape_that_was_down_at_placement_taints_the_row_even_if_it_acks_later():
    """`tape_ok` sampled only at close asks "is the tape up now?" when the question is "was it up
    for this order's whole rest". The initial subscribe goes False→True with no reconnect at all, so
    a cycle-1 order can rest with the tape dark, fill, and be scored after the ack lands — traded=0
    with every gap flag clean, i.e. a confident NOT_TRADE_THROUGH from a socket."""
    qt = QueueTracker()
    qt.track("o1", T, "buy", "0.40", 1, 10, t_placed=100.0, tape_ok=False)
    qt.on_trade(_trade("0.40", "1"))
    assert _summarize(qt, "o1", filled_qty=1, tape_ok=True)["verdict"] == UNKNOWN_TAPE_DOWN


def test_a_tape_gap_does_not_taint_orders_placed_after_it():
    qt = QueueTracker()
    _track_buy(qt, oid="before")
    qt.note_tape_state(n_reconnects=1, n_dropped=0)
    _track_buy(qt, oid="after")
    assert qt.get("before").tape_gap is True
    assert qt.get("after").tape_gap is False


def test_the_DECIDING_bound_is_completeness_checked_not_only_the_loose_total():
    """The by-ts figure is what the verdict runs on, and it has its own way of going short — a print
    whose venue timestamp falls outside the window. Guarding only `q.traded` leaves the number that
    actually decides unchecked."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "1", ts=1009.0))    # our fill, stamped well past the window
    row = _summarize(qt, "o1", filled_qty=1, fill_ts=1000.0)
    assert row["traded_ahead"] == Decimal("0"), "the LOOSE total is fine — it sees the print"
    assert row["traded_ahead_by_ts"] == Decimal("-1"), "the deciding bound is short by our own fill"
    assert row["verdict"] == UNKNOWN_TAPE_INCOMPLETE


def test_a_trade_through_that_rests_on_boundary_second_prints_is_REFUSED():
    """⚠️ THE ONE-SIDEDNESS OF THE WHOLE MEASUREMENT TURNS ON THIS BRANCH.

    `fill_ts` is truncated to the second, so prints in `(fill_ts, fill_ts+1]` cannot be ordered
    against our own fill. Including them is an OVER-count of `traded` — the opposite direction to
    every other residual bias — and it lands precisely on the adverse fill, because once we are gone
    the ask can fall to our price and prints there become opposite-side lifts. Resolve it toward
    TRADE_THROUGH and the reported adverse rate has no established direction at all.

    Here: 3 of the 10 ahead trade before the fill; 12 more print inside the boundary second. The
    loose reading clears the queue (15 >= 10) and would say the queue traded through. It did not."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "3", ts=1000.0))     # genuinely before our fill
    qt.on_trade(_trade("0.40", "1", ts=1000.0))     # our own fill
    qt.on_trade(_trade("0.40", "12", ts=1000.5))    # unorderable: same venue second, after us

    row = _summarize(qt, "o1", filled_qty=1, fill_ts=1000.0)
    assert row["boundary_traded"] == Decimal("12")
    assert row["traded_ahead_by_ts"] == Decimal("15"), "the window includes them by construction"
    assert row["verdict"] == UNKNOWN_FILL_SECOND, (
        "15 >= 10 only because of prints that cannot be ordered against our own fill — refusing is "
        "what keeps NOT_TRADE_THROUGH an upper bound rather than a number of unknown direction")


def test_a_print_beyond_the_venue_second_is_orderable_and_must_not_inflate_traded():
    """The boundary window is exactly ONE second because that is the venue's fill-time granularity —
    it is not a tuning knob. A print at `fill_ts + 1.5` is unambiguously after our fill and must be
    excluded, not absorbed. Widening the window would quietly convert adverse rows into
    UNKNOWN_FILL_SECOND, shrinking the finding population by an arbitrary amount.

    (With the refusal branch in place a wider window cannot manufacture a TRADE_THROUGH: what the
    gate compares is `A + min(B_window, n_filled)`, which stops moving once the window is wide enough
    to hold our own print. So this pins coverage more than correctness — but it still pins, because
    below that threshold the width does change the compared value.)"""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "3", ts=1000.0))     # before the fill
    qt.on_trade(_trade("0.40", "1", ts=1000.0))     # our fill
    qt.on_trade(_trade("0.40", "20", ts=1001.5))    # a full second past — orderable, not boundary

    row = _summarize(qt, "o1", filled_qty=1, fill_ts=1000.0)
    assert row["boundary_traded"] == Decimal("0")
    assert row["traded_ahead_by_ts"] == Decimal("3")
    assert row["verdict"] == NOT_TRADE_THROUGH


def test_a_trade_through_standing_on_its_own_is_still_a_finding():
    """The control for the test above: refusing must not swallow genuine trade-throughs.

    ⚠️ OUR OWN FILL PRINTS AT A FRACTIONAL TIMESTAMP HERE, and that is the whole point. `fill_ts` is
    truncated to the second while the tape is sub-second, so our print normally lands just ABOVE it
    — inside the boundary. An earlier version of this test put it at exactly `ts=1000.0`, the single
    value that hides a double-subtraction of our own size, and the suite stayed green against a
    threshold that suppressed clean trade-throughs on every realistically-stamped fill."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "10", ts=1000.0))     # the whole queue, strictly before
    qt.on_trade(_trade("0.40", "1", ts=1000.035))    # OUR fill — fractional, i.e. in the boundary
    row = _summarize(qt, "o1", filled_qty=1, fill_ts=1000.0)
    assert row["boundary_traded"] == Decimal("1"), "our own print is the only boundary print"
    assert row["verdict"] == TRADE_THROUGH, (
        "10 of the 10 ahead traded strictly before us — refusing this would demand a full own-size "
        "more flow than the FIFO identity asks, on every normally-stamped fill")


def test_a_lone_own_fill_print_in_the_boundary_does_not_suppress_the_finding():
    """The minimal form of the same defect: the ONLY print in the fill second is ours."""
    qt = QueueTracker()
    _track_buy(qt, ahead=3, size=1)
    qt.on_trade(_trade("0.40", "3", ts=995.0))       # queue traded five seconds earlier
    qt.on_trade(_trade("0.40", "1", ts=1000.035))    # our fill
    assert _summarize(qt, "o1", filled_qty=1, fill_ts=1000.0)["verdict"] == TRADE_THROUGH


def test_a_fill_with_no_venue_timestamp_is_not_a_finding():
    """`ts_src=detected` leaves only the loose, detection-bounded total — every post-fill print up to
    the poll is in it and there is no boundary to subtract. A TRADE_THROUGH there would rest on an
    over-counted `traded` with no refusal available."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "50"))                # no ts on any print
    assert _summarize(qt, "o1", filled_qty=1, fill_ts=None)["verdict"] == UNKNOWN_NO_FILL_TS


def test_the_by_ts_window_spans_the_whole_second_the_venue_reports():
    """Kalshi reports fill times to the whole SECOND while the tape is sub-second, so the true fill
    instant lies in [fill_ts, fill_ts+1) and our own fill's print normally carries a fractional
    stamp ABOVE the truncated value. Bounding at fill_ts exactly would drop it while still
    subtracting our size — a guaranteed error of `size` on every row."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "4", ts=1000.2))    # queue trading ahead of us
    qt.on_trade(_trade("0.40", "1", ts=1000.4))    # our own fill, same venue second
    row = _summarize(qt, "o1", filled_qty=1, fill_ts=1000.0)
    assert row["traded_ahead_by_ts"] == Decimal("4")
    assert row["verdict"] == NOT_TRADE_THROUGH, "not UNKNOWN_TAPE_INCOMPLETE — the tape was fine"


def test_the_implied_cancel_stays_inside_the_bounds_the_identity_allows():
    """`cancelled_ahead_implied` is a count of contracts: it cannot be negative, and it cannot
    exceed the queue we were behind. An unclamped value greps as real data."""
    qt = QueueTracker()
    _track_buy(qt, oid="over", ahead=5)
    qt.on_trade(_trade("0.40", "10"))              # more traded than we were ever behind
    qt.on_trade(_trade("0.40", "1"))
    assert _summarize(qt, "over", filled_qty=1)["cancelled_ahead_implied"] == Decimal("0")

    qt2 = QueueTracker()
    _track_buy(qt2, oid="under", ahead=5)          # tape missed our own fill entirely
    assert _summarize(qt2, "under", filled_qty=1)["cancelled_ahead_implied"] == Decimal("5")


def test_a_missing_own_fill_print_is_proof_the_tape_is_incomplete():
    """The sharp case. Our own fill ALWAYS prints at our own price, so `traded < filled` is
    arithmetic proof a print was dropped — and it is exactly the shape that otherwise yields a
    confident NOT_TRADE_THROUGH off a negative `traded_ahead`."""
    qt = QueueTracker()
    _track_buy(qt, ahead=0)
    qt.on_level(_delta("yes", "0.40", "-1"))          # the book saw our fill; the tape did not
    row = _summarize(qt, "o1", filled_qty=1)
    assert row["verdict"] == UNKNOWN_TAPE_INCOMPLETE
    assert row["traded_ahead"] == Decimal("-1"), "the negative stays visible, not clamped away"


def test_a_print_at_a_different_price_never_counts():
    qt = QueueTracker()
    _track_buy(qt, px="0.40", ahead=10)
    qt.on_trade(_trade("0.41", "50"))
    qt.on_trade(_trade("0.39", "50"))
    assert qt.get("o1").traded == Decimal("0")


def test_a_print_on_another_ticker_never_counts():
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "50", ticker="KXNPBGAME-26JUL24SOFYOM-SOF"))
    assert qt.get("o1").traded == Decimal("0")


# ── boundary hygiene ──

def test_the_decimal_boundary_refuses_a_price_it_cannot_represent_exactly():
    qt = QueueTracker()
    assert qt.track("bad", T, "buy", float("nan"), 1, 10, t_placed=100.0) is False
    assert qt.track("bad", T, "buy", "0.40", float("inf"), 10, t_placed=100.0) is False
    assert qt.track("bad", T, "buy", "1.40", 1, 10, t_placed=100.0) is False
    assert qt.track("bad", T, "hold", "0.40", 1, 10, t_placed=100.0) is False
    assert qt.live_count == 0


def test_a_float_price_does_not_launder_its_error_into_the_decimal():
    """`Decimal(0.4)` is 0.4000000000000000222…, which would never equal a venue `Decimal("0.40")`
    and would silently match nothing. Via `str` it does."""
    qt = QueueTracker()
    assert qt.track("f", T, "buy", 0.4, 1, 10, t_placed=100.0)
    qt.on_trade(_trade("0.40", "7"))
    assert qt.get("f").traded == Decimal("7")


def test_matched_counters_distinguish_broken_plumbing_from_a_quiet_market():
    qt = QueueTracker()
    assert qt.matched == (0, 0)
    _track_buy(qt, ahead=10)
    qt.on_trade(_trade("0.40", "1"))
    qt.on_level(_delta("yes", "0.40", "-1"))
    assert qt.matched == (1, 1)


def test_the_effective_book_change_wins_over_the_wire_delta():
    """A level popped below the feed's resolution floor moves the book by less than the wire said.
    Accumulating the wire value over-counts `removed`, which pushes toward NOT_TRADE_THROUGH."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    # Wire says -50, but the level only held 5 and is popped: the book moved by 5.
    ev = _delta("yes", "0.40", "-50")
    ev["qty_before"], ev["qty_after"] = Decimal("5"), Decimal("0")
    qt.on_level(ev)
    assert qt.get("o1").removed == Decimal("5")


@pytest.mark.parametrize("bad", [
    {"kind": "delta", "ticker": T, "side": "yes", "price": None, "delta": Decimal("-1")},
    {"kind": "delta", "ticker": T, "side": None, "price": Decimal("0.40"), "delta": Decimal("-1")},
    {"kind": "snapshot"},
    {},
])
def test_a_malformed_level_event_is_ignored_not_raised(bad):
    """The observer runs inline on the arb bot's own WS receive loop. A raise here is caught by
    feed._notify_level, but it would still count as an error and hide real ones."""
    qt = QueueTracker()
    _track_buy(qt, ahead=10)
    qt.on_level(bad)
    assert qt.get("o1").removed == Decimal("0")
