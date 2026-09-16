"""Why did the queue ahead of our resting quote clear — did it TRADE away, or was it CANCELLED away?

`order_meta['ahead']` records how many contracts sat in front of us when we posted; when the order
later fills, that number alone cannot say what happened in between, and the two cases carry opposite
economics — flow chewing through the queue is market-making working, while a queue that evaporated
without trading and *then* let us fill is the adverse-selection signature.

This module is the arithmetic that separates them, and nothing else: no I/O, no venue calls, no clock
of its own. (The OFFLINE version could not be made to work — fill times are reported to the SECOND
while the tape is sub-second, and cancel attrition lives in the L2 stream the trade tape lacks.)

At a single price level, over the life of one resting order: `removed = traded + cancelled`.
`removed` comes from the L2 delta observer (`feed.set_level_observer`); `traded` from the trade tape
(`bot.kalshi.trade_feed`); `cancelled` is the residual.

⚠️ **That residual is `cancelled_at_level`, NOT "cancelled ahead of us".** L2 reports a level, not a
queue position, so it also counts cancels by orders that joined BEHIND us. For a FILLED order
price-time priority forces the quantity M23 wants and needs no L2 at all:
`cancelled_ahead_implied = ahead − traded_ahead`. **So the fill verdict rests on the TAPE ALONE**;
the L2 half is load-bearing only for the UNFILLED population and stays on the row as a diagnostic.
Two details make the identity exact: our own FILL appears on both sides and cancels out of
`removed - traded` (but inflates `traded`, so the fill-side view subtracts our filled count); our own
CANCEL appears only in `removed`, so `summarize` subtracts it explicitly.

Kalshi keeps two BID ladders: a YES **buy** at p rests on `yes` at p, a YES **sell** at p is a NO bid
resting on `no` at **1 - p**. `_ladder_of` owns that mapping; backwards, it accumulates the wrong
side of the book and still looks like data.

No aggressor inference is needed WHILE the order rests: a resting YES ask at p cannot coexist with
our resting YES bid at p (`post_only` plus the engine's no-cross invariant), so any print at p hit a
YES bid. ⚠️ **It breaks after our fill, and in the direction that matters** — once we are gone the ask
CAN fall to p and prints there become offer-lifts, which happens precisely when the price runs down
through our bid, converting ADVERSE fills into `TRADE_THROUGH`. `summarize` therefore bounds the
accrual near the venue's fill timestamp rather than at our detection of it, and since fill times are
whole seconds a `TRADE_THROUGH` depending on boundary-second prints becomes `UNKNOWN_FILL_SECOND`.

⛔ FAIL LOUD, NEVER FAIL QUIET. A trade feed connected but not subscribed produces `traded = 0` on
every order — read naively, a spectacular NOT_TRADE_THROUGH finding produced by a dead socket. So the
verdict is `UNKNOWN_TAPE_DOWN` whenever the tape is not confirmed subscribed (latched at placement
AND re-checked at close), plus `UNKNOWN_TAPE_GAP` on a reconnect or lost print,
`UNKNOWN_TAPE_INCOMPLETE` when the accumulator is short of our own fill, and `UNKNOWN_FILL_SECOND` as
above. None of them ever degrades into a real-looking answer. ⚠️ There is deliberately NO
`UNKNOWN_BOOK_GAP`: a level-stream hole cannot affect a fill verdict, so a `book_gap=Y` row CAN
carry one.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

_ZERO = Decimal("0")
_ONE = Decimal("1")
# The venue reports fill times to the whole SECOND, so the true instant lies in [fill_ts, fill_ts+1).
# ⚠️ ONE constant for both the by-ts and boundary windows — the gate's window-independence needs it.
_FILL_TS_GRANULARITY_S = 1.0

# Verdicts. Only the first two are findings; everything else says "this row cannot be read", and
# every one of them is checked BEFORE either finding.
TRADE_THROUGH = "TRADE_THROUGH"           # flow reached us through the queue — MM working as billed
# ⚠️ RENAMED from `CANCEL_DRIVEN` 2026-07-24 — old docs use that word. The verdict fires on
# `best_traded < ahead`: we filled without the tape showing the queue trade through. That is the
# adverse-selection SUSPECT population, but `CANCEL_DRIVEN` asserted a cause the arithmetic cannot
# isolate. Stratify by `track_lag_s` before calling any of it adverse.
NOT_TRADE_THROUGH = "NOT_TRADE_THROUGH"   # filled without the queue trading through — adverse suspect
ALONE_AT_PRICE = "ALONE_AT_PRICE"         # `ahead` was 0 — nobody to get through; see _verdict
UNKNOWN_NO_QUEUE = "UNKNOWN_NO_QUEUE"     # `ahead` was None — we quoted where depth is unobserved
UNKNOWN_TAPE_DOWN = "UNKNOWN_TAPE_DOWN"   # tape not confirmed subscribed → `traded` is meaningless
UNKNOWN_TAPE_GAP = "UNKNOWN_TAPE_GAP"     # the tape reconnected/dropped while we rested → prints lost
UNKNOWN_TAPE_INCOMPLETE = "UNKNOWN_TAPE_INCOMPLETE"   # traded < our own fill ⇒ a print is missing
UNKNOWN_FILL_AT_CANCEL = "UNKNOWN_FILL_AT_CANCEL"     # filled at cancel time; no size/ts to score it
UNKNOWN_FILL_SECOND = "UNKNOWN_FILL_SECOND"           # verdict would rest on unorderable boundary prints
UNKNOWN_NO_FILL_TS = "UNKNOWN_NO_FILL_TS"             # no venue fill ts ⇒ only the loose bound exists
# ⚠️ There is deliberately NO `UNKNOWN_BOOK_GAP`: once the L2 corroboration left the fill path, a
# hole in the level stream cannot affect a fill verdict. `book_gap` stays on every row because it
# does invalidate the L2-derived DIAGNOSTIC columns and the unfilled population.


def to_decimal(v) -> Decimal | None:
    """Any caller value → exact Decimal, or None if it cannot be one. Via `str` deliberately:
    `Decimal(0.47)` launders a float's representation error into the Decimal (CLAUDE.md § Code
    style). `bot/kalshi/maker.py` is still float throughout, so floats DO arrive here — this is the
    boundary where they stop."""
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v if v.is_finite() else None
    try:
        d = Decimal(str(v))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return d if d.is_finite() else None


def _ladder_of(action: str, px: Decimal) -> tuple[str, Decimal]:
    """(ladder name, price key on that ladder) for one of OUR yes-space orders. Both Kalshi ladders
    are BID ladders: a yes buy rests on `yes` at its own price, a yes sell is a NO bid resting on `no`
    at the complement — backwards, it accumulates the opposite side of the book and still looks like
    data."""
    if action == "buy":
        return "yes", px
    return "no", _ONE - px


@dataclass
class OrderQueue:
    """One resting order's accumulators. Populated by the two `on_*` methods, read by `summarize`."""

    oid: str
    ticker: str
    action: str                      # "buy" | "sell", yes-space (we only ever send yes orders)
    px: Decimal                      # the yes-space price we sent
    size: Decimal                    # our own resting size, needed to un-count our own cancel
    ahead: Decimal | None            # contracts in front of us at placement; None = unobserved
    t_placed: float
    ladder: str                      # which Kalshi ladder we rest on
    level_px: Decimal                # our key on that ladder
    # ⚠️ THE ONE BLIND WINDOW, and it biases toward the alarming verdict — so it is measured rather
    # than argued away. The venue can accept the order from the moment the create request lands, but
    # we cannot watch until the response hands us the order id: prints in that gap are missed, so
    # `traded` undercounts. One REST round trip (~0.1s) against a 6-30s rest; `track_lag_s` bounds it.
    t_sent: float = 0.0

    traded: Decimal = _ZERO          # Σ tape print size at our price while resting
    prints: int = 0
    removed: Decimal = _ZERO         # Σ |negative L2 delta| at our level
    added: Decimal = _ZERO           # Σ positive L2 delta — queue JOINING behind/at our level
    book_gap: bool = False           # a snapshot replaced the book: `removed` spans a hole
    tape_gap: bool = False           # the tape reconnected/dropped while we rested: prints missing
    tape_ok_at_track: bool = True    # was the tape acked when we started watching — see track()
    # (venue ts, size) per print, so the fill-side view can also bound `traded` by the venue's own
    # fill timestamp instead of only by when we noticed. Both bounds get logged; see `summarize`.
    prints_ts: list[tuple[float | None, Decimal]] = field(default_factory=list)


class QueueTracker:
    """Accumulates level-level evidence for every live order, and derives the fill/cancel verdict.
    `on_trade` and `on_level` are called inline on two WebSocket receive loops, so this is
    deliberately synchronous and allocation-light — a slow consumer there drops tape."""

    def __init__(self) -> None:
        self._live: dict[str, OrderQueue] = {}
        self._n_trades_matched = 0
        self._n_levels_matched = 0
        # Last-seen discontinuity counters — see note_tape_state / note_observer_errors.
        self._n_reconnects = 0
        self._n_dropped = 0
        self._n_obs_errors = 0

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def track(self, oid: str, ticker: str, action: str, px, size, ahead, t_placed: float,
              *, t_sent: float | None = None, tape_ok: bool = True) -> bool:
        """Start watching one resting order. Returns False if the inputs cannot be made exact.
        A refusal is not an error to swallow: an untracked order produces no `queue_dynamics` row,
        which is honest. Inventing a zero-filled row would read as NOT_TRADE_THROUGH."""
        d_px, d_size = to_decimal(px), to_decimal(size)
        if d_px is None or d_size is None or action not in ("buy", "sell"):
            return False
        if not (_ZERO < d_px < _ONE):
            return False
        ladder, level_px = _ladder_of(action, d_px)
        self._live[oid] = OrderQueue(
            oid=oid, ticker=ticker, action=action, px=d_px, size=d_size,
            # `ahead=None` is preserved as None on purpose — it means the depth at our price was
            # never observed (we quoted outside the touch), which is NOT the same as an empty queue.
            ahead=to_decimal(ahead), t_placed=t_placed,
            ladder=ladder, level_px=level_px,
            t_sent=t_placed if t_sent is None else t_sent,
            # ⚠️ LATCHED AT PLACEMENT, and `summarize` ANDs it with the state at close: sampling only
            # at close asks "is the tape up now?" when the question is "was it up for this order's
            # whole rest". On the INITIAL subscribe `subscribed` goes False→True once, so a cycle-1
            # order could rest with the tape dark, fill, and score a confident NOT_TRADE_THROUGH.
            tape_ok_at_track=tape_ok,
        )
        return True

    def close(self, oid: str) -> OrderQueue | None:
        """Stop watching `oid` and hand back its accumulators (None if it was never tracked)."""
        return self._live.pop(oid, None)

    def get(self, oid: str) -> OrderQueue | None:
        return self._live.get(oid)

    def note_tape_state(self, n_reconnects: int, n_dropped: int) -> None:
        """Poll the trade feed's discontinuity counters; mark every live order if either moved.

        The L2 half announces its own gaps (a snapshot event) but the tape cannot: a reconnect that
        SUCCEEDS leaves `subscribed` back at True, so the healthy-looking end state hides an outage
        during which every print at our price was lost — a 3-second blip would turn every order
        resting across it into NOT_TRADE_THROUGH.

        Poll-based because the feed reconnects on its own task; call it often enough that the marking
        lands while the affected orders are still live (the maker calls it at 1 Hz).

        ⚠️ `n_dropped` here must be a LOST-PRINT counter — `KalshiTradeFeed.n_bad_trades +
        n_callback_errors` — and NEVER `errors` or `n_dropped`, which also accrue control frames and
        venue error frames: those would mark every live order on every poll.
        """
        if n_reconnects > self._n_reconnects or n_dropped > self._n_dropped:
            for q in self._live.values():
                q.tape_gap = True
        self._n_reconnects = max(self._n_reconnects, n_reconnects)
        self._n_dropped = max(self._n_dropped, n_dropped)

    def note_observer_errors(self, n_errors: int) -> None:
        """Level-observer faults are lost L2 events; treat them exactly like a tape gap. Their effect
        is to undercount `removed`; they corrupt the diagnostic columns and the unfilled population,
        and a fault counter nothing reads is not a guard (the private design notes M4)."""
        if n_errors > self._n_obs_errors:
            for q in self._live.values():
                q.book_gap = True
        self._n_obs_errors = max(self._n_obs_errors, n_errors)

    @property
    def live_count(self) -> int:
        return len(self._live)

    @property
    def matched(self) -> tuple[int, int]:
        """(trade prints, level deltas) that landed on a tracked order — the "is this wired?" pair.
        Zero on both across a run with fills means the plumbing is broken, not that the market was
        quiet, and the distinction is invisible without a counter."""
        return self._n_trades_matched, self._n_levels_matched

    # ── event intake ──────────────────────────────────────────────────────────

    def on_trade(self, event: dict) -> None:
        """One print from `bot.kalshi.trade_feed`. Accrues to every order resting at that price.
        Both of our own quotes on a ticker are tracked at once, but they sit at different prices
        (`_improve` guarantees a tick of daylight), so a print reaches at most one of them."""
        ticker = event.get("ticker")
        px = to_decimal(event.get("yes_price"))
        qty = to_decimal(event.get("qty"))
        if not ticker or px is None or qty is None or qty <= _ZERO:
            return
        ts = event.get("ts")
        for q in self._live.values():
            # Exact Decimal equality is correct HERE and would not be in float: both sides are parsed
            # from string form, so `Decimal("0.4000") == Decimal("0.40")` compares numerically. This is
            # the comparison `_queue_ahead` needs a half-tick tolerance for, since it re-parses a float.
            if q.ticker == ticker and q.px == px:
                q.traded += qty
                q.prints += 1
                q.prints_ts.append((ts if isinstance(ts, (int, float)) else None, qty))
                self._n_trades_matched += 1

    def on_level(self, event: dict) -> None:
        """One level event from `feed.set_level_observer`."""
        kind = event.get("kind")
        ticker = event.get("ticker")
        if not ticker:
            return
        if kind == "snapshot":
            # Mark every order currently resting on this ticker. Marking at the moment of the gap
            # keeps orders placed after the gap closed clean, which is most of them on a long run.
            for q in self._live.values():
                if q.ticker == ticker:
                    q.book_gap = True
            return
        if kind != "delta":
            return
        side = event.get("side")
        price = to_decimal(event.get("price"))
        # The EFFECTIVE change to the book, not the wire value. They differ when a level whose new qty
        # falls below the feed's resolution floor is popped to zero: accumulating the wire over-counts
        # `removed`, which pushes toward NOT_TRADE_THROUGH. Falls back to the raw delta if absent.
        before = to_decimal(event.get("qty_before"))
        after = to_decimal(event.get("qty_after"))
        if before is not None and after is not None:
            delta = after - before
        else:
            delta = to_decimal(event.get("delta"))
        if side is None or price is None or delta is None or delta == _ZERO:
            return
        for q in self._live.values():
            if q.ticker == ticker and q.ladder == side and q.level_px == price:
                if delta < _ZERO:
                    q.removed += -delta
                else:
                    q.added += delta
                self._n_levels_matched += 1

    # ── the verdict ───────────────────────────────────────────────────────────

    def summarize(self, q: OrderQueue, *, outcome: str, tape_ok: bool,
                  filled_qty=None, fill_ts: float | None = None,
                  now: float) -> dict:
        """Derive the reportable numbers and the verdict for one finished order.

        `outcome` is the maker's own word for what happened (`fill`, `cancelled_unfilled`,
        `gone_at_cancel`, …). `tape_ok` MUST be the trade feed's confirmed-subscribed state, not an
        assumption — a dead tape otherwise manufactures a finding.

        ⚠️ `traded_ahead` has two bounds, both reported, and **the verdict uses the TIGHTER one**:
          · `traded_ahead` accrues everything up to the moment we NOTICED the fill, so prints landing
            in that lag — from orders behind us, or from the opposite side once we are gone — count.
          · `traded_ahead_by_ts` counts only prints at or before the venue's fill timestamp; the
            boundary second is ambiguous but it excludes the post-fill window entirely.
        This measurement decides whether to commit more capital, where the dangerous error is
        UNDER-detecting adverse selection — which over-counting `traded` is. So the tighter bound wins.

        ⚠️ **Read the result accordingly: the reported `NOT_TRADE_THROUGH` rate is an UPPER bound and
        `TRADE_THROUGH` a LOWER one**, and the remaining `track_lag_s` bias pushes the same way.
        That one-sidedness holds only because the fill SECOND is REFUSED rather than resolved: the
        `fill_ts + 1.0` window OVER-counts `traded`, so a `TRADE_THROUGH` resting on boundary prints
        would leave the rate with no established direction. `_verdict` returns `UNKNOWN_FILL_SECOND`
        in exactly that case; do not "simplify" that branch away.

        `detect_lag_s` is reported so an analysis can drop rows whose poll lag was long enough to
        make even the loose bound useless.
        """
        tape_ok = bool(tape_ok and q.tape_ok_at_track)
        n_filled = to_decimal(filled_qty) or _ZERO

        # Our own fill both removes our size from the level AND prints on the tape, so it cancels out
        # of `removed - traded` with no correction. Our own CANCEL only removes.
        # ⚠️ Gated on `cancelled_unfilled` specifically, NOT on `outcome != "fill"`: `gone_at_cancel`
        # means the order FILLED between poll and cancel (its size left via a print), and
        # `cancel_failed_may_still_rest` means nothing was removed. The `q.removed >= q.size` guard
        # covers the race where our cancel's own L2 delta has not landed yet.
        own_cancel = (q.size if outcome == "cancelled_unfilled" and q.removed >= q.size
                      else _ZERO)
        cancelled_at_level = q.removed - q.traded - own_cancel

        traded_ahead = q.traded - n_filled
        # Prints inside the venue's fill SECOND — the ones that cannot be ordered against our own
        # fill. Reported separately so `_verdict` can refuse a finding that depends on them. (The
        # clearing sweep prints ~35ms after the integer fill_ts, so this is where contamination lives.)
        if fill_ts is None:
            boundary_traded = None
        else:
            boundary_traded = sum(
                (sz for ts, sz in q.prints_ts
                 if ts is not None and fill_ts < ts <= fill_ts + _FILL_TS_GRANULARITY_S), _ZERO)
        if fill_ts is None:
            traded_ahead_by_ts = None
        else:
            # ⚠️ `fill_ts + 1`, not `fill_ts`. The venue reports fill times to the whole SECOND while
            # the tape is sub-second, so the true instant lies in [fill_ts, fill_ts+1) and our own
            # fill's print usually carries a fractional ts ABOVE the truncated fill_ts. Bounding at
            # fill_ts exactly would drop it while still subtracting `n_filled`, making this short by
            # our own size and pushing `cancelled_ahead_implied` above `ahead`, which FIFO forbids.
            traded_ahead_by_ts = sum(
                (sz for ts, sz in q.prints_ts
                 if ts is not None and ts <= fill_ts + _FILL_TS_GRANULARITY_S), _ZERO
            ) - n_filled

        # The tighter bound when we have it; otherwise the loose one, and the row says so via
        # `traded_ahead_by_ts=unknown` so those rows can be dropped rather than averaged in.
        best_traded = traded_ahead if traded_ahead_by_ts is None else traded_ahead_by_ts

        # ⚠️ For a FILLED order under price-time priority THIS is the cancelled-ahead quantity, and it
        # needs no L2 at all: we filled, so everything ahead is gone, the tape says how much traded,
        # the remainder left without trading. Clamped at BOTH ends — more than `ahead` is impossible
        # under the identity, and `_verdict`'s completeness guards catch a tape/`ahead` disagreement.
        if outcome == "fill" and q.ahead is not None:
            cancelled_ahead_implied = min(q.ahead, max(_ZERO, q.ahead - best_traded))
        else:
            cancelled_ahead_implied = None

        row = {
            "oid": q.oid, "ticker": q.ticker, "action": q.action, "px": q.px,
            "outcome": outcome,
            "rest_s": max(0.0, now - q.t_placed),
            "track_lag_s": max(0.0, q.t_placed - q.t_sent),
            "ahead": q.ahead,
            "traded_ahead": traded_ahead,
            "traded_ahead_by_ts": traded_ahead_by_ts,
            "boundary_traded": boundary_traded,
            "traded_total": q.traded,
            "cancelled_ahead_implied": cancelled_ahead_implied,
            "cancelled_at_level": cancelled_at_level,
            "removed": q.removed,
            "added": q.added,
            "prints": q.prints,
            "book_gap": q.book_gap,
            "tape_gap": q.tape_gap,
            "tape_ok": tape_ok,
            "detect_lag_s": (None if fill_ts is None else max(0.0, now - fill_ts)),
            "verdict": _verdict(q, best_traded, n_filled, boundary_traded,
                                outcome=outcome, tape_ok=tape_ok),
        }
        return row


def _strict_traded(best_traded: Decimal, boundary_traded: Decimal,
                   n_filled: Decimal) -> Decimal:
    """Traded ahead of us using ONLY prints that can be ordered before our own fill.

    With `A = Σ{ts ≤ fill_ts}` and `B = Σ{fill_ts < ts ≤ fill_ts+1}`, `best_traded = A + B −
    n_filled`. What we want is `A` minus our own print — but only if our print is in `A`.

    ⚠️ WHERE OUR OWN PRINT LIVES DECIDES THE CORRECTION, AND A NAIVE `best_traded − B` GETS IT WRONG:
    `fill_ts` is truncated to the second, so our fill normally prints just ABOVE it — inside `B` — and
    `best_traded − B = A − n_filled` nets our size a SECOND time, suppressing clean trade-throughs.

    Subtracting `max(0, B − n_filled)`:
      · our print WHOLLY in `B` (the normal case), or wholly in `A`: **EXACT**, zero residual.
      · ⚠️ **STRADDLE — part of our fill in each.** Residual is `min(B_others, a)`, bounded by our own
        filled size and pointing toward `TRADE_THROUGH` (the under-detect-adverse direction).
        Reachable via MULTI-PARTIAL fills; at `--size 1` it is under one contract, and sorting the
        batch by `ts` would close it.

    ⚠️ The `max(_ZERO, …)` clamp is DEFENSIVE AND CURRENTLY UNOBSERVABLE: `_verdict` only calls this
    inside `best_traded >= ahead`, where dropping it never changes a verdict. It stays for legibility
    — do not expect a mutation of it to die, and do not test that it does.
    """
    return best_traded - max(_ZERO, boundary_traded - n_filled)


def _verdict(q: OrderQueue, best_traded: Decimal, n_filled: Decimal,
             boundary_traded: Decimal | None, *, outcome: str, tape_ok: bool) -> str:
    """The fill-cause call. The ORDER of these guards is the design: every "cannot read this row"
    reason is checked BEFORE either finding, so a broken feed can never present as a result."""
    if not tape_ok:
        return UNKNOWN_TAPE_DOWN
    if q.tape_gap:
        # The tape reconnected or dropped messages while this order rested, so prints at our price are
        # missing and `traded` undercounts — which reads as a queue that pulled. A completed reconnect
        # leaves `subscribed` back at True, so `tape_ok` alone does NOT catch this.
        return UNKNOWN_TAPE_GAP
    if outcome == "gone_at_cancel":
        # The venue's 404 on cancel: the order FILLED between the poll and the cancel, but we are on
        # the cancel path and hold neither its size nor its timestamp. Its own label rather than the
        # blank of a non-fill, so the population it removes from the findings is COUNTABLE — it is
        # the late-in-cycle fill, plausibly enriched in adverse selection.
        return UNKNOWN_FILL_AT_CANCEL
    if outcome != "fill":
        # An unfilled order has no fill to explain. Its counts are still logged — the survivorship
        # half of the population, which a fills-only log cannot make.
        return ""
    if q.traded < n_filled or best_traded < _ZERO:
        # ARITHMETIC PROOF that the tape is incomplete: our own fill always prints at our own price,
        # so neither accumulator can be short of it. Distinct from tape_gap, which is the tape
        # REPORTING a discontinuity — this is the case where it reports none and is wrong anyway.
        # ⚠️ BOTH bounds are checked: the by-ts figure decides the verdict and has its own way of
        # going short, so guarding only the loose total would leave the deciding number unchecked.
        return UNKNOWN_TAPE_INCOMPLETE
    if q.ahead is None:
        return UNKNOWN_NO_QUEUE
    if q.ahead == _ZERO:
        # ⚠️ NOT `TRADE_THROUGH`. `_queue_ahead` returns exactly 0 for any quote INSIDE the touch, so
        # `best_traded >= 0` is vacuously true and every improved fill would score TRADE_THROUGH
        # having demonstrated nothing — the verdict would restate `--improve-ticks`, the arm label.
        # Quoting alone at a price IS being the last resting order in front of whatever arrives.
        return ALONE_AT_PRICE
    if best_traded >= q.ahead:
        # Enough flow came through at our price to reach us on price-time priority alone — UNLESS the
        # margin is made up of prints inside the venue's fill second, which cannot be ordered against
        # our own fill. Counting those resolves an ambiguity silently in the TRADE_THROUGH direction,
        # the under-detect error that green-lights more capital. So: refuse.
        if boundary_traded is None:
            # No venue fill timestamp (`ts_src=detected`), so `best_traded` is the LOOSE,
            # detection-bounded total and there is no boundary to subtract — a `TRADE_THROUGH` would
            # rest on an over-counted `traded` with no refusal available, so it is not a finding.
            return UNKNOWN_NO_FILL_TS
        if _strict_traded(best_traded, boundary_traded, n_filled) < q.ahead:
            return UNKNOWN_FILL_SECOND
        return TRADE_THROUGH
    # The tape says less traded than we were behind, and we filled anyway — so under FIFO the
    # remainder left without trading. That IS the finding.
    #
    # ⚠️ NO L2 CORROBORATION HERE, DELIBERATELY. It was redundant (cancelled-ahead is identically
    # `ahead - traded_ahead` for a filled order) and WRONG in the worst direction:
    # `cancelled_at_level` accrues over the whole rest window while the threshold used the
    # fill-ts-bounded figure, so post-fill opposite-side lifts shrank one side of the inequality
    # while raising the bar — precisely on the textbook ADVERSE fill. L2 level events carry no
    # timestamp, so the two halves cannot be put on one window.
    # The honest cost: this verdict conflates "the queue pulled" with "`ahead` was already stale".
    return NOT_TRADE_THROUGH
