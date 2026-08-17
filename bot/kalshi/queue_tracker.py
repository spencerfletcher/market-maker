"""Why did the queue ahead of our resting quote clear — did it TRADE away, or was it CANCELLED away?

`order_meta['ahead']` records how many contracts sat in front of us when we posted. When the order
later fills, that number alone cannot say what happened in between, and the two possibilities carry
opposite economics:

  · **Traded through.** Real flow chewed through the queue and reached us. This is market-making
    working: we were paid the spread for providing the liquidity that flow wanted.
  · **Cancelled away.** The queue in front of us evaporated without trading, and *then* we filled.
    That is the adverse-selection signature — the makers ahead of us saw something, pulled, and left
    us as the last resting bid in front of informed flow. A fill obtained this way is the one most
    likely to mark out against us.

This module is the arithmetic that separates them, and nothing else: no I/O, no venue calls, no
clock of its own. Feed it events and ask it for a verdict.

## Why this exists at all, and why the OFFLINE version was killed

`scripts/mm_fill_scorer.py` v1 tried to answer this after the fact by joining our recorded fills
against a separately-collected trade tape. It could not be made to work, for two reasons: the
venue reports fill times to the SECOND while the tape is sub-second, so the join's own ordering was
a coin flip; and the only mechanism that actually causes sweep-through — cancel attrition — lives in
the L2 delta stream, which the trade tape does not contain. Both defects are cured by measuring
IN-PROCESS: the maker sees its own placement, the tape, and the L2 deltas on ONE clock (its own
receipt time), so nothing has to be joined across feeds after the fact.

## The two inputs, and the identity that ties them together

At a single price level, over the life of one resting order:

    removed  =  traded  +  cancelled

`removed` comes from the L2 delta observer (`feed.set_level_observer`) — the sum of negative deltas
at our level. `traded` comes from the trade tape (`bot.kalshi.trade_feed`) — the sum of print sizes
at our price. `cancelled` is the residual.

⚠️ **That residual is `cancelled_at_level`, NOT "cancelled ahead of us", and the two are different
claims.** L2 reports a level, not a queue position, so the residual also counts cancels by orders
that joined BEHIND us after we posted. For a FILLED order the quantity we actually want is forced
by price-time priority and needs no L2 at all: we filled, so everything ahead of us is gone, the
tape says how much of it traded, and the rest left without trading —
`cancelled_ahead_implied = ahead − traded_ahead`. **So the fill verdict rests on the TAPE ALONE** —
the L2 half carries no information there and does not enter it; it is genuinely load-bearing for the
UNFILLED population, where no such identity exists, and stays on every row as a diagnostic. Two
earlier versions got this wrong in opposite directions: one named the level-wide residual
`cancelled_ahead` and let it decide the verdict, the other kept it as a corroboration threshold whose
two sides were measured over different time windows.

Two bookkeeping details make that identity hold exactly rather than approximately:

  · **Our own FILL appears on both sides and cancels out.** It removes our size from the level (L2)
    *and* prints on the tape (trade). It therefore leaves `removed - traded` untouched — no
    correction needed. It DOES inflate `traded`, so the fill-side view subtracts our filled count
    to recover the queue ahead of us.
  · **Our own CANCEL appears only in `removed`.** No print, so it must be subtracted explicitly or
    every cancelled order reports one extra cancelled contract. `summarize` does this.

## Which ladder our order rests on (the inversion that would silently swap the two sides)

Kalshi keeps two BID ladders. A YES **buy** at price p rests on the `yes` ladder at p. A YES **sell**
at p is a NO bid, and rests on the `no` ladder at **1 - p** (`feed._derive`: best yes ask =
1 - max(no price)). Mapping a sell to the `yes` ladder at p would accumulate deltas from the wrong
side of the book entirely, and — because both ladders are usually busy — it would look like data.
`_ladder_of` is the single place that mapping is written down.

## Why no aggressor inference is needed

`trade_feed.aggressor_from_mid` exists because a print's direction is otherwise ambiguous, and its
own docstring warns the inference inverts if the mid is read after the delta lands. We do not need
it here — **while the order actually rests**. A resting YES ask at p cannot coexist with our resting
YES bid at p; they would match, and `post_only` plus the engine's no-cross invariant is what
forbids it. (Not `_improve`, which is a no-op at its default `--improve-ticks 0`.) So while we rest
at p the only orders at p are YES bids, and any print at p hit one. The mirror argument holds for a
sell.

⚠️ **It breaks after our fill, and it breaks in the direction that matters.** Once we are gone the
ask CAN fall to p, and prints at p become offer-lifts — flow on the opposite side of the book. That
happens precisely when the price runs down through our bid, i.e. on the ADVERSE fills, so the
contamination is correlated with the thing being measured rather than being noise, and it converts
adverse fills into `TRADE_THROUGH`. This is why `summarize` bounds the accrual near the venue's fill
timestamp rather than at our detection of it. An earlier version called this residual "benign" on
the grounds that it made the adverse verdict harder to claim — which is exactly the wrong
conservatism for a number that authorizes capital.

⚠️ The bound cannot be exact, because the venue reports fill times to the whole SECOND. Prints inside
that second are genuinely unorderable against our own fill, and resolving them either way is a
decision — so `_verdict` refuses instead: a `TRADE_THROUGH` that depends on boundary-second prints
becomes `UNKNOWN_FILL_SECOND`. See `boundary_traded`.

## Fail loud, never fail quiet

A trade feed that is connected but not subscribed produces `traded = 0` on every order. Under a
naive reading that is "the queue in front of us never traded" — i.e. `NOT_TRADE_THROUGH` on 100% of
fills, a spectacular finding produced entirely by a dead socket. This repo has already lost two
real-money runs to exactly that shape of silence (a feed that logged healthy while a filter dropped
every message). So the verdict is `UNKNOWN_TAPE_DOWN` whenever the tape is not confirmed subscribed —
latched at placement AND re-checked at close — plus `UNKNOWN_TAPE_GAP` on a reconnect or a lost print,
`UNKNOWN_TAPE_INCOMPLETE` when the accumulator is short of our own fill, and `UNKNOWN_FILL_SECOND`
when a `TRADE_THROUGH` would rest on prints inside the unorderable fill second. None of them ever
degrades into a real-looking answer.

⚠️ There is deliberately NO `UNKNOWN_BOOK_GAP`: once the L2 half left the fill path, a hole in the
level stream cannot affect a fill verdict. `book_gap` stays on the row because it does invalidate the
L2-derived diagnostic columns and the unfilled population — so a `book_gap=Y` row CAN carry a real
fill verdict, and that is correct rather than an oversight.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

_ZERO = Decimal("0")
_ONE = Decimal("1")
# The venue reports fill times to the whole SECOND, so the true fill instant lies in
# [fill_ts, fill_ts + 1). ⚠️ ONE constant, used by both the by-ts window and the boundary window —
# the window-independence of the TRADE_THROUGH gate holds only because the two agree. It is the
# venue's granularity, not a tuning knob: widening it trades findings for UNKNOWN_FILL_SECOND.
_FILL_TS_GRANULARITY_S = 1.0

# Verdicts. Only the first two are findings; everything else says "this row cannot be read", and
# every one of them is checked BEFORE either finding.
TRADE_THROUGH = "TRADE_THROUGH"           # flow reached us through the queue — MM working as billed
# ⚠️ RENAMED from `CANCEL_DRIVEN` — older notes may still use that word. The
# verdict fires on `best_traded < ahead`: we filled without the tape showing the queue trade through.
# That is the adverse-selection SUSPECT population, but the name `CANCEL_DRIVEN` asserted a cause the
# arithmetic cannot isolate — it also contains "`ahead` was already stale when our order landed", and
# a `track_lag_s`-sized slice of it is pure measurement staleness. A headline "CANCEL_DRIVEN 40%" gets
# read as "40% adverse selection"; "NOT_TRADE_THROUGH 40%" cannot be. Same correction as
# `cancelled_ahead` → `cancelled_at_level`. Stratify by `track_lag_s` before calling any of it adverse.
NOT_TRADE_THROUGH = "NOT_TRADE_THROUGH"   # filled without the queue trading through — adverse suspect
ALONE_AT_PRICE = "ALONE_AT_PRICE"         # `ahead` was 0 — nobody to get through; see _verdict
UNKNOWN_NO_QUEUE = "UNKNOWN_NO_QUEUE"     # `ahead` was None — we quoted where depth is unobserved
UNKNOWN_TAPE_DOWN = "UNKNOWN_TAPE_DOWN"   # tape not confirmed subscribed → `traded` is meaningless
UNKNOWN_TAPE_GAP = "UNKNOWN_TAPE_GAP"     # the tape reconnected/dropped while we rested → prints lost
UNKNOWN_TAPE_INCOMPLETE = "UNKNOWN_TAPE_INCOMPLETE"   # traded < our own fill ⇒ a print is missing
UNKNOWN_FILL_AT_CANCEL = "UNKNOWN_FILL_AT_CANCEL"     # filled at cancel time; no size/ts to score it
UNKNOWN_FILL_SECOND = "UNKNOWN_FILL_SECOND"           # verdict would rest on unorderable boundary prints
UNKNOWN_NO_FILL_TS = "UNKNOWN_NO_FILL_TS"             # no venue fill ts ⇒ only the loose bound exists
# ⚠️ There is deliberately NO `UNKNOWN_BOOK_GAP`. Once the L2 corroboration was removed from the fill
# path (see `_verdict`), a hole in the level stream cannot affect a fill verdict — that verdict is a
# statement about the TAPE. `book_gap` stays on every row because it does invalidate the L2-derived
# DIAGNOSTIC columns (`removed`, `added`, `cancelled_at_level`) and the unfilled population, which is
# where those numbers are load-bearing. The column is its reader.


def to_decimal(v) -> Decimal | None:
    """Any caller value → exact Decimal, or None if it cannot be one.

    Via `str` deliberately: `Decimal(0.47)` launders a float's representation error into the Decimal
    and defeats the point of using one. `bot/kalshi/maker.py` is still float throughout, so floats
    DO arrive here — this is the boundary where they stop."""
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
    """(ladder name, price key on that ladder) for one of OUR yes-space orders.

    Both Kalshi ladders are BID ladders. A yes buy rests on `yes` at its own price; a yes sell is a
    NO bid and rests on `no` at the complement. See the module docstring — getting this backwards
    accumulates the opposite side of the book and still looks like data."""
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
    # than argued away. The venue can accept the order (and flow can hit it) from the moment the
    # create request lands, but we cannot start watching until the response hands us the order id.
    # Prints and deltas in that gap are missed: `traded` undercounts, which pushes a row toward
    # NOT_TRADE_THROUGH. It is one REST round trip (~0.1s) against a rest time of one requote interval
    # (6-30s), so the exposure is small — but `track_lag_s` is on every row, so it can be bounded
    # from the data instead of assumed. Closing it properly would mean tracking before the send
    # under a temporary key and re-keying on ack; not worth it until the data says it is.
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

    Deliberately synchronous and allocation-light: `on_trade` and `on_level` are called inline on
    two WebSocket receive loops, and a slow consumer there drops tape rather than queueing it."""

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

        A refusal is not an error to swallow: an untracked order simply produces no
        `queue_dynamics` row, which is honest. Inventing a zero-filled row would put a fabricated
        `traded=0` into the population and read as NOT_TRADE_THROUGH."""
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
            # ⚠️ LATCHED AT PLACEMENT, and `summarize` ANDs it with the state at close. Sampling only
            # at close asks "is the tape up now?" when the question is "was it up for this order's
            # whole rest". The gap that opens otherwise needs no reconnect at all: on the INITIAL
            # subscribe `subscribed` goes False→True once, so a cycle-1 order can rest with the tape
            # dark, fill, and be scored after the ack lands — `traded=0`, L2 up the whole time and
            # corroborating, i.e. a confident NOT_TRADE_THROUGH reached through the one door the
            # tape-down guard left open.
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
        during which every print at our price was lost. Undetected, that reads as a queue that
        pulled — a 3-second blip would turn every order resting across it into NOT_TRADE_THROUGH, which
        is the same manufactured-finding failure this module guards against on the tape-down side.

        Poll-based rather than callback-based because the feed reconnects on its own task; call it
        often enough that the marking lands while the affected orders are still live (the maker
        calls it at 1 Hz). Marking is per-order at the moment of the poll, so orders placed after
        the gap closed stay clean.

        ⚠️ `n_dropped` here must be a LOST-PRINT counter — `KalshiTradeFeed.n_bad_trades +
        n_callback_errors` — and NEVER `errors` or `n_dropped`, which also accrue unrecognised
        control frames, reconnect exceptions and venue error frames. The ack message types are a
        guess, so a venue emitting anything periodic would drive those up forever and mark every live
        order on every poll, returning a run that is 100% UNKNOWN_TAPE_GAP: a measurement that dies
        quietly.
        """
        if n_reconnects > self._n_reconnects or n_dropped > self._n_dropped:
            for q in self._live.values():
                q.tape_gap = True
        self._n_reconnects = max(self._n_reconnects, n_reconnects)
        self._n_dropped = max(self._n_dropped, n_dropped)

    def note_observer_errors(self, n_errors: int) -> None:
        """Level-observer faults are lost L2 events; treat them exactly like a tape gap.

        Their effect is to undercount `removed` — and before the L2 corroboration was dropped that
        skewed the verdict directly. They still corrupt the diagnostic columns and the unfilled
        population, and a fault counter nothing reads is not a guard."""
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
            # Exact Decimal equality is correct HERE and would not be in float: both sides are
            # parsed from the venue's / our own string form, so `Decimal("0.4000") == Decimal("0.40")`
            # compares numerically and is True. This is the comparison that `_queue_ahead` has to
            # take a half-tick tolerance to make, because it compares a re-parsed float.
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
            # Mark every order currently resting on this ticker. Marking at the moment of the gap —
            # rather than remembering "this ticker gapped once" — keeps orders placed after the gap
            # closed clean, which is most of them on a long run.
            for q in self._live.values():
                if q.ticker == ticker:
                    q.book_gap = True
            return
        if kind != "delta":
            return
        side = event.get("side")
        price = to_decimal(event.get("price"))
        # The EFFECTIVE change to the book, not the wire value. They differ when a level whose new
        # qty falls below the feed's resolution floor is popped to zero: the wire says one thing,
        # the book did another, and accumulating the wire over-counts `removed` — which pushes
        # toward NOT_TRADE_THROUGH. Falls back to the raw delta if the observer did not supply the
        # before-value.
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
        assumption — see the module docstring on why a dead tape otherwise manufactures a finding.

        ⚠️ `traded_ahead` has two bounds, both reported, and **the verdict uses the TIGHTER one**:

          · `traded_ahead` accrues everything up to the moment we NOTICED the fill. Fills are
            polled, so prints landing in that lag — from orders BEHIND us, or (once we are gone and
            the ask can fall to our price) from the OPPOSITE side of the book — are included.
          · `traded_ahead_by_ts` counts only prints whose venue timestamp is at or before the
            venue's fill timestamp. Kalshi reports fill times to the whole second while the tape is
            sub-second, so the boundary second is ambiguous — the defect that killed the offline
            scorer — but it excludes the post-fill window entirely.

        **Which direction is conservative depends on what the number is FOR, and an earlier version
        of this module got it backwards.** It took the LARGER bound, reasoning that the adverse call
        is the alarming claim and should have to earn itself. But this measurement exists to decide
        whether to commit more capital, and there the dangerous error is the opposite one: a
        measurement that UNDER-detects adverse selection green-lights the next rung. Over-counting
        `traded` is exactly that. So the tighter bound wins.

        ⚠️ **Read the result accordingly: the reported `NOT_TRADE_THROUGH` rate is an UPPER bound
        and `TRADE_THROUGH` a LOWER one.** A smaller `best_traded` makes `best_traded < ahead` true
        more often, so the tighter bound produces MORE `NOT_TRADE_THROUGH` than the truth, and the
        remaining `track_lag_s` bias pushes the same way. (This sentence was stated BACKWARDS here
        once. It is the sentence an operator uses to decide what a run means, and inverted it turns
        "at most X% adverse-suspect" into "at least X%" — enough to kill a viable lane on a reading
        error.)

        **That one-sidedness is only true because the fill SECOND is refused rather than resolved.**
        The `fill_ts + 1.0` window is an OVER-count of `traded` — the opposite direction to every
        other residual bias — so if a `TRADE_THROUGH` were allowed to rest on boundary prints, the
        two biases would point opposite ways and the rate would have no established direction at
        all. `_verdict` returns `UNKNOWN_FILL_SECOND` in exactly that case, which is what keeps this
        paragraph true. Do not "simplify" that branch away.

        That the old choice was also inert is what exposed it: `traded_ahead_by_ts` sums a strict
        SUBSET of the same prints, so it can never exceed `traded_ahead` and the `max` was a no-op
        the tests could not distinguish from a `min`. A stated asymmetry that
        the arithmetic forces is not a design decision.

        `detect_lag_s` is reported so an analysis can drop rows whose poll lag was long enough to
        make even the loose bound useless.
        """
        # The tape must have been up at BOTH ends of this order's life; see `tape_ok_at_track`.
        tape_ok = bool(tape_ok and q.tape_ok_at_track)
        n_filled = to_decimal(filled_qty) or _ZERO

        # Our own fill both removes our size from the level AND prints on the tape, so it cancels
        # out of `removed - traded` with no correction. Our own CANCEL only removes.
        # ⚠️ Gated on `cancelled_unfilled` specifically, NOT on `outcome != "fill"`. `gone_at_cancel`
        # is the venue 404 that means the order FILLED between the poll and the cancel, so its size
        # left via a print and correcting for it again double-counts; `cancel_failed_may_still_rest`
        # means nothing was removed at all. The `q.removed >= q.size` guard covers the ordinary race
        # where our cancel's own L2 delta has not landed yet — no room for it in `removed` means it
        # is not in there, and subtracting anyway would invent a cancel.
        own_cancel = (q.size if outcome == "cancelled_unfilled" and q.removed >= q.size
                      else _ZERO)
        cancelled_at_level = q.removed - q.traded - own_cancel

        traded_ahead = q.traded - n_filled
        # Prints inside the venue's fill SECOND — the ones that cannot be ordered against our own
        # fill, because `fill_ts` is truncated to the second while the tape is sub-second. Reported
        # separately so `_verdict` can refuse a finding that depends on them instead of resolving
        # them silently. (Measured elsewhere: the clearing sweep prints ~35ms after the integer
        # fill_ts, so this window is where the contamination actually lives, not a formality.)
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
            # the tape is sub-second, so the true fill instant lies in [fill_ts, fill_ts+1) and our
            # own fill's print — which is always at our own price — usually carries a fractional ts
            # ABOVE the truncated fill_ts. Bounding at fill_ts exactly would drop it while still
            # subtracting `n_filled`, making this systematically short by our own size and pushing
            # `cancelled_ahead_implied` above `ahead`, which the FIFO identity forbids. One second of
            # over-inclusion is the venue's own reporting granularity; the alternative was a
            # guaranteed error of `size` on every row.
            traded_ahead_by_ts = sum(
                (sz for ts, sz in q.prints_ts
                 if ts is not None and ts <= fill_ts + _FILL_TS_GRANULARITY_S), _ZERO
            ) - n_filled

        # The tighter bound when we have it; otherwise the loose one, and the row says so via
        # `traded_ahead_by_ts=unknown` so those rows can be dropped rather than averaged in.
        best_traded = traded_ahead if traded_ahead_by_ts is None else traded_ahead_by_ts

        # ⚠️ For a FILLED order under price-time priority THIS is the cancelled-ahead quantity, and
        # it needs no L2 at all: we filled, so everything ahead of us is gone; the tape says how much
        # of it traded; the remainder left without trading. Clamped at BOTH ends — more than `ahead`
        # is impossible under the identity, so a value that wants to exceed it means the tape and
        # `ahead` disagree, which `_verdict`'s completeness guards are what catch.
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

    ⚠️ WHERE OUR OWN PRINT LIVES DECIDES THE CORRECTION, AND A NAIVE `best_traded − B` GETS IT
    WRONG. `fill_ts` is truncated to the second while the tape is sub-second, so our fill normally
    prints just ABOVE it — inside `B`. (That is the whole reason the window was widened to
    `fill_ts + 1` in the first place: bounding at `fill_ts` dropped our own print while still
    subtracting `n_filled`.) So `best_traded − B = A − n_filled` nets our size a SECOND time, and
    `TRADE_THROUGH` then demands a full own-size more flow than the FIFO identity asks — suppressing
    clean trade-throughs on every fill whose print carries a fractional stamp, i.e. the normal case.

    Subtracting `max(0, B − n_filled)`:
      · our print WHOLLY in `B` (the normal case): the correction is **EXACT** — it recovers `A`
        minus our own print, with zero residual.
      · our print wholly in `A` (fill stamped exactly on the second): also exact.
      · ⚠️ **STRADDLE — part of our fill in each.** Residual is `min(B_others, a)`, bounded by our own
        filled size and pointing toward `TRADE_THROUGH`, i.e. the under-detect-adverse direction this
        module refuses everywhere else. Reachable via MULTI-PARTIAL fills: `maker.py` passes the
        whole batch as `filled_qty` but `fill_ts` from whichever fill the loop saw first, and
        `get_fills` is not sorted — so a newest-first response puts an earlier partial's print in
        `A`. At `--size 1` the residual is under one contract; it is the same order as the other
        residuals named above, not a separate hazard. Sorting the batch by `ts` would close it.

    Exactly-correct is not available in general — nothing tags which print is ours.

    ⚠️ The `max(_ZERO, …)` clamp is DEFENSIVE AND CURRENTLY UNOBSERVABLE, deliberately recorded as
    such rather than covered by a test that cannot fail. `_verdict` only calls this inside
    `best_traded >= ahead`, and under that precondition dropping the clamp never changes a verdict:
    when `B > n` the two forms are identical, and when `B <= n` both land at or above `best_traded`,
    which already clears `ahead` (verified exhaustively over small values). It stays because it makes
    the intent legible and stays correct if that enclosing condition ever moves — but do not expect a
    mutation of it to die, and do not add a test asserting it does.
    """
    return best_traded - max(_ZERO, boundary_traded - n_filled)


def _verdict(q: OrderQueue, best_traded: Decimal, n_filled: Decimal,
             boundary_traded: Decimal | None, *, outcome: str, tape_ok: bool) -> str:
    """The fill-cause call. The ORDER of these guards is the design: every "cannot read this row"
    reason is checked BEFORE either finding, so a broken feed can never present as a result."""
    if not tape_ok:
        return UNKNOWN_TAPE_DOWN
    if q.tape_gap:
        # The tape reconnected or dropped messages while this order rested, so prints at our price
        # are missing and `traded` undercounts — which reads as a queue that pulled. A completed
        # reconnect leaves `subscribed` back at True, so `tape_ok` alone does NOT catch this: one
        # 3-second blip would otherwise turn every order resting across it into NOT_TRADE_THROUGH.
        return UNKNOWN_TAPE_GAP
    if outcome == "gone_at_cancel":
        # The venue's 404 on cancel: the order FILLED between the poll and the cancel, so this is a
        # fill — but we are on the cancel path and hold neither its size nor its timestamp, and by
        # closing the tracker here we also ensure the later fill poll finds nothing to report. Give
        # it its own label rather than the blank of a non-fill, so the population it removes from
        # the findings is COUNTABLE. It is the late-in-cycle fill — plausibly enriched in adverse
        # selection — so silently dropping it would bias the very rate this produces.
        return UNKNOWN_FILL_AT_CANCEL
    if outcome != "fill":
        # An unfilled order has no fill to explain. Its counts are still logged — they are the
        # survivorship half of the population, and "we sat behind 35 for 30s and 2 traded" is the
        # observation a fills-only log cannot make.
        return ""
    if q.traded < n_filled or best_traded < _ZERO:
        # ARITHMETIC PROOF that the tape is incomplete: our own fill always prints at our own price,
        # so neither accumulator can be short of it. Distinct from tape_gap, which is the tape
        # REPORTING a discontinuity — this is the case where it reports none and is wrong anyway.
        # ⚠️ BOTH bounds are checked. The by-ts figure is the one that now decides the verdict, and
        # it has its own way of going short (a print whose venue timestamp falls outside the window),
        # so guarding only the loose total would leave the deciding number unchecked.
        return UNKNOWN_TAPE_INCOMPLETE
    if q.ahead is None:
        return UNKNOWN_NO_QUEUE
    if q.ahead == _ZERO:
        # ⚠️ NOT `TRADE_THROUGH`. `_queue_ahead` returns exactly 0 for any quote INSIDE the touch,
        # so `best_traded >= 0` is vacuously true and every improved fill would score TRADE_THROUGH
        # having demonstrated nothing — the verdict would be a restatement of `--improve-ticks`,
        # which is the arm label. (maker.py's own `ahead` comment already warns it is definitional,
        # not a measurement.) There was no queue to get through, so there is no fill cause to
        # attribute: quoting alone at a price IS being the last resting order in front of whatever
        # arrives, which is the NOT_TRADE_THROUGH hypothesis rather than its refutation.
        return ALONE_AT_PRICE
    if best_traded >= q.ahead:
        # Enough flow came through at our price to reach us on price-time priority alone — UNLESS
        # the margin is made up of prints inside the venue's fill second, which cannot be ordered
        # against our own fill. Counting those toward `traded` resolves an ambiguity silently and in
        # the TRADE_THROUGH direction — i.e. it converts adverse fills into "market-making working",
        # the under-detect error that green-lights more capital. It is also the direction the module
        # is otherwise built to refuse. So: refuse.
        if boundary_traded is None:
            # No venue fill timestamp (`ts_src=detected`), so `best_traded` is the LOOSE,
            # detection-bounded total and there is no boundary to subtract. `TRADE_THROUGH` here
            # would rest on an over-counted `traded` with no refusal available — so it is not a
            # finding. Enforced as a verdict rather than left to `traded_ahead_by_ts=unknown` on the
            # row, because "remember to filter these out" is not a guard.
            return UNKNOWN_NO_FILL_TS
        if _strict_traded(best_traded, boundary_traded, n_filled) < q.ahead:
            return UNKNOWN_FILL_SECOND
        return TRADE_THROUGH
    # The tape says less traded than we were behind, and we filled anyway — so under FIFO the
    # remainder left without trading. That IS the finding; there is nothing further to check.
    #
    # ⚠️ NO L2 CORROBORATION HERE, DELIBERATELY, and an earlier version had one. Two reasons it had
    # to go. First it was redundant: cancelled-ahead is identically `ahead - traded_ahead` for a
    # filled order, so the L2 half carries no information the tape does not already give. Second it
    # was WRONG, and wrong in the worst direction — `cancelled_at_level` accrues over the whole rest
    # window while the threshold used the fill-ts-bounded figure, so post-fill prints were excluded
    # from one side of the inequality and not the other. For a BUY those prints are opposite-side
    # lifts that remove from the OTHER ladder, so they shrank `cancelled_at_level` (below zero) with
    # nothing to offset while simultaneously raising the bar — and they appear precisely when the
    # price runs down through our bid, i.e. on the textbook ADVERSE fill, which was therefore the
    # case most likely to be demoted to AMBIGUOUS. L2 level events carry no timestamp at all
    # (`feed._notify_level`), so the two halves cannot be put on one window.
    #
    # The honest cost of dropping it: this verdict now conflates "the queue ahead of us pulled" with
    # "`ahead` was already stale when our order landed". Both mean we did NOT get through a queue by
    # trading, which is what the verdict actually asserts; `cancelled_at_level` stays on the row as a
    # diagnostic for anyone who wants to separate them, with its level-wide caveat.
    return NOT_TRADE_THROUGH
