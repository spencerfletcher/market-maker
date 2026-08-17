"""
bot/poly_us/maker.py
────────────────────
The Polymarket US maker: quoting loop, inventory, cap, teardown.

⛔ **THE REBATE IS THE EDGE. NOTHING HERE IS TRYING TO EARN A SPREAD.** Gross spread capture at the
touch measures approximately zero on both venues. Poly pays a maker rebate of `0.0125·p·(1−p)` per
contract, and that credit — small per contract, and verified against a real fill rather than
assumed — is the only positive term in the economics. Every constraint below exists to keep it
reachable, and relaxing one does not "loosen a preference", it deletes the edge.

**A SEPARATE PROGRAM FROM `bot/kalshi/maker.py`, on purpose.** The two venues differ in order
semantics (an ask here is `BUY_SHORT`, priced in YES space — NOT the complement: the venue reads
short prices as yes-space sell limits, and mirroring the price makes the order unfillable), fee
sign (rebate vs zero), book transport (snapshots, not L2 deltas) and queue observability. Sharing
it would mean a third set of `if venue ==` branches through the money path.

WHAT GATES REAL MONEY
─────────────────────
`config.DRY_RUN`, and only that — `PolyUSClient.__init__` snapshots it and every order method
short-circuits on it. `scripts/poly_live_mm.py` is the ONE place that can set it, from an argv
sniff that must run before `bot.core.config` is imported. `arming_refusal` below converts a
DRY_RUN/flag mismatch into a HARD STOP rather than a silent downgrade, because the dangerous
combination is `DRY_RUN=false` with no flag: real quotes while the run reports a preview.

`shadow=True` is a SECOND, independent barrier: `_place` raises `ShadowViolation` rather than
returning, so a shadow run is not one bad branch away from an order.

THE CYCLE, per market, every `--requote-s`
──────────────────────────────────────────
 1. `is_paused()` — the FIRST statement, before any venue call. A pause checked after the book read
    has already spent the request and already decided a quote.
 2. cache-busted book read → `touch_from_md`; refuse the market this cycle if either side is None.
 3. compute quotes (join, or improve ONE tick where the spread is ≥ 2 ticks) and record WHICH,
    because joined and improved fills are different populations.
 4. cancel-then-replace ONLY where our own price moved.
 5. poll fills; on a fill read the commission back and record it.

⛔ **CANCEL/REPLACE ONLY ON A PRICE CHANGE — THIS IS THE DOMINANT VARIABLE, NOT A MICRO-OPTIMISATION.**
An unchanged quote keeps its queue position; amending to reprice FORFEITS it (Kalshi's docs are
explicit, Poly's FIX spec says the same for size). Measured on this venue: the fills arrive at the
FRONT of the queue, while orders resting hundreds deep go untouched. A loop that re-places every
cycle would sit permanently at the back of every queue and fill nothing.

⚠️ `post_only` IS A BACKSTOP, NOT A REJECTION SIGNAL. Poly documents that a would-match post-only
order "will be rejected"; it is actually ACCEPTED and RESTS (proven by a two-arm real-money A/B,
`scripts/poly_postonly_probe.py`). Never write code that expects a rejection here.

WHAT THIS DOES NOT DO
─────────────────────
It does not market-sell to flatten. Cancelling only removes exposure so it is automated; flattening
moves money at a price something must choose, so a residual after the passive flatten window is
REPORTED for an operator and left in the durable record for a recovery tool. Automate what only
REMOVES risk; leave what CHOOSES a price to a human.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from typing import Any, Iterable, Mapping, Optional, Sequence

from bot.core import durable, memguard
from bot.core.maker_state import MakerStateStore, account_loss
from bot.core.redact import safe_exc
from bot.core.money import parse_wire
from bot.core.safety import is_paused
from bot.core.venue_time import iso_to_ts
from bot.poly_us.client import PreSendRefusal, parse_book_stats, touch_from_md, transact_age_s

#: WS book-source freshness ceiling: a WS book whose transactTime content-age exceeds this at
#: decision time is not trusted — the maker fresh-REST-re-reads that one book (the backstop a
#: REST poller has for free and a naive WS maker quietly loses).
#: ⛔ THIS IS A FROZEN-BOOK TIMEOUT, NOT AN UPDATE-CADENCE GATE. On a live feed a push keeps
#: every ACTIVE book fresh within a cycle, so a tight ceiling does not catch freezes — it
#: catches quiet-but-healthy books that legitimately update every minute or two, and burns the
#: REST backstop on them. Calibrate it against a book-age histogram from your own tape: a
#: stratified WS-vs-REST comparison found no missed update in ANY age stratum, so the ceiling
#: is set from what the feed's own reconnect watchdog allows, mid-band rather than at the
#: ceiling, buying margin under that watchdog for a handful of extra REST re-reads.
WS_BOOK_STALE_S = 120.0
#: ⛔ THE BURST CAP: max fresh-REST re-reads per cycle triggered
#: by stale/absent WS books. A PARTIAL feed freeze (many books stale at once) would otherwise
#: re-read the whole slate in one cycle — the exact per-book poll burst the WS feed exists to
#: remove. Over the cap, the remaining stale books are SKIPPED that cycle (safe: no quote on an
#: unverified book), logged loudly, and the count surfaces on the cycle tape for the freeze
#: watchdog. Full escalate-to-fallback/halt is the fallback brick; this bounds the burst now.
WS_STALE_REREAD_MAX = 4
#: WHOLE-CONNECTION loss → bounded REST fallback, then halt. While the WS
#: socket is down the maker quotes a REDUCED set over REST for at most this window; if the feed
#: is still down at expiry the run HALTS into the ordinary teardown (cancel-all → flatten →
#: sweep). Fallback must never become steady state: REST for the full slate is the poll burst
#: the WS feed exists to remove.
WS_FALLBACK_WINDOW_S = 120.0
#: The reduced set: ALL held-inventory books first (never dropped — an unquoted held book can't
#: work down), then the slate in sorted order up to this many total. Sized to the REST-safe
#: budget (~8–12 books/cycle at the deployed pace).
WS_FALLBACK_MAX_BOOKS = 10
#: Near the exposure cap the fallback window is ZERO — a maker that cannot see the book near
#: its risk limit stops, it does not quote a truncated slate.
WS_NEAR_CAP_F = Decimal("0.7")


def ws_fallback_slugs(inventory: Mapping[str, Decimal], ticks: Mapping[str, Any],
                      cap: int = WS_FALLBACK_MAX_BOOKS,
                      sizes: Mapping[str, int] | None = None) -> list[str]:
    """The reduced quote set for a WS outage: every held book (|inv|>0) first — ALL of them,
    even past `cap`: an unquoted held book cannot work down, so when held > cap the REST burst
    exceeds the ~8–12 budget by design and the log says so — then the remaining slate by
    configured size DESC (the operator's own value ranking; alphabetical would be arbitrary),
    up to `cap` total. ⛔ LIMITS ONLY THE QUOTE-LOOP ITERATION — callers
    must never prune `self.ticks`/`self.inventory` with this: the reconciler and teardown
    flatten iterate those, and shrinking them blinds teardown to a held book."""
    # Intersect with the SLATE: a carried book whose tick read failed at prepare() is in
    # inventory but NOT in ticks, and _quote_market KeyErrors on it — which inside run_cycle
    # crashes the whole run. It cannot be quoted
    # anyway; teardown still sees it via self.inventory (untouched).
    held = sorted(s for s, q in inventory.items() if q != 0 and s in ticks)
    out = list(held)
    rest = [s for s in ticks if s not in out]
    rest.sort(key=lambda s: (-(sizes or {}).get(s, 0), s))
    for slug in rest:
        if len(out) >= max(cap, len(held)):
            break
        out.append(slug)
    return out


def ws_near_cap(gross_exposure: Decimal, max_total: Optional[int],
                frac: Decimal = WS_NEAR_CAP_F) -> bool:
    """True when exposure is within `frac` of the global CONTRACT cap — the immediate-halt
    predicate for a WS loss. ⚠️ The None/0 branch is defensive only: PolyMaker defaults
    max_total_contracts to Σ per-book caps, so the shim cannot produce None — and this is a
    contracts rail, an ADDITIONAL conservative check beside the dollar loss cap, not the
    'risk limit' itself."""
    if not max_total or max_total <= 0:
        return False
    return gross_exposure >= Decimal(max_total) * frac


log = logging.getLogger(__name__)

_ZERO = Decimal("0")
_ONE = Decimal("1")

# ── the constants that are measurements ──────────────────────────────────────────────────────

#: |Θ| for the Poly US maker rebate. The credit is Θ·C·p·(1−p) per fill.
REBATE_COEF = Decimal("0.0125")

#: The venue collects commissions rounded to the NEAREST CENT — established against real fills,
#: not assumed from the formula. So a predicted credit below half a cent collects as nothing at
#: all, and a rebate strategy that ignores the rounding step books revenue it never receives.
NEAREST_CENT_BOUNDARY = Decimal("0.005")

#: ⛔ THE SIZE FLOOR. At size 1 the rebate peaks at 0.0125·0.5·0.5 = 0.003125 per fill — below the
#: 0.005 rounding boundary at EVERY price, so a size-1 maker fill earns zero rebate wherever it
#: fills. Each extra contract widens the price band that clears the boundary; even at this floor
#: the extremes do not clear, so `rebate_rounds_to_zero` — the live per-price rule — is the
#: authority, not a blanket "this size always pays".
MIN_SIZE = 5
# The delayed-verify horizon for cancelled orders: past the venue's observed read lag, with
# margin. One re-read per cancelled order at this age books any fill the immediate read missed.
# ⚠️ This is the fill-loss class that silently flipped the sign of early real-money runs: the
# cancel returned, the fill had already happened, and nothing ever went back to look.
LAG_HORIZON_S = 120.0
# The venue's order states an order can never trade again from — the poll's pop condition.
# The vocabulary here is what the live WS stream was OBSERVED to emit (NEW / CANCELED / FILLED /
# PARTIALLY_FILLED, nothing containing "OPEN"); the SDK enum
# adds EXPIRED, REJECTED, REPLACED and four PENDING_* states, all unobserved. PARTIALLY_FILLED
# and the PENDING_* family still rest or can still fill, so they are NOT here; REPLACED is
# deliberately excluded — we never send replaces, so its appearance means someone else acted
# on this account and the order must stay tracked and loud.
_TERMINAL_ORDER_STATES = frozenset({
    "ORDER_STATE_FILLED",
    "ORDER_STATE_CANCELED",
    "ORDER_STATE_EXPIRED",
    "ORDER_STATE_REJECTED",
})
VERIFY_RETRY_S = 30.0        # unreadable at verify time → try again this much later
VERIFY_MAX_ATTEMPTS = 5      # then queue for ACTIVITIES RECOVERY — never a silent drop
MAX_PARKED = 64              # a venue outage must not turn the parked set into a request leak
# ── belief-recovery registered numbers — COMPLETE; nothing tunable outside this list ──
#: Cancel-probes per quote cycle, oldest-first: a correlated purge (every side at once) drains
#: over several cycles rather than stretching one cycle into a stale-book quote decision.
PROBE_MAX_PER_CYCLE = 2
#: More than this many DISTINCT orders answering not_found in ONE poll is a client/route fault,
#: not a purge — probes and retirements refuse and the recovery queue holds.
NOT_FOUND_BREAKER = 5
#: Queue residence past terminal_ts without coverage → UNRESOLVED. Never guess.
RECOVERY_MAX_S = 300.0
#: The walk's oldest-edge skew margin: coverage means the walk's oldest trade createTime is at
#: least this far before the order's placed_ts (re-derivable later from the taped pairs).
COVERAGE_SKEW_S = 60.0
# How often the maker asks the VENUE what it holds. One request; at a 10s requote this is once a
# minute, against an observed belief drift that took minutes to reach several times the cap.
RECONCILE_EVERY_CYCLES = 6
# A venue read older than this is announced, not obeyed silently — a restriction that outlives
# its evidence quietly costs fills forever.
VENUE_STALE_AFTER_S = 300.0

#: Poly's documented limit is 20 req/s per key. This is the self-imposed ceiling the cycle paces
#: to, deliberately well under it: over-limit on this venue surfaces BOTH ways — silent
#: throttling AND real 429s — so the most likely symptom of exceeding it is a late, stale 200,
#: which is the worst possible failure for a quote decision.
DEFAULT_MAX_REQ_PER_S = 8.0
VENUE_REQ_PER_S = 20.0

#: Quote actions. Strings rather than an enum so a tape row and an assertion read the same.
HOLD = "hold"
PLACE = "place"
CANCEL = "cancel"
REPLACE = "replace"

#: ⛔ TEARDOWN ORDER IS LOAD-BEARING. Sweeping before flattening re-reads a book that the flatten
#: is about to move — a sweep that "certifies clean" against a book the flatten has not yet hit
#: is worse than no sweep, because it is a false all-clear.
# reconcile_pending sits between cancel_all and flatten: cancel-all can park blind orders, and the
# flatten sizes off the inventory those parked fills belong to — flattening before reconciling
# would size the disposal against a belief the next 90 seconds could prove wrong.
TEARDOWN_PHASES = ("cancel_all", "reconcile_pending", "flatten", "sweep")

#: Default tape locations. Module-level (rather than inline in `__init__`) so a test can redirect
#: them in one place — a suite that writes to the production tape puts fabricated rows on the same
#: file a real run appends to, and once they are interleaved nothing downstream can tell them apart.
DEFAULT_QUOTE_CSV = durable.repo_path("logs", "poly_live_mm_quotes.csv")
DEFAULT_CYCLE_CSV = durable.repo_path("logs", "poly_live_mm_cycles.csv")
DEFAULT_FILL_CSV = durable.repo_path("logs", "poly_live_mm_fills.csv")

# `run_id` is the LAST column on all three tapes: attribution becomes a join instead
# of timestamp segmentation, and appending LAST keeps name-keyed DictReader consumers safe
# across the schema change.
_QUOTE_HDR = ["ts", "cycle", "slug", "tick", "bid", "ask", "my_bid", "my_ask", "improved",
              "action_bid", "action_ask", "queue_ahead_bid", "queue_ahead_ask",
              "inventory", "allow_bid", "allow_ask", "status", "read_ms", "sample_gap_s",
              # Free book-stats: Δshares_traded between rows is per-trade-exact in nearly every
              # recorded case, making this tape the prints-resolution flow series — the
              # flow-denominator/sweep-size/traded-without-us reads. BLANK when the venue
              # omits stats (absent ≠ 0 — a written 0 would read as a frozen counter).
              # LOGGING-ONLY floats — traced: no consumer compares these against a
              # tick or sizes an order — the Decimal rule's statistical carve-out. The
              # counter is CUMULATIVE, so a blank/skip row loses no trades — the next
              # populated row includes everything between; only per-cycle attribution
              # coarsens across a gap.
              "shares_traded", "last_trade_px", "last_trade_qty", "last_trade_age_s",
              # book_src: "ws" (fresh WS book), "rest_fallback"
              # (WS stale/absent → REST re-read), "rest" (default source). Partitions the tape
              # for the shadow WS-vs-REST accuracy read; a mixed-source tape excludes
              # rest_fallback cycles from ws calibration via this column.
              "book_src",
              "run_id"]
_CYCLE_HDR = ["ts", "cycle", "wall_s", "requests", "req_per_s", "markets_total", "markets_quoted",
              "markets_skipped", "missed_cycles", "max_staleness_s", "actions", "paused",
              "rss_mb", "avail_mb", "swap_free_mb", "ws_stale_rereads", "ws_capped",
              "ws_outage", "ws_down_s", "halt_reason", "run_id"]
# ⚠️ `commission_order_total` and `predicted_rebate_on_cum` are per-ORDER quantities on a
# per-INCREMENT row — see `Fill`. Total them by taking the LAST row per `order_id`, never by
# summing the column.
_FILL_HDR = ["ts", "slug", "side", "order_id", "price", "size", "filled_qty", "cum_filled_qty",
             "commission_order_total", "commission_verdict", "predicted_rebate_on_cum",
             "queue_ahead", "time_to_fill_s", "improved", "order_state",
             "mid_at_fill", "mid_age_s", "avg_px", "late_booked", "booked_via",
             "best_bid", "best_ask", "book_src", "run_id"]


class ShadowViolation(RuntimeError):
    """A shadow run reached the placement path.

    Raised rather than returned. A shadow run's whole value is that "placed nothing" is
    structural, and a soft return leaves that guarantee one refactor away from being wrong.
    """


# ── pure helpers ─────────────────────────────────────────────────────────────────────────────

def predicted_rebate(price: Decimal, size: Decimal | int) -> Decimal:
    """The documented Poly US maker credit for `size` contracts at `price`, as a POSITIVE
    magnitude. Decimal throughout: this is compared against the 0.005 rounding boundary, and a
    float's binary error there is a wrong verdict, not a rounding nit.

    ⚠️ This is the UN-ROUNDED formula. The venue collects the credit rounded to the nearest cent
    PER FILL INCREMENT, so the realised rate is a step function and any modelled figure is an
    upper bound on small orders.

    ⛔ THE ROUNDING IS PER FILL INCREMENT, NOT PER ORDER. Getting this backwards is what makes a
    size-ratchet design treat `min_rebate_size` as a guarantee that a fill earns something. It is
    not: the increment is chosen by the TAKER, not by our resting size. The `commission_verdict`
    column settles it empirically — the forfeiture rate tracks the COUNTERPARTY's median fill
    increment, not our order size. Shape of the finding (real rates are per-book, measure them):

        book filled in large clips    almost every fill earns the credit
        book nibbled in small clips   a large share of fills earn NOTHING

    A book whose orders happen to fill WHOLE looks consistent with per-order rounding; that is
    the trap. `min_rebate_size` bounds OUR ORDER; the venue rounds THEIR NIBBLE, so the floor is
    necessary and NOT sufficient. Screen books on the counterparty's increment distribution, not
    on our size alone.
    """
    return REBATE_COEF * price * (_ONE - price) * (size if isinstance(size, Decimal)
                                                   else Decimal(size))


def rebate_rounds_to_zero(price: Decimal, size: int) -> bool:
    """True when a fill of `size` at `price` earns NOTHING because the venue's nearest-cent
    collection rounds the credit away. The live per-price rule — read this, not the size floor,
    when asking whether a particular market is worth quoting."""
    return predicted_rebate(price, size) < NEAREST_CENT_BOUNDARY


def min_rebate_size(price: Decimal) -> int:
    """The smallest contract count whose predicted rebate reaches the half-cent boundary at this
    price. p(1−p) is maximal at p=0.5, so this grows sharply toward either extreme."""
    per_contract = predicted_rebate(price, 1)
    if per_contract <= _ZERO:
        return 0
    return int((NEAREST_CENT_BOUNDARY / per_contract).to_integral_value(rounding=ROUND_CEILING))


def size_refusal(size: int) -> Optional[str]:
    """Reason to refuse this quote size outright, or None.

    ⛔ Do not relax this to "save capital". The rebate is the ONLY positive term in this lane's
    economics, and below the floor the venue rounds it to zero — a smaller size does not earn less,
    it earns NOTHING, while carrying the same adverse-selection and inventory risk. A cheaper run
    that cannot earn is more expensive than no run.
    """
    if size < MIN_SIZE:
        best_case = predicted_rebate(Decimal("0.50"), 1)
        return (
            f"REFUSING size {size}: the minimum quote size is {MIN_SIZE}. The Poly maker rebate is "
            f"0.0125·C·p·(1−p) and the venue collects commissions rounded to the NEAREST cent, so a "
            f"credit below ${NEAREST_CENT_BOUNDARY} collects as $0.0000. At size 1 the credit peaks "
            f"at ${best_case} (p=0.50) and is therefore below the boundary at EVERY price — a "
            f"size-1 fill earns nothing anywhere. Size {MIN_SIZE} clears the boundary across roughly "
            f"p∈[0.10,0.90]; use `min_rebate_size(price)` for the exact figure at a given price.")
    return None


def parse_sizes(spec: str, slugs: Iterable[str]) -> dict[str, int]:
    """`"12"` → every slug at 12; `"slug-a:25,slug-b:12"` → per-book, EVERY slug required.

    ⛔ No default for an unnamed slug and no silent drop: a per-book spec that forgets a book
    would otherwise quote it at some fallback size the operator never chose, on the one axis
    that scales both the rebate and the adverse tail. Refuses (returns via ValueError) rather
    than guessing.
    """
    wanted = [s for s in slugs]
    spec = spec.strip()
    if ":" not in spec:
        try:
            uniform = int(spec)
        except ValueError:
            raise ValueError(f"--size {spec!r}: want an integer or slug:size pairs")
        return {s: uniform for s in wanted}
    sizes: dict[str, int] = {}
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        slug, _, raw = part.rpartition(":")
        slug, raw = slug.strip(), raw.strip()     # `a : 25` refuses on the SIZE, not on a
        if not slug:                              # phantom missing slug
            raise ValueError(f"--size {part!r}: want slug:size")
        if slug in sizes:
            # Last-wins would silently drop the first value — the one shape of this spec that
            # produces a wrong size with no error and no visible trace.
            raise ValueError(f"--size names {slug!r} twice ({sizes[slug]} then {raw!r}) — "
                             f"one size per book")
        try:
            sizes[slug] = int(raw)
        except ValueError:
            raise ValueError(f"--size {part!r}: {raw!r} is not an integer")
    missing = [s for s in wanted if s not in sizes]
    extra = [s for s in sizes if s not in wanted]
    if missing:
        raise ValueError(f"--size names no size for {missing} — every quoted book needs an "
                         f"explicit size, or pass one integer for all of them")
    if extra:
        raise ValueError(f"--size names {extra}, which are not in --slugs")
    return sizes


def cap_contracts(size: int, cap_fills: int) -> int:
    """The per-market inventory cap in CONTRACTS, from a cap expressed in FILLS.

    ⛔ The cap is stated in fills because a CONTRACT cap is not size-invariant: 30 contracts binds
    after 3 fills at size 10 but 30 fills at size 1. Hold the contract cap constant while raising
    size and you have not scaled the strategy — you have tightened the cap, and any "size scales"
    result read off that comparison is an artifact of the cap moving, not of size. In fills, "3"
    means the same amount of adverse selection at every size.
    """
    if cap_fills <= 0:
        raise ValueError(f"cap_fills must be positive, got {cap_fills}")
    return size * cap_fills


def _reduce_only(inventory: Decimal) -> tuple[bool, bool]:
    """(bid allowed, ask allowed) when only the side that REDUCES may quote.

    Flat means neither: with nothing to reduce, every quote adds.
    """
    if inventory > _ZERO:
        return False, True          # long: the ask reduces
    if inventory < _ZERO:
        return True, False          # short: the bid reduces
    return False, False


def sides_allowed(inventory: Decimal, cap: int) -> tuple[bool, bool]:
    """(bid allowed, ask allowed) for one market, given signed inventory and its contract cap.

    ⛔ AT THE CAP WE DO NOT STOP QUOTING — we stop quoting the side that ADDS and keep quoting the
    side that REDUCES. Pulling both leaves the inventory parked with nothing working to take it
    off, which converts a bounded maker position into an unhedged directional one.

    `>=`, not `>`: a guard written with a strict inequality is one contract away from not firing.
    Overshoot (a partial-fill race past the cap) still reduces rather than stopping everything.
    `cap=0` is a deliberate reduce-only mode.
    """
    if inventory.copy_abs() >= cap:
        return _reduce_only(inventory)
    return True, True


def global_sides_allowed(inventory: Decimal, gross_exposure: Decimal,
                         max_total: int) -> tuple[bool, bool]:
    """The book-wide exposure cap, applied to ONE market's sides. `max_total <= 0` disables it —
    the repo's 0-disables convention, same as the loss caps and the memory guard.

    `gross_exposure` is Σ|inventory| across every market: a long in one market and a short in
    another are not netted, because they do not settle against each other.
    """
    if max_total <= 0 or gross_exposure < max_total:
        return True, True
    return _reduce_only(inventory)


def quote_action(resting: Optional[Decimal], desired: Optional[Decimal]) -> str:
    """What to do with one side, given the price resting on the venue and the price we now want.

    ⛔ EQUAL PRICES MEAN HOLD, AND HOLDING IS THE POINT. Queue position is the dominant variable
    in this lane: fills arrive at the front of the queue, while orders resting hundreds deep go
    untouched. A cancel/replace at the same price gives up the whole
    queue and buys nothing. Compared as Decimals, so `0.4400` and `0.44` are the same price — a
    string compare would forfeit the queue every single cycle.
    """
    if resting is None:
        return PLACE if desired is not None else HOLD
    if desired is None:
        return CANCEL
    return HOLD if resting == desired else REPLACE


# A completed round trip that realized at least this much loss PER CONTRACT triggers the
# post-adverse cooldown. It is a CALIBRATED THRESHOLD, not a universal constant: set it from your
# own tape so that it sits between the two round-trip populations. Above it, a news-gap exit — the
# book repriced against the quote and the maker then re-entered the same book while the spike was
# still deflating; that re-entry is exactly the trade the cooldown exists to remove. Below it, an
# ordinary drift exit, which is routine market-making business and must NOT stand the book down.
# Calibrate on completed round trips only; a partially-closed trip gives you the shape, not a
# decision boundary.
ADVERSE_RT_PER_CONTRACT = Decimal("0.02")
# The mark tripwire's default threshold, in dollars PER CONTRACT of per-book marked P&L.
# ⛔ PER CONTRACT, never a fixed dollar amount: a fixed-dollar threshold TIGHTENS in per-contract
# terms as size grows — backwards for exactly the larger-size runs it exists to guard, and it
# silently means two different things in the two arms of a size comparison.
# Why it exists at all: the durable cap is price-REALIZED-only, and the adverse cooldown fires
# only after a COMPLETED round trip. Over a long recorded run the cooldown never fired once,
# because the phase both rails are blind to is the ACCUMULATION of an unrealized adverse mark —
# which is precisely what more size widens. Set it above the cooldown's own realized per-contract
# threshold so the two rails do not collide. 0 disables.
DEFAULT_MARK_TRIP_PER_CONTRACT = Decimal("0.05")


def mark_pnl(inv: Decimal, avg_entry: Decimal,
             bid: Decimal, ask: Decimal) -> Decimal | None:
    """Per-book marked P&L against the LIQUIDATION side of the live touch — a long
    liquidates into the BID, a short covers at the ASK. Marking at the mid (or the far side)
    flatters precisely when the book is running away, which is the moment this exists for.

    None when flat (no position, no mark) — and None on an UNKNOWN BASIS:
    `avg_entry` is populated by THIS run's own fills, so an INHERITED position carries no basis,
    and reading that absence as a zero basis marks the whole carried position as pure gain. That
    is the flattering-direction error class, landing on the one position class nothing else
    watches. Absence is not a basis; the caller arms conservatively on None."""
    if inv == _ZERO:
        return None
    if avg_entry <= _ZERO:
        return None
    if inv > _ZERO:
        return (bid - avg_entry) * inv
    return (avg_entry - ask) * (-inv)


def fill_accounting(
    prev_inv: Decimal, avg_entry: Decimal, rt_realized: Decimal, rt_closed: Decimal,
    side: str, price: Decimal, qty: Decimal,
) -> tuple[Decimal, Decimal, Decimal, Decimal, Optional[tuple[Decimal, Decimal]]]:
    """Average-cost accounting for one fill, in YES space. `price` is OUR quote price: a post-only
    GTC fills at its limit, and every fill recorded so far agrees — `avg_px == price` on both
    sides. ⚠️ Not an identity by construction — the rebate writer's own note says the venue
    computes at ITS price and documents divergence on a BUY_SHORT, which is why IT reads
    `avg_px or price`. If a divergent fill is ever recorded, this accounting must follow suit;
    until then the increment-level attribution below needs the limit price, which `avgPx` (an
    order-level average) cannot supply.

    Returns (new_inv, new_avg_entry, new_rt_realized, new_rt_closed, completed) where `completed`
    is (realized, closed_qty) for the round trip that this fill CLOSED — set when inventory
    returns to exactly zero, and also on a through-zero crossing fill (the old trip completes and
    the remainder opens a fresh position at `price` with fresh accumulators).
    """
    signed = qty if side == "bid" else -qty
    new_inv = prev_inv + signed
    if prev_inv == _ZERO or (prev_inv > _ZERO) == (signed > _ZERO):
        total = prev_inv.copy_abs() + qty
        new_avg = ((avg_entry * prev_inv.copy_abs()) + price * qty) / total
        return new_inv, new_avg, rt_realized, rt_closed, None
    closing = min(qty, prev_inv.copy_abs())
    # A long reduced by a sell realizes (price − avg); a short reduced by a buy, (avg − price).
    pnl = (price - avg_entry) * closing if prev_inv > _ZERO else (avg_entry - price) * closing
    rt_realized += pnl
    rt_closed += closing
    if new_inv == _ZERO:
        return new_inv, _ZERO, _ZERO, _ZERO, (rt_realized, rt_closed)
    if qty > closing:
        return new_inv, price, _ZERO, _ZERO, (rt_realized, rt_closed)
    return new_inv, avg_entry, rt_realized, rt_closed, None


def pick_quotes(bid: Decimal, ask: Decimal,
                tick: Decimal) -> tuple[Decimal, Decimal, bool, bool]:
    """(my_bid, my_ask, improved_bid, improved_ask) — join the touch, or step ONE tick inside it on
    BOTH sides when there is ≥2 ticks of room.

    ⚠️ NEVER ONE SIDE. Improving only the bid buys with sole queue priority while selling from the
    back of the queue: the run accumulates a systematic long and stops measuring the spread at all
    (a correction the offline shadow probe already had to make). A spread of EXACTLY two
    ticks is the wrinkle — both improvements land on the same price, a quote that would lock
    against itself — so that case joins both.

    ⛔ THIS IS A SECOND COPY of `scripts/poly_shadow_mm.pick_quotes`, kept deliberately: that
    collector's import allowlist forbids it from importing any module with an order path, and this
    module has one. The two are pinned EQUAL over a grid of books by
    `tests/test_poly_maker.py::test_pick_quotes_agrees_with_the_shadow_collector_on_every_book_it_has_taped`
    — if they ever diverge, every comparison of live fills against the shadow tape is meaningless.

    Raises ValueError on an unusable touch or tick, so a book we cannot quote is a skipped market
    rather than a silently wrong quote.
    """
    if tick <= _ZERO:
        raise ValueError(f"unusable tick {tick}")
    if bid <= _ZERO or ask <= bid or ask >= _ONE:
        raise ValueError(f"unusable touch bid={bid} ask={ask}")
    improve = (ask - bid) >= 2 * tick
    my_bid = bid + tick if improve else bid
    my_ask = ask - tick if improve else ask
    if my_bid >= my_ask:
        my_bid, my_ask, improve = bid, ask, False
    return my_bid, my_ask, improve, improve


def qty_at_price(md: Any, side: str, price: Decimal) -> Optional[Decimal]:
    """Total quantity resting AT `price` on `side` — i.e. the queue we would be joining behind.

    ⛔ None means "we could not read the book", NEVER 0. A fabricated 0 reads as front-of-queue,
    which is exactly the value that would make an unfilled order look like evidence about fill
    rate rather than evidence about queue arithmetic.

    Sums every level at that price rather than reading `levels[0]`: the SDK's book is a bare list
    with no ordering contract, and every sibling reader in this repo already refuses to trust it.
    An improved quote correctly reads 0 — it rests alone at the front, which is why the
    improved/joined bit has to be recorded alongside this number.
    """
    if not isinstance(md, dict):
        return None
    key = "bids" if side == "bid" else "offers"
    levels = md.get(key)
    if levels is None:
        return None
    if not isinstance(levels, list):
        return None
    total = _ZERO
    try:
        for level in levels:
            if not isinstance(level, dict):
                continue
            raw_px = level.get("px")
            if isinstance(raw_px, dict):
                raw_px = raw_px.get("value")
            if raw_px is None:
                continue
            if parse_wire(raw_px) == price:
                total += parse_wire(level.get("qty", "0"))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return total


def commission_verdict(commission: Optional[Decimal]) -> str:
    """REBATE / CHARGED / ZERO / UNREADABLE from a read-back commission.

    The rebate is documented, and docs-sourced is exactly the provenance that produced the Kalshi
    cent-vs-centicent fee bug. A positive commission must be able to surface as CHARGED rather
    than being folded into "the rebate was small".
    """
    if commission is None:
        return "UNREADABLE"
    if commission < _ZERO:
        return "REBATE"
    if commission > _ZERO:
        return "CHARGED"
    return "ZERO"


def missed_cycles(wall_s: float, requote_s: float) -> int:
    """How many whole requote intervals a cycle overran by.

    The scaling question is "can one process hold the whole slate inside the rate budget WITHOUT
    missing cycles", so an overrun has to be COUNTED. Absorbing it into a shrinking sleep is how a loop
    silently degrades from a 10s cadence to a 25s one while every log line still says 10.
    """
    if requote_s <= 0 or wall_s <= requote_s:
        return 0
    return math.ceil(wall_s / requote_s) - 1


def projected_req_per_s(requests: int, requote_s: float) -> float:
    """The sustained request rate this cycle implies if repeated at `requote_s`."""
    if requote_s <= 0:
        return float("inf")
    return requests / requote_s


def rate_budget_warning(requests: int, requote_s: float,
                        ceiling: float = DEFAULT_MAX_REQ_PER_S) -> Optional[str]:
    """A warning when the projected rate exceeds our self-imposed ceiling, or None.

    ⚠️ Over-limit on Poly is THROTTLED, not rejected — a late, stale 200. So the symptom of
    exceeding the budget is not an error we would notice; it is a quote decided on an old book.
    """
    rate = projected_req_per_s(requests, requote_s)
    if rate <= ceiling:
        return None
    return (f"projected {rate:.1f} req/s at a {requote_s:g}s requote exceeds the self-imposed "
            f"{ceiling:g} req/s ceiling (venue limit {VENUE_REQ_PER_S:g} req/s, and over-limit is "
            f"THROTTLED not rejected — the symptom is a stale book, not an error). Raise "
            f"--requote-s or quote fewer markets.")


def arming_refusal(dry_run: bool, flagged: bool) -> Optional[str]:
    """Reason to refuse this invocation outright, or None if the mode is coherent.

    ⛔ ORDERS ARE GATED BY `config.DRY_RUN` ALONE. `--i-understand-real-money` does not itself
    enable anything on the venue — it sets `DRY_RUN=false` in the shim's process environment before
    `bot.core.config` is imported. So a `.env` already carrying `DRY_RUN=false`, with no flag on the
    command line, would place REAL quotes while the run reported a shadow/preview. That is a HARD
    STOP here, never a downgrade to DRY: silently downgrading would hide a mis-configured
    environment that the next invocation might not survive.

    The reverse mismatch — flag passed, config still DRY — is NOT refused. It cannot move money, it
    is the ordinary "I forgot to configure it" case, and `main()` prints a DRY banner.

    A pure function on purpose: setting DRY_RUN in a shell is a denied operation in this repo, so a
    guard embedded in `main()` could only be tested by string-matching its source.
    """
    if not dry_run and not flagged:
        return ("config.DRY_RUN is False (from .env or the shell environment) but "
                "--i-understand-real-money was NOT passed. Orders are gated ONLY by DRY_RUN, so "
                "this would place REAL maker quotes while reporting a preview run — live money, "
                "mislabelled and uninstrumented. Pass the flag to trade for real, or set "
                "DRY_RUN=true to preview.")
    return None


def is_real_money(dry_run: bool, flagged: bool) -> bool:
    """True only when BOTH the environment and the operator agree. Never infer this from the flag
    alone: `arming_refusal` has already rejected the incoherent combination, and this is what the
    run stamps its rows with."""
    return flagged and not dry_run


# ── records ──────────────────────────────────────────────────────────────────────────────────

@dataclass
class RestingOrder:
    """One quote we believe is on the venue.

    `queue_ahead` and `improved` are captured AT PLACEMENT and never refreshed: they describe the
    conditions the order was born into, which is what a fill has to be attributed to. Refreshing
    them would silently rewrite the history of an order that is already in a queue.
    """
    slug: str
    side: str                    # "bid" | "ask"
    price: Decimal
    size: int
    order_id: Optional[str]
    intent_id: str
    placed_ts: float
    queue_ahead: Optional[Decimal]
    improved: bool
    filled_qty: Decimal = _ZERO


@dataclass
class RecoveryEntry:
    """One order awaiting an activities-ledger verdict.

    `order` is the SAME RestingOrder object every other structure holds — a copy resets
    `filled_qty` and double-books the tail. `terminal_ts` is per entry PATH: the cancel's
    2xx-confirm time (verify exhaustion / cancel-path park eviction), the terminal-state READ
    time (poll-path park eviction), or the cancel-probe's not_found answer time.
    `verdict_read_ts` is the wall time of the last page-1 activities read that landed at or
    past `terminal_ts + LAG_HORIZON_S` — the NO-FILL / over-statement verdict edge; 0.0 until
    one lands."""
    order: RestingOrder
    terminal_ts: float
    path: str            # "verify_exhausted" | "park_evicted_cancel" | "park_evicted_poll"
    #                    # | "cancel_probe"
    queued_ts: float
    verdict_read_ts: float = 0.0


@dataclass
class Fill:
    """One increment of fill on a resting order — the row the whole lane's economics is read from.

    ⛔ THE TWO QUANTITY COLUMNS ARE NOT INTERCHANGEABLE, and neither are they interchangeable with
    the commission. `filled_qty` is this row's INCREMENT; `cum_filled_qty` is the order's running
    total; `commission_order_total` is the venue's `commissionNotionalTotalCollected`, which is a
    property of the ORDER, not of this increment.

    So **the commission column must never be summed across rows.** A 25-lot filling 10 then 15
    writes −0.03 then −0.08; summing gives −0.11 against a true −0.08. To total the rebate, take
    the LAST row per `order_id` — which is why `order_id` and `cum_filled_qty` are on every row.
    The venue rounds the credit to the nearest cent PER FILL INCREMENT, so a per-ORDER rebate is
    not even well defined.
    """
    ts: float
    slug: str
    side: str
    order_id: Optional[str]
    price: Decimal
    size: int
    filled_qty: Decimal              # this row's INCREMENT
    cum_filled_qty: Decimal          # the ORDER's running total, which the commission belongs to
    commission_order_total: Optional[Decimal]
    queue_ahead: Optional[Decimal]
    time_to_fill_s: float
    improved: bool
    order_state: str
    # The last touch we read on this market, and how old it was when the fill was booked. The
    # rebate is only the CREDIT side; without a mark there is no cost side and no P&L, so a tape
    # carrying only rebates is a fill counter rather than a market-making record. This is a cheap
    # approximation (no extra request — it is the cycle's own book read), and `mid_age_s` is what
    # says how much to trust it.
    mid_at_fill: Optional[Decimal] = None
    mid_age_s: Optional[float] = None
    # ⛔ THE TWO SIDES THE MID WAS COLLAPSED FROM. `last_touch` has always carried (bid, ask, ts)
    # and the fill row kept only their midpoint, which throws away the SPREAD — and a passive
    # fill's markout is `drift + half_spread` BY CONSTRUCTION, so a tape without the spread
    # cannot separate price quality from spread capture. A raw markout headline read off a tape
    # without the spread is arithmetically just `spread + drift`, and it will report a positive
    # number while the drift term underneath it is negative.
    #
    # ⛔⛔ WHAT THESE COLUMNS ARE **NOT**: the BOOK's half-spread. `last_touch` comes from
    # `_fetch_book`, and that book CONTAINS OUR OWN RESTING ORDERS. Measured over improved
    # place/replace cycles, the next cycle's touch is our OWN quote on both sides roughly a third
    # of the time and on one side about half the time. So on a bid fill where `best_bid == price`,
    #     (best_ask − best_bid)/2  ≡  |mid_at_fill − price|
    # — algebraically the quantity the tape ALREADY carried: a width we chose against a mid we
    # set. Stamping both sides therefore does NOT make the true half-spread recoverable, and an
    # analyst subtracting (best_ask − best_bid)/2 uniformly across the tape would subtract our own
    # quoted width on every improved row and publish a drift figure wrong by an unbounded amount —
    # while LOOKING more authoritative than the number it replaced, because it cites a column name
    # instead of a derivation.
    #
    # WHAT THEY DO DELIVER, which is real and is why this merges: the PREVAILING, self-inclusive
    # half-spread, plus — for the first time — a way to DETECT the contaminated rows.
    #     improved=Y and best_bid == price   (bid)   → the width is self-quoted
    #     improved=Y and best_ask == price   (ask)   → likewise
    # That test is computable only because these columns exist. The uncontaminated baseline needs
    # the touch stamped at PLACEMENT, net of our own resting size (the `depth − our own size`
    # pattern the Kalshi side already uses) — a separate change, deliberately not bundled here.
    #
    # Same source, same read, same staleness as `mid_at_fill` (`mid_age_s` describes all three),
    # and blank on a late-booked row for the same reason the mid is — see `late_booked`.
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    # Source of the book this fill's touch/mid were stamped from — the FULL vocabulary:
    # rest / ws / rest_fallback (per-book REST backstop) / ws_capped (skipped over the cap) /
    # ws_outage (forced REST during a fallback window) / ws_degraded (WS-served DURING a
    # fallback window); blank on late_booked. Partition healthy-vs-degraded fills on
    # {ws} vs {ws_degraded, ws_outage} — filtering on book_src=='ws' alone silently drops
    # every degraded-episode fill.
    book_src: str = ""
    # ⛔ THE VENUE'S OWN FILL PRICE (`avgPx`), from the same get_order body as `cumQuantity`. NOT
    # redundant with `price`: `price` is OUR limit, and on a BUY_SHORT the venue reads that limit
    # as a yes-space "here or BETTER" sell — the two can differ by most of the price range (a
    # recorded case answered a wire limit of 0.100 with an avgPx of 0.8510). The rebate the venue
    # computes uses ITS price, so any "did the commission match the formula" check must be
    # evaluated at avg_px, never at our intent.
    avg_px: Optional[Decimal] = None
    # True when the delayed verify discovered this fill: it happened up to LAG_HORIZON_S before
    # this row's ts, its time_to_fill_s is an over-estimate, and its mid columns are blank ON
    # PURPOSE — do not backfill them from any book near ts.
    late_booked: bool = False
    # WHICH PATH booked this increment: "ws" (the order-feed drain) | "poll" (the REST
    # read-back, incl. the cancel-time reconcile) | "verify" (the delayed verify). The accepted
    # risk of taking fills off a WS order feed is corruption via a bad body, and without source
    # attribution that risk is UNMEASURABLE.
    # ⚠️ Read the tape correctly: a zero-delta poll writes NO row, so the healthy case is
    # a ws row with NO later poll/verify row — absence is the evidence (guard: the cycle tape
    # must show a completed requote cycle after the ws row, else absence = never-polled). A
    # later positive-delta row after a ws row is an UNDER-stating body, named and located.
    # An OVER-stating body is invisible here (a negative delta books nothing): the
    # over-statement check is scripts.poly_order_diff against the venue's activities, after
    # teardown.
    booked_via: str = "poll"

    @property
    def verdict(self) -> str:
        return commission_verdict(self.commission_order_total)


@dataclass
class CycleStats:
    """Per-cycle instrumentation. The question it answers is operational, not economic: can one
    process hold N books inside the rate budget without missing cycles?"""
    cycle: int
    started_ts: float
    wall_s: float = 0.0
    requests: int = 0
    # WS book source: fresh-REST re-reads this cycle triggered by a stale/absent WS book (the
    # burst cap reads this — a partial feed freeze must not re-read the whole slate in one
    # cycle, which is the poll burst the WS feed exists to remove).
    ws_stale_rereads: int = 0
    # Books SKIPPED over the burst cap this cycle (candidate partial freeze — feeds the
    # escalation) and seconds this cycle spent in whole-connection/partial-freeze fallback.
    ws_capped: int = 0
    ws_down_s: float = 0.0
    # REST reads forced by the fallback window this cycle — ZERO of these (plus zero capped)
    # is the positive recovery evidence.
    ws_outage: int = 0
    markets_total: int = 0
    markets_quoted: int = 0
    markets_skipped: int = 0
    missed: int = 0
    max_staleness_s: Optional[float] = None
    actions: dict[str, int] = field(default_factory=dict)
    paused: bool = False
    halt_reason: Optional[str] = None
    rss_mb: Optional[float] = None
    # avail/swap land on every cycle row so the NEXT false memory halt is diagnosable and the
    # memory floor is validated by time series rather than asserted — the false halts on record
    # survive only as available-RAM figures, because nothing logged swap at the time.
    avail_mb: Optional[float] = None
    swap_free_mb: Optional[float] = None

    @property
    def req_per_s(self) -> float:
        return projected_req_per_s(self.requests, self.wall_s) if self.wall_s > 0 else 0.0


def _open_writer(path: str, header: Sequence[str], rotate_tag: str = "schema"):
    """Append-with-header-once, so a restart extends the same tape instead of interleaving a
    second header row into it. A header MISMATCH first freezes the old file into
    <dir>/rotated/<stem>.pre-<tag>.csv (the repo's schema-version rule; the fills tape's
    `pre-latebooked` rotation is the precedent). Appending blind across a column change is a
    recorded failure in this repo: rows and header disagree on width, and DictReader then
    misfiles silently in either direction."""
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        with open(path, newline="") as f:
            existing = next(csv.reader(f), None)
        if existing is not None and existing != list(header):
            rot_dir = os.path.join(os.path.dirname(os.path.abspath(path)), "rotated")
            os.makedirs(rot_dir, exist_ok=True)
            stem, ext = os.path.splitext(os.path.basename(path))
            dest = os.path.join(rot_dir, f"{stem}.pre-{rotate_tag}{ext}")
            if os.path.exists(dest):
                dest = os.path.join(rot_dir,
                                    f"{stem}.pre-{rotate_tag}.{int(time.time())}{ext}")
            os.replace(path, dest)
            log.info(f"tape {path}: header changed → rotated to {dest}")
    fresh = not os.path.exists(path) or os.path.getsize(path) == 0
    handle = open(path, "a", newline="")
    writer = csv.writer(handle)
    if fresh:
        writer.writerow(list(header))
        handle.flush()
    return handle, writer


def _amount(value: Any) -> Optional[Decimal]:
    """A venue numeric field → Decimal, or None. Tolerates the {value,currency} Amount and a bare
    scalar, and parses via `str()` so a float never launders its binary error into money."""
    if isinstance(value, dict):
        value = value.get("value")
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _order_body(resp: Any) -> dict:
    """The order object from a create/read response — tolerate {'order': {...}} and flat shapes."""
    if not isinstance(resp, dict):
        return {}
    inner = resp.get("order")
    return inner if isinstance(inner, dict) else resp


def venue_order_id(resp: Any) -> Optional[str]:
    """The venue's order id, or None if the response does not carry one.

    ⛔ THE FIELD IS `id`. It is not `orderId` and not `order_id`, and getting this wrong is not a
    cosmetic bug — it disables the entire maker silently. `orders.create` returns a FLAT
    `{"id": ..., "executions": []}` and an `Order` carries `id`; both confirmed against captured
    live responses, not read off a schema.

    The first version of this module read `orderId`, so every order id was None. The consequences
    chain: no id ⇒ `poll_fills` skips the order ⇒ no fill, no commission, no inventory ⇒ BOTH caps
    read a permanent zero position and never bind ⇒ `_cancel` never sends a cancel, so every
    reprice ADDS a resting order and removes none ⇒ the teardown sweep also skips it ⇒ the process
    exits printing a clean teardown over live orders. The whole suite passed, because the test
    fixture was written from the same wrong belief.

    Returns None rather than raising, because the caller must treat a missing id as
    CANNOT-VERIFY (the order may well be resting) rather than as a failed placement.
    """
    body = _order_body(resp)
    raw = body.get("id")
    return str(raw) if raw else None


# ── the engine ───────────────────────────────────────────────────────────────────────────────

class PolyMaker:
    """The quoting loop, its inventory, its caps and its teardown.

    Venue contact is confined to five client methods — `get_market_tick`, `_fetch_book(fresh=True)`,
    `place_limit_gtc`, `cancel_order`, `get_open_orders`/`get_order`. Nothing here reads `bbo`:
    every Poly public GET is Cloudflare `max-age=30`, and a touch up to 30s old fails OPEN in the
    direction that costs money.
    """

    def __init__(
        self,
        *,
        client: Any,
        slugs: Iterable[str],
        size: int | str = MIN_SIZE,
        cap_fills: int | str = 3,
        requote_s: float = 10.0,
        max_total_contracts: Optional[int] = None,
        max_req_per_s: float = DEFAULT_MAX_REQ_PER_S,
        shadow: bool = True,
        real: bool = False,
        flatten_wait_s: float = 30.0,
        stale_cancel_cycles: int = 3,
        adverse_cooldown_s: float = 900.0,
        mark_trip_per_ct: Decimal = DEFAULT_MARK_TRIP_PER_CONTRACT,
        loss_cap: Decimal = Decimal("3.00"),
        heartbeat: Any = None,
        order_feed: Any = None,
        state_store: Optional[MakerStateStore] = None,
        run_id: Optional[str] = None,
        quote_csv: Optional[str] = None,
        cycle_csv: Optional[str] = None,
        fill_csv: Optional[str] = None,
        probe_retirement: bool = False,
        book_source: str = "rest",
        book_feed: Any = None,
    ) -> None:
        self.client = client
        # Per-cycle book READ source. "rest" = the cache-busted
        # _fetch_book (unchanged). "ws" = read the injected PolyUSOrderBookCache's get_book_md,
        # with a transactTime freshness gate + single-book fresh-REST backstop on staleness.
        # ⛔ "ws" requires a primed, running book_feed (the lifecycle wiring is the next brick);
        # with no feed it degrades to REST, never to a stale or empty book.
        self.book_source = book_source if book_source in ("rest", "ws") else "rest"
        self.book_feed = book_feed
        # slug -> the source that produced this cycle's book: "ws" (fresh WS), "rest_fallback"
        # (WS stale/absent → REST re-read), or "rest" (default source). Set in `_read_book_md`,
        # read at tape-write time so the quote/skip rows carry `book_src` without threading it
        # through every writer signature. Partitions the tape for the shadow WS-vs-REST analysis.
        self.last_book_src: dict[str, str] = {}
        # WS fallback state: wall-clock when the whole connection was first seen down
        # (None = up). While set, the quote loop runs the REDUCED set over REST; at expiry the
        # run halts. Cleared (loudly) on reconnect.
        self._ws_down_since: Optional[float] = None
        # Evidence counters: the run must record that the WS path actually SERVED. Absence of a
        # halt is NOT evidence the feed worked — a feed that never delivered a
        # book still "ran clean"). ws_reads/book_reads → ws_fraction; feed deaths counted.
        self.ws_reads = 0
        self.book_reads = 0
        self.ws_feed_deaths = 0
        # Books skipped over the burst cap LAST cycle — the partial-freeze escalation signal
        # (a cycle that capped is a cycle the feed was partially blind; see the guard).
        self._last_ws_capped = 0
        # Forced-REST outage reads LAST cycle + per-episode bookkeeping:
        # recovery requires a cycle with ZERO of both (positive WS evidence — the seam tries WS
        # first even in fallback); the dropped-books cancel runs once per EPISODE via a flag
        # (the 'entering' transition never fired on the escalation path, so those orders sat
        # unwatched); whole-connection entry needs a 2-cycle disconnect streak (a reconnect
        # blip must not cost the whole slate's queue position).
        self._last_ws_outage = 0
        self._ws_cancel_done = False
        self._ws_disconnect_streak = 0.0
        # True when LAST cycle quoted the REDUCED set — recovery evidence from a reduced cycle
        # is only good for PROBATION (resume full slate, keep the window); only a clean
        # FULL-SLATE cycle truly recovers. Accepting reduced-set-only evidence re-creates the
        # oscillation across episodes: the fallback window restarts every other cycle and the
        # run never converges on either state.
        self._last_cycle_reduced = False
        self.slugs = [s.strip() for s in slugs if s and s.strip()]
        # PER-BOOK SIZING: `size` is an int (uniform) or a "slug:size,…" spec. Everything
        # downstream reads the MAP; `self.size`/`self.cap` survive as the MAX for the uniform
        # case and for callers/tests.
        # ⚠️ The `.get(slug, self.size)` fallbacks are structurally DEAD — `parse_sizes` enforces
        # `set(sizes) == set(slugs)`, `ticks ⊆ slugs`, and every `_place` caller passes an
        # explicit size — and MAX is the UNSAFE direction if one ever fires (a bigger quote, a
        # LOOSER cap). Do not read them as a safety margin: a refactor that makes one reachable
        # should switch them to `self.sizes[slug]` and let the KeyError refuse, matching this
        # module's refuse-don't-default doctrine for ticks.
        self.sizes = parse_sizes(str(size), self.slugs) if self.slugs else {}
        for slug, sz in sorted(self.sizes.items()):
            refusal = size_refusal(sz)
            if refusal:
                raise ValueError(f"{slug}: {refusal}")
        self.size = max(self.sizes.values(), default=int(size) if str(size).isdigit()
                        else MIN_SIZE)
        # Per-book cap-fills: same spec discipline as --size — one int for all, or per-book
        # naming EVERY slug, so an unnamed book refuses rather than inheriting a headroom the
        # operator never chose. A global-only version of this flag forces every book in a run to
        # share one cap, which makes a per-book experiment impossible to run as designed.
        # parse_sizes carries the refuse-don't-default doctrine already.
        self.cap_fills_by_slug = (parse_sizes(str(cap_fills), self.slugs)
                                  if self.slugs else {})
        # MIN, not max: this scalar is the documented-unreachable fallback
        # (self.caps is keyed over self.slugs and every lookup comes from that set) — but a
        # fallback that cannot fire should still fail SAFE if a refactor ever makes it
        # reachable: an unknown book gets the tightest cap, never the loosest.
        self.cap_fills = min(self.cap_fills_by_slug.values(),
                             default=int(cap_fills) if str(cap_fills).isdigit() else 1)
        # Per-book caps: a book quoted at 25 gets a 25-contract cap, one at 12 gets 12. `self.cap`
        # is the MAX — see the fallback warning above.
        self.caps = {slug: cap_contracts(sz, self.cap_fills_by_slug[slug])
                     for slug, sz in self.sizes.items()}
        self.cap = max(self.caps.values(),
                       default=cap_contracts(self.size, self.cap_fills))
        # ── HOT SETTINGS — launch anchors + per-book quote-mode overrides ──
        # `_launch_reach` and `_launch_cap_fills` are the INTERLOCK references: the
        # pos-guard thresholds are process-fixed at launch, so the CEILING is judged
        # against these frozen copies. ⚠️ But the parser's third input, `slate_sizes`,
        # is deliberately the LIVE `self.sizes` — the cap_fills reach check must see the
        # size that will actually run, or two individually-legal hot files compose past the
        # anchor. Do not "clean this up" by freezing it; the suite pins that refactor RED.
        self._launch_cap_fills = dict(self.cap_fills_by_slug)
        # ⛔ PER-BOOK, not the slate max: the external guards are sized PER BOOK, so a small
        # book hot-raised under the SLATE-wide ceiling can pass that ceiling and still trip
        # ITS OWN guard — halting the whole run on an overshoot that was lawful by the number
        # the operator was reading. A book may never exceed the reach its own guard was sized
        # for at launch.
        self._launch_reach = {slug: sz * (self.cap_fills_by_slug[slug] + 1)
                              for slug, sz in self.sizes.items()}
        # The venue-breach threshold for a book NOT on the slate (another run's or the
        # operator's position): the launch-time max, frozen here because `self.cap`/
        # `self.size` recompute on a hot lowering and a shrunk max must not tighten the
        # halt bound on inventory this run never touched. ⚠️ It is
        # a CROSS-BOOK max — `cap` and `size` are independent maxes and can come from
        # different books, so this can exceed every real book's reach; loose is the
        # correct direction for a bound on foreign inventory, but do not read it as
        # "some book's actual reach".
        self._launch_reach_unknown = self.cap + self.size
        self.hot_quote: dict[str, str] = {}
        self._hot_sig: tuple | None = None            # (st_mtime_ns, st_size) last SEEN
        self._hot_announced: tuple | None = None      # last sig announced (apply OR ignore)
        self.requote_s = requote_s
        # A default equal to the sum of the per-market caps is NON-BINDING by construction. That is
        # deliberate — inventing a tighter number would be a risk decision this module has no basis
        # to make — but it means the global cap does nothing until an operator sets it, so the CLI
        # warns loudly at that default on any multi-market run.
        self.max_total_contracts = (max_total_contracts if max_total_contracts is not None
                                    else sum(self.caps.values()) or self.cap)
        self.max_req_per_s = max_req_per_s
        self.shadow = shadow
        self.real = real
        self.flatten_wait_s = flatten_wait_s
        # The parked-verify bound, scaled to the run: every slug can cancel both sides each cycle
        # and an entry lives ~LAG_HORIZON_S/requote cycles, so the worst honest steady state is
        # ~2 × slugs × horizon/requote. MAX_PARKED stays as the single-market floor.
        # A FIXED bound would start binding at only a couple of fully-repricing markets.
        self.max_parked = max(MAX_PARKED,
                              2 * max(1, len(self.slugs)) * int(LAG_HORIZON_S / max(1.0, requote_s)))
        # ⛔ Consecutive un-quotable cycles before a market's resting quotes are PULLED. Both
        # directions are hazards and this is the balance between them: cancelling on a single
        # failed read forfeits queue position on every routine hiccup (queue is the dominant
        # variable in this lane), while never cancelling leaves live orders on a market nothing is
        # reading any more — they can still fill, at a price no guard is checking. 3 cycles at the
        # default 10s requote is 30s, the same staleness budget `FROZEN_BOOK_AGE_S` uses.
        self.stale_cancel_cycles = stale_cancel_cycles
        # Post-adverse cooldown (0 disables): after a round trip realizes worse than
        # ADVERSE_RT_PER_CONTRACT per contract, the book quotes REDUCE-ONLY for this long — flat
        # means no quotes at all. Policy chosen by tape replay of a recorded news-gap episode: the
        # passive chase beat every crossing stop, but re-entering the same book seconds after the
        # adverse close bought the top of the deflating spike. Cooldown removes exactly that trade.
        # Deadline is wall-clock and NOT persisted — a crash-restart clears it. Accepted: the
        # startup recovery gate already refuses to start over live venue state, and a stand-down
        # is a quoting preference, not a safety rail.
        self.adverse_cooldown_s = adverse_cooldown_s
        # ⛔ REALIZED-only loss cap (0 disables). A cap whose writer is never called is not a cap:
        # this maker ran real-money sessions with `loss_cap` zero and `add_realized` uncalled,
        # so the "lifetime ratchet across processes" the durable store's docstring promised was
        # unreachable in practice. Semantics, stated honestly:
        # the in-run halt compares the RAW NET total (−total > cap), so in-session profit
        # extends the loss budget by that much; the restart refusal applies `loss_to_date`'s
        # zero floor to the same net number. PRICE-realized only — rebates (the strategy's whole
        # edge) are NOT credited, so the cap can trip on a rebate-profitable lifetime; marks are
        # NOT counted, so an open position's drawdown is invisible until it realizes. Breach
        # halts within the same cycle (run_cycle's post-poll check) into the full teardown.
        self.loss_cap = loss_cap
        self.heartbeat = heartbeat
        # The private-WS fill accelerator (bot/poly_us/order_feed.py), optional. Drained at the
        # top of every cycle into the SAME cum-idempotent booking path the REST poll uses — so
        # its only possible effect is booking fills SOONER. None ⇒ yesterday's behaviour exactly.
        self.order_feed = order_feed
        # An unattributable WS order id is correctly
        # skipped (usually another process on the same account), but the skip must be
        # visible — silent, it is indistinguishable from dropping our OWN fills via a
        # mis-keyed `resting`.
        self.order_feed_unattributed = 0
        # A venue-refused cancel under a REPLACE leaves the side on its
        # STALE price for the cycle (or dark, on the no-id path) — correct behaviour, but only
        # visible as a log.error line until now. Per-run monotone counter, surfaced in the
        # shim's 60s status line beside ws=/anom=. It is a POINTER, not a diagnosis: one number
        # covers both "the venue is refusing cancels" and "a create returned no id" — the log
        # lines beside the increment carry the which.
        self.replace_blocked = 0
        # Negative cum deltas — a monotone counter went backwards, so some read lied.
        # Zero in every healthy run; any positive value demands a reconciliation pass against
        # the venue's own activities ledger.
        self.cum_regressions = 0
        self.state = state_store
        # The MODE is stamped into the id. Non-real makers default to their own `.dry` tapes,
        # so the stamp is not what keeps the tapes apart — it earns its keep for EXPLICIT-path
        # dry runs, because nothing else durably maps a run_id back to its kind (the state file
        # is real-only and overwritten per run). The polymm- prefix stays first because it
        # is the venue discriminator the recovery split keys on (maker_state.venue_of_run_id).
        mode = "shadow" if shadow else ("real" if real else "dry")
        self.run_id = run_id or f"polymm-{mode}-{int(time.time())}-{uuid.uuid4().hex[:6]}"

        self.ticks: dict[str, Decimal] = {}
        self.inventory: dict[str, Decimal] = {}
        self.resting: dict[tuple[str, str], RestingOrder] = {}
        # ⛔ EVERY cancelled order parks here for one delayed verify AFTER the lag horizon.
        #
        # The venue's order reads lag reality two ways, and early real-money runs were eaten by
        # BOTH: a read can FAIL ("Order not found", a minute or two after create), or it can
        # ANSWER with a stale cumQuantity — a missed fill has been observed with zero not-found
        # warnings in the log. So discriminating "good read" from "stale read" at cancel time is
        # the wrong game: a readable-but-stale cum=0 passes any presence test. The robust rule: trust the
        # immediate read for fast booking, then VERIFY once more after LAG_HORIZON_S regardless of
        # what it said. cumQuantity is cumulative, so the delayed verify books the exact delta.
        # Values: (order, due_ts, attempts, terminal_ts, origin). `terminal_ts` is stamped at
        # park time (the cancel's 2xx-confirm or the poll's terminal-state read — same wall
        # moment either way) because the retry ladder rewrites `due_ts` and the recovery
        # queue's verdict edge must key on the TERMINAL moment, not the last retry.
        # `origin` ("cancel" | "poll") names the entry path an eviction inherits. Bounded by
        # max_parked; evictions route to the ACTIVITIES RECOVERY queue, never a silent drop.
        self.pending_reconcile: dict[str, tuple[RestingOrder, float, int, float, str]] = {}
        # ── belief recovery ───────────────────────────────────────────────────────────────
        # Orders whose venue order-store record is gone (purged / store-lagged) but whose
        # TRADE-LEDGER record survives: the walk reads portfolio.activities (1 req/cycle) and
        # answers FOUND / NO-FILL / OVER-STATEMENT per entry. Keyed by order_id; holds the
        # SAME RestingOrder objects as everything else.
        self.pending_activity_recovery: dict[str, RecoveryEntry] = {}
        # The NAMED terminal failure list (evictions, price-sanity refusals, over-statements,
        # coverage timeouts): order_id -> reason. Read by the teardown's cause-branched
        # operator message; entries here may carry UNBOOKED fills, so their durable order
        # records are deliberately NOT cleared and the crash record stays open over them.
        self.unresolved_recovery: dict[str, str] = {}
        # Consecutive poll not_founds per RESTING order — the cancel-probe's streak gate.
        self.not_found_streak: dict[str, int] = {}
        # Breaker: >NOT_FOUND_BREAKER distinct not_founds in ONE poll = client/route
        # fault; probes, retirements and the walk refuse until a quieter poll clears it.
        self._nf_breaker_tripped = False
        # ⛔ RETIREMENT IS GATED OFF BY DEFAULT: the structured not_found cancel verdict has
        # never been observed on a real purge, and the chain's three legs (retrieve, listing,
        # cancel) all read the SAME order store — the store already known to lag by HOURS.
        # Three agreeing reads of one lagging source are one piece of evidence, not three.
        # Until that evidence exists (the probe still fires and captures verdicts + body either
        # way), a not_found probe answer is LOGGED and QUEUED FOR READ-ONLY RECOVERY — the walk
        # books what the ledger PROVES, because recovery is separable from retirement — the
        # order STAYS in self.resting (mute side stays mute), and nothing is placed over it.
        # Enable per-run via --enable-probe-retirement once the evidence exists.
        self.probe_retirement = bool(probe_retirement)
        # Orders whose probe already answered not_found under DISABLED retirement — logged
        # once, not re-probed every cycle (the budget is for undecided zombies).
        self._probe_confirmed_gone: set[str] = set()
        # Walk coverage state. Matched executions per queued order (exec_id-deduped) persist
        # across traversals — dedup makes re-reads idempotent. ⛔ COVERAGE itself is a
        # property of ONE TRAVERSAL (a page-1 read plus the cursor chain that follows it),
        # NEVER of the process: pages are newest-first, so depth
        # earned at T₁ says nothing about rows created after T₁ and since pushed off page 1
        # — an entry crediting an older traversal's depth can be declared NO-FILL over a
        # fill that sits in the gap that traversal will never revisit. An entry may only
        # credit a traversal STARTED at or after its own QUEUED_TS — the merge is
        # queued-ids-only, so earlier reads filtered its rows out, and keying this on
        # `terminal_ts` instead lets an entry credit depth it was never in — and a
        # NEGATIVE verdict additionally requires the
        # start past terminal + LAG_HORIZON_S (the store publishes executions late).
        self._recovery_execs: dict[str, dict[str, dict]] = {}
        self._walk_started_ts: float = 0.0          # page-1 time of the CURRENT traversal
        self._walk_oldest_ts: Optional[float] = None  # oldest createTime IN this traversal
        self._walk_exhausted = False                # this traversal hit the ledger's end
        self._recovery_cursor: str = ""             # this traversal's deepening cursor
        # Operator-visible counters: through-zero re-basings on recovered fills
        # (the loss-cap's known interim window — must be visible without forensics), the
        # cancel-probe verdict distribution (reported while retirement is still unproven),
        # and orders whose activities legs carried NO commission snapshot (BLANK, never 0).
        self.recovered_through_zero = 0
        self.recovery_probe_verdicts: dict[str, int] = {}
        self.recovery_commission_absent: list[str] = []
        # Newest tape-row ts per slug — the out-of-order detector's reference: a recovered
        # booking corrupts pairing only when a LATER fill was already booked (re-basing
        # happened on the wrong order), not when it lands on a virgin/quiet book. Widen this
        # condition and it fires on every clean recovery too, which destroys its own signal.
        self._last_row_ts: dict[str, float] = {}
        self._empty_ledger_logged = False
        self.last_read_ts: dict[str, float] = {}
        # ⛔ VENUE TRUTH, refreshed on a slow cadence and used ONLY TO RESTRICT.
        #
        # Under fast flow the belief has drifted SEVERAL TIMES THE CAP behind the venue on one
        # book: asks filled whose confirmations never landed inside the read-back window. The
        # tape read flat, the maker believed flat, and the venue held a large short. Every rail
        # keyed on belief — per-book cap, global cap, cooldown, loss cap — was evaluating a
        # fiction, and an EXTERNAL guard was the only thing that saw it.
        #
        # This is that guard's job moved in-process, with one hard rule: the venue number may
        # BLOCK a side, never SIZE an order. The endpoint is known to serve divergent replicas
        # (rows many minutes stale have been observed), and a stale read that caused a trade
        # could open the very position it thinks it is closing. Blocking on a stale read costs
        # fills; trading on one costs money.
        self.venue_inventory: dict[str, Decimal] = {}
        self.venue_inventory_ts: float = 0.0
        # Slugs whose LAST venue row was unparseable — they keep their previous value in
        # venue_inventory (never silently read as flat) and the teardown flatten treats them as
        # uncorroborated. Per-slug, because one junk row on a market this run never touches must
        # not fail the whole read: that shape made the flatten refuse EVERY book over a foreign
        # expired-position row.
        self.venue_stale_rows: set[str] = set()
        # Consecutive un-quotable cycles per market; reset by any quotable one.
        self.unquotable_streak: dict[str, int] = {}
        # slug → (best_bid, best_ask, read_ts) from the most recent successful book read. Stamped
        # onto each fill so the tape carries a MARK, not only the rebate.
        self.last_touch: dict[str, tuple[Decimal, Decimal, float]] = {}
        # Round-trip accounting for the adverse cooldown: average entry of the CURRENT position,
        # realized P&L and closed quantity of the current round trip (reset at flat), and the
        # per-book stand-down deadline.
        self.avg_entry: dict[str, Decimal] = {}
        self.rt_realized: dict[str, Decimal] = {}
        self.rt_closed: dict[str, Decimal] = {}
        self.cooldown_until: dict[str, float] = {}
        self.mark_trip_per_ct = mark_trip_per_ct
        self.mark_trips: dict[str, int] = {}     # per-book tripwire fire count — never silent
        # Tripwire-attributed stand-down deadlines, SEPARATE from cooldown_until's shared
        # clock: the tripwire arms the SAME reduce-only machinery an experiment arm may be
        # measuring, so its cycles must be attributable in the tape (status=mark_trip) — a
        # safety rail that shares a clock with the mechanism under test confounds it.
        self.trip_until: dict[str, float] = {}
        # THIS RUN's realized, starting at zero. The durable ledger is a LIFETIME number and
        # carries PROFIT as well as loss, so comparing the cap against the raw lifetime net lets
        # a good session silently EXTEND the next session's loss budget by its own profit. The
        # cap binds on BOTH: this run's own loss AND the lifetime loss-to-date (which floors
        # at zero, so profit never buys budget on either axis).
        self.session_realized = _ZERO
        # Account-ledger degrade latch: ERROR on the TRANSITION into the degraded state only.
        # ⚠️ Not because repeats would "flood the alert channel" — this module's logger is a
        # plain getLogger(__name__) with NO push handler attached, so the records land on
        # stderr. The latch's real value: per-cycle repeats are stderr noise today and become a
        # flood only if a push handler is ever attached. The OPERATIONAL gap is the opposite one
        # — a degraded ledger read raises no push alert at all.
        self._account_read_degraded = False
        self.cycle_index = 0
        self.should_stop = False
        self.halt_reason: Optional[str] = None
        #: WIND-DOWN mode [set ONLY by the shim's --passive-exit-s loop after a NORMAL clock
        #: expiry — never on kill/memory/loss halts]: every book quotes reduce-only at a
        #: clamped size until flat or the wind-down deadline; the normal teardown follows.
        self.winddown: bool = False
        #: PER-BOOK CALENDAR PULL (`--pull-at slug:epoch`): slug → wall-clock epoch after
        #: which that ONE book goes reduce-only (the wind-down treatment, scoped). Empty by
        #: default. Wall-clock, not monotonic, because the deadlines are venue calendar
        #: events (a strike's heating window, a first pitch) — the same basis the operator
        #: reads them in.
        self.pull_at: dict[str, float] = {}
        self._pull_announced: set[str] = set()
        # Set ONLY at the END of `_teardown_sweep` (computed into a local, assigned as the
        # phase's last act), on the venue's own evidence. So a sweep phase that raises
        # ANYWHERE — including after the cancel loop — leaves this False and the crash record
        # open. The safe direction: a FAILED phase can only under-close, never over-close.
        self._swept_clean = False
        # None = the sweep never read the venue (listing raised / never ran) — cannot-verify,
        # NOT zero. An int is the venue's actual refused-cancel count from the last sweep.
        self._sweep_refused: Optional[int] = None
        self._seq = 0
        # Venue requests spent OUTSIDE our own book reads — placements (2 each: the client's
        # crossing-guard book read plus the create) and cancels. Folded into the cycle's request
        # count so the rate-budget number describes what we actually spend.
        self._extra_requests = 0

        # ⛔ A NON-REAL maker's DEFAULT tapes are its OWN (.dry.csv). This is not hygiene, it is
        # a recorded incident: dry makers constructed OUTSIDE pytest (a debug harness importing test fixtures
        # directly — no conftest sandbox) wrote the real default tapes, and the schema
        # rotation then renamed the LIVE real-money tape out from under a running session.
        # The real tapes are reachable only by a real maker or an explicit path; the pytest
        # sandbox is defense-in-depth now, not the only wall.
        def _default(path: str) -> str:
            if real:
                return path
            stem, ext = os.path.splitext(path)
            return f"{stem}.dry{ext}"
        self._quote_path = quote_csv or _default(DEFAULT_QUOTE_CSV)
        self._cycle_path = cycle_csv or _default(DEFAULT_CYCLE_CSV)
        self._fill_path = fill_csv or _default(DEFAULT_FILL_CSV)
        self._writers: dict[str, Any] = {}

    # ── lifecycle ────────────────────────────────────────────────────────────────────────────

    async def prepare(self) -> None:
        """Read every market's tick ONCE, and drop any market whose tick is unreadable.

        ⛔ NEVER DEFAULT A TICK. It varies per market on this venue (0.01 on most books, 0.005 and
        0.001 elsewhere) and it varies WITHIN a series, so a default is right often enough to look
        correct while silently mispricing exactly the markets that differ.
        """
        for index, slug in enumerate(self.slugs):
            if index:
                await asyncio.sleep(self._gap_s)
            tick = await self.client.get_market_tick(slug)
            if tick is None or tick <= _ZERO:
                log.warning(f"DROPPING {slug}: tick unreadable ({tick!r}) — a defaulted tick "
                            f"misprices silently, so this market is not quoted at all")
                continue
            self.ticks[slug] = tick
        if self.state is not None and self.real:
            # `self.inventory` at this moment IS the recovery-verified view:
            # the shim seeds accepted carries into it BEFORE prepare() and nothing else has
            # run — so a carry-start records the declared quantities and a clean start
            # records {}. The prior run's map never rides in by copy.
            self.state.begin_run(self.run_id, mode="real", loss_cap=self.loss_cap,
                                 tickers=list(self.ticks), inventory=dict(self.inventory))

    async def read_touches(self) -> dict[str, tuple[Decimal, Decimal]]:
        """One cache-busted book read per market, quoting NOTHING.

        Exists so the startup rebate check can run BEFORE any order is placed. Doing it with a
        real `run_cycle` would print "these markets earn nothing" after the money was already
        committed to them, which is the wrong order for a warning to arrive in.
        """
        out: dict[str, tuple[Decimal, Decimal]] = {}
        start = time.time()
        for index, slug in enumerate(sorted(self.ticks)):
            await self._pace(index, start)
            try:
                book = await self.client._fetch_book(slug, fresh=True)
            except Exception as exc:
                log.warning(f"touch read failed for {slug}: {exc!r}")
                continue
            bid, ask, _ = touch_from_md(book.get("marketData") if isinstance(book, dict) else None)
            if bid is not None and ask is not None:
                out[slug] = (bid, ask)
        return out

    def zero_rebate_markets(self, touches: dict[str, tuple[Decimal, Decimal]]) -> list[str]:
        """Markets where a fill at the current mid would earn NOTHING at this size.

        ⛔ The size floor is a floor on SIZE; this is the per-PRICE rule, and they are different
        questions. `p(1−p)` collapses toward the ends of the grid, so size 5 clears the venue's
        nearest-cent boundary only across roughly p∈[0.10,0.90]. Point `--slugs` at a longshot and
        the maker will happily quote it, collect NOTHING, and carry the full adverse-selection and
        inventory risk for it — deleting the only positive term in the lane's economics while
        every other guard reports healthy.

        Reported rather than refused: the price moves, and a market that is unprofitable now may
        not be in an hour. The operator gets the list; the decision stays theirs.
        """
        out: list[str] = []
        for slug, (bid, ask) in sorted(touches.items()):
            mid = (bid + ask) / 2
            # Per BOOK's own size — the whole point of the report is "this book, at the size it
            # will actually be quoted at, earns nothing".
            if rebate_rounds_to_zero(mid, self.sizes.get(slug, self.size)):
                out.append(f"{slug} (mid {mid}, size {self.sizes.get(slug, self.size)}, "
                           f"needs size {min_rebate_size(mid)})")
        return out

    @property
    def _gap_s(self) -> float:
        """The minimum spacing for ONE request. Used only where a single request is about to be
        spent outside the cycle's pacer (the startup tick sweep)."""
        return 1.0 / self.max_req_per_s if self.max_req_per_s > 0 else 0.0

    async def _pace(self, requests_spent: int, cycle_start: float) -> None:
        """Hold the cycle's cumulative request rate at or under `max_req_per_s`.

        ⛔ PACED ON REQUESTS SPENT, NOT ON MARKETS VISITED. A fixed gap between markets assumes
        every market costs one request; a QUOTING market costs five (its book read, plus two
        placements each carrying the client's own crossing-guard book read). Measured on a real
        DRY cycle: two markets spent 10 requests in 0.28s — a 35 req/s burst against a documented
        20 req/s ceiling. Over-limit on Poly is THROTTLED rather than rejected, so the symptom is
        not an error but a late, stale book feeding the next quote decision.
        """
        if self.max_req_per_s <= 0:
            return
        target = requests_spent / self.max_req_per_s
        elapsed = time.time() - cycle_start
        if elapsed < target:
            await asyncio.sleep(target - elapsed)

    @property
    def client_is_dry(self) -> bool:
        """Whether the CLIENT is short-circuiting orders. A DRY create returns
        `{"status": "dry_run", ...}` with no `id`, which is correct and expected — so the
        missing-id alarm must not fire on every DRY placement and train an operator to ignore it."""
        return bool(getattr(self.client, "_dry_run", False))

    @property
    def gross_exposure(self) -> Decimal:
        """Σ|inventory|. Not netted: a long in one market and a short in another do not settle
        against each other."""
        return sum((v.copy_abs() for v in self.inventory.values()), _ZERO)

    def book_pulled(self, slug: str) -> bool:
        """Has this ONE book passed its `--pull-at` calendar deadline? Announces once.

        A pulled book takes the wind-down treatment (reduce-only, clamped, sub-contract
        skip) while the rest of the slate keeps quoting normally — that is the whole point:
        one book's exit must never stand the others down. Absent from `pull_at` = never
        pulled, which is every book by default.
        """
        deadline = self.pull_at.get(slug)
        if deadline is None or time.time() < deadline:
            return False
        if slug not in self._pull_announced:
            self._pull_announced.add(slug)
            log.warning(
                f"📕 PULLED {slug}: past its --pull-at deadline — reduce-only from now "
                f"(inventory {self.inventory.get(slug, _ZERO)}); the rest of the slate is "
                f"unaffected. The teardown still reports any residual.")
        return True

    def maybe_apply_hot_settings(self) -> None:
        """The hot-settings apply seam, called at cycle top beside the kill switch. No file = no
        overrides = today's behavior; file REMOVAL is not a revert (the running config
        persists — reverting would be a config change nobody wrote down).

        Change detection keys on `(st_mtime_ns, st_size)` — key it on whole-second mtime alone
        and two writes inside one tick are invisible forever — and the announce-once
        cache keys on the same tuple, so an ignored file complains exactly once per edit
        and an applied one logs exactly once. Whole-file-or-nothing lives in the parser."""
        import os as _os
        from bot.core import config as _config
        from bot.core.hot_settings import parse_hot_settings
        path = getattr(_config, "HOT_SETTINGS_FILE", "hot_settings_poly.json")
        try:
            st = _os.stat(path)
        except OSError:
            return
        sig = (st.st_mtime_ns, st.st_size)
        if sig == self._hot_sig:
            return
        self._hot_sig = sig
        try:
            with open(path) as fh:
                raw = fh.read()
        except OSError as exc:
            if sig != self._hot_announced:
                self._hot_announced = sig
                log.warning(f"⚠️ hot_settings UNREADABLE ({type(exc).__name__}) — running "
                            f"config unchanged")
            return
        changes, why = parse_hot_settings(
            raw, slate_sizes=self.sizes, launch_cap_fills=self._launch_cap_fills,
            launch_reach=self._launch_reach, min_size=MIN_SIZE)
        if changes is None:
            if sig != self._hot_announced:
                self._hot_announced = sig
                log.warning(f"⚠️ hot_settings IGNORED ({why}) — running config unchanged, "
                            f"whole-file-or-nothing")
            return
        self._hot_announced = sig
        import csv as _csv
        applied = []
        for slug, spec in changes.items():
            for key, new in spec.items():
                old = (self.sizes.get(slug) if key == "size"
                       else self.cap_fills_by_slug.get(slug) if key == "cap_fills"
                       else self.hot_quote.get(slug, "normal"))
                if old == new:
                    continue
                if key == "size":
                    self.sizes[slug] = new
                elif key == "cap_fills":
                    self.cap_fills_by_slug[slug] = new
                else:
                    self.hot_quote[slug] = new
                applied.append((slug, key, old, new))
        if not applied:
            log.info("hot_settings: file changed but every value already current")
            return
        # caps derive from sizes × cap_fills — recompute once after all changes, so the
        # scalar fallbacks track the live maps for whatever reads them.
        # Per the constructor's warning these maxes are NOT a safety margin (MAX is the
        # loose direction) — and the venue-breach halt threshold deliberately does NOT
        # follow them: it anchors to the FROZEN launch reach, because a
        # hot lowering must not turn inventory that was lawful at launch into a false halt.
        self.caps = {slug: cap_contracts(sz, self.cap_fills_by_slug[slug])
                     for slug, sz in self.sizes.items()}
        self.cap = max(self.caps.values(), default=self.cap)
        self.size = max(self.sizes.values(), default=self.size)
        try:
            new_file = not _os.path.exists("logs/hot_settings_changes.csv")
            with open("logs/hot_settings_changes.csv", "a", newline="") as fh:
                w = _csv.writer(fh)
                if new_file:
                    w.writerow(["ts", "run_id", "slug", "key", "old", "new"])
                for slug, key, old, new in applied:
                    # ⛔ MILLISECOND precision, matching the fill/quote tapes. A whole-second
                    # stamp makes the SEGMENT boundary coarser than the rows it splits: a
                    # change inside the run's first second disappears entirely (its window is
                    # mislabelled as launch config) and a slice of old-config rows lands in
                    # the new segment. Module-level clock, same source as every other tape —
                    # a locally re-imported `time` here would be the one tape stamped off a
                    # different seam, and the only one a test clock could not reach.
                    w.writerow([f"{time.time():.3f}", self.run_id, slug, key, old, new])
        except OSError as exc:
            log.warning(f"hot_settings change-tape write FAILED ({exc!r}) — changes applied "
                        f"but UNRECORDED; the run scores [config-changed, unscored]")
        for slug, key, old, new in applied:
            # The REPLACE note is a SIZE-change fact only: a cap_fills or
            # quote change leaves the resting order untouched, and claiming otherwise
            # teaches the operator the wrong cost model for the cheap edits.
            note = (" (a size change REPLACES the resting order next cycle — queue "
                    "seniority forfeited for that book, the operator's explicit trade)"
                    if key == "size" else "")
            log.warning(f"🔧 HOT {slug} {key}: {old} → {new}{note}")

    # ── one cycle ────────────────────────────────────────────────────────────────────────────

    async def _refresh_venue_inventory(self) -> bool:
        """Ask the VENUE what we hold. True iff a read SUCCEEDED and replaced the answer.

        Failure keeps the previous answer — see the field's note: this number only ever
        RESTRICTS, so a stale restriction costs fills while a cleared one would restore exactly
        the blindness this exists to remove. ⛔ The bool matters: the first version returned None
        either way, so a caller could not tell a confirmed re-read from two failed ones, and the
        halt claimed "(confirmed by a fresh read)" over a 503."""
        fresh: dict[str, Decimal] = {}
        stale_rows: set[str] = set()

        def _keep_previous(slug: str, why: str) -> None:
            # ⛔ PER-SLUG, not per-read: one junk row must not fail the whole refresh — a single
            # foreign expired-position row would otherwise make the teardown refuse every book.
            # The slug keeps its previous value — never read as flat — and is flagged so the
            # flatten treats it as uncorroborated.
            log.warning(f"venue inventory: {slug} row {why} — keeping its previous value and "
                        f"flagging it uncorroborated")
            if slug in self.venue_inventory:
                fresh[slug] = self.venue_inventory[slug]
            stale_rows.add(slug)

        cursor, pages = "", 0
        while pages < 20:
            params: dict = {"limit": 100}
            if cursor:
                params["cursor"] = cursor
            try:
                self._extra_requests += 1
                resp = await self.client._sdk.portfolio.positions(params)
            except Exception as exc:
                log.warning(f"venue inventory read failed ({exc!r}) — keeping the previous "
                            f"answer (age {time.time() - self.venue_inventory_ts:.0f}s)")
                return False
            pos = resp.get("positions") if isinstance(resp, dict) else None
            if not isinstance(pos, dict):
                log.warning("venue inventory: unrecognised shape — keeping the previous answer")
                return False
            for slug, item in pos.items():
                if not isinstance(item, dict):
                    _keep_previous(str(slug), f"is {type(item).__name__}")
                    continue
                # ⛔ `netPositionDecimal` ONLY, confirmed against a captured live position row —
                # it is the venue's EXACT holding, sitting beside a ROUNDED display field
                # (`netPosition`) that looks just as authoritative.
                # The breach threshold compares this map, and a rounded read mis-sizes it
                # by up to half a contract per book. No `qtyAvailable` fallback (sign
                # convention never verified — it decides which side gets blocked) and no
                # rounded-field fallback (a half-contract lie): missing = keep previous.
                raw = item.get("netPositionDecimal")
                if raw is None:
                    _keep_previous(str(slug), "has no netPositionDecimal")
                    continue
                try:
                    qty = Decimal(str(raw))
                except ArithmeticError:
                    _keep_previous(str(slug), f"netPositionDecimal {raw!r} unparseable")
                    continue
                if not qty.is_finite():
                    # "NaN"/"Infinity" parse cleanly and then raise inside `sides_allowed`'s
                    # comparison, taking the rail out from under the run.
                    _keep_previous(str(slug), f"netPositionDecimal {raw!r} not finite")
                    continue
                fresh[str(slug)] = qty
            cursor = resp.get("nextCursor") or ""
            pages += 1
            if not cursor or resp.get("eof"):
                break
        else:
            log.warning("venue inventory: pagination did not terminate — keeping the previous "
                        "answer rather than trusting a truncated read")
            return False
        if (self.venue_inventory and self.venue_inventory_ts
                and time.time() - self.venue_inventory_ts > VENUE_STALE_AFTER_S):
            # Announced ONCE per refresh with its age, not per-slug per-cycle — a per-slug
            # warning scales with the slate and buries its own signal.
            log.warning(f"venue inventory was {time.time() - self.venue_inventory_ts:.0f}s stale "
                        f"before this read — restrictions were running on old evidence")
        self.venue_inventory = fresh
        self.venue_stale_rows = stale_rows
        self.venue_inventory_ts = time.time()
        # ⛔ ANNOUNCE EVERY SUCCESS. A rail that logs only on failure is indistinguishable from a
        # rail that never ran — and this one can be neutralised with a green suite (its entire
        # parse was mutable to `{}` without a single test noticing). Cycle 1 of a live run must
        # tell the operator whether the rail exists.
        held = {s: str(q) for s, q in fresh.items() if q != _ZERO}
        # ⛔ WARNING, not INFO. `bot.poly_us.maker` has no handler of its own and inherits root's
        # WARNING, so the first version of this announce was dropped entirely in production and
        # visible only to a test that forced the level — the rail stayed exactly as unobservable
        # as the review said it was. This is a safety announcement; it belongs at a level that
        # reaches the run log.
        log.warning(f"venue inventory ({pages} page(s)): {held or 'flat'}")
        return True

    def _venue_breach(self) -> Optional[str]:
        """A halt reason if VENUE truth says our position is out of control, else None.

        ONE condition: MAGNITUDE — |venue| beyond `cap + size`, i.e. past any position the caps
        can produce even allowing for a full fill on an already-resting order after the adding
        side stopped being placed. Strict `>`: `cap + size` itself is reachable lawfully through
        cancel latency.

        ⛔ A SIGN-DISAGREEMENT CONDITION WAS TRIED AND REMOVED — do not re-add it. Replayed
        against the REAL venue position series rather than a model of it, it halted EVERY
        simulated phase-run, most of them within the first hour. Belief and the venue agree on
        only about a third of samples; genuine opposite-sign states are a couple of percent —
        rare, but persistent enough that a halt keyed on them stops every phase offset.
        Disagreement is the NORMAL state of a fast book, and no reordering fixes that: the
        divergence is structural.

        ⚠️ Honest scope: this rail's threshold has essentially ONE qualifying sample in the whole
        venue record, and that one was caught in real life by an EXTERNAL position guard. This
        in-process copy is defence in depth with a faster read cadence, not a replacement for
        that guard — run one guard per book.

        ⛔ Runs over EVERY venue row from run_cycle — not inside the quote path. A book whose
        book read failed, whose touch went one-sided, or that this run no longer quotes still
        holds whatever it holds, and divergence correlates with exactly the venue stress that
        makes reads fail.
        """
        for slug, venue_inv in sorted(self.venue_inventory.items()):
            if venue_inv == _ZERO:
                continue
            book_cap = self.caps.get(slug, self.cap)
            book_size = self.sizes.get(slug, self.size)
            belief = self.inventory.get(slug, _ZERO)
            # ⛔ STRICT `>`, because `cap + size` is reachable lawfully: at belief == cap the
            # adding side stops being PLACED, but an already-resting full-size order keeps
            # filling until the next cycle cancels it.
            # Verified against the venue's own position series rather than by reasoning: sitting
            # exactly AT the bound is a routine, recurring state on a live book, so an inclusive
            # bound halts healthy runs. That same series also shows positions are NOT quantised
            # to multiples of the quote size, so no "the venue can only ever hold a multiple of
            # our size" argument survives contact with the data.
            # ⛔ FROZEN launch anchor: this threshold must NOT move with a hot lowering. An
            # external, process-fixed guard has the opposite hazard (RAISES); this guard is
            # live-recomputed, so LOWERINGS are the hazard HERE. Hot-lower a book's size and a
            # position that was lawful when it was acquired instantly reads as a breach, taking
            # the whole run into teardown. Inventory acquired lawfully under the launch config
            # stays lawful; a hot lowering shrinks the QUOTES, never this bound.
            # The anchor ALONE — deliberately NOT max(anchor, live): for
            # any lawful config the live cap+size can never exceed the anchor (equal at
            # launch, strictly below after a lowering, interlock-bounded after a paired
            # raise), so a live term that would win the max describes a config no gate
            # approved — widening the bound to match it is concealment, not defence.
            reach = self._launch_reach.get(slug, self._launch_reach_unknown)
            if venue_inv.copy_abs() > reach:
                return (f"VENUE INVENTORY {venue_inv} on {slug} is beyond the launch lawful "
                        f"reach {reach} (live cap {book_cap} + overshoot {book_size}) — "
                        f"belief said {belief}; halting into teardown")
        return None

    def _venue_restriction(self, slug: str, cap: int) -> tuple[bool, bool]:
        """(bid ok, ask ok) from VENUE truth alone — ANDed with the belief-side decision by the
        caller, so it can only ever narrow what we quote. Unknown book ⇒ no restriction."""
        inv = self.venue_inventory.get(slug)
        if inv is None:
            return True, True
        return sides_allowed(inv, cap)

    def _loss_cap_breach(self) -> Optional[str]:
        """The halt reason if the loss cap is breached, else None. TWO axes, both floored at
        zero so profit never buys budget on either:

          SESSION — this run's own realized loss. What an operator means by "this run may lose
          at most X": it does not silently grow because last night went well.
          LIFETIME — the durable loss-to-date (`loss_to_date` floors at zero, matching the disk
          refusal `assess_recovery` already applies). This is the crash-restart ratchet: a loop
          of runs each losing under the session bound still halts once their sum passes it.

        ⛔ Compare the cap against the RAW durable net and carried PROFIT silently enlarges the
        cap by its own amount — while every prose claim around it still says profit does not earn
        budget. Floor each axis at zero, and check both.
        """
        if self.loss_cap <= _ZERO:
            return None
        session_loss = max(_ZERO, -self.session_realized)
        if session_loss > self.loss_cap:
            return (f"LOSS CAP: this run has realized −{session_loss} against a −{self.loss_cap} "
                    f"cap — halting into teardown")
        if self.state is not None and self.real:
            # ⛔ ACCOUNT-WIDE and LIVE, not a startup snapshot: two lanes launched together each
            # price a full cap at startup if nothing re-reads its siblings, so the ACCOUNT can
            # lose N times the cap the operator thought they set. The lifetime axis reads Σ
            # per-lane loss_to_date from the SHARED ledger file every check — a sibling's
            # in-run losses tighten THIS run within one cycle. Solo main: account = own lane,
            # byte-identical to the old snapshot read. Unreadable file → fall back to the
            # own-lane snapshot (weaker but never blind; corruption still raises at write).
            # MAX of the live account read and the own-lane snapshot — never WEAKER than the
            # old own-lane check: a missing/unreadable/foreign-path file reads as Σ=0 (not an
            # exception), which silently disarmed the carried-breach halt in the fake-store
            # tests and would do the same on any path mismatch in production.
            own_loss = self.state.snapshot().loss_to_date
            account_read_ok = True
            try:
                lifetime_loss = max(own_loss, account_loss(self.state.path))
            except Exception as exc:
                # Designed degrade, but LOUD — a silent except here hid every route into THIS
                # except (the import itself used to sit inside the try, so a typo after a
                # refactor would have disabled the account-wide axis forever with no trace).
                # ERROR fires on the TRANSITION only (see the latch's init comment).
                # ⚠️ NOT covered here: a state path that yields ZERO lanes returns a silent Σ=0
                # WITHOUT raising (max() then falls back to own-lane), so a path mismatch
                # between sibling lanes still degrades invisibly. Known gap, not a fixed one.
                if not self._account_read_degraded:
                    log.error(f"account-ledger read failed ({safe_exc(exc)}) — lifetime axis "
                              f"degraded to the OWN-LANE snapshot until it recovers")
                self._account_read_degraded = True
                account_read_ok = False
                lifetime_loss = own_loss
            else:
                if self._account_read_degraded:
                    log.warning("account-ledger read RECOVERED — lifetime axis is Σ all "
                                "lanes again")
                self._account_read_degraded = False
            if lifetime_loss > self.loss_cap:
                scope = ("(Σ all lanes)" if account_read_ok
                         else "(OWN LANE only — account read DEGRADED)")
                return (f"LOSS CAP: durable lifetime ACCOUNT loss −{lifetime_loss} {scope} is "
                        f"beyond the −{self.loss_cap} cap (the ratchet carries across processes "
                        f"AND lanes; clearing it is a deliberate operator action on the durable "
                        f"state file)")
        return None

    async def _halt_cycle(self, stats: CycleStats, started: float) -> CycleStats:
        """End a cycle that halted mid-flight: PULL THE QUOTES, attribute, tape it, return.

        ⛔ One implementation for every halt path. The loss cap shipped twice with a halt that
        stopped quoting but left the resting orders live — the second time because a `break`
        reached the cycle's NORMAL exit, which cancels nothing and writes the tape as a healthy
        cycle. Cancelling is the safe direction and the flatten is left for teardown, exactly as
        the kill switch does.
        """
        for slug, side in list(self.resting):
            await self._cancel(slug, side)
            stats.actions[CANCEL] = stats.actions.get(CANCEL, 0) + 1
        stats.halt_reason = stats.halt_reason or self.halt_reason
        stats.requests += self._extra_requests
        self._extra_requests = 0
        stats.wall_s = time.time() - started
        self._beat(stats)
        self._write_cycle(stats)
        return stats

    async def run_cycle(self) -> CycleStats:
        started = time.time()
        self.cycle_index += 1
        stats = CycleStats(cycle=self.cycle_index, started_ts=started,
                           markets_total=len(self.ticks))

        # ⛔ FIRST STATEMENT. A kill switch checked after the book read has already spent the
        # request and already decided a quote.
        self.maybe_apply_hot_settings()   # same seam, same cadence; no file = no-op
        if is_paused():
            stats.paused = True
            # ⛔ A PAUSE PULLS THE QUOTES. Returning here without cancelling leaves resting orders
            # live and still filling, while skipping the cap and reduce-only checks entirely (they
            # live below, in the quote loop) — so the kill switch would produce a maker that keeps
            # trading with NO cap in force, which is worse than one that is merely still trading.
            # Cancelling is always the safe direction; the flatten is left for teardown.
            for slug, side in list(self.resting):
                await self._cancel(slug, side)
                stats.actions[CANCEL] = stats.actions.get(CANCEL, 0) + 1
            # ⛔ AND THE RUN ENDS — same pattern as the memguard breach below. Without this, the
            # kill switch pulled the quotes and then left the process alive and IDLE for the rest
            # of --seconds, holding whatever inventory it had: the operator touches pause.json
            # expecting the documented halt-into-teardown and gets a maker that neither quotes nor
            # flattens for hours. The shim breaks on should_stop and runs cancel-all → flatten →
            # sweep.
            self.should_stop = True
            self.halt_reason = stats.halt_reason = "kill switch (pause.json)"
            stats.requests += self._extra_requests
            self._extra_requests = 0
            stats.wall_s = time.time() - started
            self._beat(stats)
            self._write_cycle(stats)
            return stats

        # PER-LANE hard RAM floor, relaxed for SHADOW runs: a shadow run places nothing, so an
        # OOM SIGKILL costs only unwritten tape rows (they flush per cycle), and the recorded
        # kill history on this class of host shows the kernel only kills at SWAP exhaustion —
        # which the COMBINED floor still guards. The residency floor exists for a REAL maker's
        # TEARDOWN: venue HTTP cancel-all and flatten must not run out of a swapfile. Applied
        # to shadow runs it does the opposite of its job — it killed shadow attempts that had
        # gigabytes of swap free, during exactly the market episode they existed to record.
        _lim = memguard.limits_from_config()
        if self.shadow and _lim.hard_ram_floor_mb:
            import dataclasses as _dc
            _lim = _dc.replace(_lim, hard_ram_floor_mb=0.0)
        mem = memguard.check(limits=_lim, label="poly_live_mm")
        stats.rss_mb = mem.rss_mb
        stats.avail_mb = mem.avail_mb
        stats.swap_free_mb = mem.swap_free_mb
        if mem.should_halt:
            # Halt through our OWN teardown while there is still memory to run it. A SIGKILL from
            # the OOM killer runs no teardown and leaves live orders resting.
            self.should_stop = True
            self.halt_reason = stats.halt_reason = f"memory: {mem.detail}"
            stats.wall_s = time.time() - started
            self._beat(stats)
            self._write_cycle(stats)
            return stats

        self._extra_requests = 0
        # Repair the inventory belief BEFORE the quote decisions below, and AFTER the stats reset
        # so the verify reads are COUNTED and paced — the first version ran before the reset and
        # its reads were erased from stats.requests and rate_budget_warning, a request leak the
        # rate line could never show.
        await self._retry_pending_reconciles()
        # The activities walk rides the same seam: after the stats reset (its read is
        # counted and paced), before `_venue_breach` (the breach line's "belief said" must be
        # the healed number, not the pre-recovery one).
        await self._recovery_walk()
        if self.real and self.cycle_index % RECONCILE_EVERY_CYCLES == 1:
            # Venue truth, on a slow cadence. Real runs only: in shadow there is nothing to hold.
            await self._refresh_venue_inventory()
        # ⛔ FILLS FIRST, before any quote decision. The cap and the reduce-only rule are functions
        # of inventory, so polling afterwards would size every cycle's quotes off the PREVIOUS
        # cycle's position — the side that should have stopped adding keeps adding for one more
        # requote. Its requests are counted and paced here rather than spent invisibly by the
        # caller: a full poll costs roughly two requests per market, several times the
        # book-read cost, so leaving it out of the budget under-reports the true rate badly.
        await self.poll_fills()
        # ⛔ AND THE VENUE CHECK COMES AFTER THEM, for the same reason. The first version ran it
        # BEFORE the poll, so it compared a fresh venue row against a belief that was one requote
        # stale — and an ordinary buy-back-through-zero (belief −10, resting bid fills, venue +2)
        # read as a SIGN DISAGREEMENT that the rescan could not clear, because the rescan re-reads
        # the VENUE and belief was the stale side. Replayed, that ordering false-halted the
        # MAJORITY of runs, most within the hour. Still before any book read, so an unreadable
        # book is still checked.
        venue_breach = self._venue_breach()
        if venue_breach is not None and not self.should_stop:
            # ⛔ RE-READ BEFORE HALTING. The venue row can be up to RECONCILE_EVERY_CYCLES old,
            # so the likeliest explanation for a contradiction is a position we have since
            # closed. Halting a healthy run on stale evidence costs earnings and hands back a
            # manual reconcile for nothing. One extra request, only on a breach.
            log.warning(f"venue breach on stale evidence — re-reading before halting: "
                        f"{venue_breach}")
            confirmed_read = await self._refresh_venue_inventory()
            venue_breach = self._venue_breach()
            if venue_breach is not None and not self.should_stop:
                self.should_stop = True
                if self.halt_reason is None:
                    # ⛔ Only claim confirmation when a read actually SUCCEEDED. The refresh
                    # returns the same None on success and failure in the first version, so a
                    # 503 kept the stale row, the breach persisted, and the halt asserted "(
                    # confirmed by a fresh read)" over two failed reads — a false confirmation
                    # that feeds an operator hand-flatten of a position that may not exist.
                    self.halt_reason = venue_breach + (
                        " (confirmed by a fresh read)" if confirmed_read
                        else " (RE-READ FAILED — halting on the stale row, NOT confirmed)")
                log.error(self.halt_reason)
        breach = self._loss_cap_breach()
        if not self.should_stop and breach is not None:
            # Evaluated EVERY cycle, not only when a round trip completes: a carried ledger can
            # already breach (a relaunch with a tightened cap), and a large share of quoted
            # book-hours complete no round trip at all — so a check that only runs on trip
            # completion may never run.
            self.should_stop = True
            if self.halt_reason is None:
                self.halt_reason = breach
        if self.should_stop:
            # A halt raised during the fill polls ends the cycle here.
            return await self._halt_cycle(stats, started)

        # ── WS whole-connection guard ──
        quote_slugs = sorted(self.ticks)
        if self.book_source == "ws" and self.book_feed is not None:
            connected = self.book_feed.connected
            # DECAY, not reset: a flapping socket (alternating cycles) zeroes an
            # integer streak forever and disabled the guard entirely; decay by 0.5 per healthy
            # cycle lets sustained flapping accumulate to the threshold while a lone blip fades.
            self._ws_disconnect_streak = (max(0.0, self._ws_disconnect_streak - 0.5) if connected
                                          else min(4.0, self._ws_disconnect_streak + 1.0))
            # NEAR-CAP is hoisted ABOVE the blip tolerance: a maker near its contract cap that
            # cannot verify the book halts on the FIRST dark cycle — one cycle against a cache
            # that may be minutes old, near the risk limit, is the exact exposure this rail
            # forbids.
            # Dark THIS CYCLE, deliberately: keying the halt on "we were down at any point"
            # makes it true at the start of the RECOVERY cycle too, so the halt preempts
            # recovery and blames a dark feed on the very cycle proving it healthy — and once
            # near cap, recovery becomes structurally unreachable.
            clean_last = self._last_ws_capped == 0 and self._last_ws_outage == 0
            dark_now = (not connected
                        or (self._ws_down_since is not None and not clean_last))
            if dark_now and ws_near_cap(self.gross_exposure, self.max_total_contracts):
                self.should_stop = True
                self.halt_reason = stats.halt_reason = (
                    f"ws feed UNVERIFIED near cap (connected={connected}, last cycle "
                    f"capped={self._last_ws_capped}/outage={self._last_ws_outage}; gross "
                    f"{self.gross_exposure} / {self.max_total_contracts}) — immediate halt")
                log.error(self.halt_reason)
                return await self._halt_cycle(stats, started)
            if connected and self._ws_down_since is not None and clean_last \
                    and not self._last_cycle_reduced:
                # TRUE RECOVERY: a clean FULL-SLATE cycle — every book read fresh from WS with
                # zero capped and zero forced-REST reads. Reduced-set evidence never clears the
                # window (see probation below).
                log.warning(f"WS book feed RECOVERED after "
                            f"{time.time() - self._ws_down_since:.0f}s — full slate confirmed")
                self._ws_down_since = None
                self._ws_cancel_done = False
            probation = (connected and self._ws_down_since is not None and clean_last
                         and self._last_cycle_reduced)
            if (connected or self._ws_disconnect_streak < 2) and self._ws_down_since is None:
                self._last_cycle_reduced = False    # healthy
            elif probation:
                # PROBATION: the reduced set came back all-WS-fresh, so TRY the full slate —
                # but the WINDOW KEEPS RUNNING (monotonic to expiry) and the cancel flag is NOT
                # reset: if the dropped books are still dark, this cycle caps (the burst cap
                # bounds the REST cost), the next cycle is fallback again, and the episode continues on
                # the ORIGINAL clock. Only the full-slate success above ends it.
                self._last_cycle_reduced = False
                log.warning(f"WS probation: reduced set clean after "
                            f"{time.time() - self._ws_down_since:.0f}s down — trying the full "
                            f"slate (window keeps running)")
            else:
                now = time.time()
                if self._ws_down_since is None:
                    self._ws_down_since = now
                    self.ws_feed_deaths += 1
                    log.error(f"WS book feed DARK — bounded fallback for "
                              f"≤{WS_FALLBACK_WINDOW_S:.0f}s on a reduced set, then halt")
                elif now - self._ws_down_since > WS_FALLBACK_WINDOW_S:
                    self.should_stop = True
                    self.halt_reason = stats.halt_reason = (
                        f"ws feed degraded >{WS_FALLBACK_WINDOW_S:.0f}s — fallback window "
                        f"expired, halting (fallback is never steady state)")
                    log.error(self.halt_reason)
                    return await self._halt_cycle(stats, started)
                elif self._last_cycle_reduced is False and self._ws_cancel_done:
                    # PROBATION RELAPSE: the full-slate try failed — orders it placed on
                    # still-dark books get cancelled again. ⚠️ HONEST COST: a
                    # persistent partial freeze therefore alternates probation-place / relapse-
                    # cancel every two cycles until the window expires — dropped books'
                    # queue position resets each pair, and probation's REST reads are NOT
                    # burst-capped (one read per dark book). Bounded by the window, safe-direction
                    # (no blind quoting), but it is churn, not free.
                    self._ws_cancel_done = False
                # Reduced set limits ONLY this iteration — never self.ticks/self.inventory
                # (the reconciler + teardown flatten iterate those; pruning blinds teardown).
                quote_slugs = ws_fallback_slugs(self.inventory, self.ticks,
                                                sizes=self.sizes)
                self._last_cycle_reduced = True
                # ⛔ CANCEL the dropped books' resting orders ONCE PER EPISODE-SEGMENT: the flag
                # resets on true recovery or on a probation relapse — see the relapse comment
                # above for the bounded churn this implies.
                if not self._ws_cancel_done:
                    self._ws_cancel_done = True
                    reduced = set(quote_slugs)
                    for r_slug, r_side in list(self.resting):
                        if r_slug not in reduced:
                            await self._cancel(r_slug, r_side)
                            stats.actions[CANCEL] = stats.actions.get(CANCEL, 0) + 1
                log.warning(f"WS down {time.time() - self._ws_down_since:.0f}s — quoting "
                            f"reduced set ({len(quote_slugs)} of {len(self.ticks)}) over REST")

        await self._pace(stats.requests + self._extra_requests, started)
        for slug in quote_slugs:
            if self.should_stop:
                # ⛔ `_book_fill` is ALSO reachable from inside this loop — a requote's cancel
                # read-back books the fill that breaches the cap. Two separate near-misses here:
                # the loop CONTINUING after a breach, and a `break` reaching the NORMAL cycle
                # exit, which cancels nothing — leaving orders resting while the tape records a
                # healthy cycle. Both paths run the SAME teardown-lite.
                return await self._halt_cycle(stats, started)
            await self._quote_market(slug, stats)
            if self.should_stop:
                # A halt raised BY this market (venue-inventory breach, or a cancel read-back
                # booking a cap-breaching fill) — checking only at the top of the loop misses it
                # entirely when the breaching book is the last one, and the cycle would exit
                # normally, cancelling nothing.
                return await self._halt_cycle(stats, started)
            # Pace AFTER each market on everything spent so far, including the placements this
            # market just made. Pacing before the read, on a market count, is what produced the
            # 35 req/s burst — see `_pace`.
            await self._pace(stats.requests + self._extra_requests, started)
        stats.requests += self._extra_requests

        # PARTIAL-FREEZE ESCALATION: a cycle that hit the re-read burst
        # cap AND still had stale books left (ws_capped > 0) ran partially blind — over
        # consecutive cycles that is a partial feed freeze, and skipping forever was the gap.
        # Escalate into the SAME bounded-fallback-then-halt path as a connection loss; recovery
        # requires a clean cycle (see the guard above).
        if (self.book_source == "ws" and self.book_feed is not None
                and stats.ws_capped > 0 and self._ws_down_since is None):
            self._ws_down_since = time.time()
            self.ws_feed_deaths += 1
            log.error(f"WS PARTIAL FREEZE: {stats.ws_capped} book(s) still stale past the "
                      f"re-read cap this cycle — escalating to bounded fallback "
                      f"(≤{WS_FALLBACK_WINDOW_S:.0f}s), then halt")
        self._last_ws_capped = stats.ws_capped
        self._last_ws_outage = stats.ws_outage

        finished = time.time()
        stats.wall_s = finished - started
        stats.missed = missed_cycles(stats.wall_s, self.requote_s)
        # ⛔ Measured against NOW, not against this cycle's start. Against `started` a market read
        # during the cycle scores NEGATIVE — measuring against `started` made every live status
        # line report `stale_max=-0.0s`, which cannot detect a market whose reads have
        # stopped, the one thing this number exists for. A market never read at all counts from
        # the cycle start, so it is stale rather than absent from the maximum.
        ages = [finished - self.last_read_ts.get(slug, started) for slug in self.ticks]
        stats.max_staleness_s = max(ages) if ages else None
        warning = rate_budget_warning(stats.requests, self.requote_s, self.max_req_per_s)
        if warning:
            log.warning(warning)
        self._beat(stats)
        self._write_cycle(stats)
        return stats

    async def _read_book_md(self, slug: str,
                            stats: "CycleStats") -> tuple[Optional[dict], float, Optional[str]]:
        """(marketData, read_ms, error_reason) — the ONE book-read seam.

        `rest` (default): the cache-busted `_fetch_book`.
        `ws`: read the injected feed's `get_book_md`; trust it only if its transactTime
        content-age ≤ `WS_BOOK_STALE_S`, else fresh-REST-re-read THAT ONE book (the backstop
        the arb had via its pre-fire REST re-check and a naive WS maker would lose). WS absent /
        no feed / unverifiable freshness all degrade to a fresh REST read — never to a stale or
        empty book. ⛔ `error_reason` non-None means SKIP this market this cycle (a failed REST
        read); `md` is None only alongside an error. `fresh=True` is MANDATORY on every REST
        read here (CDN max-age=30 would stamp our clock onto a ≤30 s-old book).

        ⛔ `stats.requests` is incremented ONLY when a REST read is actually issued — a WS-fresh
        cycle spends NO request, and counting one would make the rate budget blind to the exact
        saving WS exists to produce (and over-pace against a ceiling we are not approaching)."""
        read_start = time.time()
        self.book_reads += 1
        book_src = "rest"
        if self._ws_down_since is not None:
            # FALLBACK STILL TRIES WS FIRST: early-returning to REST here bypasses the WS read
            # entirely, so `ws_capped` is 0 BY CONSTRUCTION and the guard reads that as
            # "recovered" every other cycle — an oscillation that makes the fallback window
            # unexpirable. A fresh WS book costs no request and IS the recovery evidence.
            if self.book_source == "ws" and self.book_feed is not None:
                md = self.book_feed.get_book_md(slug)
                if md is not None:
                    age = transact_age_s(md.get("transactTime"), time.time())
                    if age is not None and age <= WS_BOOK_STALE_S:
                        # "ws_degraded", not "ws": a fill during a fallback
                        # episode must be separable from a healthy-cycle fill on the fill tape
                        # alone (the first post-episode question is whether they perform worse).
                        self.last_book_src[slug] = "ws_degraded"
                        self.ws_reads += 1
                        return md, (time.time() - read_start) * 1000.0, None
            # Still dark → forced REST. The burst cap does NOT apply (the bound here is the
            # REDUCED quote set, and skipping held books during an outage is the harmful
            # direction). "ws_outage", NOT "rest_fallback": the per-book backstop already owns
            # rest_fallback, and sharing one label hides outages from the tape.
            self.last_book_src[slug] = "ws_outage"
            stats.ws_outage += 1
            stats.requests += 1
            try:
                book = await self.client._fetch_book(slug, fresh=True)
            except Exception as exc:
                log.warning(f"book read failed for {slug}: {exc!r}")
                return None, (time.time() - read_start) * 1000.0, "book_read_failed"
            md = book.get("marketData") if isinstance(book, dict) else None
            return md, (time.time() - read_start) * 1000.0, None
        if self.book_source == "ws" and self.book_feed is not None:
            md = self.book_feed.get_book_md(slug)
            if md is not None:
                age = transact_age_s(md.get("transactTime"), time.time())
                if age is not None and age <= WS_BOOK_STALE_S:
                    self.last_book_src[slug] = "ws"
                    self.ws_reads += 1
                    return md, (time.time() - read_start) * 1000.0, None
            # WS book absent/stale/unverifiable → fresh-REST backstop — but CAP the re-reads per
            # cycle: past the cap a partial feed freeze would re-read the whole slate. Over
            # the cap, SKIP this book (no quote on an unverified book), don't burst the poll.
            if stats.ws_stale_rereads >= WS_STALE_REREAD_MAX:
                log.warning(f"WS book stale/absent for {slug} and re-read cap "
                            f"{WS_STALE_REREAD_MAX} hit this cycle — skipping (partial feed "
                            f"freeze? the cycle tape's ws_stale_rereads is the signal)")
                self.last_book_src[slug] = "ws_capped"
                stats.ws_capped += 1
                return None, (time.time() - read_start) * 1000.0, "ws_stale_capped"
            stats.ws_stale_rereads += 1
            book_src = "rest_fallback"
        self.last_book_src[slug] = book_src
        stats.requests += 1
        try:
            book = await self.client._fetch_book(slug, fresh=True)
        except Exception as exc:
            log.warning(f"book read failed for {slug}: {exc!r}")
            return None, (time.time() - read_start) * 1000.0, "book_read_failed"
        md = book.get("marketData") if isinstance(book, dict) else None
        return md, (time.time() - read_start) * 1000.0, None

    async def _quote_market(self, slug: str, stats: CycleStats) -> None:
        tick = self.ticks[slug]
        read_start = time.time()
        # The GAP since this market was last successfully read — i.e. its real sampling interval,
        # which is ~requote_s when healthy and grows every cycle its read fails. Captured BEFORE
        # the read updates the clock; comparing against the post-read stamp would make every row
        # report ~0 and hide a market that is being skipped.
        prev_read = self.last_read_ts.get(slug)
        gap_s = (read_start - prev_read) if prev_read is not None else None
        md, read_ms, err = await self._read_book_md(slug, stats)
        if err is not None:
            await self._skip_market(stats, slug, tick, None, err,
                                    read_ms=read_ms, gap_s=gap_s)
            return
        self.last_read_ts[slug] = time.time()

        bid, ask, _tob = touch_from_md(md)
        # Stamped HERE, at read time — stamping at write time (end of the quote work +
        # placements) would put last_trade_age_s on a different clock from read_ms.
        book_stats = parse_book_stats(md, time.time())
        if bid is None or ask is None:
            await self._skip_market(stats, slug, tick, None, "no_two_sided_touch",
                                    read_ms=read_ms, gap_s=gap_s)
            return
        try:
            my_bid, my_ask, improved, _ = pick_quotes(bid, ask, tick)
        except ValueError:
            await self._skip_market(stats, slug, tick, (bid, ask), "unquotable_touch",
                                    read_ms=read_ms, gap_s=gap_s)
            return

        self.unquotable_streak[slug] = 0
        self.last_touch[slug] = (bid, ask, self.last_read_ts[slug])
        inv = self.inventory.get(slug, _ZERO)
        book_size = self.sizes.get(slug, self.size)
        book_cap = self.caps.get(slug, self.cap)
        pulled = self.book_pulled(slug)
        # ⛔ ONE reduce-only implementation, THREE flags. Commit to a NAMED predicate rather
        # than a longer boolean chain: the next person adds a fourth condition at the first
        # call site and misses the second. Both call sites below use THIS name.
        reduce_only_now = (self.winddown or pulled
                           or self.hot_quote.get(slug) == "reduce_only")
        if reduce_only_now:
            # WIND-DOWN, and its PER-BOOK form, the calendar PULL (`--pull-at slug:epoch`):
            # one book reaches its own deadline — a weather strike's heating
            # window, a game's first pitch — and works its inventory off reduce-only while
            # the rest of the slate keeps quoting normally. Same machinery, same guards:
            # cap-0 is the DOCUMENTED deliberate reduce-only mode (`sides_allowed`'s own
            # docstring) — a flat book quotes nothing, a positioned book quotes only the
            # reducing side, and the add side's resting order is cancelled by the existing
            # disallowed-side machinery. The size clamp is the FLIP GUARD: a full-size reduce
            # order can fill through flat into a NEW position — a short reduced by an
            # oversized bid comes out LONG, which is a fresh position opened by the very
            # machinery that exists to close one. min(size, |inv|) makes the flip
            # unrepresentable rather than unlikely.
            book_cap = 0
            if inv.copy_abs() >= 1:
                book_size = min(book_size, int(inv.copy_abs()))
        per_bid, per_ask = sides_allowed(inv, book_cap)
        if reduce_only_now and _ZERO < inv.copy_abs() < 1:
            # Sub-contract residue: `_reduce_only` would allow a side but the clamp floors its
            # size to 0 — no passive close exists below one contract. Quote NOTHING and let
            # the teardown report it (the "no runnable close" rule, poly_close's discipline).
            per_bid = per_ask = False
        glob_bid, glob_ask = global_sides_allowed(inv, self.gross_exposure,
                                                  self.max_total_contracts)
        # ⛔ AND IN VENUE TRUTH. Belief sizes the order; the venue may only take a side away.
        # When belief reads flat while the venue holds a real short, this is what blocks the
        # ask (the ADDING side of that short) and leaves only the buy-back quoting.
        #
        # ⛔ A PLAIN AND. The obvious objection is that when belief and venue disagree on SIGN
        # the AND pulls BOTH sides, leaving a real position with no working exit. The obvious
        # fix — "keep belief's reducing side" — is WORSE: with belief long and the venue short,
        # belief's "reducer" is an ASK, so permitting it quotes a full-size sell INTO a growing
        # short. That is precisely the order this feature exists to prevent, and the
        # disagreement state persists for a meaningful fraction of a fast run.
        #
        # ⛔ Opposite-sign states DO reach this line (a sign-disagreement HALT was tried and
        # removed — it false-halted every replayed run; see _venue_breach), and here the AND
        # deliberately goes (False, False): belief blocks one side, the venue blocks the other,
        # and the book quotes NOTHING until they reconverge. Measured dwell in that state is
        # about a minute — a short pause, not a parked position. Re-adding an
        # `or _reduce_only(inv)` carve-out was measured flipping allow_ask to True on EVERY
        # observed opposite-sign state, i.e. a full-size ask into a venue short. Do not re-add it.
        ven_bid, ven_ask = self._venue_restriction(slug, book_cap)
        allow_bid = per_bid and glob_bid and ven_bid
        allow_ask = per_ask and glob_ask and ven_ask
        size_by_side = {"bid": book_size, "ask": book_size}
        # ── the unrealized-mark tripwire ─────────────────────────────────────────────────
        # The durable cap is price-REALIZED-only and the adverse cooldown fires only after a
        # COMPLETED round trip — over a long recorded run the cooldown never fired once, while
        # the accumulation phase both are blind to is exactly what inventory headroom widens.
        # The mark is inventory against the LIQUIDATION side of the touch just read (zero
        # extra requests); a breach rides the PROVEN cooldown/reduce-only machinery — the
        # exit keeps working, re-entry is blocked, and while the mark stays breached each
        # cycle re-arms the stand-down.
        if self.mark_trip_per_ct > _ZERO and inv != _ZERO:
            marked = mark_pnl(inv, self.avg_entry.get(slug, _ZERO), bid, ask)
            threshold = self.mark_trip_per_ct * inv.copy_abs()
            breach = marked is not None and marked <= -threshold
            # UNKNOWN BASIS arms too: an inherited position has no avg_entry
            # from this run, mark_pnl returns None, and cannot-verify is not clean — the
            # rail built for accumulating positions must not pass silently on the one
            # position class nothing else watches. Effect: inherited inventory exits
            # (reduce-only) before its book re-enters.
            unknown = marked is None
            if breach or unknown:
                already = time.time() < self.cooldown_until.get(slug, 0.0)
                deadline = time.time() + self.adverse_cooldown_s
                self.cooldown_until[slug] = deadline
                self.trip_until[slug] = deadline
                if not already:
                    self.mark_trips[slug] = self.mark_trips.get(slug, 0) + 1
                    if breach:
                        log.warning(
                            f"🛑 MARK TRIPWIRE {slug}: marked P&L {marked} ≤ −{threshold} "
                            f"({self.mark_trip_per_ct}/contract × |{inv}|; avg "
                            f"{self.avg_entry.get(slug, _ZERO)}, touch {bid}/{ask}) — "
                            f"reduce-only; re-arms while the mark stays breached")
                    else:
                        log.warning(
                            f"🛑 MARK TRIPWIRE {slug}: inventory {inv} has NO BASIS from "
                            f"this run (inherited) — un-markable, arming reduce-only until "
                            f"the position exits; absence is not a basis")
        in_cooldown = time.time() < self.cooldown_until.get(slug, 0.0)
        if in_cooldown:
            # Post-adverse cooldown: only the REDUCING side may quote (flat → neither), so a
            # residual position keeps its exit working while re-entry is blocked. The teardown
            # flatten does not pass through here and is unaffected.
            cool_bid, cool_ask = _reduce_only(inv)
            allow_bid, allow_ask = (allow_bid and cool_bid), (allow_ask and cool_ask)
            # ⛔ The reducer is sized to the INVENTORY, never to --size:
            # a cooling short quoted a full-size bid fills through zero into a LONG — a
            # brand-new position opened DURING the stand-down that exists to prevent exactly that.
            # Same hazard `_place`'s docstring records for the teardown residual. A sub-contract
            # residue truncates to 0 and quotes nothing rather than rounding up into a flip.
            reduce_size = min(book_size, int(inv.copy_abs()))
            if inv > _ZERO:
                size_by_side["ask"] = reduce_size
                allow_ask = allow_ask and reduce_size > 0
            elif inv < _ZERO:
                size_by_side["bid"] = reduce_size
                allow_bid = allow_bid and reduce_size > 0

        desired = {"bid": my_bid if allow_bid else None,
                   "ask": my_ask if allow_ask else None}
        queue = {"bid": qty_at_price(md, "bid", my_bid),
                 "ask": qty_at_price(md, "ask", my_ask)}
        actions: dict[str, str] = {}
        for side in ("bid", "ask"):
            existing = self.resting.get((slug, side))
            action = quote_action(existing.price if existing else None, desired[side])
            if (action == HOLD and existing is not None and desired[side] is not None
                    and existing.size != size_by_side[side]):
                # Same price but the wrong size: a full-size reducer resting from before the
                # cooldown must shrink to the inventory. Forfeits queue position — the one
                # place correctness outranks the queue rule.
                action = REPLACE
            actions[side] = action
            stats.actions[action] = stats.actions.get(action, 0) + 1
            await self._apply_action(action, slug, side, desired[side], queue[side], improved,
                                     size=size_by_side[side])

        stats.markets_quoted += 1
        # "cooldown" in the status column: allow=(N,N) alone is
        # ambiguous — the global gross cap at its limit produces the same flags — and any
        # pass/fail criterion or fills-per-hour de-confounding needs
        # cooldown-minutes to be readable straight off the tape.
        # "mark_trip" outranks "cooldown": the tripwire arms the same reduce-only
        # clock a mechanism experiment would measure, so its cycles must be excludable
        # straight off the tape — a safety rail that silently moves the outcome variable
        # confounds the experiment it runs inside.
        _status = ("mark_trip" if (in_cooldown and time.time() < self.trip_until.get(slug, 0.0))
                   else ("cooldown" if in_cooldown else "ok"))
        self._write_quote(stats, slug, tick, (bid, ask), (my_bid, my_ask), improved=improved,
                          actions=actions, queue=queue, inventory=inv,
                          allow=(allow_bid, allow_ask), read_ms=read_ms, gap_s=gap_s,
                          status=_status, book_stats=book_stats)

    async def _skip_market(self, stats: CycleStats, slug: str, tick: Decimal,
                           touch: Optional[tuple[Decimal, Decimal]], status: str, *,
                           read_ms: Optional[float], gap_s: Optional[float]) -> None:
        """One market could not be quoted this cycle: tape it, and decide whether to pull its
        resting quotes.

        The streak, not the single failure, is the signal — see `stale_cancel_cycles`. Every skip
        reason counts toward it, not just a failed HTTP read: a market that has gone one-sided or
        crossed for 30s is exactly as unreadable, from a quoting point of view, as one that is
        timing out.
        """
        stats.markets_skipped += 1
        # Tape the REAL recorded inventory, not the signature default of 0: a skipped FIRST
        # cycle on a carried book writes inventory=0, and any downstream carry-baseline reads
        # that as "started flat" — either accepting a flat-start fiction or refusing a correct
        # carry declaration. Reports may also exclude skip rows by status, but that is a
        # second line of defence; this is what makes the rows themselves trustworthy.
        self._write_quote(stats, slug, tick, touch, None, status=status,
                          inventory=self.inventory.get(slug, _ZERO),
                          read_ms=read_ms, gap_s=gap_s)
        streak = self.unquotable_streak.get(slug, 0) + 1
        self.unquotable_streak[slug] = streak
        if streak < self.stale_cancel_cycles:
            return
        pulled = [side for side in ("bid", "ask") if (slug, side) in self.resting]
        if not pulled:
            return
        log.warning(f"{slug}: un-quotable for {streak} consecutive cycles ({status}) — PULLING "
                    f"{len(pulled)} resting quote(s). A resting order on a market we can no "
                    f"longer read is exposure nothing is checking.")
        for side in pulled:
            await self._cancel(slug, side)
            stats.actions[CANCEL] = stats.actions.get(CANCEL, 0) + 1

    async def _apply_action(self, action: str, slug: str, side: str,
                            price: Optional[Decimal], queue_ahead: Optional[Decimal],
                            improved: bool, size: Optional[int] = None) -> None:
        if action == HOLD:
            return                                   # ⛔ the queue position we are protecting
        if action in (CANCEL, REPLACE):
            cancelled = await self._cancel(slug, side)
            # ⛔ NO PLACE OVER AN UNCONFIRMED CANCEL. `_cancel`'s own docstring says cannot-verify
            # is not cancelled — and this call site used to discard its return and place anyway,
            # which overwrote `self.resting` and made the maybe-live old order INVISIBLE: two
            # orders resting where belief says one, the cap checked against a wrong number, the
            # flatten sized to half the real position. Refusing costs one requote cycle (the next
            # cycle retries the cancel; the teardown sweep is the last resort) — and a run
            # repricing all day gets many chances for one 502 to breach the bound.
            if not cancelled and action == REPLACE:
                self.replace_blocked += 1
                return
            if self.should_stop:
                # ⛔ THE CANCEL'S OWN READ-BACK CAN HALT US. `_cancel` → `_reconcile_order` →
                # `_book_fill` books the fill that breaches the loss cap, and without this the
                # very next statement re-places the quote we just cancelled — new exposure taken
                # after the budget is known to be gone, on the book that spent it.
                return
        if action in (PLACE, REPLACE) and price is not None:
            if self.shadow:
                # Shadow keeps a VIRTUAL resting book so the requote decision is genuinely
                # exercised and taped — that is the whole point of a shadow run — without ever reaching
                # `_place`, which raises.
                self.resting[(slug, side)] = RestingOrder(
                    slug=slug, side=side, price=price,
                    size=self.sizes.get(slug, self.size) if size is None else size, order_id=None,
                    intent_id="shadow", placed_ts=time.time(), queue_ahead=queue_ahead,
                    improved=improved)
                return
            await self._place(slug, side, price, improved=improved, queue_ahead=queue_ahead,
                              size=size)

    async def _place(self, slug: str, side: str, price: Decimal, *,
                     improved: bool, queue_ahead: Optional[Decimal],
                     size: Optional[int] = None) -> Optional[RestingOrder]:
        """Send ONE resting quote. `post_only=True` always.

        ⛔ The durable intent is written BEFORE the order is sent. The window that cannot be covered
        any other way is "sent, then died before the response arrived"; recording after the venue
        answers leaves exactly that window with no trace.

        `size` defaults to the quote size but the TEARDOWN passes the residual: sizing a reducing
        order at `--size` overshoots whenever the residual is smaller, so a 3-contract long
        flattened with a 5-lot ask ends up 2 SHORT — a teardown opening a position in the opposite
        direction to the one it was clearing.
        """
        if self.shadow:
            raise ShadowViolation(
                f"shadow run reached _place({slug}, {side}, {price}) — a shadow run places "
                f"NOTHING, and that has to be structural rather than incidental.")
        count = self.sizes.get(slug, self.size) if size is None else size
        self._seq += 1
        intent_id = f"{slug}:{side}:{self._seq}"
        if self.state is not None and self.real:
            self.state.record_intent(intent_id, ticker=slug, side=side, price=price,
                                     count=count)
        try:
            # TWO requests, not one: `place_limit_gtc` fetches the book itself for its crossing
            # guard before it sends. Counting only our own reads understates the live rate ~5x.
            self._extra_requests += 2
            resp = await self.client.place_limit_gtc(
                slug, price, count, label=f"polymm {side}",
                post_only=True, side=("buy" if side == "bid" else "sell"))
        except PreSendRefusal as exc:
            # The client refused BEFORE any venue call — nothing was placed, KNOWN. The durable
            # intent is therefore a phantom: left in place it reads as `maybe_live_orders` and
            # blocks a sibling lane's start on an order that never existed.
            # ONLY this type clears — a generic exception may have a resting order behind it.
            # And no requests happened: un-charge the 2 pre-charged above.
            self._extra_requests -= 2
            log.error(f"place refused {slug} {side} @ {price}: {safe_exc(exc)}")
            if self.state is not None and self.real:
                self.state.clear_order(intent_id)
            return None
        except Exception as exc:
            # An error is NOT an outcome — we do not know whether the order rested. The intent
            # stays in the durable record so the teardown sweep and `maker_recover` both see it.
            log.error(f"place failed {slug} {side} @ {price}: {safe_exc(exc)}")
            return None
        if resp is None:
            # The client's crossing guard refused, or the touch was unreadable. Nothing rested.
            if self.state is not None and self.real:
                self.state.clear_order(intent_id)
            return None
        order_id = venue_order_id(resp)
        if order_id is None and not self.client_is_dry:
            # LOUD. A silent None here is what disabled fills, cancels and both caps at once, so
            # it must never look like an ordinary placement. The order is still recorded (with a
            # None id) so the teardown sweep and `maker_recover` can find it on the venue.
            log.error(f"⚠️ {slug} {side} @ {price}: the venue's create response carried NO `id` "
                      f"(keys={sorted(_order_body(resp))}). The order MAY BE RESTING and cannot "
                      f"be cancelled or polled by id — the teardown sweep is the only backstop.")
        if self.state is not None and self.real:
            self.state.record_placed(intent_id, order_id)
        if self.order_feed is not None and order_id is not None and self.real:
            # The echo watchdog's input: a real placement must echo on the private WS within
            # its deadline. Real only — a dry client's orders never reach the venue, so
            # expecting an echo would declare every dry --order-ws run dead within a cycle.
            self.order_feed.expect_echo(order_id)
        order = RestingOrder(slug=slug, side=side, price=price, size=count,
                             order_id=order_id, intent_id=intent_id,
                             placed_ts=time.time(), queue_ahead=queue_ahead, improved=improved)
        self.resting[(slug, side)] = order
        return order

    async def _cancel(self, slug: str, side: str) -> bool:
        """Cancel one resting quote. True if the venue confirmed it.

        ⛔ THE ORDER IS FORGOTTEN ONLY ON A CONFIRMED CANCEL. `client.cancel_order` returns False
        on failure and does NOT raise, so popping first and discarding the result turns a failed
        cancel into an invisible live order — and clearing the durable record at the same time
        deletes the one thing an offline recovery tool has to read. Cannot-verify is not
        cancelled.

        ⛔ AND THE ORDER'S FINAL FILL IS RECONCILED FIRST. An order can fill in the requote window
        between two polls; dropping it at cancel time without a last read loses those contracts
        permanently — the position is real, our inventory never sees it, and the cap is then
        computed against a number that is simply wrong. One extra read per cancel is cheap:
        the whole point of the hold-on-equal-price rule is that cancels are a small minority
        of quote actions.
        """
        order = self.resting.get((slug, side))
        if order is None or self.shadow:
            self.resting.pop((slug, side), None)
            return True
        if order.order_id is None:
            # Never had an id (a create whose response carried none). Nothing to cancel by id; the
            # teardown sweep is the backstop. Keep the durable intent so recovery still sees it.
            log.warning(f"{slug} {side}: no venue id — cannot cancel by id, leaving it for the "
                        f"teardown sweep.")
            self.resting.pop((slug, side), None)
            return False
        readable = await self._reconcile_order(order)
        self._extra_requests += 1
        ok = await self.client.cancel_order(order.order_id, slug)
        if ok:
            # ⛔ EVERY confirmed cancel parks for a delayed verify — not just the blind ones. The
            # immediate read above can ANSWER with a stale cumQuantity and still miss a fill
            # (a whole lot has been lost through exactly that, with zero not-found warnings), so
            # a presence test on the read cannot decide safety. One extra read at
            # t+LAG_HORIZON_S per cancel is the whole cost.
            self._park(order)
            # No expect_echo here: the watchdog is PLACEMENT-driven. Cancel echoes DO appear on
            # the tape, but no per-order cancel timestamps exist, so neither their rate nor
            # their latency can be computed — and an unvalidated expectation is a false-death
            # storm waiting to happen.
            if not readable:
                log.warning(f"{slug} {side}: cancelled {order.order_id} while its read was BLIND — "
                            f"delayed verify will book anything it missed.")
        if not ok:
            log.error(f"⚠️ {slug} {side}: cancel of {order.order_id} was REFUSED by the venue. "
                      f"The order is treated as STILL RESTING — it stays in our book and its "
                      f"durable record is kept, so the next cycle retries and the sweep catches "
                      f"it. Assuming it died is how a live order becomes invisible.")
            return False
        self.resting.pop((slug, side), None)
        # No clear_order here — the durable record retires at the parked VERIFY, same as the
        # poll path. Clearing at cancel-confirm erased the record 120s before the verify could
        # book a stale-read fill. An order that never got a venue id has no parked entry, so
        # its intent record is cleared on the no-id path instead.
        return True

    def _drain_order_feed(self) -> None:
        """Book every order body the WS feed has delivered since last cycle. Zero requests.

        Runs BEFORE poll_fills so the poll's later read-back of the same order sees its cum
        already counted (delta 0) — WS accelerates, REST verifies, and cum-idempotency makes
        double-booking structurally impossible. An unknown order id is NOT booked (we cannot
        attribute a side/slug we never placed); the teardown sweep remains the net for those.
        Built after a real run missed a full-size fill through the REST poll alone — a whole
        cap's worth of invisible exposure, from ONE dropped read.
        """
        if self.order_feed is None:
            return
        drained = 0
        booked_any = False
        while True:
            try:
                body = self.order_feed.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            drained += 1
            oid = str(body.get("id"))
            ro = None
            for candidate in self.resting.values():
                if candidate.order_id == oid:
                    ro = candidate
                    break
            if ro is None and oid in self.pending_reconcile:
                ro = self.pending_reconcile[oid][0]
            if ro is None and oid in self.pending_activity_recovery:
                # A recovery-queued order is still OURS — a pushed body for it books
                # normally and must never count as unattributed.
                ro = self.pending_activity_recovery[oid].order
            if ro is None:
                # Not ours to book (we cannot attribute a side/slug we never placed — usually a
                # different process on the same account). Counted, never silent.
                self.order_feed_unattributed += 1
                continue
            if self._book_fill(ro, _order_body({"order": body}), booked_via="ws") is not None:
                booked_any = True
        # ⛔ The DURABLE record must sync HERE, on the booking path itself: the poll's sync is
        # gated on its OWN fills list, and a ws-booked fill leaves that poll a zero delta — so
        # without this line NO path ever writes the record, and the state file keeps carrying
        # the PRIOR run's inventory. That is a crash record naming a position we do not hold.
        # Once per drain, not per event — same gate as the other two sites.
        if booked_any and self.state is not None and self.real:
            self.state.set_inventory(self.inventory)
        if drained:
            log.info(f"order feed: drained {drained} event(s)")
        # The echo watchdog rides the same per-cycle seam as the drain. Compute liveness and
        # never READ it in-run and a mid-run feed death surfaces only at teardown, hours late.
        # On death the feed has already torn itself down for its reconnect
        # ladder; booking degrades to the REST poll, which reads everything regardless.
        reason = self.order_feed.check_liveness()
        if reason:
            log.warning(f"⚠️ order feed DEAD ({reason}) — booking degrades to the REST poll "
                        f"until the reconnect ladder restores the subscription")

    async def _reconcile_order(self, order: RestingOrder) -> bool:
        """Read one order's final state and book any fill we have not already counted.
        Returns True if the read was USABLE (a cumQuantity was present — fill or no fill),
        False if the order could not be read.

        ⛔ The previous version returned None and claimed "silent on a read failure — the order
        stays in our book, which is the safe direction." That claim was FALSE at its only call
        site: `_cancel` popped the order after a confirmed cancel regardless, so an order that
        filled inside the venue's create-lag blind window vanished with its fill unbooked.
        The caller now parks unreadable orders in `pending_reconcile` instead of letting them
        vanish.
        """
        if order.order_id is None:
            return False
        self._extra_requests += 1
        body = _order_body(await self.client.get_order(order.order_id))
        self._book_fill(order, body)
        return _amount(body.get("cumQuantity")) is not None

    def _park(self, order: RestingOrder, origin: str = "cancel") -> None:
        """Park a cancelled/terminal order for its delayed verify. Bounded: past the scaled
        bound the NEWEST entry is EVICTED into the activities-recovery queue rather than
        dropped (the ledger outlives the order store, so an eviction is a routing decision,
        not a loss)."""
        if order.order_id is None:
            return
        now = time.time()
        # ⛔ The bound SCALES WITH THE RUN and eviction removes the NEWEST entry. A FIXED bound
        # starts binding at only a couple of fully-repricing markets (an entry lives
        # ~horizon/requote cycles, so steady state ≈ cancels/cycle × that); and
        # evicting by min(due_ts) — the obvious choice — removes the entry CLOSEST to
        # verifiable, i.e. the highest-information one. Evicting the NEWEST keeps every older
        # entry marching toward its verify; the victim keeps its shot at a verdict through the
        # recovery queue, carrying ITS OWN terminal_ts and origin path.
        if len(self.pending_reconcile) >= self.max_parked:
            victim = max(self.pending_reconcile, key=lambda k: self.pending_reconcile[k][1])
            v_order, _due, _att, v_terminal, v_origin = self.pending_reconcile[victim]
            log.error(f"⚠️ parked set at bound {self.max_parked} — EVICTING NEWEST {victim} "
                      f"into activities recovery (older entries are closer to their verify "
                      f"and keep their place).")
            del self.pending_reconcile[victim]
            self._queue_recovery(v_order, v_terminal, f"park_evicted_{v_origin}")
        self.pending_reconcile[order.order_id] = (order, now + LAG_HORIZON_S, 0, now, origin)

    async def _retry_pending_reconciles(self, *, ignore_due: bool = False) -> None:
        """Run the DELAYED VERIFY on parked orders whose horizon has passed.

        Every cancelled order gets exactly one verify read after LAG_HORIZON_S — regardless of
        what its immediate pre-cancel read said, because that read can ANSWER with a stale
        cumQuantity and still be wrong — a whole lot has been lost that way, with zero
        not-found warnings. cumQuantity is cumulative, so the verify books the exact missed delta or
        nothing. Unreadable at verify time → retry in VERIFY_RETRY_S, up to VERIFY_MAX_ATTEMPTS,
        then a LOUD drop. A parked order is already cancelled, so this can never double-place —
        it only repairs the number every cap and flatten reads. Reads are charged to
        `_extra_requests` BEFORE the cycle's stats reset consumes them, so they show up in the
        rate budget instead of leaking."""
        if self.shadow or not self.pending_reconcile:
            return
        now = time.time()
        for oid, (order, due, attempts, terminal_ts, origin) in list(
                self.pending_reconcile.items()):
            if now < due and not ignore_due:
                continue
            self._extra_requests += 1
            body = _order_body(await self.client.get_order(oid))
            if _amount(body.get("cumQuantity")) is None:
                if attempts + 1 >= VERIFY_MAX_ATTEMPTS:
                    # ⛔ Not a drop: the order-store record may be
                    # purged while the TRADE ledger still carries its executions — queue for
                    # the activities walk with the terminal moment this entry was parked at.
                    log.error(f"⚠️ {order.slug} {order.side}: order {oid} UNREADABLE after "
                              f"{VERIFY_MAX_ATTEMPTS} verify attempts — queueing for "
                              f"activities recovery (the trade ledger outlives the order "
                              f"store).")
                    del self.pending_reconcile[oid]
                    self._queue_recovery(order, terminal_ts, "verify_exhausted")
                else:
                    self.pending_reconcile[oid] = (order, now + VERIFY_RETRY_S, attempts + 1,
                                                   terminal_ts, origin)
                continue
            fill = self._book_fill(order, body, late_booked=True, booked_via="verify")
            if fill is not None:
                if self.state is not None and self.real:
                    self.state.set_inventory(self.inventory)
                log.warning(f"{order.slug} {order.side}: delayed verify booked {fill.filled_qty} "
                            f"on {oid} — a fill the immediate read missed.")
            # ⛔ A READABLE answer RETIRES the entry only past the horizon. `ignore_due` schedules
            # the READ early (teardown cannot wait 120s), but retiring on an early readable answer
            # reinstates "readable = safe" — the rule this design abolishes: a stale cum=0 at
            # teardown answers, books nothing, retires — and the run prints several confident
            # FLAT lines over a real position. Booking early is always safe (cum
            # is monotone, deltas are real); retiring early is the bug. An entry the horizon has
            # not passed stays parked and is NAMED in the teardown give-up line; the venue-verify
            # remains the final word.
            if now >= due:
                del self.pending_reconcile[oid]
                # The durable order record retires HERE, with the verify — not at the pop.
                # Clearing at the pop (the pre-fix shape, cancel path) erased the record before
                # the verify ran; never clearing (the first park-fix, poll path) accumulated
                # verified-dead records until a crash-restart refused over them via
                # PriorRunUnresolved.
                if self.state is not None and self.real:
                    self.state.clear_order(oid)

    # ── fills ────────────────────────────────────────────────────────────────────────────────

    async def poll_fills(self) -> list[Fill]:
        """Detect fills and read the COMMISSION back for each one.

        One `get_open_orders()` for the whole book, then a `get_order` read-back per resting
        order. The listing is a HINT for retirement, never the authority: it carries no
        pagination token (probed live — the envelope is `{orders}` alone), so a
        silent server-side cap cannot be ruled out, and the pop decision below therefore keys
        on the per-order read's STATE, which cannot be truncated.

        `cumQuantity` is CUMULATIVE, so the increment is `cum - already_seen`. Adding the
        cumulative figure on every poll would double the position and silently mis-state every
        number built on it.
        """
        # The WS accelerator drains FIRST, here rather than in run_cycle, so every caller —
        # the cycle loop AND each teardown phase — books pushed fills before its own reads.
        self._drain_order_feed()
        if self.shadow or not self.resting:
            # An empty book cannot accumulate not_founds, so a tripped breaker here can only
            # be STALE — and leaving it latched walls the recovery walk off forever (every
            # queued entry ages out UNRESOLVED during a fault that is over). Clearing is the
            # safe direction: a real ongoing outage just fails the walk's own reads, which
            # never stamp coverage.
            self._nf_breaker_tripped = False
            return []
        try:
            self._extra_requests += 1
            # Slate-scoped at the source: the listing is a retirement HINT and a
            # truncation on an account-wide read is invisible; bounding it to our own slugs
            # shrinks that surface (the state authority still decides the pop either way).
            open_orders = await self.client.get_open_orders(slugs=self.slugs)
        except Exception as exc:
            # Cannot-verify is not "nothing filled" — skip this poll rather than invent a state.
            log.warning(f"open-orders read failed ({exc!r}) — fills UNVERIFIED this poll")
            return []
        still_open = {oid for oid in (venue_order_id(o) for o in open_orders) if oid}

        fills: list[Fill] = []
        nf_this_poll: set[str] = set()
        probe_candidates: list[RestingOrder] = []
        for key, order in list(self.resting.items()):
            if order.order_id is None:
                continue
            self._extra_requests += 1
            raw, verdict = await self.client.get_order_ex(order.order_id)
            body = _order_body(raw)
            # The not_found STREAK: reset only by an `ok` read — `error` is transport
            # noise and neither advances nor clears the evidence. The streak alone never
            # retires anything (store lag on this venue has run to HOURS); it only makes the
            # order a cancel-probe CANDIDATE, and the probe's own structured answer decides.
            if verdict == "ok":
                self.not_found_streak.pop(order.order_id, None)
            elif verdict == "not_found":
                nf_this_poll.add(order.order_id)
                self.not_found_streak[order.order_id] = \
                    self.not_found_streak.get(order.order_id, 0) + 1
            fill = self._book_fill(order, body)
            if fill is not None:
                fills.append(fill)
            state = str(body.get("state") or "")
            missing_from_listing = order.order_id not in still_open
            if (verdict == "not_found"
                    and self.not_found_streak.get(order.order_id, 0) >= 2
                    and time.time() - order.placed_ts > LAG_HORIZON_S
                    and missing_from_listing):
                # Streak ≥2 AND past the create-lag horizon AND absent from the poll's OWN
                # listing (a different endpoint, free, NECESSARY — presence contradicts a
                # purge). Still only a candidate: the probe below is the decider.
                probe_candidates.append(order)
            if missing_from_listing and state in _TERMINAL_ORDER_STATES:
                # ⛔ PARK BEFORE POPPING — the read above can be served STALE (cum 0 on a filled
                # order), and a pop without a parked verify loses the fill FOREVER: nothing else
                # ever re-reads a popped order. The cancel path parks for exactly this reason;
                # this path was once left out, and it is where fills actually go — dropped
                # buys here drove belief to the OPPOSITE SIGN of the real venue position.
                # The parked verify is cum-idempotent, so a fresh read that finds nothing new
                # books nothing. clear_order moves to the verify's retirement, not here.
                # origin="poll" so an eviction of this entry carries the right recovery path
                # (terminal_ts = this read's time).
                self._park(order, origin="poll")
                self.resting.pop(key, None)
            elif missing_from_listing and state:
                # A LIVE or unrecognised state contradicting the listing (NEW and
                # PARTIALLY_FILLED still rest; the PENDING_* family can still fill; REPLACED
                # would mean someone else acted on this account). Keep tracking — wrongly
                # keeping costs a log line and a retried cancel; wrongly popping leaves a live
                # order resting on the venue that no code path ever cancels. The old condition
                # (`"OPEN" not in state`) popped every one of these: the venue emits NO state
                # containing "OPEN" at all — a guess about the vocabulary, never verified.
                log.warning(f"{order.slug} {order.side}: {order.order_id} absent from the "
                            f"open-orders listing but state={state} — keeping it (listing "
                            f"stale or truncated; the state is the authority)")
            elif missing_from_listing:
                # Neither authority answered: absent from the listing AND the read-back
                # carried NO state. Keep (cannot-verify is not terminal) but say so — under a
                # get_order outage every resting order lands here at once, and a blind poll
                # must not look like a quiet one.
                log.warning(f"{order.slug} {order.side}: {order.order_id} absent from the "
                            f"open-orders listing and its read-back carried NO state — kept; "
                            f"cannot-verify is not terminal")
        # ── the breaker, then the cancel-probes ──────────────────────────────────────────
        self._nf_breaker_tripped = len(nf_this_poll) > NOT_FOUND_BREAKER
        if self._nf_breaker_tripped:
            log.error(f"⛔ {len(nf_this_poll)} distinct orders answered not_found in ONE poll "
                      f"(> {NOT_FOUND_BREAKER}) — this is a client/route fault, not a purge. "
                      f"Probes and retirements REFUSED; the recovery queue holds (same "
                      f"client, same fault). SDK base URL is the first suspect.")
        elif probe_candidates:
            await self._run_cancel_probes(probe_candidates)
        # Streak hygiene: entries for orders no longer resting are
        # dead weight (popped through the park/probe paths) — prune, don't leak.
        alive = {o.order_id for o in self.resting.values() if o.order_id}
        self.not_found_streak = {k: v for k, v in self.not_found_streak.items()
                                 if k in alive}
        self._probe_confirmed_gone &= alive
        if self.state is not None and self.real and fills:
            self.state.set_inventory(self.inventory)
        return fills

    async def _run_cancel_probes(self, candidates: list[RestingOrder]) -> None:
        """ONE deliberate cancel per zombie candidate, oldest-first, ≤PROBE_MAX_PER_CYCLE.
        The probe's structured not_found is the only path that retires a resting
        order the STORE disowns; the `ok` branch below also pops, but through the normal
        cancelled machinery (the venue just accepted a real cancel). A false retirement
        places a second untracked order over a live one, which is
        why the chain is this long — streak ≥2 + age gate + listing-absence got the order
        HERE, and the venue's own cancel answer decides — and why retirement additionally
        gates on `probe_retirement` until the evidence exists. Budget-charged; verdict
        distribution reported per run. ⚠️ In DRY the client short-circuits every
        cancel to `ok`, so the distribution is meaningless there."""
        # ⛔ Confirmed-gone orders are excluded BEFORE the budget slice:
        # they are the OLDEST candidates, so slicing first lets a couple of them occupy every
        # slot forever under the default retirement-off gate — capping the evidence
        # distribution at a handful of samples no matter how many purges occurred.
        undecided = [o for o in candidates
                     if o.order_id and o.order_id not in self._probe_confirmed_gone]
        for order in sorted(undecided, key=lambda o: o.placed_ts)[:PROBE_MAX_PER_CYCLE]:
            oid = order.order_id
            if oid is None:
                continue
            self._extra_requests += 1
            ok, verdict = await self.client.cancel_order_ex(oid, order.slug)
            self.recovery_probe_verdicts[verdict] = \
                self.recovery_probe_verdicts.get(verdict, 0) + 1
            if verdict == "not_found" and not self.probe_retirement:
                # Retirement is UNPROVEN: the three corroborating reads
                # share one (demonstrably laggy) store, and a false retirement re-quotes
                # over a live order. Keep it resting — the mute side stays mute, which
                # costs fills. ⛔ But RECOVERY is separable from retirement:
                # the activities walk is read-only and places nothing, and skipping
                # it leaves the worst case — a PURGED order that had already FILLED —
                # unbooked under the safe-looking default, with the teardown sweep then
                # certifying clean over it, because a purged order is absent from the very
                # listing the sweep trusts. Queue it; the walk books what the ledger
                # proves, and an unresolved entry keeps the crash record honest.
                self._probe_confirmed_gone.add(oid)
                self._queue_recovery(order, time.time(), "cancel_probe")
                log.error(f"⛔ {order.slug} {order.side}: cancel-probe answered structured "
                          f"not_found for {oid} but retirement is DISABLED "
                          f"(probe_retirement=False, the evidence gate). Order kept "
                          f"(this side will not re-quote); activities recovery QUEUED — "
                          f"any fill it carries books from the ledger. Review the "
                          f"captured 404 body and relaunch with --enable-probe-retirement "
                          f"to act on the retirement half.")
            elif verdict == "not_found":
                # Terminal: the order store disowns it twice over (reads AND the cancel).
                # Retire so the mute side can speak again (quote_action(resting=None) PLACEs
                # next cycle) and queue for the activities walk.
                self.resting.pop((order.slug, order.side), None)
                self.not_found_streak.pop(oid, None)
                log.warning(f"{order.slug} {order.side}: cancel-probe confirmed {oid} GONE "
                            f"from the order store — retired; activities recovery will "
                            f"answer whether it filled first.")
                self._queue_recovery(order, time.time(), "cancel_probe")
            elif ok:
                # WAS-LIVE (store lag served the reads a stale miss): the venue just accepted
                # a real cancel → the normal cancelled path, delayed verify included. ⚠️ If
                # the venue cancels idempotently this verdict is uninformative — an
                # instrumentation read settles that; until then the distribution
                # above is the record.
                self.resting.pop((order.slug, order.side), None)
                self.not_found_streak.pop(oid, None)
                log.warning(f"{order.slug} {order.side}: cancel-probe found {oid} WAS-LIVE "
                            f"(store lag) — cancelled through the normal path.")
                self._park(order)
            # error → keep everything; the next poll retries the whole chain.

    # ── belief recovery: the activities walk ────────────────────────────────────────────────

    def _queue_recovery(self, order: RestingOrder, terminal_ts: float, path: str) -> None:
        """Enter `order` into the activities-recovery queue. Bounded by `self.max_parked`;
        past the bound the NEWEST entry is routed to the named UNRESOLVED list (the oldest is
        closest to a verdict and evicting it wastes the most walk progress) — never silent."""
        oid = order.order_id
        if oid is None or oid in self.pending_activity_recovery:
            return
        if len(self.pending_activity_recovery) >= self.max_parked:
            # REFUSE the incoming entry — it is by construction the newest and has zero
            # walk progress, so evicting an existing entry to admit it (the earlier shape)
            # would contradict the stated policy that walk progress is the thing worth
            # keeping. Named UNRESOLVED either way, never silent.
            log.error(f"⚠️ recovery queue at bound {self.max_parked} — REFUSING incoming "
                      f"{oid} (existing entries keep their walk progress).")
            self.unresolved_recovery[oid] = "refused: recovery queue at bound"
            return
        self.pending_activity_recovery[oid] = RecoveryEntry(
            order=order, terminal_ts=terminal_ts, path=path, queued_ts=time.time())

    def _dequeue_recovery(self, oid: str) -> None:
        """A CLEAN exit (NO-FILL confirmed, or nothing left to learn): the durable order
        record retires here, exactly as the delayed verify's retirement does.

        ⛔ UNLESS the order is still in `self.resting`: the
        retirement-off probe path queues recovery while deliberately KEEPING the order
        tracked as possibly-live (the whole premise of the gate is "we do not believe the
        store"). A NO-FILL verdict answers "did it fill BEFORE terminal" — it says nothing
        about the order being dead, and clearing its durable record here would leave a
        SIGKILL with a live venue order and `maybe_live_orders == 0`: the invisible crash
        `begin_run`'s own docstring calls strictly worse than the crash."""
        entry = self.pending_activity_recovery.pop(oid, None)
        self._recovery_execs.pop(oid, None)
        still_resting = entry is not None and any(
            o.order_id == oid for o in self.resting.values())
        if self.state is not None and self.real and not still_resting:
            self.state.clear_order(oid)
        self._reset_walk_if_idle()

    def _reset_walk_if_idle(self) -> None:
        """An emptied queue resets the traversal — stale coverage state must not greet the
        next entry, and the exec store must not outlive its readers."""
        if not self.pending_activity_recovery:
            self._walk_started_ts = 0.0
            self._walk_oldest_ts = None
            self._walk_exhausted = False
            self._recovery_cursor = ""
            self._recovery_execs.clear()

    def _resolve_unresolved(self, oid: str, reason: str) -> None:
        """A TERMINAL failure (eviction / price refusal / over-statement / coverage timeout):
        named for the teardown message. ⛔ The durable order record is deliberately NOT
        cleared — an unresolved entry may carry an unbooked fill, and the crash record
        staying open over it is the design, not an oversight."""
        entry = self.pending_activity_recovery.pop(oid, None)
        self._recovery_execs.pop(oid, None)
        self.unresolved_recovery[oid] = reason
        detail = f" ({entry.path}, terminal {entry.terminal_ts:.0f})" if entry else ""
        log.error(f"⚠️ activities recovery UNRESOLVED for {oid}{detail}: {reason}")
        self._reset_walk_if_idle()

    @staticmethod
    def _coverage_start_required(entry: RecoveryEntry, *, for_verdict: bool) -> float:
        """The earliest traversal START whose reads this entry may credit.

        ⛔ `queued_ts`, not `terminal_ts`: the merge is
        queued-ids-only, so a traversal started before the entry JOINED filtered its rows
        out of every page it read — crediting it is coverage over deleted evidence, and it
        is the NORMAL shape, since an entry typically joins minutes after its terminal.
        queued_ts ≥ terminal_ts on every entry path, so this gate subsumes the terminal one.

        For a NEGATIVE verdict (NO-FILL / OVER-STATEMENT) the start must ALSO clear
        `terminal_ts + LAG_HORIZON_S`: the activities store can publish
        an execution late, and deep pages read before it existed cover its createTime
        window without containing it. FOUND booking keeps the looser gate — positive
        evidence is monotone-safe, and a too-early traversal can only understate what a
        later one re-books."""
        req = entry.queued_ts
        if for_verdict:
            req = max(req, entry.terminal_ts + LAG_HORIZON_S)
        return req

    def _oldest_covered(self, entry: RecoveryEntry, *, for_verdict: bool = False) -> bool:
        """Coverage for THIS entry: the current traversal must
        have STARTED at or after `_coverage_start_required` (a cursor walking backwards
        from a later start passes all older rows), and reached the entry's placement minus
        the skew margin — or the ledger's true end."""
        if self._walk_started_ts < self._coverage_start_required(
                entry, for_verdict=for_verdict):
            return False
        return self._walk_exhausted or (
            self._walk_oldest_ts is not None
            and self._walk_oldest_ts <= entry.order.placed_ts - COVERAGE_SKEW_S)

    def _merge_activities(self, acts: list[dict]) -> None:
        """Fold one page's executions into the per-order store — QUEUED ids only (a late
        joiner forces its own fresh traversal, so keeping executions for every resting order
        would only leak memory), matched by our-id presence on EITHER leg, deduped by
        execution id. ⛔ NOT by isAggressor: every recorded trade carries that flag
        present-but-False, so its True behaviour is entirely unobserved — and a self-trade
        carries our id on BOTH legs anyway.

        Quantity is the LEG's `lastShares` (the number that belongs to OUR order), falling
        back to the trade's `qtyDecimal`, which agreed with it on every recorded leg. ⛔ NEVER
        `trade.qty` — it is ROUNDED to the nearest integer, and it disagrees with the exact
        field often enough to matter. (Booking itself rides `cumQuantity`; the leg quantity
        feeds the partial-sight detector, where a rounded value fakes or hides partial sight.)"""
        queued = set(self.pending_activity_recovery)
        for a in acts:
            trade = a.get("trade") if isinstance(a, dict) else None
            if not isinstance(trade, dict):
                continue
            ts = iso_to_ts(trade.get("createTime"))
            for leg_key in ("aggressorExecution", "passiveExecution"):
                ex = trade.get(leg_key)
                if not (isinstance(ex, dict) and isinstance(ex.get("order"), dict)):
                    continue
                order = ex["order"]
                oid = str(order.get("id") or "")
                if oid not in queued:
                    continue
                exec_id = str(ex.get("id") or f"{trade.get('id')}:{leg_key}")
                qty = _amount(ex.get("lastShares"))
                if qty is None:
                    qty = _amount(trade.get("qtyDecimal"))
                self._recovery_execs.setdefault(oid, {})[exec_id] = {
                    "ts": ts,
                    "qty": qty,
                    "price": _amount(trade.get("price")),   # yes-space on EVERY leg
                    "avg_px": _amount(order.get("avgPx")),  # the VENUE's own, on every leg
                    "cum": _amount(order.get("cumQuantity")),
                    "comm_total": _amount(order.get("commissionNotionalTotalCollected")),
                    "comm_leg": _amount(ex.get("commissionNotionalCollected")),
                    "state": str(order.get("state") or ""),
                }

    @staticmethod
    def _page_oldest(acts: list[dict]) -> Optional[float]:
        stamps = [iso_to_ts((a.get("trade") or {}).get("createTime"))
                  for a in acts if isinstance(a, dict) and isinstance(a.get("trade"), dict)]
        stamps = [s for s in stamps if s is not None]
        return min(stamps) if stamps else None

    async def _recovery_walk(self) -> None:
        """At most ONE activities request per quote cycle while the queue is non-empty,
        charged to `_extra_requests` after the cycle's stats reset. Page-1 verdict
        edges take priority, then cursor deepening for the current traversal, then a
        traversal RESTART for entries the current traversal cannot cover (queued after its
        start; needing a verdict-capable start only now reachable; or the cursor died
        before the ledger's proven end). The walk keeps its OWN coverage state — never
        `page_poly_activities.reached`, which reads an empty page and a missing cursor as
        covered (the `[] means flat` shape). An EMPTY page is never coverage, and a FULL
        page with no cursor is not exhaustion either — only a short final
        page proves the ledger's end."""
        if self.shadow or not self.pending_activity_recovery:
            return
        if self._nf_breaker_tripped:
            return                        # the queue HOLDS during a client/route fault
        now = time.time()
        entries = self.pending_activity_recovery
        need_page1 = any(
            now >= e.terminal_ts + LAG_HORIZON_S
            and e.verdict_read_ts < e.terminal_ts + LAG_HORIZON_S
            for e in entries.values())
        # Deepening serves entries the CURRENT traversal can ever satisfy (started ≥ their
        # queued_ts — the merge saw them from its first read); a restart serves entries it
        # cannot, but only once a restart taken NOW would clear their required start — a
        # page-1 read before `terminal + LAG` cannot ever ground a negative verdict, so
        # restarting for one earlier is a wasted request.
        coverable = [e for e in entries.values()
                     if self._walk_started_ts >= e.queued_ts]
        need_deepen = (not self._walk_exhausted and bool(self._recovery_cursor)
                       and any(not self._oldest_covered(e) for e in coverable))
        need_restart = any(
            self._walk_started_ts < e.queued_ts
            or (self._walk_started_ts < self._coverage_start_required(e, for_verdict=True)
                and now >= self._coverage_start_required(e, for_verdict=True))
            for e in entries.values()) or (
            # A traversal with NO cursor left and no proven end has nothing more to give
            # but has not covered everyone — without this the walk goes completely silent
            # (zero requests) until the timeout, holding unbooked venue evidence
            # — and a full page 1 with no cursor is exactly this shape.
            not self._recovery_cursor and not self._walk_exhausted
            and any(not self._oldest_covered(e) for e in coverable))
        cursor: Optional[str] = None
        if need_page1:
            cursor = ""
        elif need_deepen:
            cursor = self._recovery_cursor
        elif need_restart:
            cursor = ""
        if cursor is not None:
            self._extra_requests += 1
            read_ts = now
            try:
                acts, next_cursor = await self.client.get_activities_page(cursor)
            except Exception as exc:
                log.warning(f"recovery walk: activities read failed ({exc!r}) — "
                            f"no coverage this cycle")
                acts, next_cursor, read_ts = [], "", None
            if read_ts is not None and not acts and cursor == "" \
                    and not self._empty_ledger_logged:
                # A genuinely empty ledger can never produce coverage (an empty page is
                # not evidence), so entries will age out UNRESOLVED — make the cause
                # readable rather than a mystery refusal.
                self._empty_ledger_logged = True
                log.warning("recovery walk: the activities ledger returned EMPTY — no "
                            "coverage is derivable; queued entries will time out "
                            "UNRESOLVED unless trades appear.")
            if read_ts is not None and acts:
                self._merge_activities(acts)
                # A missing cursor proves exhaustion only on a SHORT page: a full page
                # with no cursor is indistinguishable from a truncated listing, and the
                # safe misread direction is "not exhausted" (entries time out UNRESOLVED
                # rather than resolving NO-FILL on phantom coverage).
                proved_end = not next_cursor and len(acts) < 50
                if cursor == "":
                    # A NON-EMPTY page-1 read is the verdict edge for every entry whose
                    # gate has passed.
                    for e in entries.values():
                        if read_ts >= e.terminal_ts + LAG_HORIZON_S:
                            e.verdict_read_ts = read_ts
                    # Start a FRESH traversal from this read when there is no live one, or
                    # when a restart is due and the current traversal has finished serving
                    # its own cohort — never mid-deepening (that would discard an older
                    # entry's progress).
                    if self._walk_started_ts == 0.0 or (need_restart and not need_deepen):
                        self._walk_started_ts = read_ts
                        self._walk_oldest_ts = self._page_oldest(acts)
                        self._walk_exhausted = proved_end
                        self._recovery_cursor = next_cursor
                else:
                    page_min = self._page_oldest(acts)
                    if page_min is not None:
                        self._walk_oldest_ts = page_min if self._walk_oldest_ts is None \
                            else min(self._walk_oldest_ts, page_min)
                    self._recovery_cursor = next_cursor
                    if proved_end:
                        self._walk_exhausted = True
        self._evaluate_recovery(now)

    @staticmethod
    def _venue_cum(execs: dict[str, dict]) -> Optional[Decimal]:
        """The venue's own running total for the order: the max `order.cumQuantity` across
        matched executions. ⛔ NOT Σ of leg quantities — Σ equals cum only
        under COMPLETE execution sight, and silently UNDERSTATES under partial sight, which
        is exactly when a derived quantity would freeze an under-booking as "NO-FILL". cum
        is the same field `get_order` serves, and partial sight can only UNDERSTATE it
        (monotone-safe). None = no matched execution carried the field."""
        cums = [x["cum"] for x in execs.values() if x["cum"] is not None]
        return max(cums) if cums else None

    def _evaluate_recovery(self, now: float) -> None:
        # VENUE-TIME order, not insertion order: two recovered fills on
        # one slug booked newest-first manufacture the very out-of-order re-basing the
        # through-zero counter exists to report. Keyed on the NEWEST exec ts — the value
        # `_book_recovered` actually stamps the row with; sorting on the MIN ts re-creates
        # the inversion for multi-execution orders. No executions sorts last.
        def _entry_ts(item):
            execs = self._recovery_execs.get(item[0], {})
            stamps = [x["ts"] for x in execs.values() if x["ts"] is not None]
            return max(stamps) if stamps else float("inf")

        for oid, entry in sorted(self.pending_activity_recovery.items(), key=_entry_ts):
            execs = self._recovery_execs.get(oid, {})
            venue_cum = self._venue_cum(execs)
            booked = entry.order.filled_qty
            book_cov = self._oldest_covered(entry)
            # A NEGATIVE verdict needs the STRICTER coverage: a traversal started after the
            # entry joined AND after terminal + LAG (the store publishes executions late —
            # deep pages read earlier cover the createTime window without containing them).
            verdict_cov = self._oldest_covered(entry, for_verdict=True)
            verdict_ok = (entry.verdict_read_ts > 0
                          and entry.verdict_read_ts >= entry.terminal_ts + LAG_HORIZON_S)
            if venue_cum is not None and venue_cum > booked and book_cov:
                # FOUND — the booking edge alone suffices: booking is monotone-safe (cum can
                # only rise as sight completes), so waiting for the verdict edge would delay
                # a real fill's entry into every cap for no protection.
                self._book_recovered(oid, entry, execs, venue_cum)
                continue
            if verdict_cov and verdict_ok and (venue_cum is not None or not execs):
                seen = venue_cum if venue_cum is not None else _ZERO
                if seen == booked:
                    # Verb keyed on the NUMBER beside it: an order that
                    # filled through the normal path and is merely confirmed here must not
                    # log "NO-FILL" next to a nonzero booked.
                    verb = "recovery complete" if booked > _ZERO else "confirms NO-FILL"
                    log.info(f"{entry.order.slug} {entry.order.side}: activities recovery "
                             f"{verb} for {oid} (venue cum == booked {booked}); dequeued.")
                    self._dequeue_recovery(oid)
                    continue
                if seen < booked:
                    # The earlier read OVER-stated (the bad-WS-body signature): booking
                    # nothing is the only honest move — mirror of the cum-regression rule.
                    self._resolve_unresolved(
                        oid, f"OVER-STATEMENT: venue cum {seen} < booked {booked} — "
                             f"reconcile against the venue's activities ledger after the run")
                    continue
            # Residence timeout: from the LATER of terminal and queue entry. The
            # verify-exhaustion path joins minutes after its terminal, so measuring from
            # terminal alone leaves it only a handful of walk requests before UNRESOLVED.
            if now - max(entry.terminal_ts, entry.queued_ts) > RECOVERY_MAX_S:
                booked_at_deadline = booked
                if venue_cum is not None and venue_cum > booked:
                    # ⛔ POSITIVE venue evidence in hand at the deadline is BOOKED, not
                    # discarded: `cumQuantity` is the venue's own total and
                    # booking it is monotone-safe regardless of coverage — throwing it away
                    # under-states inventory in the direction every cap reads, and the
                    # flatten then calls a slug whose whole position is this fill "flat".
                    # The entry still routes UNRESOLVED (the coverage failure is real; the
                    # record stays open), but the ledger's number reaches belief first.
                    self._book_recovered(oid, entry, execs, venue_cum)
                    if oid not in self.pending_activity_recovery:
                        continue          # price-sanity refusal already routed it
                self._resolve_unresolved(
                    oid, f"no coverage within {RECOVERY_MAX_S:.0f}s "
                         f"(book_cov={book_cov}, verdict_cov={verdict_cov}, "
                         f"verdict_edge={verdict_ok}, venue_cum={venue_cum}, "
                         f"booked_at_deadline={booked_at_deadline}, "
                         f"booked_now={entry.order.filled_qty})")

    def _book_recovered(self, oid: str, entry: RecoveryEntry, execs: dict[str, dict],
                        venue_cum: Decimal) -> None:
        """Book the unbooked TAIL through `_book_fill` (cum-idempotent — the synthesized body
        carries the venue's own CUMULATIVE total, so the already-booked head is skipped by
        construction)."""
        order = entry.order
        prices = {x["price"] for x in execs.values()}
        if None in prices or prices != {order.price}:
            # Price sanity: resting post-only fills execute AT our limit — EXACTLY, on every
            # row recorded so far. Sign-aware by construction: equality
            # refuses a bid-side "improvement" (lower px) and the short-side trap (an
            # "improvement" on an ask is a HIGHER yes number) identically. An alarm without
            # a rail is not a defence: refusal ROUTES, it does not just log.
            self._resolve_unresolved(
                oid, f"price sanity: activity prices {sorted(str(p) for p in prices)} ≠ "
                     f"our limit {order.price} — refused, nothing booked")
            return
        newest = max(execs.values(),
                     key=lambda x: (x["cum"] if x["cum"] is not None else Decimal(-1),
                                    x["ts"] or 0.0))
        row_ts = newest["ts"]
        if row_ts is None:
            # An unreadable createTime must never fall through to _book_fill's wall-clock
            # default — but refusing outright discards POSITIVE venue evidence, which is the
            # one direction this module never throws away. Fallback chain:
            # the newest execution WITH a readable ts, else the entry's terminal_ts (every
            # execution predates its terminal — a true upper bound, never a fresh-looking
            # lie). Logged, because it means venue shape drift.
            readable = [x["ts"] for x in execs.values() if x["ts"] is not None]
            row_ts = max(readable) if readable else entry.terminal_ts
            log.warning(f"{order.slug} {order.side}: {oid} newest execution's createTime "
                        f"is UNREADABLE (venue shape drift?) — stamping the recovered row "
                        f"with {'the newest readable execution ts' if readable else 'the entry terminal_ts'} "
                        f"({row_ts:.0f}) instead of a wall-clock guess.")
        qty_sum = sum((x["qty"] for x in execs.values() if x["qty"] is not None), _ZERO)
        if all(x["qty"] is not None for x in execs.values()) and qty_sum != venue_cum:
            # Partial EXECUTION sight: cum is still
            # safe to book (it is the venue's own total at the newest seen execution), but
            # the walk has not seen every leg — worth a line, not a refusal. Gated on
            # all-qtys-readable: with SOME legs unreadable an under-sum is a field
            # artifact, not evidence.
            log.warning(f"{order.slug} {order.side}: {oid} partial or divergent execution "
                        f"sight — Σ leg qty {qty_sum} ≠ venue cum {venue_cum}.")
        # ⛔ avgPx is the VENUE's field or ABSENT — never synthesized from our limit.
        # The tape contract says an empty avg_px marks "fell back to our
        # limit", and any commission check evaluates at the venue's own avgPx; a
        # derived value defeats the marker exactly on the rows where it matters and makes
        # any "avg_px == price" statistic self-confirming.
        body: dict = {"cumQuantity": str(venue_cum)}
        if newest["avg_px"] is not None:
            body["avgPx"] = str(newest["avg_px"])
        if newest["state"]:
            body["state"] = newest["state"]     # the venue's own token, never an invented one
        if newest["comm_total"] is not None:
            # The NEWEST matched execution's snapshot: the field is a RUNNING-TOTAL snapshot
            # that differs across the executions of one order, so reading the oldest copy
            # under-reports the commission — sometimes by nearly all of it.
            body["commissionNotionalTotalCollected"] = str(newest["comm_total"])
        else:
            self.recovery_commission_absent.append(oid)
            log.warning(f"{order.slug} {order.side}: recovered order {oid} carries NO "
                        f"commission snapshot in its activities legs — column left BLANK "
                        f"(absence is not zero).")
        leg_sum = sum((x["comm_leg"] for x in execs.values() if x["comm_leg"] is not None),
                      _ZERO)
        if newest["comm_total"] is not None and all(
                x["comm_leg"] is not None for x in execs.values()) \
                and leg_sum != newest["comm_total"]:
            # all(), not any(): with SOME legs missing the field, an
            # under-sum is a missing-field artifact, not evidence of unseen executions.
            log.warning(f"{order.slug} {order.side}: {oid} partial commission sight — "
                        f"Σ per-execution {leg_sum} ≠ newest snapshot "
                        f"{newest['comm_total']} (some executions unseen).")
        inv_before = self.inventory.get(order.slug, _ZERO)
        prior_newest = self._last_row_ts.get(order.slug)
        fill = self._book_fill(order, body, late_booked=True, booked_via="activities",
                               ts_override=row_ts)
        if fill is None:
            return
        inv_after = self.inventory.get(order.slug, _ZERO)
        out_of_order = prior_newest is not None and fill.ts < prior_newest
        if out_of_order and (inv_before == _ZERO or inv_after == _ZERO
                            or (inv_before > _ZERO) != (inv_after > _ZERO)):
            # ⛔ A recovered booking that touches or crosses flat AND arrives OUT OF ORDER
            # (a later fill was already booked on this slug). Both halves are load-bearing:
            # a sign-flip-only condition misses the from-flat case that actually corrupts
            # the ratchet (fill_accounting re-based on the wrong order), and widening it
            # further fires on every CLEAN recovery onto a quiet book — a corruption tell
            # that cries on the happy path trains the operator to ignore it. Order-invariance
            # does NOT hold for re-basing on out-of-order arrival, and the recovery horizon
            # widens that window — ACCEPTED as interim, but the corrupted case must be
            # visible without forensics.
            self.recovered_through_zero += 1
            log.warning(f"⚠️ {order.slug}: recovered booking touched/crossed flat OUT OF "
                        f"ORDER ({inv_before} → {inv_after}; row ts {fill.ts:.0f} < newest "
                        f"booked {prior_newest:.0f}) — re-basing window; realized pairing "
                        f"may be order-corrupted; "
                        f"recovered_through_zero={self.recovered_through_zero}")
        log.warning(f"{order.slug} {order.side}: activities recovery booked "
                    f"{fill.filled_qty} on {oid} at {order.price} "
                    f"(venue cum {venue_cum}, path {entry.path}).")
        if self.state is not None and self.real:
            self.state.set_inventory(self.inventory)

    def _book_fill(self, order: RestingOrder, body: dict, *,
                   late_booked: bool = False, booked_via: str = "poll",
                   ts_override: Optional[float] = None) -> Optional[Fill]:
        """Book any NEW fill on `order` from a read-back body. None if nothing new filled.

        `ts_override` stamps the row with the VENUE's own execution time (the activities
        `createTime`) instead of the booking wall-clock — a recovered fill can land minutes
        after it happened, and `rebate_total`'s final-row selection keys on (cum, ts), so a
        wall-clock ts on a recovered row would also be a lie the tape carries forever.

        `cumQuantity` is CUMULATIVE, so the increment is `cum − already_seen`; adding the
        cumulative figure on every poll would double the position and mis-state every number built
        on it. An unreadable `cumQuantity` books nothing — cannot-verify is not a fill.

        `late_booked=True` marks a fill discovered by the delayed verify: the fill happened up to
        LAG_HORIZON_S ago, so the current touch is NOT a mark for it — mid columns are blanked
        rather than written with a fresh-looking staleness. A late fill stamped with a
        seconds-old mid over a minutes-old fill silently poisons any markout built on the tape,
        and the late subset is exactly the adverse-heavy subset."""
        cum = _amount(body.get("cumQuantity"))
        if cum is None:
            return None
        increment = cum - order.filled_qty
        if increment < _ZERO:
            # ⛔ `cumQuantity` is monotone by contract — a negative delta means one of the two
            # reads is WRONG, and until now it was swallowed on the same silent path as a
            # healthy zero. Which read is right is undecidable here, so booking is still
            # refused and belief unchanged; the arbiter for the direction this hides (an
            # OVER-stating earlier body, e.g. a bad WS push) is a post-teardown reconciliation
            # against the venue's own activities ledger.
            self.cum_regressions += 1
            log.warning(f"⚠️ {order.slug} {order.side} {order.order_id}: cumQuantity went "
                        f"BACKWARDS ({order.filled_qty} → {cum}) — one of the two reads is "
                        f"wrong; booking refused, belief unchanged. If the earlier read was "
                        f"a WS body this is the over-statement signature — reconcile against "
                        f"the venue's activities ledger after the run.")
            return None
        if increment == _ZERO:
            return None
        touch = None if late_booked else self.last_touch.get(order.slug)
        mid = ((touch[0] + touch[1]) / 2) if touch else None
        row_ts = ts_override if ts_override is not None else time.time()
        fill = Fill(
            ts=row_ts, slug=order.slug, side=order.side, order_id=order.order_id,
            price=order.price, size=order.size, filled_qty=increment, cum_filled_qty=cum,
            commission_order_total=_amount(body.get("commissionNotionalTotalCollected")),
            queue_ahead=order.queue_ahead,
            time_to_fill_s=row_ts - order.placed_ts, improved=order.improved,
            order_state=str(body.get("state") or ""), mid_at_fill=mid,
            mid_age_s=(time.time() - touch[2]) if touch else None,
            best_bid=touch[0] if touch else None,
            best_ask=touch[1] if touch else None,
            book_src="" if late_booked else self.last_book_src.get(order.slug, ""),
            avg_px=_amount(body.get("avgPx")), late_booked=late_booked,
            booked_via=booked_via)
        order.filled_qty = cum
        slug = order.slug
        new_inv, new_avg, rt_r, rt_c, completed = fill_accounting(
            self.inventory.get(slug, _ZERO), self.avg_entry.get(slug, _ZERO),
            self.rt_realized.get(slug, _ZERO), self.rt_closed.get(slug, _ZERO),
            order.side, order.price, increment)
        self.inventory[slug] = new_inv
        self.avg_entry[slug] = new_avg
        self.rt_realized[slug] = rt_r
        self.rt_closed[slug] = rt_c
        if completed is not None and self.state is not None and self.real:
            # The LOSS CAP feeds on every completed round trip, INCLUDING late-booked ones —
            # realized-at-flat is order-invariant (unlike the cooldown's per-trip attribution
            # below), so a late delta's dollars are real dollars. Durable via add_realized: the
            # ratchet survives the process. `and self.real` guards the DURABLE CROSS-PROCESS
            # ledger like every sibling state write: a DRY maker handed a store must never push
            # fictional round trips into the budget a real launch is refused on.
            self.session_realized += completed[0]
            self.state.add_realized(completed[0])
            breach = self._loss_cap_breach()
            if breach is not None:
                self.should_stop = True
                if self.halt_reason is None:
                    # First cause wins: a breach during a kill-switch teardown must not rewrite
                    # the durable exit record's reason.
                    self.halt_reason = breach
                log.error(breach)
        # ⛔ Late-booked fills never ARM the cooldown: the delayed verify
        # books up to LAG_HORIZON_S after the fact, so arrival order ≠ fill order and the round
        # trip a late delta appears to complete can be fiction (a missed bid landing after the
        # next bid books a winning trip that never happened, then a phantom fresh position).
        # Inventory stays correct either way; only the TRIGGER declines to act on re-ordered
        # history. The adverse-heavy subset is exactly the late subset, so this errs toward
        # missing a cooldown, never toward inventing one.
        prev_newest = self._last_row_ts.get(slug)
        self._last_row_ts[slug] = row_ts if prev_newest is None else max(prev_newest, row_ts)
        if completed is not None and not late_booked and self.adverse_cooldown_s > 0:
            realized, closed = completed
            if closed > _ZERO and realized / closed <= -ADVERSE_RT_PER_CONTRACT:
                self.cooldown_until[slug] = time.time() + self.adverse_cooldown_s
                log.warning(
                    f"adverse round trip on {slug}: realized {realized} over {closed} contracts "
                    f"(≤ −{ADVERSE_RT_PER_CONTRACT}/contract) — reduce-only for "
                    f"{self.adverse_cooldown_s:.0f}s")
        self._write_fill(fill)
        return fill

    # ── teardown ─────────────────────────────────────────────────────────────────────────────

    async def teardown(self) -> list[tuple[str, str]]:
        """Run the four phases IN ORDER and return (phase, outcome) for each.

        ⛔ cancel-all → reconcile-pending → flatten → sweep. Sweeping before flattening re-reads a book that the
        flatten is about to move. The order is driven off `TEARDOWN_PHASES` rather than written
        out as separate statements, so the invariant is one pinnable value rather than four places
        that can drift apart (which is exactly what happened to the Kalshi maker).
        """
        results: list[tuple[str, str]] = []
        for phase in TEARDOWN_PHASES:
            handler = getattr(self, f"_teardown_{phase}")
            try:
                results.append((phase, await handler()))
            except Exception as exc:
                # One phase failing must not skip the ones after it — the sweep is the backstop.
                log.error(f"teardown phase {phase} failed: {exc!r}")
                results.append((phase, f"FAILED: {exc!r}"))
        left_record_open = False
        # Unresolved recovery = possibly-unbooked fills: entries still queued at exit
        # or terminally UNRESOLVED. Either keeps the crash record open below — realized may be
        # incomplete, and the durable ledger must not be closed (or deleted) over that.
        recovery_open = bool(self.pending_activity_recovery) or bool(self.unresolved_recovery)
        if recovery_open:
            log.error(
                "⚠️ activities recovery left entries unresolved "
                f"(queued: {list(self.pending_activity_recovery)}; "
                f"unresolved: {self.unresolved_recovery}) — realized may be INCOMPLETE. "
                "The next start will refuse over these order records and its message will "
                "cite possibly-RESTING orders — for these ids that cause is "
                + ("already disproven (this teardown's sweep confirmed the venue clear); "
                   if self._swept_clean else "plausible but unconfirmed; ")
                + "the real reason the record is open is UNRECONCILED REALIZED. Closure "
                "sequence, in order: (1) run scripts.poly_order_diff against the venue's "
                "activities and note any venue≠tape gap; (2) note realized/loss_to_date "
                "from the state file; (3) only THEN close the record by deleting the state "
                "file, carrying the noted loss forward via --loss-cap on the next launch. "
                "Do NOT delete before reconciling — deletion discards the durable loss "
                "ledger, and an understated ledger is the thing this flag exists to stop.")
        if self.state is not None and self.real:
            # ── CLOSE THE CRASH RECORD — but only on the VENUE's evidence ────────────────────
            # `_swept_clean` is True only when the sweep's listing succeeded and every order on
            # our markets cancelled cleanly. Order records outlive their parked verifies here
            # (the 120s horizon exceeds the ~80s teardown window), so without this clear every
            # CLEAN run left opswatch warning "N orders may be RESTING" — a false alarm that
            # trains the operator to ignore the true one. Writing "clean" off our own BELIEF
            # instead of the venue's answer would put a fiction in the one flag that tells an
            # offline recovery tool there is nothing to do.
            if self._swept_clean and not recovery_open:
                for _iid in list(self.state.snapshot().orders):
                    self.state.clear_order(_iid)
                self.state.end_run(self.halt_reason or "clean")
            elif self._swept_clean:
                # The venue is swept clean of RESTING orders, but the
                # ACCOUNTING is not closed — a queued/unresolved recovery entry can carry a
                # fill the realized ledger never saw. The operator message above names the
                # remedy, and the record stays open so the next start
                # refuses rather than trading on an understated loss ledger.
                left_record_open = True
            else:
                left_record_open = True
                # ⛔ Do NOT point this message at the Kalshi-side recovery tool — it builds a
                # KalshiClient unconditionally, so it reads the WRONG VENUE: it prints "the
                # venue confirms flat" over a live POLY order and writes clean_exit=True.
                # The honest remediation is on the Poly venue.
                # ⛔ And confirmation is by API, not the UI: the tool
                # runs with the SAME credentials that placed the orders, where "nothing in
                # the UI" is also what a wrong-account login shows.
                msg = ("⚠️ the venue could not confirm that nothing is resting — leaving the "
                       "crash record OPEN (clean_exit stays False). The next start will "
                       "REFUSE cleanly (the preflight for a readable stray; begin_run for "
                       "an unattributable one) and its message carries the Poly remedy. "
                       "Resolve on the POLY venue: confirm what is actually resting with "
                       "`.venv/bin/python -m scripts.poly_us_orders` (read-only, same "
                       "credentials that placed the orders); cancel strays by hand and "
                       "close positions with scripts.poly_close; once the venue is "
                       "confirmed clear, close the record by deleting the state file named "
                       "in the refusal — ⚠️ that also discards the durable loss ledger "
                       "(realized_pnl and the cap ratchet reset to zero), so note "
                       "realized/loss_to_date first. Do NOT run scripts.maker_recover for "
                       "this — it is Kalshi-only and would certify a false clean from the "
                       "wrong venue.")
                if self._sweep_refused:
                    # Only honest when the venue ACTUALLY refused a cancel — the other two
                    # causes of an unswept exit (unattributable order, unreadable listing)
                    # are not ghosts and must not inherit this diagnosis.
                    msg += (f" The venue REFUSED {self._sweep_refused} cancel(s): if "
                            "poly_us_orders shows nothing resting and positions flat, this "
                            "is the terminal-but-listed case (the sweep's listing carried "
                            "an already-dead order) — the record is open over a ghost and "
                            "the confirm-flat-then-delete path above applies; it is not a "
                            "wrong-account signal.")
                elif self._sweep_refused is None:
                    msg += (" ⛔ The sweep's LISTING FAILED — the venue was never read. "
                            "This is a cannot-verify, not a ghost: do NOT delete anything "
                            "until poly_us_orders answers.")
                log.error(msg)
        if self.heartbeat is not None:
            _exit = "clean" if self.halt_reason is None else f"halted:{self.halt_reason}"
            if left_record_open:
                # The branch above just left the crash record OPEN — the
                # heartbeat must carry the same verdict, or the two shutdown artifacts
                # disagree and the deadman reads "clean" over a record the next start will
                # refuse on. Any non-"clean" status fails Deadman.ok, so opswatch surfaces
                # it. The flag is set IN the record-open branch itself, so this gate cannot
                # drift from the population it describes.
                _exit = f"record_open:{_exit}"
            self.heartbeat.mark_exit(_exit)
        self._close_writers()
        return results

    async def _teardown_reconcile_pending(self) -> str:
        """READ every parked order before the flatten sizes itself (ignore_due schedules early
        READS — a teardown cannot wait out the 120s horizon), booking any delta each pass over
        ~80s. ⛔ Entries do NOT retire early: an early readable answer can be STALE — that is
        exactly how a teardown prints several confident FLAT lines over a real position — so
        anything still inside its horizon stays parked, is NAMED below, and the venue-verify at
        exit remains the last word. The flatten therefore sizes off the best booked number the
        lag allows, never off a prematurely-trusted one."""
        if not self.pending_reconcile and not self.pending_activity_recovery:
            return "nothing parked"
        if self.pending_reconcile:
            for _ in range(4):
                await self._retry_pending_reconciles(ignore_due=True)
                if not self.pending_reconcile:
                    break
                await asyncio.sleep(20)
        # ── the recovery drain: after reconcile-pending, before the flatten (which
        # then sizes off healed belief). HARD BUDGET: ≤3 requests, ≤5s wall, NO sleeps — a
        # pause.json halt must not stretch by minutes. The walk spends ≤1 request per call
        # and a call that spends nothing has nothing left to read, so looping past it is
        # pointless.
        drain_deadline = time.monotonic() + 5.0
        drain_reqs = 0
        if self.pending_activity_recovery and self._nf_breaker_tripped:
            # The flag is stamped by the last COMPLETED poll and poll_fills early-returns
            # once resting empties (cancel-all runs first), so at teardown it can only be
            # stale-tripped, never stale-cleared — say so rather than draining silently
            # into zero requests.
            log.error("⚠️ teardown recovery drain SKIPPED — the not_found breaker was "
                      "tripped on the last live poll (client/route fault); queued entries "
                      "will be named below.")
        while (self.pending_activity_recovery and drain_reqs < 3
               and time.monotonic() < drain_deadline):
            before = self._extra_requests
            await self._recovery_walk()
            spent = self._extra_requests - before
            drain_reqs += spent
            if spent == 0:
                break
        # ⛔ The success string is conditioned on BOTH queues: an unresolved recovery
        # queue must never read as "all parked orders reconciled".
        left = list(self.pending_reconcile)
        rec_left = list(self.pending_activity_recovery)
        if not left and not rec_left:
            return "all parked orders reconciled"
        if left:
            log.error(f"⚠️ {len(left)} parked order(s) still INSIDE their verify horizon at "
                      f"teardown: {left} — their deltas (if any) were booked on each pass, "
                      f"but they are NOT certified; the flatten sizes off the best booked "
                      f"number and the venue-verify line is the only flatness statement that "
                      f"counts.")
        if rec_left:
            log.error(f"⚠️ {len(rec_left)} order(s) still in activities recovery at "
                      f"teardown: {rec_left} — coverage incomplete inside the drain budget; "
                      f"any of them may carry an unbooked fill.")
        parts = []
        if left:
            parts.append(f"{len(left)} uncertified (inside horizon): {left}")
        if rec_left:
            parts.append(f"{len(rec_left)} in activities recovery: {rec_left}")
        return "; ".join(parts)

    async def _teardown_cancel_all(self) -> str:
        if self.shadow:
            return "shadow: nothing was ever placed"
        count = 0
        for slug, side in list(self.resting):
            await self._cancel(slug, side)
            count += 1
        return f"cancelled {count} recorded order(s)"

    async def _teardown_flatten(self) -> str:
        """Passive, reducing-only, and it REPORTS what it could not clear.

        ⛔ It never crosses the spread. Cancelling is safe, flattening is not: a residual is left
        in the durable record for a recovery tool and an operator. A maker that market-sells its
        own inventory to look tidy at exit is a maker that pays the taker fee it spent all day
        avoiding.
        """
        residual = {s: q for s, q in self.inventory.items() if q != _ZERO}
        if not residual:
            return "flat (LOCAL BELIEF — the verify line below is the certification)"
        if self.shadow:
            return f"shadow: would flatten {residual}"
        # ⛔ READ THE VENUE FRESH, HERE. The gate below decides whether to TRADE, and the
        # scheduled refresh is up to RECONCILE_EVERY_CYCLES old before teardown even starts —
        # then `_teardown_reconcile_pending` sleeps through its own wait ahead of us. A row that
        # is minutes old corroborates nothing, and refusing on it declines to flatten inventory
        # we know about with perfect confidence. One request.
        venue_fresh = await self._refresh_venue_inventory() if self.real else False
        unsafe: dict[str, str] = {}
        for slug, qty in residual.items():
            if not venue_fresh:
                unsafe[slug] = f"{qty} (venue unreadable at teardown — not corroborated)"
                continue
            # ⛔ THE FLATTEN MUST AGREE WITH THE VENUE BEFORE IT TRADES.
            #
            # This is the one path that can send an order in the direction that GROWS a position
            # nothing is measuring, and it is reachable from every halt — including the venue
            # breach, whose own message says "we do not know our position". Belief LONG against
            # a venue SHORT sends a post-only SELL at the offer: a "flatten" that makes the
            # short bigger. That is a reproduced shape, not a hypothetical.
            #
            # So: trade only where the venue CORROBORATES belief in sign, and size to the
            # smaller of the two so we cannot overshoot either estimate. Everything else is
            # REPORTED for the operator — the same
            # report-don't-force-sell discipline this function already applies to a residual it
            # cannot price.
            if slug in self.venue_stale_rows:
                unsafe[slug] = f"{qty} (venue row unparseable — not corroborated)"
                continue
            venue_qty = self.venue_inventory.get(slug)
            if venue_qty is None or venue_qty == _ZERO:
                # "No row" and "row says zero" are the same instruction here — the venue does not
                # think we hold this — and both must REPORT rather than trade. Distinguished in
                # the text so an operator can tell a missing book from a flat one, and kept out
                # of the sub-contract branch below, which would otherwise print "residual 0"
                # over a real position after the qty rebind.
                unsafe[slug] = (f"{qty} (venue says flat)" if venue_qty == _ZERO
                                else f"{qty} (no venue row)")
                continue
            if (qty > _ZERO) != (venue_qty > _ZERO):
                unsafe[slug] = f"belief {qty} vs venue {venue_qty} (opposite sign)"
                continue
            if venue_qty.copy_abs() < qty.copy_abs():
                qty = venue_qty          # never flatten more than the venue says we hold
            tick = self.ticks.get(slug)
            if tick is None:
                continue
            try:
                book = await self.client._fetch_book(slug, fresh=True)
            except Exception as exc:
                log.warning(f"flatten: book read failed for {slug}: {exc!r}")
                continue
            bid, ask, _ = touch_from_md(book.get("marketData") if isinstance(book, dict) else None)
            if bid is None or ask is None:
                continue
            # Reducing side only, joining the touch: a long is reduced by an ask at the offer.
            side = "ask" if qty > _ZERO else "bid"
            price = ask if qty > _ZERO else bid
            # ⛔ Sized to the RESIDUAL, never to `--size`. Truncated toward zero, so a sub-contract
            # residue places nothing rather than rounding up into a fresh opposite position.
            count = int(qty.copy_abs())
            if count <= 0:
                log.warning(f"flatten: {slug} residual {qty} is under one contract — nothing to "
                            f"place; reported instead.")
                continue
            await self._place(slug, side, price, improved=False, size=count,
                              queue_ahead=qty_at_price(
                                  book.get("marketData"), side, price))
        if self.flatten_wait_s > 0:
            await asyncio.sleep(self.flatten_wait_s)
            await self.poll_fills()
        # `left` excludes the unsafe slugs — they were `continue`d, so they are still in
        # `self.inventory` and were being re-listed under "Other residual", which an operator
        # could read as twice the position.
        left = {s: str(q) for s, q in self.inventory.items()
                if q != _ZERO and s not in unsafe}
        if unsafe:
            return (f"⛔ NOT FLATTENED — the venue does not corroborate belief on {unsafe}. "
                    f"Trading on a position we cannot confirm is how a flatten GROWS it. "
                    f"Reconcile by hand (scripts.poly_cancel for orders; close with "
                    f"`scripts.poly_close` — scripts.maker_recover is Kalshi-only)."
                    + (f" Also holding: {left}." if left else ""))
        if not left:
            return "flat (LOCAL BELIEF — the verify line below is the certification)"
        return (f"⚠️ RESIDUAL INVENTORY, reported not force-sold: {left}. Flatten by hand with "
                f"`scripts.poly_close`; cancel strays with `scripts.poly_cancel` (scripts.maker_recover "
                f"is Kalshi-only and reads the wrong venue).")

    async def _teardown_sweep(self) -> str:
        """LAST. Cancel anything still resting ON THE MARKETS THIS RUN QUOTES, including orders we
        never recorded.

        A create whose response was lost leaves an order we do not know about — the hole that
        has stranded real orders on a live run before. `get_open_orders` RAISES on an
        unreadable shape rather than returning [], so a failed sweep is loud rather than looking
        like a clean one.

        ⛔ SCOPED TO OUR OWN SLUGS, AND THIS IS A SAFETY PROPERTY, NOT TIDINESS. `get_open_orders`
        is ACCOUNT-WIDE: it returns every resting order on the API key, including ones placed by a
        different process — a probe, another maker run, a hand-placed order. An unscoped sweep
        would cancel all of them at exit. That is not hypothetical; the account has carried
        unrelated resting probe orders while this maker was being built. A teardown may only undo
        what this run could have done.

        An order with no readable slug is REPORTED, never cancelled: we cannot establish it is
        ours, and cancelling on a guess is the failure this scoping exists to prevent.
        """
        if self.shadow:
            return "shadow: nothing to sweep"
        self._swept_clean = False
        self._sweep_refused = None      # cannot-verify until the listing actually answers
        mine = set(self.ticks) | set(self.slugs)
        # ⛔ DELIBERATELY ACCOUNT-WIDE — the poll is scoped, this is NOT, and the asymmetry is
        # the point: `swept_clean` certifies on listing-ABSENCE, and this venue is already
        # known to accept-and-silently-ignore at least one filter parameter elsewhere in its
        # API. A mishandled `slugs` filter returning [] here
        # would close the durable crash record over live resting orders — a certified false
        # clean, the exact failure the venue split exists to end. One account-wide request
        # once per run is the cheap side of that trade; the client-side `mine` filter below
        # scopes what we CANCEL.
        orders = await self.client.get_open_orders()
        swept = foreign = unattributable = refused = 0
        for entry in orders:
            if not isinstance(entry, dict):
                continue
            oid = venue_order_id(entry)
            slug = entry.get("marketSlug") or entry.get("market_slug")
            if not oid or not slug:
                unattributable += 1
                continue
            if str(slug) not in mine:
                foreign += 1
                continue
            self._extra_requests += 1
            # ⛔ `cancel_order` returns False rather than raising (it runs in cleanup paths).
            # Counting the CALL as swept records a clean exit over a live order.
            if await self.client.cancel_order(oid, str(slug)):
                swept += 1
            else:
                refused += 1
        # The venue's own evidence that nothing of ours can still be resting ON THIS RUN'S
        # MARKETS (the sweep is slug-scoped by design — see the docstring): the listing
        # succeeded (a raise fails this phase), every order on our markets cancelled cleanly,
        # and nothing was unattributable — an order we cannot attribute might be ours (a lost
        # create-response records no venue id), so the record may not close over it. Foreign
        # orders are not ours and do not block. Held in a LOCAL until the phase completes, so
        # a raise anywhere in this method leaves `self._swept_clean` False. Assign the ATTRIBUTE
        # here instead and a raise further down (poll_fills, say) closes the durable record on a
        # phase that actually FAILED.
        swept_clean = (refused == 0 and unattributable == 0)
        # The refused count outlives the phase: the teardown's
        # remediation text branches on it — the terminal-but-listed ("ghost") diagnosis is
        # only honest when the venue actually refused a cancel. It stays None (cannot-verify)
        # if the listing raised, and the two must never be conflated.
        self._sweep_refused = refused
        # The residual reported by `flatten` was computed BEFORE this sweep cancelled the flatten
        # order, so it can be stale high. Re-read here, last, and report the reconciled number —
        # an operator hand-flattening a phantom residual opens a position in the other direction.
        await self.poll_fills()
        left = {s: str(q) for s, q in self.inventory.items() if q != _ZERO}
        note = f"swept {swept} resting order(s) on this run's {len(mine)} market(s)"
        # "LOCAL BELIEF" because it is one — the venue-certified statement is the shim's verify
        # line, and printing an unlabeled "FLAT" three lines above it invited exactly the
        # misreading the verify exists to prevent.
        note += f"; final inventory (LOCAL BELIEF, not certified) {left or 'flat'}"
        if foreign:
            note += (f"; LEFT ALONE {foreign} order(s) on other markets — they are not this "
                     f"run's to cancel")
        if unattributable:
            note += (f"; ⚠️ {unattributable} order(s) had no readable id/slug and were NOT "
                     f"cancelled — inspect by hand")
        if refused:
            note += (f"; ⛔ the venue REFUSED {refused} cancel(s) — those orders may still be "
                     f"LIVE; the crash record stays OPEN")
        self._swept_clean = swept_clean
        return note

    # ── tapes ────────────────────────────────────────────────────────────────────────────────

    def _writer(self, path: str, header: Sequence[str], rotate_tag: str):
        if path not in self._writers:
            # The tag is PER TAPE and names that tape's LAST schema change (a mismatched old
            # file freezes to rotated/<stem>.pre-<tag>.csv) — a shared tag would stamp a
            # future unrelated fills/cycles drift with a misleading name, and a misleading
            # rotation name is worse than none: it dates the freeze to the wrong change.
            self._writers[path] = _open_writer(path, header, rotate_tag=rotate_tag)
        return self._writers[path]

    def _write_quote(self, stats: CycleStats, slug: str, tick: Decimal,
                     touch: Optional[tuple[Decimal, Decimal]],
                     quotes: Optional[tuple[Decimal, Decimal]], *,
                     improved: bool = False, actions: Optional[dict] = None,
                     queue: Optional[dict] = None, inventory: Decimal = _ZERO,
                     allow: tuple[bool, bool] = (False, False),
                     status: str = "ok", read_ms: Optional[float] = None,
                     gap_s: Optional[float] = None,
                     book_stats: Optional[dict] = None) -> None:
        """EVERY market gets a row every cycle, quotable or not.

        A silent GAP is unrecoverable once the tape is written, and the dominant gap cause — a
        failed book read — correlates with venue stress, i.e. with the busiest moments. Counting
        the drop INTO the data is the fix the passive-markout probe already had to make.

        `gap_s` is this market's REAL sampling interval — the wait since its last successful read,
        blank on the first cycle. It is the per-market half of the throughput question: the cycle tape
        says the loop kept up on average, and this says whether it kept up on THIS market.
        """
        actions = actions or {}
        queue = queue or {}
        _handle, writer = self._writer(self._quote_path, _QUOTE_HDR, rotate_tag="booksrc")
        writer.writerow([
            f"{time.time():.3f}", stats.cycle, slug, str(tick),
            str(touch[0]) if touch else "", str(touch[1]) if touch else "",
            str(quotes[0]) if quotes else "", str(quotes[1]) if quotes else "",
            "Y" if improved else "N",
            actions.get("bid", ""), actions.get("ask", ""),
            "" if queue.get("bid") is None else str(queue["bid"]),
            "" if queue.get("ask") is None else str(queue["ask"]),
            str(inventory), "Y" if allow[0] else "N", "Y" if allow[1] else "N",
            status, f"{read_ms:.1f}" if read_ms is not None else "",
            f"{gap_s:.3f}" if gap_s is not None else "",
            # shares_traded is :.0f — ":g" caps at 6 significant digits, so past 1e6 the
            # counter would quantize and Δ-exactness silently dies. The rest are :g.
            ("" if (book_stats or {}).get("shares_traded") is None
             else f"{(book_stats or {})['shares_traded']:.0f}"),
            *(("" if (book_stats or {}).get(k) is None else f"{(book_stats or {})[k]:g}")
              for k in ("last_trade_px", "last_trade_qty", "last_trade_age_s")),
            self.last_book_src.get(slug, self.book_source),   # book_src
            self.run_id,
        ])
        _handle.flush()

    def _write_cycle(self, stats: CycleStats) -> None:
        # ⚠️ tag history: runid → memcols (avail/swap cols) → wsrereads → wsdown → wsoutage with the avail_mb/swap_free_mb columns — the tag names
        # the tape's LAST schema change; an old-header file freezes to rotated/*.pre-memcols.csv.
        if self._ws_down_since is not None:
            stats.ws_down_s = max(0.0, time.time() - max(self._ws_down_since, stats.started_ts))
        _handle, writer = self._writer(self._cycle_path, _CYCLE_HDR, rotate_tag="wsoutage")
        writer.writerow([
            f"{stats.started_ts:.3f}", stats.cycle, f"{stats.wall_s:.3f}", stats.requests,
            f"{stats.req_per_s:.2f}", stats.markets_total, stats.markets_quoted,
            stats.markets_skipped, stats.missed,
            f"{stats.max_staleness_s:.3f}" if stats.max_staleness_s is not None else "",
            ";".join(f"{k}={v}" for k, v in sorted(stats.actions.items())),
            "Y" if stats.paused else "N",
            f"{stats.rss_mb:.0f}" if stats.rss_mb is not None else "",
            f"{stats.avail_mb:.0f}" if stats.avail_mb is not None else "",
            f"{stats.swap_free_mb:.0f}" if stats.swap_free_mb is not None else "",
            stats.ws_stale_rereads, stats.ws_capped, stats.ws_outage,
            f"{stats.ws_down_s:.1f}" if stats.ws_down_s else "0",
            stats.halt_reason or "",
            self.run_id,
        ])
        _handle.flush()

    def _write_fill(self, fill: Fill) -> None:
        # ⚠️ BUMPED "runid" → "touch" with the best_bid/best_ask columns. The tag names the
        # tape's LAST schema change, and `logs/rotated/poly_live_mm_fills.pre-runid.csv`
        # ALREADY EXISTS from the previous one — leaving the tag would have archived this
        # change under a timestamped `pre-runid.<epoch>.csv`, i.e. two different schema
        # boundaries filed under the same name. No data loss (the collision path appends an
        # epoch), but this function's own comment records that a misleading rotation name
        # has cost a review round before.
        _handle, writer = self._writer(self._fill_path, _FILL_HDR, rotate_tag="fillsrc")
        writer.writerow([
            f"{fill.ts:.3f}", fill.slug, fill.side, fill.order_id or "", str(fill.price),
            fill.size, str(fill.filled_qty), str(fill.cum_filled_qty),
            "" if fill.commission_order_total is None else str(fill.commission_order_total),
            fill.verdict,
            # Predicted on the CUMULATIVE quantity, so it is comparable with the order-total
            # commission beside it. Predicting on the increment would compare a per-fill number
            # against a per-order one and report a different error on every partial.
            # ⛔ And predicted at the VENUE's fill price when it is readable, not at our limit —
            # the venue computes the rebate at ITS price, and on a BUY_SHORT the two are
            # documented to diverge. Falling back to our limit is marked by the avg_px column
            # being empty on the same row.
            str(predicted_rebate(fill.avg_px if fill.avg_px is not None else fill.price,
                                 fill.cum_filled_qty)),
            "" if fill.queue_ahead is None else str(fill.queue_ahead),
            f"{fill.time_to_fill_s:.3f}", "Y" if fill.improved else "N", fill.order_state,
            "" if fill.mid_at_fill is None else str(fill.mid_at_fill),
            "" if fill.mid_age_s is None else f"{fill.mid_age_s:.3f}",
            "" if fill.avg_px is None else str(fill.avg_px),
            "Y" if fill.late_booked else "N",
            fill.booked_via,
            # ⚠️ BEFORE run_id, not after: run_id is LAST on all three tapes by convention
            # (tests/test_poly_maker.py pins it), and the append-last safety that convention
            # rests on is about name-keyed DictReader consumers, which these are.
            "" if fill.best_bid is None else str(fill.best_bid),
            "" if fill.best_ask is None else str(fill.best_ask),
            fill.book_src,
            self.run_id,
        ])
        _handle.flush()

    def _close_writers(self) -> None:
        for handle, _writer in self._writers.values():
            try:
                handle.close()
            except OSError:
                pass
        self._writers.clear()

    def _beat(self, stats: CycleStats) -> None:
        """The operator's three questions, every cycle. Never raises — the deadman is
        instrumentation, and instrumentation must not be able to stop trading."""
        if self.heartbeat is None:
            return
        try:
            self.heartbeat.beat(
                markets_quoted=stats.markets_quoted,
                markets_total=stats.markets_total,
                inventory={k: str(v) for k, v in self.inventory.items() if v != _ZERO},
                gross_exposure=str(self.gross_exposure),
                resting=len(self.resting),
                cycle=stats.cycle,
                requests=stats.requests,
                missed_cycles=stats.missed,
                paused=stats.paused,
                replace_blocked=self.replace_blocked,
                # cum_regressions rides too: "a fill read lied" is the more alarming of
                # the two counters and must not die with a SIGKILLed process.
                cum_regressions=self.cum_regressions,
                # Through-zero re-basings on RECOVERED fills: the loss-cap's known
                # interim window — a fictional trip writes the DURABLE ratchet, so the
                # corrupted case must be visible without forensics.
                recovered_through_zero=self.recovered_through_zero,
                mode="shadow" if self.shadow else ("real" if self.real else "dry"),
            )
        except Exception as exc:                      # pragma: no cover - defensive
            log.error(f"heartbeat failed ({exc!r}) — this process is INVISIBLE to the deadman")
