"""
bot/poly_us/maker.py
────────────────────
The Polymarket US maker: quoting loop, inventory, cap, teardown.

⛔ **THE REBATE IS THE EDGE. NOTHING HERE IS TRYING TO EARN A SPREAD.** Spread capture at the
touch is not relied on. Poly pays `0.0125·p·(1−p)` per contract. Every constraint below exists to
keep that credit reachable, and relaxing one does not "loosen a preference", it removes the term
the design treats as the positive one.

**A SEPARATE PROGRAM FROM `bot/kalshi/maker.py`, on purpose** — the venues differ in order
semantics (an ask here is `BUY_SHORT`, priced in YES space, NOT the complement), fee sign, book
transport and queue observability. Sharing it would mean a third set of `if venue ==` branches
through the money path.

WHAT GATES REAL MONEY
─────────────────────
`config.DRY_RUN`, and only that — `PolyUSClient.__init__` snapshots it and every order method
short-circuits on it. `scripts/poly_live_mm.py` is the ONE place that can set it, from an argv
sniff that must run before `bot.core.config` is imported. `arming_refusal` converts a
DRY_RUN/flag mismatch into a HARD STOP rather than a silent downgrade, because the dangerous
combination is `DRY_RUN=false` with no flag: real quotes while the run reports a preview.

`shadow=True` is a SECOND, independent barrier: `_place` raises `ShadowViolation` rather than
returning, so a shadow run is not one bad branch away from an order.

THE CYCLE, per market, every `--requote-s`
──────────────────────────────────────────
 1. `is_paused()` — the FIRST statement, before any venue call.
 2. cache-busted book read → `touch_from_md`; refuse the market this cycle if either side is None.
 3. compute quotes (join, or improve ONE tick where the spread is >= 2 ticks) and record WHICH,
    because joined and improved fills are different populations.
 4. cancel-then-replace ONLY where our own price moved.
 5. poll fills; on a fill read the commission back and record it.

⛔ **CANCEL/REPLACE ONLY ON A PRICE CHANGE — THE DOMINANT VARIABLE, NOT A MICRO-OPTIMISATION.**
An unchanged quote keeps its queue position; amending to reprice FORFEITS it. The design treats
queue position as the dominant variable.

⚠️ `post_only` IS A BACKSTOP, NOT A REJECTION SIGNAL. Poly documents that a would-match post-only
order "will be rejected"; do not rely on that. Treat acceptance-and-rest as possible and never
write code that expects a rejection here.

WHAT THIS DOES NOT DO
─────────────────────
It does not market-sell to flatten. Cancelling only removes exposure so it is automated;
flattening moves money at a price something must choose, so a residual after the passive flatten
window is REPORTED for an operator and left in the durable record for `scripts/maker_recover.py`
(the `the private design notes` rule).
"""
from __future__ import annotations

import asyncio
import csv
import datetime as _dt
import json
import hashlib as _hashlib
import logging
import math
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_HALF_UP
from typing import Any, Container, Iterable, Mapping, MutableMapping, Optional, Sequence

from bot.core import durable, memguard, probe_config, probe_notify
from bot.core.maintenance import in_maintenance_window
from bot.core import venue_close
from bot.core.tape_paths import fold_paths, lane_tape_path
from bot.core.maker_state import (DEFAULT_LANE, MakerStateStore, account_loss,
                                  cap_line, live_sibling_lanes)
from bot.core.redact import safe_exc
from bot.core.money import ONE as _ONE, ZERO as _ZERO, parse_wire
from bot.core.safety import is_paused
from bot.core.venue_time import iso_to_ts
from bot.poly_us.client import (PolyUSClient, PreSendRefusal, parse_book_stats, touch_from_md,
                                transact_age_s)
from bot.poly_us.feed import RECONNECT_ON_REJECT_MIN_S

#: WS book-source freshness ceiling: a WS book whose transactTime content-age exceeds this at
#: decision time is not trusted — the maker fresh-REST-re-reads that one book.
#: ⚠️ A FROZEN-book timeout, NOT an update-cadence gate: 15 s wrongly capped moderately-active
#: books updating slowly. An age-stratified accuracy probe agreed in every stratum, so the
#: supported band sits under the feed's own slate-wide reconnect watchdog; 120 is mid-band, not ceiling.
WS_BOOK_STALE_S = 60.0  # placeholder — production value withheld
#: ⛔ C-a CAP: max fresh-REST re-reads per cycle triggered by stale/absent WS books. A PARTIAL
#: feed freeze would otherwise re-read the whole slate in one cycle — the exact per-book poll
#: burst the WS feed exists to remove. Over the cap the remaining stale books are SKIPPED that
#: cycle (safe: no quote on an unverified book), logged loudly, and counted on the cycle tape.
WS_STALE_REREAD_MAX = 4
#: ⛔ COLD-START SEED. ⚠️ READ THE MEASUREMENT BEFORE TOUCHING THIS: "a slate wider than
#: `WS_STALE_REREAD_MAX` always books a structural feed death" is REFUTED — `prepare()`'s own
#: tick sweep is one paced round-trip per book, so the socket has ~N/max_req_per_s seconds to
#: deliver first frames and a 100-book shadow quoted 99/100 on cycle 1 with `requests=0`.
#: The residual class is SLOW FIRST FRAMES (measured 15–105 s per book on the one run where a
#: cold-start death fired), so the bound is sized off that tail: 120 s covers it with margin.
#: ⚠️ A BOUND, NOT A TARGET, and never a gate: a warm cache exits on the FIRST poll at zero cost,
#: it places no order and spends no request, and a book still unservable at the bound simply
#: starts REST-served. 0 disables the wait entirely.
WS_SEED_TIMEOUT_S = 60.0  # placeholder — production value withheld
#: How often the seed loop re-asks the servability question. Cache reads only — no request, no
#: lock — so this is a latency knob only.
WS_SEED_POLL_S = 0.1
#: ⛔ RECEIPT FRESHNESS. A Poly `transactTime` is the book's last MUTATION, so on a book nobody
#: trades it ages without bound while the book stays CORRECT — book CONTENT age cannot
#: distinguish no-news from frozen, and no tightening of WS_BOOK_STALE_S ever could. Socket-frame
#: RECEIPT can: a book is servable if its content is young OR the publisher is provably still
#: delivering on this socket (`feed.last_frame_age()`), with the periodic fresh-REST re-verify as
#: the hard bound underneath. It keys on ANY frame, so the transport's heartbeats bump the same
#: clock. 60 s is ~1.5× the websockets ping cycle (ping_interval=20/ping_timeout=20 closes a dead
#: connection in ~40 s, after which the whole-connection guard owns it), so the only thing this
#: window can span is a ZOMBIE socket (TCP open, no data) — which the re-verify bounds.
#: ⚠️ UNMEASURED: the venue's heartbeat CADENCE. If it exceeds 60 s the OR-rule is inert on a
#: silent slate — safe-direction, but a shadow must show `ws_quiet` reads actually occurring.
WS_CONN_LIVE_S = 60.0
#: The hard bound under the OR-rule. Socket liveness proves the PUBLISHER is alive, NOT that any
#: specific book's stream is still served — a silent unsubscribe is indistinguishable from a
#: quiet book. So every book is re-confirmed against fresh REST at least this often, whatever
#: the socket says. 900 s: ~0.1 req/s on a 95-book slate, and a silently-dropped book cannot be
#: quoted off a frozen cache for more than a quarter hour.
WS_REVERIFY_S = 600.0  # placeholder — production value withheld
#: ⛔ Per-cycle bound on the above. Every book seeds its clock in the same cycle, so one period
#: later they ALL come due together — unbounded that is one REST read per book, the whole-slate
#: burst the C-a cap forbids. Drain rate must exceed arrival: sustainable slate is
#: N <= MAX × WS_REVERIFY_S / requote_s; past that the oldest
#: books' interval stretches and `_reverify_backlog` logs it.
#: ⛔ THE CLOCK IS SEEDED **DUE**, NOT FRESH: `_ws_book_or_none` seeds a first-served book at
#: `now − WS_REVERIFY_S`, so its first re-verify runs on the next cycle it is served. Seeding it
#: fresh gave every book 900 s of UNVERIFIED quoting per run and disarmed the only per-book
#: freeze detector for exactly that window.
WS_REVERIFY_MAX_PER_CYCLE = 2
#: Gap between the startup touch reads (`read_touches`), one cache-busted ORIGIN book read each.
#: Only the books the WS cache cannot serve reach it (2026-09-14), so on a WS slate it is usually 0.
#: ⛔ OBSERVED: an unpaced startup sweep drew a 429 before cycle 1; that observation establishes no
#: limiter threshold. Cost is wall time only: the sweep quotes nothing and places nothing.
STARTUP_TOUCH_PACE_S = 5.0  # placeholder — production value withheld
#: Verdicts from `_ws_rest_touch_compare` — the ONE place a ws-vs-REST touch disagreement is
#: classified, shared by the §7-4 audit and the periodic re-verify.
_WS_CMP_UNAVAILABLE = "unavailable"   # the REST read failed / returned no book — no verdict
_WS_CMP_AGREE = "agree"               # touch prices match at the SAME transactTime
_WS_CMP_AGREE_STALE_TT = "agree_stale_tt"  # prices match but tt differs: correct right now,
                                      # but NOT proof the stream is alive
_WS_CMP_MOVED = "moved"               # prices differ AND transactTime differs — benign motion
_WS_CMP_DIVERGED = "diverged"         # prices differ at the SAME tt — the fresh-but-WRONG class
#: ⛔ CONSECUTIVE stale-cache re-verifies on the SAME book before it is treated as WS-DEAD
#: (REST-only until a fresh snapshot arrives). A stale cache on the QUIET arm is the
#: silent-unsubscribe SIGNATURE; one hit can be a race, N in a row cannot. The clock is left
#: unrefreshed on a hit so the book re-checks EVERY cycle — 3 is ~3 requote intervals, not 3
#: re-verify periods.
WS_STALE_CACHE_DEAD_N = 3
#: WHOLE-CONNECTION loss → bounded REST fallback, then halt. While the WS socket is down the
#: maker quotes a REDUCED set over REST for at most this window; still down at expiry, the run
#: HALTS into the ordinary teardown. Fallback must never become steady state: REST for the full
#: slate is the poll burst the WS feed exists to remove.
WS_FALLBACK_WINDOW_S = 120.0
#: The reduced set: ALL held-inventory books first (never dropped — an unquoted held book can't
#: work down), then the slate in sorted order up to this many total. Sized to the REST-safe
#: budget (~8–12 books/cycle at the deployed pace).
WS_FALLBACK_MAX_BOOKS = 1  # placeholder — production value withheld
#: Near the exposure cap the fallback window shrinks to WS_NEARCAP_GRACE_S — a maker that cannot
#: see the book near its risk limit rides the REST-verified reduced set briefly, then stops; it
#: never quotes off a stale cache.
#: ⚠️ ONE fraction, TWO axes: `ws_near_cap` (contracts) and `ws_near_cap_notional` (dollars).
#: run_cycle ORs them, so this threshold governs both and the OR can only tighten the rail.
WS_NEAR_CAP_F = Decimal("0.7")
#: NEAR-CAP GRACE. The venue bounces the WS socket in normal operation (2 deaths in ~10 h, both
#: reconnected in <10 s), and the zero-window rail turned an 8-second blip into a 12-hour halt.
#: Close-code classification is REFUTED by that pair — the unclean death was as benign as the
#: clean one — so the grace keys on RECONNECT OUTCOME: near cap a dark feed force-enters the
#: bounded REST fallback IMMEDIATELY (no blip tolerance: the blip path serves a possibly-stale
#: ws cache, the exact exposure this rail forbids), and the halt fires only if the episode is
#: still dark after this many seconds. 60 s = observed reconnect + the probation→recovery ladder.
#: ⚠️ Direction: the grace can only DELAY the same halt; nothing quotes unverified.
WS_NEARCAP_GRACE_S = 0.0  # placeholder — production value withheld
#: Graced near-cap episodes allowed per run [review CONCERN 3]: recovery clears
#: `_ws_down_since`, so a flapping socket earns a fresh grace each episode — bounded here.
#: Past this many, the pre-grace rail returns (first dark cycle near cap halts).
WS_NEARCAP_GRACE_EPISODES = 3
#: PROBATION CHURN BOUND. A persistent PARTIAL freeze alternates probation-place /
#: relapse-cancel every 2 cycles, each pair forfeiting the dropped books' queue position and
#: spending one un-C-a-bounded REST read per dark book. After this many RELAPSES in one episode
#: the maker rides the reduced set to the window expiry. ⛔ RELAPSES, not attempts: a probation
#: that SUCCEEDS ends the episode, so counting attempts would burn the budget on the recoveries
#: this exists to allow. Direction is safe (fewer places, never more).
WS_PROBATION_MAX_RELAPSES = 2
#: How many requote intervals a book's last touch may age before it stops counting as a MARK
#: [mm-review 2026-08-20]. Marks feed the near-cap NOTIONAL rail and the fallback ordering; an
#: expired one is DROPPED, and unmarked already means <n>/contract — the conservative side.
MARK_STALE_CYCLES = 10


def ws_fallback_slugs(inventory: Mapping[str, Decimal], ticks: Mapping[str, Any],
                      cap: int = WS_FALLBACK_MAX_BOOKS,
                      sizes: Mapping[str, int] | None = None,
                      marks: Mapping[str, Decimal] | None = None) -> list[str]:
    """The reduced quote set for a WS outage: every held book (|inv|>0) first — ALL of them,
    even past `cap`, because an unquoted held book cannot work down — then the remaining slate
    by configured size DESC, up to `cap` total.

    ⛔ LIMITS ONLY THE QUOTE-LOOP ITERATION — callers must never prune `self.ticks`/
    `self.inventory` with this: the reconciler and teardown flatten iterate those, and shrinking
    them blinds teardown to a held book.

    ⛔ HELD BOOKS ARE ORDERED BY MEASURED EXPOSURE. MEMBERSHIP IS UNCHANGED — no held book is
    ever dropped — but order is the only bound available once `held > cap`: the cycle paces
    after every market and can be cut short by a halt, so the most-exposed book is quoted FIRST.
    `marks` is the cycle's own touch mid NET of our resting orders; a book with NO mark is
    valued at <n>/contract, the same conservative convention `ws_near_cap_notional` uses, so
    it sorts EARLY rather than vanishing. Ties break alphabetically for determinism."""
    # Intersect with the SLATE: a carried book whose tick read failed at prepare() is in
    # inventory but NOT in ticks, and `_quote_market` KeyErrors on it. Teardown still sees it.
    def _exposure(slug: str) -> Decimal:
        """Dollars at risk in this book. An unmarked book is priced at <n>/contract, so the
        unit is DOLLARS on every branch — the two arms are not mixing contracts with dollars,
        they are applying the conservative price to a book we could not price."""
        qty = Decimal(inventory[slug]).copy_abs()
        return qty * (marks or {}).get(slug, _ONE)

    held = sorted((s for s, q in inventory.items() if q != 0 and s in ticks),
                  key=lambda s: (-_exposure(s), s))
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
    predicate for a WS loss. ⚠️ The None/0 branch is defensive only, and this is a CONTRACTS
    rail beside the dollar loss cap, not the 'risk limit' itself."""
    if not max_total or max_total <= 0:
        return False
    return gross_exposure >= Decimal(max_total) * frac


def _cap_weighted_mark(caps: Mapping[str, int], cap_marks: Mapping[str, Decimal]) -> Decimal:
    """p̄ — the cap-weighted average price of the capped slate, over the MARKED books ONLY, and
    <n> when none of them is marked.

    ⛔ ONE FORMULA, TWO CALLERS: this is the denominator of `ws_near_cap_notional` (where the
    axis trips) AND the number `near_cap_trip_contracts` prints to the operator (where we TOLD
    them it trips). Two copies diverged the moment a capped book went unmarked. Any change here
    moves both.

    Unmarked books are valued AT p̄ rather than <n>, which is the same as leaving them out: a
    book priced at the mean cannot move the mean, so an unreadable book can never RAISE the bar
    for the books we can price."""
    marked_qty = sum((Decimal(c) for s, c in caps.items()
                      if c > 0 and s in cap_marks), _ZERO)
    if marked_qty <= _ZERO:
        return _ONE                                      # nothing priceable: contract fraction
    return sum((Decimal(c) * cap_marks[s] for s, c in caps.items()
                if c > 0 and s in cap_marks), _ZERO) / marked_qty


def ws_near_cap_notional(inventory: Mapping[str, Decimal], caps: Mapping[str, int],
                         marks: Mapping[str, Decimal], max_total: Optional[int],
                         frac: Decimal = WS_NEAR_CAP_F,
                         cap_marks: Optional[Mapping[str, Decimal]] = None) -> bool:
    """The NOTIONAL axis of the near-cap rail. A contract is not a unit of risk: one at <n>
    ties up nineteen times the capital of one at <n>.

        p̄     = Σ cap_s·p_s / Σ cap_s          (cap-weighted average price, MARKED books only)
        frac  = Σ |inv_s|·p_s / (max_total · p̄)

    ⛔ THIS DOES TIGHTEN THE RAIL, by a slate-dependent amount. No new CONSTANT is introduced
    (the same `WS_NEAR_CAP_F` governs both axes), but the axis trips at a contract-equivalent of
    `max_total · frac · (p̄ / p_held)` — on a mixed mid-price + longshot slate (p̄ ≈ 0.35,
    inventory at 0.65) that is ≈ 0.37 of the contract cap, roughly a 2× tightening. It equals
    the contract axis ONLY under uniform pricing. The caller ORs the two, so the rail can only
    fire EARLIER; `prepare()` prints the trip point so a run plan knows where its own bar sits.

    ⛔ TWO MARK MAPS, ONE PER TERM. `marks` is the HELD-VALUE NUMERATOR and is NET of our own
    resting orders (valuing inventory at a gross mid we are the touch of inflates the headroom).
    `cap_marks` is the THRESHOLD (p̄) and stays the GROSS mid over EVERY capped book, held or
    not — a net mark there would move the threshold in the LOOSENING direction. The rule:
    **our own quote never moves the THRESHOLD.** `cap_marks=None` means "same map for both",
    correct only where they are the same map (DRY/shadow, hand-built test dicts).

    ⛔ AN UNMARKED BOOK NEVER RAISES THE BAR: it is priced at <n> (the most a contract can be
    worth) in the NUMERATOR, so an unmarkable HELD position reads as maximally expensive rather
    than vanishing — and at p̄ ITSELF in the DENOMINATOR, so a book valued at the mean cannot
    move the mean. Monotone in the safe direction: an unmarked UNHELD book leaves the bar where
    it was, an unmarked HELD one can only trip EARLIER. With NO capped book marked, p̄ = <n>
    and the axis collapses back to the contract fraction.

    Returns False when the axis cannot be evaluated (no cap, no priced caps): the contract axis
    is the backstop, and a second rail must not invent a halt out of missing data."""
    if not max_total or max_total <= 0:
        return False
    cap_qty = sum((Decimal(c) for c in caps.values() if c > 0), _ZERO)
    if cap_qty <= _ZERO:
        return False
    _cap_marks = marks if cap_marks is None else cap_marks
    p_bar = _cap_weighted_mark(caps, _cap_marks)
    if p_bar <= _ZERO:
        return False
    held_notional = sum((Decimal(q).copy_abs() * marks.get(s, _ONE)
                         for s, q in inventory.items() if q != 0), _ZERO)
    return held_notional >= Decimal(max_total) * p_bar * frac


log = logging.getLogger(__name__)

#: The venue's collection grain and its scale — see `paid_rebate_cents`.
_ONE_CENT = Decimal("0.01")
_CENTS_PER_DOLLAR = Decimal("100")

#: ⛔ THE **FALLBACK**, NOT THE RULE. `minimumTradeQty` is PER-MARKET and the spread is three
#: orders of magnitude wide across families. Generalising one family's minimum sent dust closes
#: into books whose minimum is larger — a guaranteed refusal, every teardown.
#: **The live value is read per book** by `PolyMaker.prepare()` out of the SAME metadata request
#: that reads the tick, and `PolyMaker.min_trade_qty(slug)` is the ONE reader. This constant is
#: used only when the venue omits or garbles the field, and that substitution is LOGGED per book.
#: ⚠️ The fail direction is safe both ways: too HIGH and we report a residue we might have closed;
#: too LOW and the venue refuses the order, which `_teardown_flatten` logs loudly and continues past.
MIN_TRADE_QTY = Decimal("0.01")

#: The venue statuses that mean a market has RESOLVED and paid out — the only ones under which a
#: belief with no venue position row is a SETTLED book rather than a divergence. The positions
#: endpoint DROPS a settled market, so a paid-out position read as "the venue does not corroborate
#: belief" and refused the teardown flatten.
#: ⛔ `MARKET_STATUS_HALTED` IS DELIBERATELY ABSENT: a halted market is trading-suspended, NOT
#: settled, and the venue still SERVES its position row — admitting it would retire a belief the
#: venue still holds.
#: ⛔ `MARKET_STATUS_CLOSED` IS DELIBERATELY ABSENT: it is a DISTINCT state from RESOLVED in the
#: only population that measured both, and no read in this repo records a CLOSED book PAYING OUT.
#: ⛔ `MARKET_STATUS_OPEN` and anything UNRECOGNISED (including an unreadable status) are NOT
#: settled either. Fail closed.
SETTLED_MARKET_STATUSES: frozenset[str] = frozenset({"MARKET_STATUS_RESOLVED"})

# ── the constants that are measurements ──────────────────────────────────────────────────────

#: |Θ| for the Poly US maker rebate. The credit is Θ·C·p·(1−p) per fill.
REBATE_COEF = Decimal("0.0125")

#: The venue collects commissions rounded to the NEAREST cent (measured: a <n> formula fee
#: collected as <n>, a <n> one as $0.0000). So a predicted credit below half a cent
#: collects as $0.0000 and earns literally nothing.
NEAREST_CENT_BOUNDARY = Decimal("0.005")

#: ⛔ THE SIZE FLOOR. At size 1 the rebate maxes at 0.0125·0.5·0.5 = <n> — below the <n>
#: boundary at EVERY price, so a size-1 maker fill earns zero rebate wherever it fills. Size 2
#: clears only p∈[0.28,0.72], size 3 to ~p∈[0.16,0.84], size 5 to ~p∈[0.10,0.90] — NOT
#: "everywhere". `rebate_rounds_to_zero` is the live per-price rule.
MIN_SIZE = 5
# The delayed-verify horizon for cancelled orders: past the venue's observed ~60–90s read lag,
# with margin. One re-read per cancelled order at this age books any fill the immediate read
# missed — the fill-loss class that ate <run-id> (−5 vs +5) and 2b (−12 vs 0).
LAG_HORIZON_S = 600.0  # placeholder — production value withheld
# The venue's order states an order can never trade again from — the poll's pop condition.
# PARTIALLY_FILLED and the PENDING_* family still rest or can still fill, so they are NOT here;
# REPLACED is deliberately excluded — we never send replaces, so its appearance means someone else
# acted on this account and the order must stay tracked and loud.
_TERMINAL_ORDER_STATES = frozenset({
    "ORDER_STATE_FILLED",
    "ORDER_STATE_CANCELED",
    "ORDER_STATE_EXPIRED",
    "ORDER_STATE_REJECTED",
})
# A parked order the venue keeps SIGHTING ALIVE is re-cancelled, at most this often PER ORDER.
PARKED_RECANCEL_MIN_S = 30.0
# …and at most this many RE-CANCELS across ALL parked orders per poll — the bound that holds.
# ⛔ THE PER-ORDER BOUND IS NOT A RATE LIMIT. A sighted-alive entry is never retired, so the
# parked-alive set is bounded by the run: at ~1,200 retained entries a 30 s per-order bound still
# authorises ~40 req/s against a 20 req/s budget, which `_pace` absorbs as quote latency — it
# degrades the fire path instead of erroring. Oldest ghosts go first.
# ⚠️ THIS BOUNDS THE RE-CANCELS ONLY, NOT THE RETAINED SET'S VERIFY READS.
# `_retry_pending_reconciles` spends ONE `get_order` per due entry, uncapped and serial inside the
# cycle, so the same regime is ~1,200 serial reads per 30 s. That STALLS the quote cycle rather
# than breaching the venue budget (the safe direction), but it is unbounded — a per-poll verify
# budget with the same skip-keeps-counting property is the fix if it is ever observed.
PARKED_RECANCEL_MAX_PER_POLL = 8
#: Consecutive `poll_fills` listings an unknown order must appear in before the ORPHAN rail adopts
#: and cancels it. ⛔ THE DEBOUNCE IS THE WHOLE SAFETY ARGUMENT, and it is about a
#: CREATE, not about the venue: an id enters `self.resting` only when `_place` returns, so any
#: listing read taken while a create is unresolved shows an order we are about to know about. That
#: window is structurally empty today (one task, no `gather`); the debounce keeps it empty if that
#: changes, and cancelling on a FIRST sighting would kill the quote we just placed.
ORPHAN_MIN_SIGHTINGS = 2
VERIFY_RETRY_S = 30.0        # unreadable at verify time → try again this much later
VERIFY_MAX_ATTEMPTS = 5      # then queue for ACTIVITIES RECOVERY — never a silent drop
# The TEARDOWN verify pass reads every parked order at once (`ignore_due`); an unpaced burst of
# reads drew a CDN ban that blinded the sweep, the flatten and the verify reads. Paced.
TEARDOWN_VERIFY_PACE_S = 1.0  # placeholder — production value withheld
# …and the pacing is BOUNDED by wall time across the whole teardown pass
# the phase runs up to 4× over as many as MAX_PARKED entries, and every paced
# second is a second the flatten waits on a halt. Past the budget the reads go unpaced.
TEARDOWN_VERIFY_PACE_BUDGET_S = 10.0
MAX_PARKED = 64              # a venue outage must not turn the parked set into a request leak
# ── belief-recovery registered numbers ──
#: Cancel-probes per quote cycle, oldest-first: the correlated-purge case (66 sides at once)
#: drains at 2/cycle rather than stretching one cycle into a stale-book quote decision.
PROBE_MAX_PER_CYCLE = 2
#: More than this many DISTINCT orders answering not_found in ONE poll WHILE the listing still
#: carries them (two endpoints contradicting) is a client/route fault — probes and retirements
#: refuse and the recovery queue holds. ⛔ Only CONTRADICTED not_founds count: listing-corroborated
#: ones are a genuine purge, and the any-not_found form measured 0% precision live, blocking its
#: own remedy.
NOT_FOUND_BREAKER = 5
#: Queue residence past terminal_ts without coverage → UNRESOLVED. Never guess.
RECOVERY_MAX_S = 300.0
#: The walk's oldest-edge skew margin: coverage means the walk's oldest trade createTime is at
#: least this far before the order's placed_ts (measurable later from the 433 taped pairs).
COVERAGE_SKEW_S = 60.0
# How often the maker asks the VENUE what it holds. One request per interval, against a drift
# that took minutes to reach several times the cap. [belief divergence]
RECONCILE_EVERY_CYCLES = 2  # placeholder — production value withheld
#: How often a REAL run re-runs the LAUNCH-TIME verify over the whole slate [continuous maker
#: P1b]. The hourly-window maker got an independent venue read (positions + open orders, compared
#: against belief) at every start; a day-long child would get exactly one in 14 hours, so it is
#: put on a clock at the same cadence the windows had. ⛔ NOT `RECONCILE_EVERY_CYCLES`: that one
#: refreshes the maker's OWN restriction map (`venue_inventory`) and can only tighten sizing,
#: while this one is the independent CROSS-CHECK — a disagreement is a page and a stand-down,
#: which is far too expensive to spend on the sub-minute belief races the fast cadence sees.
#: Two reads per hour.
VERIFY_S = 3600.0
#: How many CONSECUTIVE verify reads must agree that a book disagrees before it goes reduce-only
#:. ⛔ NOT 1: this endpoint serves divergent replicas (belief and
#: the venue often disagree — see `_venue_breach`) and the ORDER listing lags a reprice
#: by up to `LAG_HORIZON_S`, so a single read holds a healthy book down for a whole hour on an
#: ordinary requote. The confirming read runs on the NEXT CYCLE, not the next hour — the same
#: re-read-before-you-act discipline `_venue_breach` uses before a halt.
VERIFY_CONFIRM_READS = 1  # placeholder — production value withheld
#: How stale a recorded two-sided touch may be and still SEED an empty DECLARED non-sports book
#: [continuous maker P1b]. Four hours = the venue's documented maintenance blackout, so a touch
#: recorded before the window is still usable at the reopen. ⚠️ The VENUE documents that books
#: reopen empty; our own tape has not yet observed a venue-side empty (four windows unused), so
#: the book-time this rail addresses is UNMEASURED — the first real night's `no_two_sided_touch`
#: count on the declared seats is the read. ⛔ AND AGE IS NOT THE ECONOMIC RAIL: the adverse
#: markout of seeded fills concentrates in 5-minute empties no age gate excludes, which is why
#: `SEED_SIZE_MAX` exists. Older → the book keeps skipping, exactly as it did before.
EMPTY_BOOK_TOUCH_MAX_S = 4 * 3600.0
#: Contracts per side a SEEDED quote may rest, whatever the seat's size [mm-review 2026-09-10].
#: The ceiling bounds every declared seat's short-horizon |Δmid| exposure, and the adverse markout
#: of seeded fills concentrates in short empties that no age gate excludes — so SIZE, not age, is the rail that bounds this.
#: ⛔ A CEILING, never a size: it is applied as a `min` after every other clamp has voted.
SEED_SIZE_MAX = 1  # placeholder — production value withheld
# Conditional order poll: when armed, an order PRESENT in the open-orders listing with an UNCHANGED
# cumQuantity skips its per-order read-back — 2 of every 3 maker requests, ~99.9% redundant. Every
# Nth poll is a forced FULL read so a stale listing can defer a fill by at most N polls, never lose
# it. Listing ABSENCE never skips: the listing carries no pagination token, so a silent
# server-side cap cannot be ruled out.
CONDITIONAL_POLL_FULL_EVERY = 6
# A venue read older than this is announced, not obeyed silently — a restriction that outlives
# its evidence quietly costs fills forever.
VENUE_STALE_AFTER_S = 300.0

#: Poly's documented limit is 20 req/s per key. This is the self-imposed ceiling the cycle paces
#: to, deliberately well under it: over-limit surfaces BOTH ways — throttling AND real 429s — so
#: the symptom is a late, stale 200, the worst possible failure for a quote decision.
#: Paced well under the documented limit: the venue trips on short bursts, so the ceiling is
#: per-second, not per-minute, and it leaves the reseat and the operator tools room.
DEFAULT_MAX_REQ_PER_S = 0.5  # placeholder — production value withheld
VENUE_REQ_PER_S = 20.0

# ── GTD order-TTL (the venue-side dead man) ──────────────────────────────────────────────
# OFF by default (`--order-ttl-s 0`). When armed, every create carries ONE LONG `goodTillTime`, so
# a maker that dies (SIGKILL, OOM, a severed box) leaves orders that self-delete. The teardown
# flatten's closes carry their own short deadline (`_flatten_ttl_s`) regardless of the flag.
#
# ⛔ THERE IS NO HEARTBEAT AND NOTHING RENEWS A DEADLINE. The quote loop HOLDs whenever the desired
# quote equals the resting one — deliberately, because queue position is the thing being protected
# so replaces are rare (an order rests untouched for hours), and
# there is no age-based requote anywhere.
#
# So the deadline is a DEAD-MAN'S BRAKE, not a keepalive: long enough that a LIVE maker's orders
# are organically replaced long before it binds. Queue position is untouched precisely BECAUSE
# nothing renews.
#
# ⚠️ Nothing here may become load-bearing: enforcement is UNOBSERVED, so the TTL is a second net
# under our own cancels, never a replacement.

#: The TTL floor, enforced at startup, DERIVED FROM RESIDENCE — not from the requote cadence. The
#: binding quantity is how long an order rests untouched (hours, measured). A TTL below that
#: expires live quotes IN LIFE: the venue deletes an order we still believe rests, the tape
#: records `hold` over nothing, and the next cycle re-places at the BACK of the queue. The floor is
#: set where expiry-in-life is rare rather than routine.
TTL_MIN_S = 86400.0  # placeholder — production value withheld
#: Format tolerance on the deadline we read back vs the one we sent. The venue stamps sub-second
#: precision where we send whole seconds; anything beyond this is a real disagreement, not rounding.
TTL_STALE_TOLERANCE_S = 2.0
#: How far past its own deadline a still-RESTING order must be read before we call the venue's
#: enforcement absent. ⛔ It is `LAG_HORIZON_S` BY REFERENCE, never a second number for the same
#: venue read lag: this file already budgets that constant for exactly this lag, and a grace BELOW
#: it lets a venue that expires on time but flips its read endpoint late be recorded as having
#: broken its promise — a false venue-defect claim written into a tape, on the very probe night
#: whose purpose is to measure enforcement.
TTL_NOT_ENFORCED_GRACE_S = LAG_HORIZON_S
#: Stale/missing read-back deadlines confirmed in ONE poll before the run falls back to plain GTC.
#: One is a blip; a cluster means the venue is not holding the deadlines we send, and quoting on
#: deadlines that do not exist is strictly worse than quoting GTC and knowing it.
TTL_STALE_FALLBACK_PER_POLL = 3

#: Quote actions. Strings rather than an enum so a tape row and an assertion read the same.
HOLD = "hold"
PLACE = "place"
CANCEL = "cancel"
REPLACE = "replace"

#: ⛔ TEARDOWN ORDER IS LOAD-BEARING. Sweeping before flattening re-reads a book that the flatten
#: is about to move. `bot/kalshi/maker.py` documents this order and had it backwards in four
#: places until 2026-07-27.
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
#: ⛔ A FOURTH tape, created ONLY when `--order-ttl-s` is armed. A dedicated file rather than new
#: columns on the three above, for two reasons: an OFF-mode run must be byte-identical (a widened
#: header would freeze-rotate every existing tape on the next start of a run that does not even use
#: the feature), and an order EXPIRY is a per-order terminal event with no home on a per-cycle or
#: per-fill row. This is where enforcement quality is MEASURED continuously rather than assumed
#: from one experiment [operator addendum 2026-08-27 §3].
DEFAULT_TTL_CSV = durable.repo_path("logs", "poly_live_mm_ttl.csv")
#: ⛔ A FIFTH tape — the  counterfactual, moved OFF `log.warning` [2026-09-01]. The
#: decision line was written into the run log, which is subject to log rotation and retention no
#: tape discipline protects, so "accumulate n≥20 and then read it" meant grepping unrotated run
#: logs and could silently stop growing [the private design notes §3.5;
#: the private design notes §A-2]. A dedicated file, for the TTL tape's reasons:
#: a breach is a per-EPISODE terminal event with no home on a per-cycle or per-fill row, and
#: widening an existing header would freeze-rotate a tape every consumer folds.
DEFAULT_ADVEXIT_CSV = durable.repo_path("logs", "poly_live_mm_advexit.csv")

# ⛔ COLUMN SEMANTICS, UNITS AND BLANK MEANINGS LIVE IN the private design notes — one line each
# here, and the dictionary is the owner. `run_id` is the LAST column on all three tapes:
# append-last keeps the name-keyed DictReader consumers safe across a schema change.
_QUOTE_HDR = ["ts", "cycle", "slug", "tick", "bid", "ask", "my_bid", "my_ask", "improved",
              "action_bid", "action_ask", "queue_ahead_bid", "queue_ahead_ask",
              "inventory", "allow_bid", "allow_ask", "status", "read_ms", "sample_gap_s",
              # Free book-stats: the CUMULATIVE counter, so Δ between rows is flow. LOGGING-ONLY
              # floats (nothing quotes, sizes or gates off these). BLANK = venue omitted stats.
              "shares_traded", "last_trade_px", "last_trade_qty", "last_trade_age_s",
              # The touch NET of our own resting size (`net_of_self_touch`) — the self-clean
              # per-cycle baseline; equals the raw touch whenever we rest nothing.
              "net_bid", "net_ask",
              # Which book served this cycle; partitions the tape for the ws-vs-REST accuracy
              # read. Calibrate ws accuracy on {ws, ws_quiet} only — the rest ARE the REST book.
              "book_src",
              # my_*_size = what THIS cycle intends to rest AFTER the cooldown reducer;
              # resting_*_qty = the belief REMAINDER at the END of the cycle (post-action).
              # ⚠️ THE ROW MIXES TWO SNAPSHOTS: `net_*` is PRE-action, `resting_*_qty` POST-action
              # — same expression, different instants. Never reconstruct one from the other.
              # ⚠️ A BELIEF, not a venue read, and in SHADOW/DRY these describe orders that never
              # reached the venue.
              "my_bid_size", "my_ask_size", "resting_bid_qty", "resting_ask_qty",
              # One `QUOTE_REASONS` value per side, on EVERY row incl. skips. Read
              # `quote_reason`'s docstring before grouping — blank here is a WRITER BUG.
              "reason_bid", "reason_ask",
              # ⛔ EXISTS BECAUSE `status` CANNOT ANSWER THE QUESTION: `adverse_latch` is fourth
              # in the precedence chain and skips are written before the chain runs at all. Read
              # off the latch map directly, so it is INDEPENDENT of that chain. A STATE flag,
              # not a duration.
              "latch_active",
              # What `place_limit_gtc`'s crossing guard SAW on this cycle's last place/replace —
              # a SECOND, INDEPENDENT book from `bid`/`ask`. ⚠️ ONE ROW CARRIES ONE GUARD READ
              # (the last placement), so never join these columns to a specific side.
              "guard_bid", "guard_ask", "guard_age_s", "guard_src", "guard_refused",
              # What the rebate-step rule reached on this book this cycle; join `my_*_size` for
              # which side. ⛔ `sized:N` is NOT a claim that the fill earned anything — the venue
              # rounds PER FILL INCREMENT and the increment is the TAKER's choice.
              "rebate_step",
              # The WS witness that overrode a stale-REST cross (`guard_src=ws_over_stale_rest`
              # rows only). Pair `guard_ws_age_s` with `guard_age_s` — the override is auditable
              # only as the two ages together.
              "guard_ws_bid", "guard_ws_ask", "guard_ws_age_s",
              # Same `_parse_book_stats` block as the flow columns, no extra venue read.
              # `open_interest` is a LEVEL, not a counter: Δ is creation/redemption, not flow.
              "open_interest", "oi_age_s",
              # Which quote mode the RUN ran (`improve`/`join`), stamped per row so the A/B's
              # (run × book) clusters form from the quote tape alone. It says which MODE was
              # armed, never whether this row improved — that is `improved`.
              "quote_mode",
              # WHICH gate silenced an otherwise-healthy cycle: written ONLY on a `status=ok`
              # row with BOTH sides disallowed, blank everywhere else (every other stand-down
              # already names itself in `status`). DERIVED from flags the quote path already
              # computed — it decides nothing.
              "hold_cause",
              # ── I1/I2 OBSERVABILITY the private design notes. ⛔ EVERY
              # COLUMN BELOW IS TAPE-ONLY: no branch in this module reads one, and quoting is
              # byte-identical without them. BLANK MEANS NOT OBSERVED — NEVER ZERO, on all five.
              # I1: the belief `_reach_permits` reads (`self.inventory`), signed contracts, at
              # the instant this row is written — NOT `inventory` above, which is the value the
              # caller snapshotted earlier in the cycle; a REPLACE's own cancel-reconcile can
              # move the belief between the two, and that difference is the point.
              # `venue_position_src` is the `booked_via` of the last fill that moved it
              # (ws/poll/verify/activities/cross), or `carry` while nothing has moved it.
              "venue_position", "venue_position_src",
              # I2: the last create ACK span on this book since the previous row (the client's
              # `last_ack_lag_ms`, ms), the last cancel read-back verdict on this book since the
              # previous row, and the `_unresolved_exposure` term `_reach_permits` actually
              # evaluated (GROWTH branch only — the shrink exemption returns before that term,
              # and then this is blank, which is NOT the same as the term being 0).
              # ⚠️ ONE ROW CARRIES ONE OF EACH, from whichever SIDE produced it last — never
              # join these to a specific side, exactly as for the `guard_*` cells.
              "ack_lag_ms", "cancel_readback", "unresolved_exposure",
              # ⛔ THE HOT-SLATE EPOCH [continuous maker P0]. `self.slate_epoch`, 0 at launch and
              # +1 on every applied slate file — the join key that partitions a day-long child's
              # tape into the book sets it actually ran. NEVER BLANK: a real 0 is written, and a
              # blank cell means a pre-P0 tape era.
              "epoch",
              "run_id"]


def _guard_cols(seen: Optional[dict]) -> tuple[str, str, str, str, str]:
    """The five `guard_*` cells for one quote row.

    `None` (no placement this cycle, or a pre-send refusal that never reached the guard's book
    read) tapes five blanks — the documented "the guard did not run" shape. Within a read, each
    field is independently blank when the guard itself could not read it, exactly like the
    `bid`/`ask` columns above: blank is "not readable", never the string "None" and never 0."""
    if not seen:
        return ("", "", "", "", "")
    age = seen.get("age_s")
    return (
        "" if seen.get("bid") is None else str(seen["bid"]),
        "" if seen.get("ask") is None else str(seen["ask"]),
        "" if age is None else f"{age:.1f}",
        str(seen.get("src") or ""),
        "Y" if seen.get("refused") else "N",
    )


def _guard_ws_cols(seen: Optional[dict]) -> tuple[str, str, str]:
    """The three `guard_ws_*` cells [newer-witness rule 2026-09-03].

    Blank unless this placement was an OVERRIDE: the client only records a witness on the
    `ws_over_stale_rest` path, so "no witness" and "the guard did not run" are the same three
    blanks here — `guard_src` is what separates them."""
    if not seen:
        return ("", "", "")
    ws_age = seen.get("ws_age_s")
    return (
        "" if seen.get("ws_bid") is None else str(seen["ws_bid"]),
        "" if seen.get("ws_ask") is None else str(seen["ws_ask"]),
        "" if ws_age is None else f"{ws_age:.1f}",
    )


_CYCLE_HDR = ["ts", "cycle", "wall_s", "requests", "req_per_s", "markets_total", "markets_quoted",
              "markets_skipped", "missed_cycles", "max_staleness_s", "actions", "paused",
              "rss_mb", "avail_mb", "swap_free_mb", "ws_stale_rereads", "ws_capped",
              "ws_outage",
              # ⛔ On a frame-alive socket the C-a stale-re-read path is UNREACHABLE for a quiet
              # book, so the periodic re-verify is the only per-book freeze detector left.
              # `ws_reverify_stale_cache` > 0 is the SILENT-UNSUBSCRIBE signal; read it beside
              # `ws_reverifies` (0 re-verifies means the bound was UNEXERCISED, not clean).
              "ws_reverifies", "ws_reverify_stale_cache",
              "ws_down_s", "halt_reason",
              # Books on THIS run's slate currently latched — the per-cycle roll-up of the quote
              # tape's `latch_active`; a real 0 is written, never blank. Off-slate latches are
              # EXCLUDED. A CONVENIENCE for spotting a latch storm; per-book truth is the quote
              # tape.
              "latched_books",
              # In-run ORPHAN rail — RUN-CUMULATIVE, not per-cycle: read as a step function and
              # diff adjacent rows for "when". Nonzero `orphans_seen` = the venue held an order
              # this process never recorded. `orphans_reported` needs a HUMAN: a listed order we
              # could not attribute and did not adopt, so its exposure is UNCOUNTED for the rest
              # of the run. Taped here rather than on the TTL tape because that tape is not
              # written at all without `--order-ttl-s`.
              "orphans_seen", "orphans_cancelled", "orphans_reported",
              # ⛔ THE HOT-SLATE EPOCH + THE CADENCE THAT CYCLE RAN [continuous maker P0]. Both
              # are the LIVE values (`slate_epoch`, `requote_s`), never the launch line's: a
              # day-long child re-registers its cadence arm at every reseat, so the cadence
              # corpus keys on this pair and not on the manifest's single `requote_s`.
              # Never blank; a blank cell is a pre-P0 tape era.
              "epoch", "requote_s",
              "run_id"]
# ⚠️ `commission_order_total` and `predicted_rebate_on_cum` are per-ORDER quantities on a
# per-INCREMENT row — see `Fill`. Total them by taking the LAST row per `order_id`, never by
# summing the column.
_FILL_HDR = ["ts", "slug", "side", "order_id", "price", "size", "filled_qty", "cum_filled_qty",
             "commission_order_total", "commission_verdict", "predicted_rebate_on_cum",
             "queue_ahead", "time_to_fill_s", "improved", "order_state",
             "mid_at_fill", "mid_age_s", "avg_px", "late_booked", "booked_via",
             "best_bid", "best_ask", "book_src", "net_bid_place", "net_ask_place",
             # BOOKING-instant touch, that frame's content age, and the venue-measured
             # trade→booking lag — ⛔ NOT a fill-instant mark; most rows are booked by a REST
             # poll one or more requote intervals after the trade.
             "booking_bid", "booking_ask", "booking_touch_age_s", "fill_stamp_lag_s",
             "venue_ts",
             # NET-OF-SELF booking touch: most improved-fill stamps were OUR OWN quote's width,
             # so the self-inclusive columns above cannot answer the improve-profit question.
             # Blank in shadow/DRY, when late_booked, and when the net side empties.
             "booking_net_bid", "booking_net_ask",
             # ADVERSE-LATCH RULE INPUTS — populated on the ONE probe-lane fill whose round trip
             # armed a latch, blank on every other row and every other lane. `latch_rule` is the
             # assignment MECHANISM behind `latch_ban_s` (drawn arms are independent of severity;
             # the retired `sev_mult` ladder was not), and is ABSENT entirely on pre-2026-09-01
             # tapes — which is how the eras are told apart.
             "latch_hs", "latch_hs_src", "latch_ratio", "latch_ban_s", "latch_fallback",
             "latch_rule",
             # Was the private order/fill WS subscribed when this increment was booked — 1/0,
             # DELIBERATELY not the tape's Y/N spelling: this column exists to be averaged into
             # a per-fill blind duty cycle, and 1/0 is the form that arithmetic reads.
             "feed_subscribed",
             # THE REQUOTE-AWARE FILL CLOCKS — `time_to_fill_s` restarts on every REPLACE and so
             # cannot separate "we requoted into a fill" from "the market came to us";
             # `order_rest_s` is the honest per-order age, `price_rest_s` the side's continuous
             # time AT THIS PRICE. Blank means CANNOT-DATE, never 0.
             "order_rest_s", "price_rest_s",
             # ⛔ THE HOT-SLATE EPOCH THIS FILL WAS BOOKED UNDER [continuous maker P0]. The
             # epoch→arm map lives in the supervisor journal, so this cell is what lets a fill
             # be attributed to the cadence/size arm the book was seated with. Never blank.
             "epoch",
             "run_id"]

#: ONE row per TTL-relevant observation. `run_id` last, like the other three tapes.
#:   event  — the vocabulary is CLOSED and each value is a different claim:
#:     `stamped`        a create went out carrying this deadline (the denominator).
#:     `expired`        the VENUE said so. The only value that is evidence of enforcement, and
#:                      `lag_s` is an UPPER bound bounded by our poll cadence, never a
#:                      venue-measured latency.
#:     `purged_past_deadline`  vanished from the order store past its deadline. CONSISTENT with
#:                      expiry, never proof of it; changes NO retirement decision.
#:     `stale_deadline` the venue is not holding our deadline. We cancel; the next cycle
#:                      re-places with a fresh one.
#:     `not_enforced`   ⛔ past deadline + grace and STILL RESTING — the venue accepted the field
#:                      and does not act on it. TTL mode is disabled for the rest of the run.
#:     `disabled`       TTL mode turned itself off; `verdict` carries why.
#:   `lag_s` = observed − deadline (blank when either is unknown).
_TTL_HDR = ["ts", "event", "slug", "side", "order_id", "deadline_ts", "observed_ts", "lag_s",
            "state", "verdict", "run_id"]

#: The  counterfactual, ONE ROW PER MARK-TRIPWIRE BREACH (breaches only, never per
#: cycle). Column semantics: the private design notes.
#:   resolves_at    the epoch this run was given, or BLANK. ⛔ Blank is an ABSENCE and `reason`
#:                  says which — never 0, never "now".
#:   ttr_h          hours to that instant, signed. NEGATIVE is a real state (the stamp is 00:00Z
#:                  of the slug's EVENT date, a LOWER bound), not a parse error.
#:   side           `bid` = a long exits into the bid; `ask` = a short covers at the ask.
#:   crossable_px   the touch price that exit would take; `depth` its displayed qty.
#:   would_cross    `YES` only when the book resolves inside 24 h AND displayed depth covers the
#:                  whole position. ⛔ DATA ONLY: this maker is `post_only=True` always and has
#:                  no taker path — a YES is evidence, never an instruction.
#:                  ⛔ ON THE PROBE LANE `YES` IS UNREACHABLE BY CONSTRUCTION (in-play books whose
#:                  event-date stamp is already PAST, so the `0 < ttr < 24 h` limb cannot hold),
#:                  SO A FUTURE "n>=20 WITH 0 YES" IS NOT EVIDENCE AGAINST CROSSING.
#:   reason         blank when a resolve-at was supplied; see ADVEXIT_UNDATED / ADVEXIT_NO_MAP.
_ADVEXIT_HDR = ["ts", "slug", "resolves_at", "ttr_h", "side", "crossable_px", "depth", "inv",
                "would_cross", "reason", "run_id"]
#: ⛔ TWO NAMES, DELIBERATELY: `undated` is a property of the BOOK (undatable slug),
#: `no_resolves_at` of the LAUNCH (the flag was never passed). Collapsing them would make the
#: degenerate era indistinguishable from an undatable book on the same tape.
ADVEXIT_UNDATED = "undated"
ADVEXIT_NO_MAP = "no_resolves_at"


def gtd_stamp(epoch_s: float) -> str:
    """Epoch seconds → the RFC-3339 form the venue echoed verbatim in the T5 preview probe:
    WHOLE SECONDS, UTC, `Z` suffix. The fraction is TRUNCATED, not rounded — a rounded stamp can
    land a hair in the future of the deadline we recorded locally, and the comparison that
    decides "did the venue keep our deadline" must never be the thing that invents a
    disagreement.
    """
    return _dt.datetime.fromtimestamp(
        int(epoch_s), _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class ShadowViolation(RuntimeError):
    """A shadow run reached the placement path.

    Raised rather than returned. A shadow run's whole value is that "placed nothing" is
    structural, and a soft return leaves that guarantee one refactor away from being wrong.
    """


# ── pure helpers ─────────────────────────────────────────────────────────────────────────────

def predicted_rebate(price: Decimal, size: Decimal | int) -> Decimal:
    """The documented Poly US maker credit for `size` contracts at `price`, as a POSITIVE
    magnitude. Decimal throughout: this is compared against a <n> rounding boundary, and a
    float's binary error there is a wrong verdict, not a rounding nit.

    ⚠️ This is the UN-ROUNDED formula. The venue collects the credit rounded to the nearest cent
    PER FILL INCREMENT (measured: <n>→<n>, <n>→<n>, <n>→<n>), so the realised
    rate is a step function of order size and the modelled daily figure is an upper bound.
    ⛔ THE ROUNDING IS PER FILL INCREMENT, NOT PER ORDER, and the increment is chosen by the
    TAKER, not by our resting size — so `min_rebate_size` is NECESSARY, NOT SUFFICIENT, and is
    not a guarantee that a fill earns anything. A material share of fills forfeit the rebate to
    sub-size increments. Screen books on the counterparty's increment distribution, not on our size alone.
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


def paid_rebate_cents(price: Decimal, size: Decimal | int) -> int:
    """What the venue actually PAYS for one fill increment of `size` at `price`, in whole cents.

    The venue collects the maker credit `0.0125·p·(1−p)·C` rounded to the NEAREST cent per FILL
    INCREMENT (venue-reference skill § maker rebate; measured exact by fill —
    <n>→<n>, <n>→<n>, <n>→<n>). Nearest cent, ties UP: the <n> boundary is
    the same one `rebate_rounds_to_zero` calls dead below and paid at, so both read the rule
    one way. ⛔ The one rounding helper — no caller re-implements this quantize.
    """
    paid = predicted_rebate(price, size).quantize(_ONE_CENT, rounding=ROUND_HALF_UP)
    return int(paid * _CENTS_PER_DOLLAR)


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
    than guessing. [per-book sizing, 2026-07-30]
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
        if not slug:                              # phantom missing slug [review N2]
            raise ValueError(f"--size {part!r}: want slug:size")
        if slug in sizes:
            # Last-wins would silently drop the first value — the one shape of this spec that
            # produces a wrong size with no error and no visible trace. [review N1]
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
    after 3 fills at size 10 but 30 fills at size 1, and that difference — not any real change in
    risk appetite — was the entire fake 23× size ramp
    (`the private design notes`). In fills, "3" means the same amount of
    adverse selection at every size.
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


def norm_slug(slug: str) -> str:
    """The comparison key for a market slug: stripped and lower-cased.

    ⛔ Exists because a venue row key and a slate string differing only in case or whitespace
    used to produce a LOUD halt and, once the guard became lane-scoped, a SILENT SKIP.
    Normalizing makes the two match, and matching means JUDGED — the fail-closed direction."""
    return str(slug).strip().lower()


def quote_action(resting: Optional[Decimal], desired: Optional[Decimal]) -> str:
    """What to do with one side, given the price resting on the venue and the price we now want.

    ⛔ EQUAL PRICES MEAN HOLD, AND HOLDING IS THE POINT. Queue position is the dominant variable
    in this lane, and a cancel/replace at the same price gives up the
    whole queue and buys nothing. Compared as Decimals, so `0.4400` and `0.44` are the same
    price — a string compare would forfeit the queue every single cycle.
    """
    if resting is None:
        return PLACE if desired is not None else HOLD
    if desired is None:
        return CANCEL
    return HOLD if resting == desired else REPLACE


#: `quote_reason`'s closed vocabulary — the tape's `reason_bid`/`reason_ask` values.
#: ⛔ `seeded_last_touch` is NOT produced by `quote_reason` — it is written OVER its answer by the
#: empty-book seed [continuous maker P1b], because that cycle's prices came from the durable
#: touch record and not from any book this cycle read. It labels the whole row, both sides.
QUOTE_REASONS = ("hold", "place", "pull", "inventory_skew", "net_touch_move",
                 "improve_tick_rounding", "no_prior_touch", "net_unreadable", "other",
                 "seeded_last_touch")


def quote_reason(action: str, *, resting: Optional[Decimal], desired: Optional[Decimal],
                 resting_size: Optional[int], desired_size: Optional[int],
                 prev_net: Optional[Decimal], net: Optional[Decimal],
                 improved: bool) -> str:
    """WHY this side is being acted on — one value from `QUOTE_REASONS`.

    ⛔ THIS DECIDES NOTHING. It is a label written beside an action the caller has already
    chosen, and it must stay that way: the moment a reason feeds a quoting decision, the tape
    stops being a record of what the maker did and becomes part of it.

    The classification for a REPLACE, in order:
      · `inventory_skew`        — SAME price, different size: this replace is about position.
      · `net_touch_move`        — our side's NET touch moved. The expected majority.
      · `improve_tick_rounding` — net touch unchanged, price changed, and we are IMPROVING: the
                                  RAW touch (which contains our own order) can move the target
                                  while the net touch does not.
      · `net_unreadable`        — THIS cycle's net touch is None on that side. ⛔ Its own label,
                                  not `no_prior_touch`: that state is the IMPROVE-ALONE-ON-A-THIN
                                  -BOOK regime, the exact population this column exists to study.
                                  The caller must NOT overwrite its stored net with the None, or
                                  one thin cycle latches every later row into the same bucket.
      · `other`                 — a join whose price moved with the net touch unchanged. ⚠️ NOT a
                                  synonym for "bug" — count it, don't assume it.
      · `no_prior_touch`        — no PREVIOUS net touch to compare against (first quoted cycle).

    ⛔ THE LABELS ARE ORDERED TESTS, NOT A PARTITION OF CAUSES. The first matching test wins, so
    `net_touch_move` absorbs every replace where the touch ALSO moved. Therefore
    `improve_tick_rounding` is a LOWER BOUND on the rounding population, never a measure of it,
    and `net_touch_move` is an upper bound on genuine market-following. To reproduce the churn
    dive's statistic, count `improve_tick_rounding + other + net_unreadable` over
    `action_* == "replace"` rows — not `improve_tick_rounding` alone.

    Non-replace actions carry their action's own reason, so `reason_*` is populated on EVERY row
    and a blank means a writer bug, not a quiet action.
    """
    if action == PLACE:
        return "place"
    if action == CANCEL:
        return "pull"
    if action != REPLACE:
        return "hold"
    if (resting is not None and desired is not None and resting == desired
            and resting_size != desired_size):
        return "inventory_skew"
    if net is None:
        return "net_unreadable"
    if prev_net is None:
        return "no_prior_touch"
    if prev_net != net:
        return "net_touch_move"
    if improved:
        return "improve_tick_rounding"
    return "other"


# A completed round trip that realized at least this much loss PER CONTRACT triggers the
# post-adverse cooldown. Calibrated on live episodes: a news-gap exit should trigger while an
# ordinary drift exit should not.
ADVERSE_RT_PER_CONTRACT = Decimal("0.01")  # placeholder — production value withheld
#: How far AHEAD of the maker's clock a `clear_adverse_latch` ack may be dated and still count.
#: Small on purpose: it exists to absorb operator/host clock skew, not to accept a future date.
#: A stamp past it is refused, because a future ack does not clear once — it stays in the
#: level-triggered file and out-dates every re-fire after it. [r3 polish item 1]
ACK_MAX_SKEW_S = 60.0

# ── PROBE-LANE ADVERSE RULE [cycle-4 P1, the private design notes §1] ────
# ⛔ PROBE LANE ONLY. Every other lane keeps the incumbent rail BIT-FOR-BIT: the absolute
# ADVERSE_RT_PER_CONTRACT bar above and a RUN-LIFETIME latch. The lane conditioning is the most
# load-bearing line of this change — the private suite pins it in BOTH directions.
#   TRIGGER  realized/contract ≤ −max(ADVERSE_WIDTH_BAR_FLOOR, ADVERSE_WIDTH_K × hs_at_entry),
#            the half-spread taken from the ENTRY fill's own net_bid_place/net_ask_place (net of
#            our own resting size, stamped at PLACEMENT). ⛔ NEVER best_bid/best_ask — those are
# SELF-INCLUSIVE, and a replay that used them mis-banded armings and flipped conclusions. With no usable
#            net width the trigger falls back to the INCUMBENT ABSOLUTE BAR and TAPES that it did.
#   SCOPE    ⛔ SUPERSEDED 2026-09-01 — see LATCH_BAN_ARMS_S below. `adverse_ban_seconds` is
#            retained and still pinned: it is the revert target. Re-admission returns the book to
#            NORMAL quoting, not reduce-only; run-lifetime survives only via repeated re-trips.
# k=2: spares genuinely wide books (sub-spread trips are less than one fill's own credit) and
# fires on every class the measured era showed negative; k=1 is a measured no-op, k≥3 buys measured-negative skips.
ADVERSE_WIDTH_K = Decimal("1")  # placeholder — production value withheld
#: ⛔ THE FLOOR UNDER THE WIDTH-NORMALISED BAR. Deliberately EQUAL to the incumbent absolute bar,
#: and that equality is the whole point: it makes width-normalisation a one-way LOOSENING. A
#: tight book (hs < <n>) gets exactly the incumbent rule; only a book wide enough that 2×hs
#: exceeds <n> is spared anything. Without it the rule TIGHTENS on the books we actually quote
#: (where a tight book latches on a single tick of adverse move). Do
#: not "simplify" it away or re-key it without re-measuring the arming bank.
ADVERSE_WIDTH_BAR_FLOOR = ADVERSE_RT_PER_CONTRACT
#: Ban ceiling, seconds. ⚠️ `adverse_ban_seconds` clamps the MIN to the configured cooldown and
#: the MAX to this, so a cooldown ABOVE this ceiling would make the min clamp unreachable and
#: every ban exactly the ceiling — the CLI refuses that combination rather than silently resolving it.
#: ⚠️ The randomized rule does not read `adverse_cooldown_s`, so this binds nothing on that lane;
#: it is retained because `adverse_ban_seconds` (the revert target) still reads it.
ADVERSE_BAN_MAX_S = 86400.0  # placeholder — production value withheld

# ── RANDOMIZED BAN LENGTH ──────────────────────────────────────────────────────────────
# ⛔ PAUSED PRE-ARMING — UNREACHABLE UNDER THE SHIPPED CONFIG, AND KEPT ANYWAY: cycle-4 prereg §1's
# falsifier (b) fired and its registered response is "revert to the incumbent latch on the probe
# lane", so the probe lane writes NO deadline and this draw is not reached.
# ⛔ THE SWITCH IS `the probe registration file` `latch.rule`, NOT THIS CODE.
# ⛔ RE-ARMING IS NOT MERELY A CONFIG LINE: the prereg's STATUS block requires RE-REGISTRATION
# FIRST (latch_active-era data, a harm cap denominated in the measured re-admission harm, and a
# precedence statement against cycle-4 §1),
# THEN the config flip, THEN a per-run go.
# ⛔ PROBE LANE ONLY. Off it `ban_s` stays None and the latch is the incumbent RUN-LIFETIME one.
# WHY: `ban = 900 × ⌈severity⌉` is DETERMINISTIC, so "split by ban length" and "split by severity"
# are the SAME split and the multiplier's benefit is structurally unidentifiable. Drawing the ban
# INDEPENDENTLY OF SEVERITY breaks that collinearity: severity is still taped (`latch_ratio`) as a
# measured COVARIATE rather than the thing that sets the treatment.
#: The registered arms, seconds — FOUR, LOG-SPACED the private design notes.
#: The estimand is a CURVE (re-entry-window price P&L on log(ban), pooled), which needs rungs over
#: more than one octave; armings taped under an earlier tag score separately, never pooled in.
#: ⛔ Changing this tuple is a RE-REGISTRATION, not a tuning knob: the n target and the stop rules
#: are written against the arm set, and the tape's `latch_rule` tag must move with it.
#: ⛔ IT IS THE FOUR-ARM TAG'S SET, NOT "THE MAKER'S ARMS". Every randomized rule draws from its
#: OWN registered set (`ban_arms_for_rule`); this constant survives as the default for a caller
#: that names no rule, and the assert below pins the two spellings together.
LATCH_BAN_ARMS_S: tuple[float, ...] = (60.0, 180.0, 600.0, 1800.0)
#: `latch_rule` tape vocabulary — the column exists so a reader can tell a RANDOMIZED draw from a
#: severity-derived one WITHOUT re-deriving it from `latch_ratio`, which cannot distinguish them.
#: ⛔ ALIASED to `bot.core.probe_config`, never re-spelled here: the same string is the config
#: file's VOCABULARY and the tape's TAG, and two literals is how those drift apart.
LATCH_RULE_RANDOM = probe_config.LATCH_RULE_RAND_60_180_600_1800
#: ⛔ ONE SPELLING OF THE FOUR ARMS. `LATCH_BAN_ARMS_S` predates the per-rule map and is kept as
#: the no-rule default; if the two ever disagree, a run would draw one set and tape the other.
assert LATCH_BAN_ARMS_S == tuple(
    float(a) for a in probe_config.LATCH_BAN_ARMS_BY_RULE[LATCH_RULE_RANDOM])
#: The retired severity-multiplier rule. Not emitted by this build; kept as the vocabulary a
#: revert tapes, and as the label for pre-2026-09-01 rows (which carry NO column at all — the
#: fills tape rotates at the schema bump, so absence, not this value, marks the old era).
LATCH_RULE_SEVERITY = "sev_mult"
#: ⛔ THE POST-KILL PROBE RULE, AND THE SHIPPED DEFAULT [cycle-4 §1 falsifier (b) FIRED
#: 2026-09-01, the private design notes]. Run-lifetime scope — the SAME rail the
#: main lane has always had — but it gets its OWN tag rather than the main lane's blank, because
#: a probe-lane latch under this rule still fires on the width-normalised cycle-4 TRIGGER and a
#: reader must be able to separate this era from `sev_mult`, `rand900_1800` and
#: `rand60_180_600_1800` row by row.
LATCH_RULE_LIFETIME_REVERTED = probe_config.LATCH_RULE_LIFETIME
#: ⛔ THE LATCH-ONLY CONTROL ARM'S TAG [AMENDMENT 19c,
#: the private design notes]. A cell drawn into the control arm
#: EVALUATES the trigger, TAPES the arming row under this tag with `latch_ban_s` BLANK, and does
#: NOT latch. That row IS the marker: `latch_rule` is written only inside the trigger branch, so
#: a fills row carrying `off` is precisely a would-have-armed instant that was not armed — the
#: counterfactual the replay could not observe, because every spared book was held reduce-only
#: the private design notes.
#: ⛔ ALIASED, never re-spelled — the config vocabulary and the tape tag are one string.
LATCH_RULE_CONTROL_OFF = probe_config.LATCH_RULE_OFF


# ── the GAME-CLOCK GUARD [AMENDMENT 41, operator 2026-09-10] ─────────────────────────────────────
#
# ⛔ WHY IT IS NOT THE ADVERSE LATCH. A live run lost most of a night's price-realized in derivative books
# at game end: bids filled in the last minutes and the mid collapsed at the final whistle. The adverse
# latch fires AFTER a round trip — on a book that is RESOLVING, the first trip IS the whole loss and
# there is no second one to protect. This rail arms on the GAME's own clock
# instead, before the entry.
#
# ⛔ IT GUARDS DERIVATIVES ONLY, BY SEAT, NEVER BY SPORT. The guarded set is whatever
# `--clock-guard-slugs` names (the registry's `clock_guard.classes`). Moneyline books are not guarded.
#
#: A row older than this is UNKNOWN, and unknown NEVER stands a book down. The recorder polls on a
#: 60 s grid, so this is three missed cycles — long enough that a single overrun or a retry ladder
#: does not disarm the guard, short enough that a dead sidecar cannot hold a guard armed on a state
#: nobody has observed. ⛔ THE FAIL DIRECTION IS "QUOTE AS BEFORE": the guard is an ADDITION to
#: every rail that already governs (caps, latch, tripwire, loss cap), so its absence is the
#: pre-amendment behaviour and never an unprotected one.
CLOCK_GUARD_STALE_S = 180.0
#: Football's final stretch: the FOURTH quarter at or under `CLOCK_GUARD_FOOTBALL_CLOCK_S`
#: remaining, or ANY period at or above 5 — overtime, where every snap can resolve the book.
CLOCK_GUARD_FOOTBALL_PERIOD = 4
CLOCK_GUARD_FOOTBALL_CLOCK_S = 300
#: Baseball has no clock (the recorder writes `0:00`), so the INNING is the whole rule: the ninth
#: and anything past it (extras).
CLOCK_GUARD_MLB_INNING = 9
#: `state == "post"` — the game is OVER and the book is waiting to resolve. Guarded on EVERY sport,
#: including the ones with no clock rule at all, because it needs no clock to be unambiguous.
CLOCK_GUARD_STATE_POST = "post"
#: The sports whose rule is the football one. Keyed on the slug's SECOND token (`asc-nfl-…` → nfl).
CLOCK_GUARD_FOOTBALL_SPORTS = frozenset({"nfl", "cfb"})
CLOCK_GUARD_MLB_SPORT = "mlb"
#: The three rule names `clock_guard_rule_for` answers. `post` is not "unguarded" — it is guarded
#: from the final whistle on, with NO pre-whistle protection, which is a different promise and the
#: launch banner has to say which books got which.
CLOCK_GUARD_RULE_FOOTBALL = "football"
CLOCK_GUARD_RULE_MLB = "mlb"
CLOCK_GUARD_RULE_POST_ONLY = "post"
#: ⛔ DUPLICATED FROM `scripts.poly_gamestate_watch.LATEST_PATH`, DELIBERATELY: importing a
#: `scripts.*` module from the maker is an import cycle at the fire path. A test pins the two
#: strings equal, which is the whole reason the duplicate is safe.
CLOCK_GUARD_FILE = ""  # placeholder — production value withheld


def clock_guard_rule_for(slug: str) -> str:
    """WHICH clock rule this book gets, off the slug's SECOND token (`asc-nfl-…` → `nfl`).

    ⛔ ONE SPORT TABLE, TWO READERS: `clock_guard_active` decides with it and the launch banner
    prints with it. A banner that spelled the table itself would keep promising pre-whistle
    protection on a sport whose rule had been retired — the `latch_off_slugs` lesson, applied to a
    rail whose scope is per-book.
    """
    parts = slug.split("-")
    sport = parts[1] if len(parts) > 1 else ""
    if sport in CLOCK_GUARD_FOOTBALL_SPORTS:
        return CLOCK_GUARD_RULE_FOOTBALL
    if sport == CLOCK_GUARD_MLB_SPORT:
        return CLOCK_GUARD_RULE_MLB
    return CLOCK_GUARD_RULE_POST_ONLY


def _int_or_none(raw: str) -> Optional[int]:
    """`"4"` → 4; blank, `"OT"` or anything else → None = UNKNOWN, which never guards."""
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _clock_seconds(raw: str) -> Optional[int]:
    """`"M:SS"` → seconds remaining in the period; anything else → None = UNKNOWN.

    ⛔ THE RECORDER's OWN SHAPE (data_dictionary § 8c): ESPN's `displayClock`, `"0:00"` on a sport
    with no clock. A blank (failed poll) and an unparseable value are the same answer — None — and
    None never arms the guard on its own; only `state == post` can, and it is read first.
    """
    parts = raw.split(":")
    if len(parts) != 2:
        return None
    minutes, seconds = _int_or_none(parts[0]), _int_or_none(parts[1])
    if minutes is None or seconds is None or minutes < 0 or not 0 <= seconds < 60:
        return None
    return minutes * 60 + seconds


def ban_arms_for_rule(rule: str) -> tuple[float, ...]:
    """The REGISTERED arm set for one randomized latch rule [2026-09-04].

    ⛔ THE RULE OWNS ITS ARMS, and `bot/core/probe_config.LATCH_BAN_ARMS_BY_RULE` is where that
    pairing lives. Before this, the maker drew from ONE module constant whatever tag the config
    named, so `rand180_1800` — registered, gated and loadable — would have drawn FOUR arms and
    taped them under a tag that promises two. The loader already refuses a tag whose
    `latch.ban_arms_s` disagrees with the map; this is the same rail at the DRAW, so a config
    the loader never saw (a hand-built engine, a test) cannot slip past it either.

    ⛔ FAIL-CLOSED ON A MAP MISS, exactly as the loader is: a `rand*` tag with no entry is a
    HALF-REGISTERED rule — its name promises a draw and nothing says over what — and it must
    refuse rather than inherit whatever constant happens to be in scope.
    """
    arms = probe_config.LATCH_BAN_ARMS_BY_RULE.get(rule)
    if not arms:
        raise ValueError(
            f"latch rule {rule!r} names a randomized ban but registers NO arms in "
            f"bot/core/probe_config.py LATCH_BAN_ARMS_BY_RULE "
            f"(registered: {sorted(probe_config.LATCH_BAN_ARMS_BY_RULE)}). REFUSING to draw: a "
            f"tag that promises a draw and does not say over what must not inherit another "
            f"rule's arms.")
    return tuple(float(a) for a in arms)


def draw_ban_seconds(rng: random.Random,
                     arms: Sequence[float] = LATCH_BAN_ARMS_S) -> float:
    """One arming's ban length, drawn uniformly from `arms` — the REGISTERED set for the rule
    that is running (`ban_arms_for_rule`).

    ⛔ The RNG is an argument, not a module global, so the draw is deterministic under test and
    every arming in a run comes off the maker's one `random.Random` stream.

    ⛔ INDEPENDENT OF SEVERITY BY CONSTRUCTION — that independence IS the experiment. Do not
    condition the draw on `per_ct`, the bar, the book or the time of day: any of those
    re-introduces the collinearity the prereg exists to break.
    """
    return rng.choice(tuple(arms))


def entry_half_spread(net_bid: Optional[Decimal],
                      net_ask: Optional[Decimal]) -> Optional[Decimal]:
    """The PLACEMENT-time, net-of-self half-spread of one fill, or None if unusable.

    None on: either side missing, and on a non-positive width (a crossed or zero net book). A
    zero width would make the trigger `<= 0`, i.e. fire on every losing trip however small — so
    it is treated as UNRESOLVED and routed to the absolute fallback.
    """
    if net_bid is None or net_ask is None:
        return None
    hs = (net_ask - net_bid) / Decimal("2")
    return hs if hs > _ZERO else None


def entry_width_accounting(prev_inv: Decimal, mean: Decimal, qty_open: Decimal,
                           side: str, qty: Decimal,
                           hs: Optional[Decimal]) -> tuple[Decimal, Decimal]:
    """Carry the entry half-spread across one fill: (new mean, new resolved-open qty).

    ⛔ MIRRORS `fill_accounting`'s branches, because `fill_accounting` is AVERAGE-COST, not FIFO:
    a round trip's "entry" is generally several fills, so the denominator of the registered
    trigger is the qty-weighted mean half-spread OVER THE CURRENTLY-OPEN INVENTORY — the
    convention `avg_entry` uses. Four branches:

      · ADDING (or opening from flat) → weight the running mean against the OPEN quantity.
      · PARTIAL REDUCE → mean unchanged, but the WEIGHT it carries into the next add shrinks with
        the position. ⛔ Not cosmetic: the trigger's NUMERATOR is realized P&L computed against
        `avg_entry`, an average-cost basis over open inventory, and pairing it with a
        lifetime-sum denominator compares two different populations of contracts. The denominator
        must be the same SHAPE as the numerator. (The bias is TWO-SIDED, not fail-open.)
      · CLOSING TO EXACTLY FLAT → untouched here; the caller clears AFTER the trigger has read
        it, because the completed trip must be priced off the width it was ENTERED at.
      · THROUGH-ZERO → the old trip completes and the REMAINDER opens fresh, so the carrier
        resets to this fill's own width. ⛔ The caller must SNAPSHOT before calling.

    An UNRESOLVED width (`hs is None`) leaves the mean alone rather than entering as zero: zero
    would drag the mean toward a trigger that fires on every loss. The mean is therefore over the
    RESOLVED open contracts only, and when nothing resolves the caller falls back to the
    absolute bar.
    """
    signed = qty if side == "bid" else -qty
    new_inv = prev_inv + signed
    if prev_inv == _ZERO or (prev_inv > _ZERO) == (signed > _ZERO):
        if hs is None:
            return mean, qty_open
        total = qty_open + qty
        return ((mean * qty_open + hs * qty) / total, total)
    closing = min(qty, prev_inv.copy_abs())
    if new_inv == _ZERO:
        return mean, qty_open
    if qty <= closing:
        # Scale the weight with the position, leaving the MEAN exactly as it was (carrying the
        # mean rather than a running sum is what makes that exact — a sum scaled by a ratio
        # would re-round the mean on every reduce).
        return mean, qty_open * (new_inv.copy_abs() / prev_inv.copy_abs())
    remainder = qty - closing
    return ((hs, remainder) if hs is not None else (_ZERO, _ZERO))


def adverse_ban_seconds(cooldown_s: float, per_ct: Decimal,
                        trigger_level: Decimal) -> float:
    """The SEVERITY-MULTIPLIER timed-ban length, in seconds.

    ⛔ RETIRED FROM THE LIVE PATH 2026-09-01 — the probe lane now draws from `LATCH_BAN_ARMS_S`.
    Kept, and kept pinned, because it is the REVERT TARGET named by the prereg's abort rules.

    `per_ct` is the trip's realized-per-contract (signed), `trigger_level` is `k·hs`. Severity is
    whole multiples of the level that fired, so the multiplier is >= 1 by construction — the min
    clamp is belt-and-braces against a caller passing a level it did not breach. `math.ceil` on a
    Decimal is exact; the ratio never touches float.
    """
    if trigger_level <= _ZERO:                       # unreachable via the live caller
        return max(0.0, cooldown_s)
    mult = math.ceil(per_ct.copy_abs() / trigger_level)
    return min(ADVERSE_BAN_MAX_S, max(cooldown_s, cooldown_s * mult))


def _utc_iso(epoch: float) -> str:
    """Epoch → the tz-aware ISO-8601 UTC form `clear_adverse_latch` accepts. Operator-facing:
    the refusal log prints an ack the operator can paste, so the control channel is never a
    format-guessing exercise during an incident."""
    return (_dt.datetime.fromtimestamp(epoch, _dt.timezone.utc)
            .replace(microsecond=0).isoformat().replace("+00:00", "Z"))


def _hot_settings_path() -> str:
    """The SHARED default hot-settings path (config, cwd-relative).

    ⚠️ ONE PATH PER CWD. Two maker lanes launched from the same directory read the SAME file, and
    `parse_hot_settings` is whole-file-or-nothing over the READING lane's slate — so one lane
    writing its own books makes the other's slugs read as slate ADDITIONS and voids its entire
    hot lever. A lane sharing a box passes `--hot-settings-file` to get its own.
    """
    from bot.core import config as _config
    return str(getattr(_config, "HOT_SETTINGS_FILE", "hot_settings_poly.json"))


def _hot_slate_path() -> str:
    """The SHARED default hot-SLATE path (config, cwd-relative), beside `hot_settings_poly.json`.

    ⚠️ ONE PATH PER CWD, and the sharing hazard is WORSE here than for hot_settings: this file is
    the whole book set, so a lane reading another lane's file would RETIRE every book of its own
    slate in one apply. A lane sharing a box passes `--hot-slate-file`.
    """
    from bot.core import config as _config
    return str(getattr(_config, "HOT_SLATE_FILE", "hot_slate_poly.json"))


#: The hot-slate change tape. ⛔ One row per DECISION, never per cycle: `add`, `retire`, `drop`,
#: `requote`, `refused`. `poly_teardown_row.manifest_slugs` unions the `add` rows into the
#: declared slate — without it the slug cross-check ⛔-flags every hot add as a slug the manifest
#: never declared. Column semantics: the private design notes.
#: ⚠️ CWD-RELATIVE, exactly like its sibling `logs/hot_settings_changes.csv` and the reader that
#: folds it (`poly_teardown_row`): the pair has to resolve to the same file from the same launch
#: directory, and one absolute and one relative half is how they stop meeting.
HOT_SLATE_CHANGES_CSV = os.path.join("logs", "hot_slate_changes.csv")
SLATE_RETRYABLE_METADATA = "metadata_unavailable"
_SLATE_CHANGES_HDR = ["ts", "run_id", "epoch", "slug", "action", "size", "mode", "requote_s",
                      # ⛔ THE ADOPTED POSITION AND ITS BASIS [mm-review 2026-09-10]. `adopt` rows
                      # only. This is the ONLY record of a mid-run carry: the run manifest's
                      # `carries` column is written at launch, so without these two cells
                      # `poly_teardown_row.price_realized` replays an adopted book from FLAT and
                      # books its first reducing fill's GROSS PROCEEDS as realized.
                      "qty", "basis"]
#: The CLOSED action vocabulary of that tape. `refused` is the whole FILE being voided (slug names
#: the book that caused it, or is blank when the refusal is file-level); `epoch` is an epoch
#: advance that changed nothing else.
SLATE_ACTIONS = ("add", "adopt", "retire", "drop", "requote", "refused", "epoch")
#: The per-book keys a slate entry may carry. Closed: an unknown key voids the WHOLE file, which
#: is how a never-hot knob arrives.
SLATE_BOOK_KEYS = frozenset({"size", "mode", "pinned", "latch_off", "clock_guard",
                             "maintenance_guard", "resolves_at", "park"})
SLATE_TOP_KEYS = frozenset({"run_id", "epoch", "updated", "requote_s", "books",
                            "apply_deadline_ts"})
#: The quote modes a slate entry may name — `hot_quote`'s own vocabulary plus `normal`.
SLATE_MODES = ("normal", "reduce_only", "gated")
#: The slate-SCOPED per-book maps, popped when a book is DROPPED [continuous maker P1]. ⛔ The
#: durable / run-level safety maps are deliberately NOT here — `adverse_latched`,
#: `adverse_ban_until`, `cooldown_until`, `mark_trips`, `_clock_guard_post_latched` and the basis
#: survive a drop, so a re-add of the same book inherits the ban it earned.
_SLATE_SCOPED_MAPS = ("last_touch", "last_touch_net", "_last_net", "last_read_ts",
                      "_last_verified_ts", "_ws_stale_cache_streak", "unquotable_streak",
                      "not_found_streak", "_price_clock", "_entry_hs_mean", "_entry_hs_qty")


def heartbeat_stale_after_s(requote_s: float, flatten_wait_s: float) -> float:
    """The deadman's staleness budget for a maker at this cadence. ⛔ ONE OWNER: the launcher
    sizes the heartbeat at construction and the hot-slate cadence change re-sizes it mid-run — two
    copies of this arithmetic would drift, and the drifting one pages on a CORRECT shutdown.

    +120 s covers the teardown's delayed-verify phase (4 × 20 s), which legitimately blocks
    between cancel-all and the flatten [mm-review C2]; the 90 s floor covers a fast cadence.
    """
    return max(requote_s * 3.0 + flatten_wait_s + 120.0, 90.0)


def parse_hot_slate(raw_text: str, *, run_id: str, min_size: int,
                    requote_max_s: float) -> tuple[Optional[dict], Optional[str]]:
    """(slate, None) or (None, refusal-reason) — `parse_hot_settings`' discipline verbatim:
    WHOLE-FILE-OR-NOTHING over a CLOSED schema, every value validated before anything applies.

    ⛔ `run_id` IS REQUIRED HERE, unlike hot_settings [continuous maker P1]. hot_settings tolerates
    an unstamped file because the launch flow writes drain overrides before the run id exists, and
    its worst unstamped outcome is one book reduce-only. A SLATE is the whole book set: an
    unstamped file from a prior evening would retire every book this run is quoting, in one apply,
    and the retires would look exactly like a lawful reseat. So an unstamped slate never applies.

    ⛔ `requote_max_s` is the caller's OWN ws-grace product rail, not a constant here: a hot
    cadence slower than `WS_NEARCAP_GRACE_S / 3` re-opens the halt the launcher refuses at start
    time, so the same bound has to hold at the hot seam.
    """
    try:
        data = json.loads(raw_text)
    except (ValueError, TypeError) as exc:
        return None, f"unparseable JSON ({exc})"
    if not isinstance(data, dict):
        return None, "top level is not an object"
    unknown_top = sorted(set(data) - SLATE_TOP_KEYS)
    if unknown_top:
        return None, (f"unknown top-level key(s) {unknown_top} — never-hot knobs arrive exactly "
                      f"this way, so the whole file is ignored")
    stamp = data.get("run_id")
    if stamp is None:
        return None, ("no `run_id` — a slate is the whole book set, so an UNSTAMPED file never "
                      "applies: a prior run's file would retire this run's slate whole")
    if str(stamp) != str(run_id):
        return None, (f"stamped for run {stamp!r}, this is {run_id!r} — a prior run's slate must "
                      f"not steer this one")
    books = data.get("books")
    if not isinstance(books, dict) or not books:
        return None, ("`books` missing or empty — an empty slate is a malformed file, never a "
                      "retire-everything instruction")
    out_books: dict[str, dict] = {}
    for slug, spec in books.items():
        if not isinstance(spec, dict):
            return None, f"{slug}: spec is not an object"
        unknown = sorted(set(spec) - SLATE_BOOK_KEYS)
        if unknown:
            return None, f"{slug}: unknown key(s) {unknown}"
        size = spec.get("size")
        if not isinstance(size, int) or isinstance(size, bool) or size < min_size:
            return None, f"{slug}: size {size!r} is not an int ≥ the venue minimum {min_size}"
        mode = spec.get("mode", "normal")
        if mode not in SLATE_MODES:
            return None, f"{slug}: mode {mode!r} is not one of {list(SLATE_MODES)}"
        entry: dict = {"size": size, "mode": mode}
        for flag in ("pinned", "latch_off", "clock_guard", "maintenance_guard"):
            value = spec.get(flag, False)
            if not isinstance(value, bool):
                return None, f"{slug}: {flag} {value!r} is not a boolean"
            entry[flag] = value
        if "resolves_at" in spec:
            value = spec["resolves_at"]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
                return None, f"{slug}: resolves_at {value!r} is not a positive epoch"
            entry["resolves_at"] = float(value)
        if "park" in spec:
            parts = str(spec["park"]).split(":")
            if len(parts) != 2 or parts[0] not in ("bid", "ask") or not parts[1].isdigit():
                return None, f"{slug}: park {spec['park']!r} is not `<bid|ask>:<ticks>`"
            if int(parts[1]) < PARK_MIN_TICKS_OFF:
                return None, (f"{slug}: park rests {parts[1]} tick(s) off the touch, below the "
                              f"{PARK_MIN_TICKS_OFF}-tick floor")
            entry["park"] = (parts[0], int(parts[1]))
        out_books[slug] = entry
    requote_s = data.get("requote_s")
    if requote_s is not None:
        if isinstance(requote_s, bool) or not isinstance(requote_s, (int, float, str)):
            return None, f"requote_s {requote_s!r} is not a number"
        try:
            requote_s = float(Decimal(str(requote_s)))
        except (ArithmeticError, ValueError):
            return None, f"requote_s {data['requote_s']!r} is not a number"
        if not 0.0 < requote_s <= requote_max_s:
            return None, (f"requote_s {requote_s:g} is outside (0, {requote_max_s:g}] — past it "
                          f"the near-cap WS grace is under three cycles, which is the refusal "
                          f"the launcher already makes at start time")
    #: ⛔ REQUIRED, AND IT IS THE AUTHORITY. The maker does not
    #: count applies: the epoch on the FILE is what the tapes are stamped with, so the writer and
    #: every reader agree on which generation a row belongs to. A maker-side counter would drift
    #: from the reseater's journal the first time a file was refused.
    epoch = data.get("epoch")
    if epoch is None or isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 0:
        return None, (f"epoch {epoch!r} is not a non-negative int — the FILE's epoch is what the "
                      f"tapes are stamped with, so an unstamped generation is unjoinable")
    slate = {"books": out_books, "requote_s": requote_s, "epoch": epoch}
    if "apply_deadline_ts" in data:
        deadline = data["apply_deadline_ts"]
        if (isinstance(deadline, bool) or not isinstance(deadline, (int, float))
                or not 0 < deadline < float("inf")):
            return None, "apply_deadline_ts is not a finite positive epoch"
        slate["apply_deadline_ts"] = deadline
    return slate, None
# The mark tripwire's default threshold, in dollars PER CONTRACT of per-book marked P&L. ⚠️ PER
# CONTRACT, not fixed-dollar: a fixed-dollar threshold TIGHTENS in per-contract terms as size
# grows, i.e. backwards for exactly the headroom runs it guards. Why it exists: the durable cap
# is price-REALIZED-only and the adverse cooldown fires only after a COMPLETED round trip, so
# both rails are blind to the accumulation phase headroom widens. 0 disables. ⚠️ A marked breach
# trips only when ALSO drift-dominated (|mid drift| > half-spread); width-class breaches shield.
DEFAULT_MARK_TRIP_PER_CONTRACT = Decimal("0.01")  # placeholder — production value withheld


async def carve_out_settled_books(
        client: Any,
        believed: MutableMapping[str, Decimal],
        venue_held: Container[str],
        *,
        store: Any | None = None,
        avg_entry_yes: Mapping[str, Decimal] | None = None,
        basis_carried: Container[str] = (),
        observed: MutableMapping[str, str] | None = None,
        run_id: str = "") -> dict[str, str]:
    """Retire every belief whose MARKET has settled. Returns `{slug: status}` for what it moved.

    ⛔ THE MECHANISM. The venue's positions endpoint DROPS a market once it resolves, so a book
    that settled while we held it presents exactly as a real belief-venue divergence — belief
    ≠ 0, no venue row — and the teardown flatten's corroboration gate refuses the whole
    uncorroborated set over a position that has already paid out.

    So the absent row is CLASSIFIED before it is refused: `SETTLED_MARKET_STATUSES` is the
    admitted set and nothing else qualifies — an OPEN market with no row is the genuine
    divergence and keeps today's refusal untouched.

    ⛔ FAILS CLOSED IN EVERY UNKNOWN DIRECTION: a status read that raises, a client with no
    status reader, a None status, and any status outside the set all leave the belief exactly
    where it is. Nothing here can retire a belief on silence.

    ⛔ IT MOVES NO MONEY HERE. The belief goes to `settled_pending` in the durable record;
    `realized_pnl` is untouched by THIS function. `poly_settle_book.settle_pending_books` later
    prices the row from the VENUE's settlement price and books it through `add_settlement`,
    which FOLDS into `realized_pnl`. ⛔ `avg_entry_yes` is THIS RUN's own mean entry price in YES
    space, NOT the venue's cost basis — it is the basis the settlement is MEASURED AGAINST,
    never the price it settles at. A book whose basis is a carry or a `fill_accounting` reset is
    stamped `basis_source="reset"` and is NEVER auto-booked.

    ⛔ NO STORE, NO CARVE, and no carve without a status reader.

    `venue_held` must be a CONFIRMED-FRESH venue read AND must include the slugs whose row was
    UNPARSEABLE (`PolyMaker.venue_stale_rows`) — the venue DID answer for those, so reading them
    as "no row" would certify flat over a live position.
    """
    carved: dict[str, str] = {}
    reader = getattr(client, "get_market_status", None)
    if reader is None:
        log.warning("settled carve-out inert: client has no get_market_status — every row-less "
                    "belief stays uncorroborated.")
        return carved
    # ⛔ NO STORE, NO CARVE. Zeroing a belief is only safe because the quantity lands somewhere
    # durable; without a record the position is retired to NOWHERE.
    if store is None:
        log.warning("settled carve-out skipped: no durable store — every row-less belief stays "
                    "uncorroborated.")
        return carved
    for slug, qty in sorted(believed.items()):
        if qty == _ZERO or slug in venue_held:
            continue
        try:
            status = await reader(slug)
        except Exception as exc:
            log.warning(f"settled check: {slug} status read RAISED ({safe_exc(exc)}) — NOT "
                        f"settled; the belief stands and the divergence refusal stands.")
            continue
        if status is not None and observed is not None:
            observed[slug] = status
        if status not in SETTLED_MARKET_STATUSES:
            if status is None:
                log.warning(f"settled check: {slug} status UNREADABLE — NOT settled; not "
                            f"carved to the settlement channel.")
            else:
                # ⛔ THE STATUS GOES ON THE TAPE EVEN WHEN IT CHANGES NOTHING [C1]. Only
                # RESOLVED is admitted, and what a just-ended game actually reports is the
                # measurement this line exists to collect.
                # ⛔ IT NO LONGER SAYS "the divergence refusal stands" [2026-09-03]: a row-less
                # belief on an unresolved market is a HAND CLOSE, and `venue_close
                # .book_hand_closes` prices or parks it after this carve. The status is still
                # the measurement this line exists to collect.
                log.warning(f"settled check: {slug} has no venue row and status {status} — "
                            f"NOT settled (only {sorted(SETTLED_MARKET_STATUSES)} is); not "
                            f"carved to the settlement channel.")
            continue
        px = (avg_entry_yes or {}).get(slug)
        if px is None or px <= _ZERO:
            px = None
        # ⛔ `basis_source` DECIDES WHETHER THIS ROW MAY BE PRICED AUTOMATICALLY [C2]. "run"
        # only when the basis is this run's own fills AND there is one; a carried book, or a
        # book with no basis at all, is "reset" and `poly_settle_book --auto` refuses it.
        basis_source = "run" if (px is not None and slug not in basis_carried) else "reset"
        booked = store.record_settled_pending(slug=slug, qty=qty, avg_entry_yes=px,
                                              basis_source=basis_source,
                                              status=status, run_id=run_id)
        believed[slug] = _ZERO
        carved[slug] = status
        log.warning(f"settled: {slug} {qty} @ {px if px is not None else 'avg entry UNKNOWN'} — "
                    f"market {status}; belief moved to the settlement channel"
                    + ("" if booked else " (already recorded)"))
    return carved


def mark_pnl(inv: Decimal, avg_entry: Decimal,
             bid: Decimal, ask: Decimal) -> Decimal | None:
    """Per-book marked P&L against the LIQUIDATION side of the live touch — a long
    liquidates into the BID, a short covers at the ASK. Marking at the mid (or the far side)
    flatters precisely when the book is running away, which is the moment this exists for.

    None when flat (no position, no mark) — and None on an UNKNOWN BASIS [review §2]:
    `avg_entry` is populated by this run's own fills, so an INHERITED position carries no
    basis, and reading absence as zero marked a carried 22-lot as +<n> of gain — the
    flattering-direction error class (`mid = 1 − best_no_bid`) on the one position class
    nothing else watches. Absence is not a basis; the caller arms conservatively on None."""
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
    """Average-cost accounting for one fill, in YES space. `price` is OUR quote price: a
    post-only GTC fills at its limit, and every recorded fill agrees (90/90 `avg_px == price`).
    ⚠️ Not an identity by construction — the venue computes at ITS price and documents divergence
    on a BUY_SHORT. If a divergent fill is ever recorded this accounting must follow suit; until
    then the increment-level attribution needs the limit price, which the order-level `avgPx`
    cannot supply.

    Returns (new_inv, new_avg_entry, new_rt_realized, new_rt_closed, completed), where
    `completed` is (realized, closed_qty) for the round trip this fill CLOSED — set when
    inventory returns to exactly zero, and also on a through-zero crossing fill.
    """
    signed = qty if side == "bid" else -qty
    new_inv = prev_inv + signed
    if prev_inv == _ZERO or (prev_inv > _ZERO) == (signed > _ZERO):
        # ⛔ `_ZERO` here is the NO-BASIS SENTINEL, not a basis of zero: `avg_entry` is populated
        # by THIS run's own fills, so an INHERITED position carries none. Blending the sentinel
        # into the average dilutes the cost basis in the FLATTERING direction and retires the
        # unknown-basis arm forever. An unknown basis stays unknown: reset to THIS fill's price.
        if avg_entry <= _ZERO and prev_inv != _ZERO:
            return new_inv, price, rt_realized, rt_closed, None
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

    ⚠️ NEVER ONE SIDE. Improving only the bid buys with sole queue priority while selling from
    the back of the queue: the run accumulates a systematic long and stops measuring the spread
    at all. A spread of EXACTLY two ticks is the wrinkle — both improvements land on the same
    price, a quote that would lock against itself — so that case joins both.

    ⛔ THIS IS A SECOND COPY of `scripts/poly_shadow_mm.pick_quotes`, kept deliberately: that
    collector's import allowlist forbids it from importing any module with an order path. The two
    are pinned EQUAL in the private suite — if they diverge, every comparison of live
    fills against the shadow tape is meaningless.

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


#: ── THE QUOTE MODES ───────────────────────────────────────────────────────────
#: `improve` is `pick_quotes` unchanged; `join` posts AT the touch on any side `improve` would
#: have stepped inside. ⛔ `pick_quotes` ITSELF IS NOT PARAMETERISED — it is pinned byte-equal to
#: `scripts/poly_shadow_mm.pick_quotes`, and a mode argument here would have to be mirrored there
#: or the equality pin would stop meaning anything. The mode is applied by the CALLER.
QUOTE_MODE_IMPROVE = "improve"
QUOTE_MODE_JOIN = "join"
QUOTE_MODES = (QUOTE_MODE_IMPROVE, QUOTE_MODE_JOIN)

#: How `--rebate-step` chooses an ADDING side's size once it is judged. `floor` leaves a paying
#: base alone, otherwise raises to `min_rebate_size(px)` or drops the side. `optimal` picks the
#: size with the best PAID cents per contract inside the same ceiling — the venue's per-increment
#: nearest-cent rounding makes the paid rate a STEP FUNCTION of size, so the cheapest paid size
#: is rarely the floor. See `_rebate_step_size`.
REBATE_STEP_MODE_FLOOR = "floor"
REBATE_STEP_MODE_OPTIMAL = "optimal"
REBATE_STEP_MODES = (REBATE_STEP_MODE_FLOOR, REBATE_STEP_MODE_OPTIMAL)

#: Contracts of reach the rebate step must LEAVE UNUSED, so that a ghost fill landing after the
#: step sized the side does not breach the reach. A ghost is at most ONE resting order of the
#: cell's BASE size on that side ( item 1 clamps the replacement to base),
#: so the reserve IS the base size — `None` means exactly that, an int overrides it with a fixed
#: reserve. Every `_venue_breach` book since the rule shipped was stepped to the exact bound with
#: no reserve. The reserve bounds the STEP only:
#: a base-size order with less headroom than `base + reserve` still goes out, judged by
#: `_reach_permits` as before.
REBATE_STEP_HEADROOM_CT: Optional[int] = None

#: The MINIMUM declared distance, in ticks, of a PARK seat from its own touch.
#:
#: ⛔ IT IS A CLASS BOUNDARY, NOT A TUNING KNOB. A park seat is admitted by a battery that skips
#: the flow-recency, queue and rebate-step gates and NARROWS the two-sided-book gate to the side
#: the seat rests on. What makes that safe is that the seat is NOT competing for fills: it rests
#: far enough off the touch that a fill requires the market to come to it. At 0 ticks a "park" is
#: a touch-join under a shorter battery. Five is the smallest distance at which that is
#: structurally true on the venue's own scoring rule (`Score = d^ticks × size`): at the
#: a low-decay family five ticks retains almost nothing of best, while the high-decay family it is
#: built for still retains most.
#:
#: ⚠️ A FLOOR on the operator's declaration, not a recommendation: `poly_prelaunch`'s park gate
#: separately refuses a declaration whose `d^ticks_off` is negligible on the book's OWN discount.
PARK_MIN_TICKS_OFF = 100  # placeholder — production value withheld


def park_price(anchor: Optional[Decimal], tick: Decimal,
               side: str, ticks_off: int) -> Decimal:
    """The ADD price for a PARK seat: `ticks_off` ticks OFF `anchor`, on `side`.

    ⛔ THIS PRICE IS FOR ADDING ONLY — never for reducing. A park seat is paid for PRESENCE, so
    the distance is what buys the score; an EXIT is paid for getting out, and 10 ticks off the
    touch is where an exit goes to die. The caller decides which a placement is (from the SIGN OF
    INVENTORY, not from which side was declared) and prices a reducing placement at the RAW
    touch. Returning ONE price and letting the caller assemble is the whole point of this shape.

    ⛔ WHAT A PARK SEAT IS. Reward income is `d^(ticks from best) × size` per side per one-second
    snapshot, with QUEUE POSITION ABSENT from the formula — so on a high-discount book (d=0.90)
    an order ten ticks off best still scores 35% of an at-touch order and essentially never
    fills. Presence is the product. There is no improve logic here and there never should be.

    ⛔ `anchor` MUST BE THE TOUCH NET OF OUR OWN RESTING SIZE, and the side we rest on. Anchoring
    on the RAW touch self-ratchets: once our own park order is the best bid, each cycle
    re-derives `our price − ticks_off` and walks the seat down the book, at-touch at every step —
    exactly the disguised at-touch seat `PARK_MIN_TICKS_OFF` exists to prevent, and invisible in
    DRY. A net touch our own size EMPTIES is `None`: there is no market behind us to measure
    from, so the caller HOLDS rather than inventing one.

    Never anchors off the mid or the opposite side: these books run 17–40 tick spreads and are
    routinely one-sided.

    Raises ValueError on an unusable tick/side/distance/anchor, or when the resting price would
    fall outside the tradable range — park distance is price-bounded before it is anything else.
    """
    if tick <= _ZERO:
        raise ValueError(f"unusable tick {tick}")
    if side not in ("bid", "ask"):
        raise ValueError(f"unusable park side {side!r} — bid or ask")
    if ticks_off < PARK_MIN_TICKS_OFF:
        raise ValueError(f"park distance {ticks_off} ticks is below the {PARK_MIN_TICKS_OFF}-tick "
                         f"floor — a park seat that close is a touch-join under a shorter battery")
    if anchor is None or anchor <= _ZERO or anchor >= _ONE:
        raise ValueError(f"park anchor (net {side} touch) is unreadable or off-range: {anchor}")
    offset = tick * ticks_off
    px = anchor - offset if side == "bid" else anchor + offset
    # One whole tick of margin at each end: `(0, 1)` exclusive is the venue's range, and a
    # resting price ON either bound is not a quote, it is a settled outcome.
    if px < tick or px > _ONE - tick:
        raise ValueError(f"park price {px} ({side} anchor {anchor} ∓ {ticks_off}×{tick}) is "
                         f"outside [{tick}, {_ONE - tick}] — no {ticks_off}-tick room at this "
                         f"price")
    return px


def park_order_is_stale(side: str, price: Decimal, ticks_off: int, tick: Decimal,
                        net_bid: Optional[Decimal], net_ask: Optional[Decimal],
                        bid: Optional[Decimal], ask: Optional[Decimal]) -> Optional[str]:
    """Is a resting park order too far out of position to keep HOLDING? A reason, or None.

    ⛔ THE TWO CONDITIONS THAT OUTRANK "HOLD" [round-2 concern 6; round-3 concern 1]. The
    hold-in-place path exists because cancelling forfeits reward snapshots — but holding is only
    right while the order is still approximately where we meant it to be. Two ways it stops
    being, and they are NOT the same test:

      · **THROUGH THE OPPOSITE TOUCH.** A resting BUY at or above the offer is not presence, it
        is a standing gift: the market came to us and kept going, and the order now fills at a
        price nobody would choose. Measured on the price-bounded hold path, which held a
        resting buy 29 ticks THROUGH the offer.

      · **STALE-THROUGH ON OUR OWN SIDE** — the same collapse from the other direction, and the
        one the opposite-side test structurally CANNOT see (with no offers at all there is no ask
        to be through, and the hold path keeps re-declaring a price the market left far behind).
        So we also measure our own side: a bid resting more than `ticks_off` ABOVE the net bid is
        not a park, it is the best bid by a margin. `>`, not `>=`: exactly `ticks_off` away IS
        the declared position.

    ⚠️ ACCEPTED AND UNTESTABLE: when the same-side net touch is None there is nothing to measure
    staleness against BY CONSTRUCTION, so neither condition can fire and that order is held on no
    positional evidence. It is bounded by the venue-truth rail, the cap rails and the teardown
    sweep — not here. An unreadable side simply does not fire its condition: cannot-tell must
    never cancel the seat this class exists to keep resting.
    """
    if side == "bid":
        if ask is not None and price >= ask:
            return f"resting bid {price} is AT or THROUGH the offer {ask}"
        if net_bid is not None and price - net_bid > tick * ticks_off:
            return (f"resting bid {price} sits {(price - net_bid) / tick:.0f} ticks ABOVE the "
                    f"net bid {net_bid} — more than the {ticks_off} declared; the market "
                    f"collapsed away and left us the best bid by a margin")
        return None
    if bid is not None and price <= bid:
        return f"resting ask {price} is AT or THROUGH the bid {bid}"
    if net_ask is not None and net_ask - price > tick * ticks_off:
        return (f"resting ask {price} sits {(net_ask - price) / tick:.0f} ticks BELOW the net "
                f"ask {net_ask} — more than the {ticks_off} declared; the market ran away and "
                f"left us the best offer by a margin")
    return None


def qty_at_price(md: Any, side: str, price: Decimal) -> Optional[Decimal]:
    """Total quantity resting AT `price` on `side` — i.e. the queue we would be joining behind.

    ⛔ None means "we could not read the book", NEVER 0. A fabricated 0 reads as front-of-queue.

    Sums every level at that price rather than reading `levels[0]`: the SDK's book is a bare list
    with no ordering contract. An improved quote correctly reads 0 — it rests alone at the front,
    which is why the improved/joined bit has to be recorded alongside this number.
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


def net_of_self_touch(md: Any,
                      own_bid: Optional[tuple[Decimal, Decimal]],
                      own_ask: Optional[tuple[Decimal, Decimal]],
                      ) -> tuple[Optional[Decimal], Optional[Decimal]]:
    """(net_bid, net_ask): the book's touch NET of our own believed remaining size — the
    `depth − our own size` pattern the Kalshi side uses, applied to the touch itself.

    `own_bid`/`own_ask` are (price, remaining) for OUR resting order on that side, or None. The
    subtraction clamps at zero and a level it empties is promoted past — an emptied SIDE is None,
    the same "we could not tell / nothing behind us" semantics as `touch_from_md`, never a
    fabricated number. Never raises: a malformed book must skip a market, not break the cycle.

    ⛔ CALLERS GATE ON `self.real`. A shadow/DRY maker's orders never rest on the venue, so
    the book never contains them — subtracting virtual size there fabricates a thinner
    market than exists. In those modes the raw touch already IS the net touch.
    """
    if not isinstance(md, dict):
        return None, None
    out: list[Optional[Decimal]] = []
    for key, own, best in (("bids", own_bid, max), ("offers", own_ask, min)):
        levels = md.get(key)
        if not isinstance(levels, list):
            out.append(None)
            continue
        totals: dict[Decimal, Decimal] = {}
        try:
            for level in levels:
                if not isinstance(level, dict):
                    continue
                raw_px = level.get("px")
                if isinstance(raw_px, dict):
                    raw_px = raw_px.get("value")
                if raw_px is None:
                    continue
                px = parse_wire(raw_px)
                totals[px] = totals.get(px, _ZERO) + parse_wire(level.get("qty", "0"))
        except (InvalidOperation, TypeError, ValueError):
            out.append(None)
            continue
        if own is not None and own[0] in totals:
            # No zero-clamp: the `qty > _ZERO` filter below already excludes a level our
            # over-belief drives negative (a clamp here was measured DEAD by mutation —
            # keeping it would let the next reader think it carries the promotion behaviour).
            totals[own[0]] = totals[own[0]] - own[1]
        live = [px for px, qty in totals.items() if qty > _ZERO]
        out.append(best(live) if live else None)
    return out[0], out[1]


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


def _elapsed_since(now_ts: float, since_ts: Optional[float]) -> Optional[float]:
    """`now_ts − since_ts`, or None when the row CANNOT BE DATED [the two fill rest clocks].

    Two ways it cannot: no start stamp, and a start stamp LATER than the row's own ts — real,
    not defensive, because a recovered fill is stamped with the VENUE's execution time. Blank is
    the honest answer; a 0 would read as "filled the instant it was placed".
    """
    if since_ts is None:
        return None
    elapsed = now_ts - since_ts
    return elapsed if elapsed >= 0 else None


def missed_cycles(wall_s: float, requote_s: float) -> int:
    """How many whole requote intervals a cycle overran by.

    The <run-id> question is "can one process hold 33 books inside the rate budget WITHOUT missing
    cycles", so an overrun has to be COUNTED. Absorbing it into a shrinking sleep is how a loop
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

    ⛔ ORDERS ARE GATED BY `config.DRY_RUN` ALONE. `--i-understand-real-money` only sets
    `DRY_RUN=false` in the shim's process environment. So a `.env` already carrying
    `DRY_RUN=false` with no flag would place REAL quotes while the run reported a preview. That
    is a HARD STOP here, never a downgrade to DRY — downgrading would hide a mis-configured
    environment. The reverse mismatch (flag passed, config still DRY) is NOT refused: it cannot
    move money. A pure function on purpose, so the guard is testable without setting DRY_RUN.
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
    #: Whole contracts for every quote; a `Decimal` fraction only for the teardown flatten's
    #: sub-contract dust close (`_teardown_flatten`). Readers already wrap it in `Decimal(...)`,
    #: which is exact for both — never a float round-trip.
    size: int | Decimal
    order_id: Optional[str]
    intent_id: str
    placed_ts: float
    queue_ahead: Optional[Decimal]
    improved: bool
    filled_qty: Decimal = _ZERO
    # The touch NET of our own resting size at the birth cycle's book read (`net_of_self_touch`)
    # — captured AT PLACEMENT like `queue_ahead`/`improved`, never refreshed. This is the
    # uncontaminated baseline the fill tape's best_bid/best_ask (self-quoted on ~all clean
    # improved rows) cannot provide.
    net_bid_place: Optional[Decimal] = None
    net_ask_place: Optional[Decimal] = None
    #: The GTD deadline this order was CREATED with — the deadline WE SENT, never one read back:
    #: the verification rail compares this against what the venue says, so overwriting it with a
    #: venue value would make the rail agree with itself.
    #: ⚠️ A deadline is NOT evidence the order is gone once it passes — see `_check_ttl`.
    deadline_ts: Optional[float] = None
    #: THIS ORDER's birth stamp for the fills tape's `order_rest_s` — the same wall moment as
    #: `placed_ts` but a SEPARATE optional field, so any path that puts an order into
    #: `self.resting` without having watched it being placed writes a BLANK rest column rather
    #: than a fabricated age. A 0 would read as "filled the instant it was placed".
    rest_since_ts: Optional[float] = None


@dataclass
class RecoveryEntry:
    """One order awaiting an activities-ledger verdict [belief-recovery v5 §3].

    `order` is the SAME RestingOrder object every other structure holds — a copy resets
    `filled_qty` and double-books the tail. `terminal_ts` is per entry PATH. `verdict_read_ts`
    is the wall time of the last page-1 activities read at or past `terminal_ts +
    LAG_HORIZON_S` — the NO-FILL verdict edge; 0.0 until one lands."""
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
    #: The ORDER's size, copied from `RestingOrder.size` — whole contracts for every quote, a
    #: `Decimal` fraction only for the teardown flatten's sub-contract dust close.
    size: int | Decimal
    filled_qty: Decimal              # this row's INCREMENT
    cum_filled_qty: Decimal          # the ORDER's running total, which the commission belongs to
    commission_order_total: Optional[Decimal]
    queue_ahead: Optional[Decimal]
    time_to_fill_s: float
    improved: bool
    order_state: str
    # ⛔ FIELD SEMANTICS, UNITS AND BLANK CAUSES LIVE IN the private design notes. What is kept
    # here is only what a reader must not get wrong at the site.
    # The last touch we read on this market and how old it was when the fill was BOOKED. A cheap
    # approximation (the cycle's own book read, no extra request); `mid_age_s` says how much to
    # trust it.
    mid_at_fill: Optional[Decimal] = None
    mid_age_s: Optional[float] = None
    # ⛔⛔ NOT THE BOOK'S HALF-SPREAD: `last_touch` comes from `_fetch_book`, and that book
    # CONTAINS OUR OWN RESTING ORDERS — on a bid fill where `best_bid == price`,
    # `(best_ask − best_bid)/2 ≡ |mid_at_fill − price|`, a width we chose against a mid we set.
    # Subtracting it uniformly across the tape publishes a drift figure wrong by an unbounded
    # amount. What they DO deliver is the prevailing self-inclusive half-spread plus a way to
    # DETECT the contaminated rows (`improved=Y and best_bid == price`).
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    # The PLACEMENT-time net-of-self touch — the self-clean baseline the two columns above
    # cannot provide. ⚠️ Three analyst-facing caveats: blank on THREE causes (so filtering to
    # non-blank selects AGAINST maximum own-impact and biases capture UP); it is the BIRTH-cycle
    # touch, NOT the fill-time width the markout identity needs (pair with `time_to_fill_s`); and
    # the subtraction uses BELIEF, so over-subtraction reports a WIDER market-behind-us than
    # exists — the direction that flatters capture.
    net_bid_place: Optional[Decimal] = None
    net_ask_place: Optional[Decimal] = None
    # ── ADVERSE-LATCH RULE INPUTS [cycle-4 P1] ───────────────────────────────────────────────
    # ⛔ POPULATED ON EXACTLY ONE ROW PER ARMING — the probe-lane fill whose completed round trip
    # armed the latch — and BLANK everywhere else. Blank means "this fill armed nothing", never
    # "unknown". ⛔ `latch_ratio` NO LONGER SETS THE BAN: under the randomized rule it is a
    # measured COVARIATE, so do not reconstruct `latch_ban_s` from it. `latch_rule` is the
    # ASSIGNMENT MECHANISM, which is what makes a draw identifiable on a tape spanning the switch.
    # ⛔ `ts + latch_ban_s` is the SCHEDULED re-admission, NOT the observed one — an operator ack,
    # the clean-run-end expiry, a re-trip's re-stamp, or a deadline past the end of the run all
    # move it. The boundary of record is the QUOTE TAPE.
    latch_hs: Optional[Decimal] = None
    latch_hs_src: str = ""
    latch_ratio: Optional[Decimal] = None
    latch_ban_s: Optional[float] = None
    latch_fallback: Optional[bool] = None
    latch_rule: str = ""
    # ── THE BOOKING-INSTANT TOUCH ───────────────────────────────────────
    # ⛔⛔ BOOKING-INSTANT, NOT FILL-INSTANT: stamped when the maker DISCOVERS the fill, and ~85%
    # of rows are discovered by a REST poll one or more requote intervals after the trade. The
    # bias is not random — a passive maker is filled BECAUSE the market is about to move — so a
    # post-move touch has already absorbed the adverse component and the error runs BENIGN, which
    # is the dangerous direction.
    #   · `booking_touch_age_s` bounds the FRAME's content age, ⛔ NOT the trade→booking gap.
    #   · `fill_stamp_lag_s` = row_ts − the venue's `lastTransactTime` [VERIFIED against recorded
    #     live bodies]. ⛔⛔ IT IS THE ORDER'S LAST TRANSITION, AND A CANCEL IS A TRANSITION:
    #     `_cancel` reads a fresh body on EVERY REPLACE, so a body read after the venue processed
    #     the cancel dates the CANCEL and a fill from minutes earlier reports a lag of ~0.
    #   · ⛔ THE READING RULE: a markout off these columns is only clean on `booked_via == "ws"`
    #     rows. Everywhere else, subtract `fill_stamp_lag_s` from the CLAIM, not the number.
    #   · ⛔ SELF-INCLUSIVE — these fix the AGE of the mark, not its self-inclusion.
    booking_bid: Optional[Decimal] = None
    booking_ask: Optional[Decimal] = None
    #   · NET-OF-SELF variants: the same booking-instant frame with our believed remainder
    #     subtracted — the columns that make an IMPROVED fill's spread readable, since improved
    #     means WE are the touch. Over-subtraction promotes past the level, the flattering-wide
    #     direction.
    booking_net_bid: Optional[Decimal] = None
    booking_net_ask: Optional[Decimal] = None
    booking_touch_age_s: Optional[float] = None
    fill_stamp_lag_s: Optional[float] = None
    # The venue's own `lastTransactTime` as a RAW epoch, so a venue-clock markout anchor needs no
    # `ts − lag` reconstruction. Same cancel caveat as the lag: it is only an execution stamp on
    # `booked_via == "ws"` rows.
    venue_ts: Optional[float] = None
    # Source of the book this fill's touch/mid were stamped from. ⛔ PARTITION:
    #   healthy  = {ws, ws_quiet}      degraded = {ws_degraded, ws_outage, rest_reverify}
    # Keep `ws` and `ws_quiet` SEPARABLE when asking whether the weaker arm's fills perform
    # worse — that is the whole reason they are two labels. Dropping the quiet arm from the
    # healthy side undercounts most of a quiet futures slate.
    book_src: str = ""
    # ⛔ THE VENUE'S OWN FILL PRICE (`avgPx`). NOT redundant with `price`: `price` is OUR limit,
    # and on a BUY_SHORT the venue reads that limit as a yes-space "here or BETTER" sell — a
    # recorded case differs materially. The venue computes the rebate at ITS price, so the
    # <run-id> gate must be evaluated at `avg_px`, not at our intent.
    avg_px: Optional[Decimal] = None
    # True when the delayed verify discovered this fill: its `time_to_fill_s` is an over-estimate
    # and its mid columns are blank ON PURPOSE — do not backfill them from any book near ts.
    late_booked: bool = False
    # WHICH PATH booked this increment: "ws" | "poll" | "verify". ⚠️ Read the tape correctly: a
    # zero-delta poll writes NO row, so the healthy case is a ws row with NO later poll/verify row
    # — ABSENCE is the evidence. A later positive-delta row after a ws row is an UNDER-stating
    # body; an OVER-stating body is invisible here and is `scripts.poly_order_diff`'s job.
    booked_via: str = "poll"
    # Was the private ORDER/FILL WS subscribed when this increment was BOOKED. `booked_via` is
    # NOT a blindness flag, so it was only ever a proxy; this is the measurement. ⚠️ FALSE also
    # covers "no order feed wired at all" — the question is "were we blind to fills right now".
    feed_subscribed: bool = False
    # ── THE TWO REST CLOCKS [requote-aware time-at-price] ────────────────────────────────────
    # ⛔ WHY `time_to_fill_s` COULD NOT ANSWER THE QUESTION: a REPLACE cancels and re-places, so
    # it restarts on every requote and "fast" conflates two opposite stories — we requoted INTO a
    # fill, or the market walked into a price we had rested at for minutes.
    #   · `order_rest_s`  seconds THIS order_id had rested. ⛔ Equals `time_to_fill_s`
    #                     numerically on today's maker; what it adds is the blank-on-unknown
    #                     contract. ⚠️ It measures rest→DISCOVERY, not rest→fill.
    #   · `price_rest_s`  seconds this (book, SIDE) had been CONTINUOUSLY QUOTED AT THIS PRICE.
    #                     A same-price REPLACE does NOT restart it; a price change or the side
    #                     going dark DOES. This is the new signal.
    # ⚠️ BLANK IS NOT ZERO on either — every blank cause is "cannot date this row".
    order_rest_s: Optional[float] = None
    price_rest_s: Optional[float] = None

    @property
    def verdict(self) -> str:
        return commission_verdict(self.commission_order_total)


@dataclass
class CycleStats:
    """Rung-0 instrumentation. The question <run-id> answers is operational, not economic: can one
    process hold N books inside the rate budget without missing cycles?"""
    cycle: int
    started_ts: float
    wall_s: float = 0.0
    requests: int = 0
    # Fresh-REST re-reads this cycle triggered by a stale/absent WS book — the C-a cap reads this
    # (a partial feed freeze must not re-read the whole slate in one cycle).
    ws_stale_rereads: int = 0
    # Books SKIPPED over the C-a cap (candidate partial freeze — feeds the escalation) and
    # seconds spent in whole-connection/partial-freeze fallback.
    ws_capped: int = 0
    ws_down_s: float = 0.0
    # REST reads forced by the fallback window — ZERO of these (plus zero capped) is the positive
    # recovery evidence.
    ws_outage: int = 0
    # Periodic fresh-REST re-verifies issued this cycle. ⛔ DELIBERATELY separate from
    # `ws_stale_rereads`: that one feeds the C-a cap and the partial-freeze escalation, and a
    # SCHEDULED re-verify is not evidence of a freeze — folding it in re-creates the false halt
    # this brick removes.
    ws_reverifies: int = 0
    # Re-verifies that caught a STALE CACHE on the quiet arm. With C-a unreachable on a
    # frame-alive socket, THIS is the only per-book freeze detector the ws path has, so it must
    # be on the cycle tape or a silent unsubscribe is invisible to every after-the-fact read.
    ws_reverify_stale_cache: int = 0
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
    # combined floor is validated by time series, not asserted.
    avail_mb: Optional[float] = None
    swap_free_mb: Optional[float] = None

    @property
    def req_per_s(self) -> float:
        return projected_req_per_s(self.requests, self.wall_s) if self.wall_s > 0 else 0.0


def _open_writer(path: str, header: Sequence[str], rotate_tag: str = "schema"):
    """Append-with-header-once, so a restart extends the same tape instead of interleaving a
    second header row. A header MISMATCH first freezes the old file into
    <dir>/rotated/<stem>.pre-<tag>.csv — appending blind across a column change leaves rows and
    header disagreeing on width, and DictReader misfiles silently in either direction."""
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
    cosmetic bug — it disables the entire maker SILENTLY: no id ⇒ `poll_fills` skips the order ⇒
    no fill, no commission, no inventory ⇒ both caps read a permanent zero position ⇒ `_cancel`
    never sends a cancel, so every reprice ADDS a resting order ⇒ the teardown sweep skips it and
    the process exits printing a clean teardown over live orders.

    Returns None rather than raising, because the caller must treat a missing id as
    CANNOT-VERIFY (the order may well be resting) rather than as a failed placement.
    """
    body = _order_body(resp)
    raw = body.get("id")
    return str(raw) if raw else None


#: The ONLY venue intents this maker ever ships, mapped to the LANE SIDE that shipped them.
#: ⛔ A WHITELIST: an unrecognised or absent value must read as NO INFORMATION, never fall
#: through to a side. Soundness is the YES-SPACE DIRECTION, not the producer — this rail does
#: meet orders from other producers on our slugs. BUY_LONG adds long (+, our "bid" lane);
#: BUY_SHORT (−) and SELL_LONG (−) both sit on the "ask" lane. SELL_SHORT is + and is OMITTED
#: DELIBERATELY: it resolves to None and the order is CANCELLED, not adopted — the
#: exposure-reducing direction.
_ORPHAN_INTENT_SIDE: dict[str, str] = {
    "ORDER_INTENT_BUY_LONG": "bid",
    "ORDER_INTENT_BUY_SHORT": "ask",
    "ORDER_INTENT_SELL_LONG": "ask",
}


def orphan_lane_side(body: dict) -> Optional[str]:
    """Our lane side ("bid"/"ask") for a listed order, or None if it cannot be established.

    None is a real answer and the caller must treat it as CANNOT-ATTRIBUTE: the order is still
    ours to cancel (its slug decides that), but nothing may be booked against a guessed side.
    """
    raw = body.get("intent") or body.get("orderIntent")
    return _ORPHAN_INTENT_SIDE.get(str(raw)) if raw else None


# ── the engine ───────────────────────────────────────────────────────────────────────────────

class PolyMaker:
    """The quoting loop, its inventory, its caps and its teardown.

    Venue contact is confined to five client methods. Nothing here reads `bbo`: every Poly public
    GET is Cloudflare `max-age=30`, and a touch up to 30 s old fails OPEN in the direction that
    costs money.
    """

    #: NOTIFY-ONLY probe-stats channel — a DISABLED default at CLASS level so a rail that
    #: announces on it works on an instance built by `__new__`. ⛔ Disabled, never live: a
    #: class-level live notifier would post from any instance.
    _probe_notes: "probe_notify.ProbeNotifier" = probe_notify.ProbeNotifier("")

    #: The rebate step, OFF at CLASS level. ⛔ THE CLASS DEFAULT IS THE **OFF** ONE and must stay
    #: that way: an instance built by `__new__` (tests do this to exercise one rail without a
    #: whole maker) has consented to nothing, so the fail-safe answer for a sizing rule is "do not
    #: act". ⛔ NO CLASS-LEVEL `_rebate_step_state`: it is a MUTABLE map, and a class attribute
    #: would be shared by every instance.
    rebate_step: bool = False
    rebate_step_max: Optional[int] = None
    #: ⛔ THE CLASS DEFAULT IS TODAY'S BEHAVIOUR [operator decision 2026-09-04]: a
    #: `__new__`-built instance sizes by the 2026-09-03 floor rule. `__init__` replaces it.
    rebate_step_mode: str = REBATE_STEP_MODE_FLOOR
    #: Same rule, same reason: OFF is the fail-safe answer for a
    #: sizing rule, so a `__new__`-built instance stands nothing down.
    dust_hold: bool = False
    #: ⛔ THE CLASS DEFAULT IS TODAY'S BEHAVIOUR. A `__new__`-built
    #: instance quotes exactly as the maker did before the A/B existed; `__init__` replaces it.
    quote_mode: str = QUOTE_MODE_IMPROVE
    #: ⛔ THE GAME-CLOCK GUARD's GATE, EMPTY AT CLASS LEVEL [AMENDMENT 41]. Same rule and same
    #: reason as every default above: a `__new__`-built instance has consented to nothing, so the
    #: fail-safe answer is "guard nothing" — and `clock_guard_active` tests THIS first and returns
    #: before it reads the file or touches any of the rail's mutable state, which is why the gate
    #: is the ONE attribute that needs a class default. ⛔ A `frozenset`, not a `set`: a mutable
    #: class attribute would be shared by every instance. ⛔ AND NO CLASS-LEVEL DEFAULT FOR THE
    #: THREE MUTABLE SETS (`_clock_guard_armed`, `_clock_guard_post_latched`,
    #: `_clock_guard_unguarded_announced`) or the cache — they are per-engine, `__init__` is the
    #: only writer of the gate, so an instance with a non-empty gate always has them.
    #: ⛔ NEVER a `getattr` default at the read site: the guard is on the quote path.
    clock_guard_slugs: Container[str] = frozenset()
    clock_guard_file: Optional[str] = None
    #: ⛔ THE MAINTENANCE POSTURE's GATE, EMPTY AT CLASS LEVEL, for `clock_guard_slugs`' reasons
    #: exactly. `maintenance_guard_active` tests THIS first, so a
    #: `__new__`-built instance stands nothing down. The 503-WALL HOLD is NOT gated on it: that
    #: one is venue-wide and asks only the clock (see `_maintenance_now`).
    maintenance_guard_slugs: Container[str] = frozenset()
    #: ⛔ THE EMPTY-BOOK SEED's GATE, EMPTY AT CLASS LEVEL, for the two above's reasons exactly
    #: [P1b]: `_seed_touch` tests THIS FIRST and returns before it reads any of the seed's mutable
    #: state, so a `__new__`-built instance (the park-seat tests drive `_quote_market` on one)
    #: seeds nothing instead of raising on the quote path. ⛔ A `frozenset`, not a `set`.
    seed_last_touch_slugs: Container[str] = frozenset()

    def __init__(
        self,
        *,
        client: Any,
        slugs: Iterable[str],
        size: int | str = MIN_SIZE,
        cap_fills: int | str = 1,  # placeholder — production value withheld
        requote_s: float = 60.0,  # placeholder — production value withheld
        max_total_contracts: Optional[int] = None,
        max_req_per_s: float = DEFAULT_MAX_REQ_PER_S,
        shadow: bool = True,
        real: bool = False,
        flatten_wait_s: float = 120.0,  # placeholder — production value withheld
        #: OPT-IN teardown cross. Default OFF: a probe-lane residual is REPORTED and CARRIED into
        #: the next cycle at the replayed basis, never force-crossed at the taker price.
        teardown_cross: bool = False,
        stale_cancel_cycles: int = 1,  # placeholder — production value withheld
        adverse_cooldown_s: float = 86400.0,  # placeholder — production value withheld
        mark_trip_per_ct: Decimal = DEFAULT_MARK_TRIP_PER_CONTRACT,
        resolves_at: Optional[dict[str, float]] = None,
        presence_workdown: Optional[set[str]] = None,
        #: OPT-IN per-side rebate-step sizing. Default OFF: every book quotes its configured
        #: size at every price, exactly as before. See `_rebate_step_size`.
        rebate_step: bool = False,
        #: The absolute ceiling the step may raise a side to. `None` = `2 × book_size`, i.e.
        #: per-book. Every existing cap still binds on top of it — see `_rebate_step_size`.
        rebate_step_max: Optional[int] = None,
        #: Which size the step picks inside that ceiling. `floor` (default) is byte-identical to
        #: the 2026-09-03 rule; `optimal` maximises PAID cents per contract.
        rebate_step_mode: str = REBATE_STEP_MODE_FLOOR,
        #: OPT-IN dust-aware reducing size. Default
        #: OFF: a sub-contract residue quotes exactly as it did before. See `_quote_one`.
        dust_hold: bool = False,
        #: `improve` (default, today's behaviour) or `join` [, prereg
        #: the private design notes § 8]. See `QUOTE_MODES`.
        quote_mode: str = QUOTE_MODE_IMPROVE,
        #: ⛔ AMENDMENT 19c — the LATCH-ONLY CONTROL cells of this run, drawn at PICK time. See
        #: `self.latch_off_slugs`. `None`/empty = every book latches as it always did.
        latch_off_slugs: Optional[set[str]] = None,
        #: ⛔ AMENDMENT 41 — the books the GAME-CLOCK GUARD may stand down (the derivative seats;
        #: see `clock_guard_active`). `None`/empty = the guard never fires, the pre-amendment
        #: behaviour.
        clock_guard_slugs: Optional[set[str]] = None,
        #: The guard's input file. `None` → `CLOCK_GUARD_FILE` resolved AT CALL TIME, never bound
        #: here: the test sandbox redirects the module constant and a bound default escapes it.
        clock_guard_file: Optional[str] = None,
        #: ⛔ The books that go REDUCE-ONLY inside the venue's maintenance window [operator
        #: 2026-09-10]: the game-clock-guarded classes PLUS every in-play sports class, and
        #: NEVER a declared non-sports fixed seat. The class → slug resolution is the launcher's
        #: (`--maintenance-guard-slugs`), never the maker's.
        maintenance_guard_slugs: Optional[set[str]] = None,
        #: ⛔ The books whose EMPTY book may be re-seeded at their last recorded two-sided touch
        #: [continuous maker P1b]: declared NON-SPORTS fixed seats and nothing else. The
        #: class → slug resolution is the launcher's (`--seed-last-touch-slugs`), never the
        #: maker's — the maker knows no class rules, and it still requires the book to be
        #: `pinned` on the live slate before it seeds a price.
        seed_last_touch_slugs: Optional[set[str]] = None,
        loss_cap: Decimal = Decimal("0.01"),  # placeholder — production value withheld
        heartbeat: Any = None,
        order_feed: Any = None,
        state_store: Optional[MakerStateStore] = None,
        guard_thresholds: Optional[dict[str, int]] = None,
        lane: Optional[str] = None,
        lane_scoped: Optional[bool] = None,
        run_id: Optional[str] = None,
        quote_csv: Optional[str] = None,
        cycle_csv: Optional[str] = None,
        fill_csv: Optional[str] = None,
        probe_retirement: bool = False,
        book_source: str = "rest",
        book_feed: Any = None,
        ws_stale_s: float = WS_BOOK_STALE_S,
        ws_conn_live_s: float = WS_CONN_LIVE_S,
        ws_reverify_s: float = WS_REVERIFY_S,
        ws_audit_slug: Optional[str] = None,
        ws_seed_timeout_s: float = WS_SEED_TIMEOUT_S,
        conditional_poll: bool = False,
        hot_settings_file: Optional[str] = None,
        hot_slate_file: Optional[str] = None,
        requote_ab_armed: bool = False,
        order_ttl_s: float = 0.0,
        ttl_fallback: bool = True,
        ttl_csv: Optional[str] = None,
        advexit_csv: Optional[str] = None,
    ) -> None:
        self.client = client
        # Per-cycle book READ source. "rest" = the cache-busted `_fetch_book`; "ws" = the
        # injected cache's `get_book_md` behind a transactTime freshness gate + fresh-REST
        # backstop. ⛔ With no running feed "ws" degrades to REST, never to a stale or empty book.
        self.book_source = book_source if book_source in ("rest", "ws") else "rest"
        self.book_feed = book_feed
        # ── GTD order TTL — DEFAULT OFF (0), and OFF is byte-identical to every run before it.
        # When armed, `_place` stamps ONE LONG `goodTillTime` on each create. NOTHING RENEWS IT.
        # `_observe_ttl` is the rail that keeps this honest.
        self.order_ttl_s = float(order_ttl_s or 0.0)
        if self.order_ttl_s < 0:
            raise ValueError(f"REFUSING to start: --order-ttl-s must be ≥ 0, got {order_ttl_s!r}")
        if self.order_ttl_s > 0 and self.order_ttl_s < TTL_MIN_S:
            # ⛔ THE RESIDENCE FLOOR, at startup, before an order exists. The binding quantity is
            # how long an order RESTS untouched (hours, measured), not the requote cadence — a TTL
            # under the floor expires live quotes in life and resets the queue every TTL. Refuse
            # the configuration rather than discover it at 03:00.
            raise ValueError(
                f"REFUSING to start: --order-ttl-s {self.order_ttl_s:g} is below the "
                f"{TTL_MIN_S:g}s ({TTL_MIN_S / 3600:g}h) residence floor. The quote loop does NOT "
                f"renew deadlines — it HOLDs (an order's natural residence runs for hours) — so a TTL shorter "
                f"than that "
                f"expires LIVE quotes, records `hold` over nothing, and resets the queue position "
                f"every TTL. This is a dead-man's brake, not a keepalive; set it well above the floor as the "
                f"setting.")
        #: Falling back to plain GTC when the rail catches the venue misbehaving. True (the
        #: default) is the safe direction: quoting GTC and knowing it beats quoting on deadlines
        #: that are not being honoured. False keeps TTL on and only warns — for a probe night
        #: whose whole purpose is to observe the misbehaviour.
        self.ttl_fallback = bool(ttl_fallback)
        #: Set once the rail (or an operator config) turns TTL off mid-run; the reason is taped.
        self._ttl_disabled: Optional[str] = None
        self.ttl_stamped = 0
        self.ttl_expired = 0
        #: ⛔ Order ids already counted as expired. `_observe_ttl` sees a terminal read but does
        #: NOT pop, and under the store lag the listing can carry the order for further polls —
        #: without this the counter grew once PER OBSERVATION while the summary presented them as
        #: orders.
        self._ttl_expired_ids: set[str] = set()
        self.ttl_purged_past_deadline = 0
        self.ttl_stale_deadline = 0
        self.ttl_not_enforced = 0
        #: Stale-deadline confirmations within the CURRENT poll — reset per poll, because the
        #: fallback trigger is "a cluster in one poll", not "N over a night".
        self._ttl_stale_this_poll = 0
        #: Observed enforcement latencies (deadline → the poll that saw it gone), for the run
        #: summary. Bounded by our poll cadence, so it is an UPPER bound on the venue's own.
        self.ttl_expiry_lags: list[float] = []
        # Conditional order poll — DEFAULT OFF; a run arms it explicitly (--conditional-poll)
        # per the design's rollout: shadow, then a one-market probe, then slate-wide.
        self.conditional_poll = bool(conditional_poll)
        self._poll_seq = 0
        self.conditional_poll_skips = 0
        # listing-vs-read cum disagreements, split by direction (see poll_fills) — the
        # evidence base for ever widening --conditional-poll past one book.
        self.poll_listing_behind = 0
        self.poll_listing_ahead = 0
        # Session rebate context for the loss-cap MESSAGE (never the rail): latest
        # commissionNotionalTotalCollected per order — per-ORDER cumulative, so the total is
        # Σ of LAST values, never a sum across fill increments [flow-analysis rule].
        self._order_commissions: dict[str, Decimal] = {}
        # Per-run freshness gate: the module default was tuned for a wide shadow slate where the
        # REST backstop was expensive; on a small real slate the operator passes a tighter value.
        self.ws_stale_s = float(ws_stale_s)
        # ── RECEIPT FRESHNESS — the OR-rule and its hard bound ───────────────────────────
        # `ws_stale_s` alone cannot serve a QUIET book: transactTime is last-MUTATION, so an
        # untraded-but-correct book ages past any gate. A book is servable if its content is
        # young OR the socket is provably still receiving frames within `ws_conn_live_s`.
        self.ws_conn_live_s = float(ws_conn_live_s)
        # Socket liveness proves the PUBLISHER is alive, never that THIS book's stream still is
        # (Poly has no per-book sequence number). So each book is re-confirmed against fresh REST
        # at least every `ws_reverify_s`, whatever the socket says.
        self.ws_reverify_s = float(ws_reverify_s)
        # ── COLD-START SEED — see WS_SEED_TIMEOUT_S for the MEASUREMENT ──────────────────
        # Total seconds `prepare()` may wait for the slate to become SERVABLE before cycle 1.
        # 0 = no wait. The three attributes below are REPORTING ONLY — no rail reads them.
        self.ws_seed_timeout_s = max(0.0, float(ws_seed_timeout_s))
        # Did the WHOLE slate reach SERVABILITY inside the bound — NOT "is a frame present". A
        # present-but-stale frame on a quiet socket is exactly what the seam declines to serve,
        # so presence would declare success on the population the seed exists to wait out.
        self.ws_seed_servable = False
        self.ws_seeded_books = 0
        self.ws_seed_unframed = 0
        # slug -> wall-clock of that book's last VENUE confirmation (any successful fresh-REST
        # read). Seeded on a book's first ws serve rather than treated as "never verified" — an
        # unseeded clock would fresh-REST the WHOLE slate on cycle 1.
        self._last_verified_ts: dict[str, float] = {}
        # Re-verifies that came back SAME-VERSION-DIFFERENT-PRICE — the fresh-but-WRONG class.
        # Loud, counted, and the REST value serves that cycle.
        self.ws_reverify_mismatches = 0
        self.ws_reverifies = 0
        # Stale-cache detections (quiet arm, venue moved, we never got the frame) — run total.
        self.ws_reverify_stale_cache = 0
        # slug -> CONSECUTIVE stale-cache detections. Reset by any agreement on that book.
        self._ws_stale_cache_streak: dict[str, int] = {}
        # Books escalated to WS-DEAD: their stream is gone even though the socket is not, so they
        # are REST-only until a genuinely FRESH snapshot arrives.
        self._ws_dead_books: set[str] = set()
        # The slugs this cycle may re-verify, chosen OLDEST-CLOCK-FIRST once per cycle. Draining
        # in iteration order starved the genuinely oldest book — the backlog the per-cycle bound
        # creates has to drain by AGE to be a bound on anything.
        self._reverify_allowed: set[str] = set()
        self._reverify_plan_cycle: Optional[int] = None
        # §7-4's first-real-use rung: ONE market read per cycle from fresh REST beside the ws
        # value the cycle actually used. Only runs when that slug's read really came from ws.
        self.ws_audit_slug = ws_audit_slug
        self.ws_audit_checks = 0
        self.ws_audit_divergences = 0
        self.ws_audit_moved = 0
        # Prices AGREED at a DIFFERING transactTime. Broken out rather than folded into agreement
        # because the audit slug has no periodic re-verify, so this is its only
        # silent-unsubscribe tell.
        self.ws_audit_agree_stale_tt = 0
        # slug -> the source that produced this cycle's book (vocabulary and partition rule on
        # `Fill.book_src`). Read at tape-write time so quote/skip rows carry `book_src` without
        # threading it through every writer signature.
        self.last_book_src: dict[str, str] = {}
        # WS fallback state: wall-clock when the whole connection was first seen down (None =
        # up). While set, the quote loop runs the REDUCED set over REST; at expiry the run halts.
        self._ws_down_since: Optional[float] = None
        # N1 evidence counters: the marker must record that the WS path actually SERVED —
        # halt-absence is not feed-worked. ⛔ `ws_reads` counts CONTENT-FRESH serves ONLY: a quiet
        # serve is evidence the SOCKET is alive, never that the book's stream is being delivered,
        # and folding it in would let a slate served entirely on socket liveness certify the feed
        # at ws_fraction 1.00 — the very claim the N1 marker exists to test.
        self.ws_reads = 0
        self.ws_quiet_reads = 0
        self.book_reads = 0
        self.ws_feed_deaths = 0
        # Books skipped over the C-a cap LAST cycle — the partial-freeze escalation signal.
        self._last_ws_capped = 0
        # Forced-REST outage reads LAST cycle + per-episode bookkeeping: recovery requires a
        # cycle with ZERO of both (positive WS evidence); the dropped-books cancel runs once per
        # EPISODE via a flag; whole-connection entry needs a 2-cycle disconnect streak so a
        # reconnect blip does not cost the whole slate's queue position.
        self._last_ws_outage = 0
        self._ws_cancel_done = False
        self._ws_disconnect_streak = 0.0
        # True when LAST cycle quoted the REDUCED set — recovery evidence from a reduced cycle is
        # only good for PROBATION; only a clean FULL-SLATE cycle truly recovers.
        self._last_cycle_reduced = False
        # Relapsed full-slate probation attempts in the CURRENT outage episode.
        self._ws_probation_relapses = 0
        # The near-cap trip-point line is printed once per run — see `_announce_near_cap_trip`.
        self._near_cap_announced = False
        # NEAR-CAP GRACE state: True only while the grace branch is riding a dark feed near cap —
        # `_read_book_md` then refuses the ws-cache serve WHILE THE SOCKET IS DOWN and forces
        # REST. A RECONNECTED socket's serves stay allowed: they are the recovery evidence, and
        # blanket-skipping the cache would turn every grace into a delayed halt.
        self._ws_nearcap_grace = False
        # Graced episodes THIS RUN — unbounded, a flapping socket is an all-night flap-ride near
        # cap. Counted per EPISODE via the boolean below, NOT via `_ws_down_since is None`: the
        # partial-freeze escalation sets `_ws_down_since` at the END of a cycle, so episodes
        # entered that way never began on a None and the None-based count read 0 forever.
        self._ws_nearcap_grace_episodes = 0
        self._ws_grace_episode_counted = False
        # Wall-clock of the last cycle that OBSERVED the socket down. `_cache_frozen` uses it to
        # demand a frame that POST-DATES the outage before the ws cache may serve again during a
        # near-cap grace: `connected` goes True at handshake, before any frame, and the pre-death
        # cache is never cleared — so keying on `connected` served the frozen cache.
        self._ws_last_down_ts = 0.0
        self.slugs = [s.strip() for s in slugs if s and s.strip()]
        # normalized key → the slate string it came from, for the venue-row membership test.
        # Built here so it covers every DECLARED book, not only the ones whose tick read worked.
        self._slate_norm: dict[str, str] = {norm_slug(s): s for s in self.slugs}
        self._slug_norm_warned: set[str] = set()
        # PER-BOOK SIZING: `size` is an int (uniform) or a "slug:size,…" spec. Everything
        # downstream reads the MAP; `self.size`/`self.cap` survive as the MAX.
        # ⚠️ The `.get(slug, self.size)` fallbacks are structurally DEAD, and MAX is the UNSAFE
        # direction if one ever fires (a bigger quote, a LOOSER cap). Do not read them as a
        # safety margin: a refactor that makes one reachable should switch to `self.sizes[slug]`
        # and let the KeyError refuse.
        self.sizes = parse_sizes(str(size), self.slugs) if self.slugs else {}
        for slug, sz in sorted(self.sizes.items()):
            refusal = size_refusal(sz)
            if refusal:
                raise ValueError(f"{slug}: {refusal}")
        self.size = max(self.sizes.values(), default=int(size) if str(size).isdigit()
                        else MIN_SIZE)
        # Per-book cap-fills: same spec discipline as --size — one int for all, or per-book
        # naming EVERY slug (an unnamed book refuses rather than inheriting a headroom the
        # operator never chose).
        self.cap_fills_by_slug = (parse_sizes(str(cap_fills), self.slugs)
                                  if self.slugs else {})
        # MIN, not max [review r2 NIT]: this scalar is the documented-unreachable fallback
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
        # ── HOT SETTINGS (M0.8) — launch anchors + per-book quote-mode overrides ──
        # `_launch_reach`/`_launch_cap_fills` are the INTERLOCK references: the pos-guard
        # thresholds are process-fixed at launch, so the CEILING is judged against these frozen
        # copies. ⚠️ But `slate_sizes` is deliberately the LIVE `self.sizes` — the cap_fills reach
        # check must see the size that will actually run, or two individually-legal files compose
        # past the anchor. Do not "clean this up" by freezing it; the suite pins it RED.
        self._launch_cap_fills = dict(self.cap_fills_by_slug)
        # ⛔ PER-BOOK, not the slate max [M0.8 verification (a)]: the external guards are
        # per book (own reach + 25), so a small book hot-raised under the SLATE max can
        # trip ITS OWN guard and halt the whole run on a lawful overshoot — a small book hot-raised
        # under the slate max passes the slate ceiling and fires its own guard. A book may
        # never exceed the reach its own guard was sized for at launch.
        self._launch_reach = {slug: sz * (self.cap_fills_by_slug[slug] + 1)
                              for slug, sz in self.sizes.items()}
        #: The FLOOR under the anchor above, frozen for the process. The anchor moves in both
        #: directions, but may never fall BELOW this — a hot lowering would otherwise turn
        #: inventory that was lawful at launch into a breach.
        self._launch_reach0 = dict(self._launch_reach)
        #: slug → the cycle a hot size/cap_fills change landed on. The reach anchor may not be
        #: RELEASED until a LATER cycle has run: on the cycle a lowering applies, an order placed
        #: under the RAISED config can still be resting, and `reach = cap + size` exists to
        #: absorb that in-flight overshoot.
        self._resize_cycle: dict[str, int] = {}
        #: books whose release is currently deferred, so the reason is announced once and not
        #: once per cycle.
        self._release_defer_announced: dict[str, str] = {}
        # ──  v1.1: the EXTERNAL guards' REAL thresholds ────────────────────────────
        # What the launcher started each `poly_pos_guard` with. ⛔ TOLD, NEVER DERIVED:
        # re-deriving the launcher's margin here is a second implementation of a rail that
        # already has one, and the two agree only until one changes. Absent = a maker that does
        # not know what is watching it, so the parser refuses every raise. A threshold BELOW the
        # book's own launch reach is refused at construction — it means the guard fires on
        # inventory this run may lawfully hold.
        self._guard_thresholds: dict[str, int] = {}
        #: slug → the guard's headroom above the reach it was sized for, measured at launch.
        #: The stale-guard line quotes it back so the operator restarts at the same margin —
        #: the maker still does not own the launcher's constant, it only remembers what
        #: arrived.
        self._guard_margin: dict[str, int] = {}
        for _slug, _thr in dict(guard_thresholds or {}).items():
            if _slug not in self._launch_reach:
                raise ValueError(f"guard_thresholds names {_slug!r}, which is not on the "
                                 f"slate {sorted(self._launch_reach)} — a threshold for a "
                                 f"book this run does not quote is a wiring error")
            _thr = int(_thr)
            if _thr < self._launch_reach[_slug]:
                raise ValueError(
                    f"{_slug}: guard threshold {_thr} is BELOW its lawful reach "
                    f"{self._launch_reach[_slug]} — that guard fires on inventory this run "
                    f"may lawfully hold, so the run would false-halt; fix the guard, not this")
            self._guard_thresholds[_slug] = _thr
            self._guard_margin[_slug] = _thr - self._launch_reach[_slug]
        # The venue-breach threshold for a book NOT on the slate (another run's or the
        # operator's position), FROZEN here because a hot lowering must not move the halt bound
        # on inventory this run never touched.
        # ⛔ The slate MINIMUM per-book reach, not the max: an
        # off-slate book is one we sized at NOTHING, so the tightest slate bound is the honest
        # one and the fail direction stays toward HALTING. As `cap + size` (the slate MAX) a
        # large declared seat lifted this bound far above the small off-slate residuals
        # — the out-of-process-close class — that no longer halted.
        self._launch_reach_unknown = min(self._launch_reach.values(),
                                         default=self.cap + self.size)
        self.hot_quote: dict[str, str] = {}
        #: ⛔ PER-LANE HOT SETTINGS. `None` = the shared config default, which is what every
        #: single-lane run uses and is byte-identical to the pre-2026-08-22 behaviour. A
        #: two-lane night MUST give each lane its own path: the parser is
        #: whole-file-or-nothing and judges every slug against the READING lane's slate, so one
        #: file shared by two slates means each lane's books are slate ADDITIONS to the other
        #: and every drain, size override and latch ack in it is voided whole. Resolved lazily
        #: through `_hot_file()` so the config default is still read at USE time, never frozen
        #: at construction.
        self.hot_settings_file: Optional[str] = hot_settings_file or None
        self._hot_sig: tuple | None = None            # (st_mtime_ns, st_size) last SEEN
        self._hot_announced: tuple | None = None      # last sig announced (apply OR ignore)
        #: ⛔ THE ACK CHANNEL for hot_settings WRITERS. Stamped into every beat so a writer can
        #: tell APPLIED from REFUSED without grepping this process's log. It cannot be read off
        #: `hot_settings_changes.csv`: that tape carries applied CHANGES only, so a legal file
        #: whose values were already current and a file the parser voided WHOLE both write nothing
        #: — the two failure directions the writer most needs to separate. LEVEL, not edge: the
        #: read is mtime-gated, so it must persist across every unchanged cycle. `hot_file` rides
        #: along because the heartbeat FILE is shared by every lane of a mode.
        self._hot_ack: dict[str, str] = {"hot_hash": "", "hot_status": "absent",
                                         "hot_refused_why": "",
                                         "hot_file": self._hot_file()}
        # ── HOT SLATE [continuous maker P1] ────────────────────────────────────────────────
        #: ⛔ PER-LANE, for hot_settings' reason and harder: this file is the whole BOOK SET, so
        #: a lane reading a sibling's file retires its own slate whole on the first apply.
        self.hot_slate_file: Optional[str] = hot_slate_file or None
        #: The slate generation. 0 at launch, +1 on every APPLIED slate file (adds, retires and a
        #: cadence change alike), stamped on every quote/fill/cycle row.
        self.slate_epoch = 0
        self._slate_sig: tuple | None = None         # (st_mtime_ns, st_size) last SEEN
        self._slate_announced: tuple | None = None   # last sig announced (apply OR refuse)
        #: The last APPLIED parse result, for the no-op test. A rewritten file with identical
        #: content and the same epoch must bump nothing and write no row — the mtime gate cannot
        #: answer that (a `cp` gives new bytes' mtime to the same bytes).
        self._slate_applied: dict | None = None
        #: ⛔ TOLD, NEVER DERIVED [mm-review 2026-09-10]: was this run launched INSIDE the cadence
        #: A/B? While it is, a slate `requote_s` change is refused whole — the A/B's reader keys
        #: its arm on the RUN (the supervisor journal's `requote_s_arm`), so a mid-run cadence
        #: change re-labels the whole day with the launch arm and silently pools two cadences
        #: into one arm. Lifted when the reader is re-keyed on `epoch` (P4).
        self.requote_ab_armed = bool(requote_ab_armed)
        self._slate_ack: dict[str, str] = {"slate_hash": "", "slate_status": "absent",
                                           "slate_epoch": "0"}
        #: ⛔ THE LAUNCH `cap_fills` SCALAR, frozen. The slate schema carries NO `cap_fills` key —
        #: a hot add takes the launch scalar, so a re-add's cap is the cap the launch line chose
        #: and no slate file can widen a book's reach by naming a bigger headroom.
        self._slate_cap_fills = self.cap_fills
        #: Books the slate has RETIRED: reduce-only until flat, then dropped. The sixth
        #: `reduce_only_now` term — never a cancel, because a retire must not forfeit the queue
        #: a still-holding book needs to work down.
        self._slate_retiring: set[str] = set()
        #: EVERY book this run ever quoted, adds included. The latch-expiry sweep at a clean end
        #: iterates THIS, not `slugs` — a dropped book's latch is still a fact about the night.
        self._ever_slugs: set[str] = set(self.slugs)
        #: Books the slate PINNED (`pinned: true`): never retired by a reseater's diff.
        self._slate_pinned: set[str] = set()
        #: slug → (tick, min_qty, admitted_ts) for an admitted book NOT YET IN `self.ticks`. A
        #: book enters the quote set only once the WS feed has framed it (or the seed bound
        #: lapsed): staged straight in, cycle 1 of the add would fresh-REST it and, at
        #: `WS_STALE_REREAD_MAX`+ unframed books in one cycle, book a partial-feed death.
        self._slate_pending: dict[str, tuple[Decimal, Optional[Decimal], float]] = {}
        self.requote_s = requote_s
        # A default equal to the sum of the per-market caps is NON-BINDING by construction — that
        # is deliberate (inventing a tighter number is a risk decision this module has no basis to
        # make), but it means the global cap does nothing until an operator sets it.
        self.max_total_contracts = (max_total_contracts if max_total_contracts is not None
                                    else sum(self.caps.values()) or self.cap)
        self.max_req_per_s = max_req_per_s
        self.shadow = shadow
        self.real = real
        self.flatten_wait_s = flatten_wait_s
        self.teardown_cross = teardown_cross
        # The parked-verify bound, scaled to the run: every slug can cancel both sides each cycle
        # and an entry lives ~LAG_HORIZON_S/requote cycles, so the worst honest steady state is
        # ~2 × slugs × horizon/requote. MAX_PARKED stays as the single-market floor.
        self.max_parked = max(MAX_PARKED,
                              2 * max(1, len(self.slugs)) * int(LAG_HORIZON_S / max(1.0, requote_s)))
        # ⛔ Consecutive un-quotable cycles before a market's resting quotes are PULLED. Both
        # directions are hazards: cancelling on a single failed read forfeits queue position on
        # every routine hiccup, while never cancelling leaves live orders on a market nothing is
        # reading any more — they can still fill, at a price no guard is checking.
        self.stale_cancel_cycles = stale_cancel_cycles
        # Post-adverse cooldown (0 disables): after a round trip realizes worse than
        # ADVERSE_RT_PER_CONTRACT per contract, the book quotes REDUCE-ONLY for this long — flat
        # means no quotes at all. Chosen by tape replay: the passive chase beat every crossing
        # stop, but re-entering the same book soon after the adverse close bought the top of the
        # deflating spike. The deadline is wall-clock and NOT persisted — a crash-restart clears
        # it, which is accepted: a stand-down is a quoting preference, not a safety rail.
        self.adverse_cooldown_s = adverse_cooldown_s
        #: The probe lane's ban-length assignment stream. ⛔ ITS OWN `random.Random`, never the
        #: `random` module globals: a shared global stream makes the assignment depend on whatever
        #: else in the process drew from it, so the sequence is neither reproducible nor
        #: auditable. Unseeded on purpose; TESTS replace this attribute with a seeded instance.
        self._ban_rng = random.Random()
        # ⛔ REALIZED-only loss cap (0 disables). Semantics, stated honestly: the in-run halt
        # compares the RAW NET total (−total > cap), so in-session profit extends the loss budget
        # by that much; the restart refusal applies `loss_to_date`'s zero floor to the same net
        # number. PRICE-realized only — rebates (the strategy's whole edge) are NOT credited, so
        # the cap can trip on a rebate-profitable lifetime; marks are NOT counted, so an open
        # position's drawdown is invisible until it realizes. Breach halts within the same cycle.
        self.loss_cap = loss_cap
        self.heartbeat = heartbeat
        # The private-WS fill accelerator (bot/poly_us/order_feed.py), optional. Drained at the
        # top of every cycle into the SAME cum-idempotent booking path the REST poll uses — so
        # its only possible effect is booking fills SOONER. None ⇒ yesterday's behaviour exactly.
        self.order_feed = order_feed
        # An unattributable WS order id is correctly skipped (usually another process), but the
        # skip must be VISIBLE — silent, it is indistinguishable from dropping our OWN fills via
        # a mis-keyed `resting`.
        self.order_feed_unattributed = 0
        # A venue-refused cancel under a REPLACE leaves the side on its STALE price for the cycle
        # (or dark, on the no-id path). Per-run monotone counter, surfaced in the shim's status
        # line. It is a POINTER, not a diagnosis: one number covers both "the venue is refusing
        # cancels" and "a create returned no id".
        self.replace_blocked = 0
        # [r2] Negative cum deltas — a monotone counter went backwards, so some read lied.
        # Zero in every healthy run; any positive value demands a poly_order_diff pass.
        self.cum_regressions = 0
        self.state = state_store
        # ── PROBE-STATS NOTIFY CHANNEL ───────────────────────────────────────────────────────
        # ⛔ NOTIFY-ONLY, and OFF unless this is a REAL run on the PROBE lane with
        # DISCORD_PROBE_WEBHOOK_URL set. Nothing here can raise into a quote cycle and nothing
        # reads it back. Shadow runs are excluded deliberately: a simulated fill posted as "FILL"
        # is the valuation/attribution error class mm-review exists to catch.
        self._probe_notes = probe_notify.for_lane(
            str(getattr(state_store, "lane", DEFAULT_LANE)) if real else "")
        # ⛔ THE LANE, AS A PLAIN FIELD — the one conditioning the cycle-4 P1 adverse rule reads.
        # The state store is the authority when there IS one; the explicit `lane` is the fallback
        # for the case that has none, which is DRY. The shim passes `lane` unconditionally, so a
        # DRY probe run exercises the REAL rule (only the durable ledger is absent, which is what
        # DRY means) — otherwise a dry run certifies behaviour the real run will not have.
        # `self.lane == PROBE_LANE` is the single predicate: this parameter names which lane you
        # ARE, it is not a switch that puts the probe rule on the main lane.
        self.lane = str(getattr(state_store, "lane", None) or lane or DEFAULT_LANE)
        #: The probe lane's adverse-latch SCOPE, from `the probe registration file` `latch.rule`.
        #: ⛔ RESOLVED HERE, ONCE, AND ONLY ON THE PROBE LANE: (1) `probe_config` is FAIL-CLOSED,
        #: so a bad registry must refuse the LAUNCH — reading it lazily at the latch site would
        #: raise inside `_book_fill`, on the money path, with a fill already booked; (2) the main
        #: lane does not read this file at all, so a probe typo cannot refuse a main-lane launch.
        self._probe_latch_rule = (
            probe_config.get_config().latch_rule
            if self.lane == probe_notify.PROBE_LANE else "")
        # ⛔ DEFENCE IN DEPTH, AT THE LAUNCH. The loader already refuses a `rand*` tag with no
        # registered arm set; this refuses it again, because a construction path that never went
        # through the loader would otherwise reach the draw site MID-RUN with a fill already
        # booked — the one place this must never raise.
        if (self._probe_latch_rule.startswith("rand")
                and self._probe_latch_rule not in probe_config.LATCH_BAN_ARMS_BY_RULE):
            raise ValueError(
                f"REFUSING to launch: latch.rule {self._probe_latch_rule!r} names a randomized "
                f"ban but registers NO arms in bot/core/probe_config.py "
                f"LATCH_BAN_ARMS_BY_RULE (registered: "
                f"{sorted(probe_config.LATCH_BAN_ARMS_BY_RULE)}).")
        #: ⛔ AMENDMENT 19c — THE LATCH-ONLY CONTROL CELLS, drawn at PICK time. On these books, and
        #: only these, `_latch_rule_for` answers `LATCH_RULE_CONTROL_OFF`, which skips the
        #: adverse-latch ARMING while still evaluating the trigger and taping its row.
        #: ⛔ EVERY LANE HONOURS IT [AMENDMENT 33, operator 2026-09-09]: the set is whatever the
        #: line named, on the probe lane and off it. ⛔ EMPTY IS THE SHIPPED STATE and it is the
        #: fail direction — a lost or unparsed label leaves a book LATCHED, never un-latched.
        self.latch_off_slugs: set[str] = set(latch_off_slugs or ())
        #: ⛔ AMENDMENT 41 — THE GAME-CLOCK GUARD's scope. Empty is the shipped state and it is the
        #: fail direction in the sense that matters: an unnamed book quotes exactly as it did
        #: before, with every other rail still governing.
        self.clock_guard_slugs: set[str] = set(clock_guard_slugs or ())
        self.clock_guard_file = clock_guard_file
        #: THE MAINTENANCE POSTURE's scope — see `maintenance_guard_active`. Empty = no book is
        #: stood down inside the window; the 503-wall HOLD still applies (it is venue-wide).
        self.maintenance_guard_slugs: set[str] = set(maintenance_guard_slugs or ())
        #: The window's ws-dark HOLD says itself ONCE per episode, not once per cycle.
        self._maint_hold_announced = False
        #: THE EMPTY-BOOK SEED's scope — see `seed_last_touch_slugs` above. Empty = no book ever
        #: quotes off a recorded touch, which is the pre-P1b behaviour exactly.
        self.seed_last_touch_slugs: set[str] = set(seed_last_touch_slugs or ())
        #: slug → the last two-sided touch this PROCESS saw, mirrored durably each cycle for the
        #: seed-eligible books. ⛔ SEPARATE FROM `self.last_touch`, which is a live-book reading
        #: every rail (half-spread, markout, flatten pricing) trusts to be THIS cycle's market:
        #: merging a restored row into it would hand a stale price to readers that never asked
        #: for one. The seed is the only reader of this map.
        self._durable_last_touch: dict[str, tuple[Decimal, Decimal, float]] = {}
        #: What is actually ON DISK for each of those books — the write-on-change comparison.
        #: Seeded by the restore in `prepare()`, so a restart writes nothing until a price moves.
        self._durable_last_touch_written: dict[str, tuple[Decimal, Decimal, float]] = {}
        #: Books the periodic verify found the VENUE disagreeing about on `VERIFY_CONFIRM_READS`
        #: consecutive reads — reduce-only (the SEVENTH `reduce_only_now` term) until the NEXT
        #: verify reads them clean. ⛔ A FAILED read never writes here: cannot-verify is not a
        #: mismatch.
        self._verify_reduce_only: set[str] = set()
        #: slug → CONSECUTIVE mismatched verify reads. The unconfirmed half of the rail: a book in
        #: here below the threshold is SUSPECTED and quoting normally.
        self._verify_streak: dict[str, int] = {}
        #: Does a suspicion need confirming on the NEXT cycle? — the whole reason the confirming
        #: read is not an hour away.
        self._verify_recheck = False
        #: When the last periodic verify RAN (clean or not). Launch time, because `prepare()`'s
        #: own start gate already spent the launch-time verify — the clock measures time since a
        #: venue cross-check, not since this attribute was created.
        self._verify_ts: float = time.time()
        #: `verify_unavailable` is an ONCE-PER-EPISODE line: an unreadable venue repeats every
        #: hour and the page-class channel is not a heartbeat. Cleared by the next read that works.
        self._verify_unavailable_logged = False
        #: `(mtime, books)` — the snapshot read at most once per file WRITE, not once per book:
        #: `reduce_only_now` is asked per market per cycle (and again by the hot-settings raise
        #: rail), and the recorder rewrites the file once a minute.
        self._clock_guard_cache: Optional[tuple[float, dict[str, Any]]] = None
        #: The slugs currently ARMED, so each arm/disarm prints ONE line instead of one per cycle.
        self._clock_guard_armed: set[str] = set()
        #: ⛔ POST IS LATCHED FOR THE REST OF THE RUN [mm-review 2026-09-10]. A game that is OVER
        #: does not become not-over, so once a `state = post` row has been OBSERVED for a slug the
        #: guard stays armed even if the sidecar then dies, drops the book past its `MAX_BOOKS`
        #: cap, or goes stale — the generic "unknown never stands a book down" rule is about a
        #: state we have NOT seen, and here we have seen it. It is the fail-safe direction on the
        #: one state that is monotone: the alternative re-admits a resolved book to full quoting.
        self._clock_guard_post_latched: set[str] = set()
        #: An unreadable or absent snapshot is one line for the whole run, never one per cycle.
        self._clock_guard_unreadable_announced = False
        #: …and a guarded slug the snapshot does not cover, or covers only with a stale row, gets
        #: ONE line of its own per run — a different failure from the file being unreadable.
        self._clock_guard_unguarded_announced: set[str] = set()
        #: The SUB-CONTRACT CLOSE lane, from `the probe registration file` `lane.fractional_close`.
        #: Resolved HERE, ONCE, and ONLY on the probe lane — the same two reasons
        #: `_probe_latch_rule` gives: the loader is fail-closed so a bad registry must refuse the
        #: LAUNCH rather than raise inside `_teardown_flatten` mid-halt, and the MAIN lane does
        #: not read this file at all. ⛔ OFF-PROBE IS `off` — EXCEPT the per-market
        #: `minimumTradeQty`, which is ungated and moves in both directions.
        self.fractional_close_mode = (
            probe_config.get_config().fractional_close
            if self.lane == probe_notify.PROBE_LANE else probe_config.FRACTIONAL_CLOSE_OFF)
        #: The one boolean every dust path reads. Kept beside the string because the STRING is
        #: what the run manifest tapes (the era must be separable), and the BOOL is what decides.
        self.fractional_close = (
            self.fractional_close_mode == probe_config.FRACTIONAL_CLOSE_ON)
        # ── LANE SCOPING of the runtime venue-inventory guard ────────────────────
        # `None` = DERIVE exactly as the START-time gate does: a non-main lane, or a LIVE sibling
        # lane, means a PAIR is possible and foreign inventory is attributable to somebody else.
        # ⛔ FAIL-CLOSED ON EVERY ERROR: an unreadable ledger derives False, i.e. the historical
        # ACCOUNT-GLOBAL guard, which halts MORE, not less.
        self._lane_scope_pinned = lane_scoped is not None
        self.lane_scoped = (bool(lane_scoped) if lane_scoped is not None
                            else self._derive_lane_scope(state_store))
        # slug → the PEAK |magnitude| announced for an off-slate venue row under lane scoping.
        # Magnitudes, not a set: a foreign position that DOUBLES is re-announced, because a
        # position growing beside ours is new information. PEAK by construction, so an
        # oscillating position cannot re-announce on every up-leg.
        self._foreign_inventory_announced: dict[str, Decimal] = {}
        #: Carried off-slate books already announced — ONCE PER RUN, never per cycle: the carried
        #: class is a decided, unchanging state, so a per-cycle line would be a repeating alarm
        #: about a book nothing will act on this run.
        self._carried_announced: set[str] = set()
        # The MODE is stamped into the id. Nothing else durably maps a run_id back to its kind —
        # the state file is real-only and overwritten per run. The polymm- prefix stays FIRST
        # because it is the venue discriminator the recovery split keys on
        # (`maker_state.venue_of_run_id`).
        mode = "shadow" if shadow else ("real" if real else "dry")
        self.run_id = run_id or f"polymm-{mode}-{int(time.time())}-{uuid.uuid4().hex[:6]}"

        self.ticks: dict[str, Decimal] = {}
        #: slug → the venue's OWN `minimumTradeQty` for that book, read in `prepare()` beside the
        #: tick. A slug ABSENT here means the venue did not give us one and `min_trade_qty()`
        #: falls back to `MIN_TRADE_QTY` — absence is "could not tell", never a value.
        self.min_qty: dict[str, Decimal] = {}
        #: Slugs whose metadata was ACTUALLY ASKED FOR. ⛔ NOT `set(self.min_qty)`: the two differ
        #: exactly on "we asked and the venue had no readable minimum". What this set catches is
        #: the OTHER case — a slug that reached an order path without anyone ever asking, where a
        #: silent fallback would be a guess wearing a measurement's clothes.
        self._min_qty_probed: set[str] = set()
        #: One warning per slug, not per cycle — `min_trade_qty` is on the quote path.
        self._min_qty_unprobed_announced: set[str] = set()
        self.inventory: dict[str, Decimal] = {}
        #: ⛔ NOT INVENTORY, AND DELIBERATELY NOT IN `self.inventory`. Own OFF-SLATE positions the
        #: recovery gate carried past the start because record and venue agree exactly. This run
        #: never quotes or flattens them, so they must not enter `gross_exposure`, the caps or a
        #: lawful-reach check — they exist ONLY to be handed to `begin_run`, which keeps them in
        #: the DURABLE record so the settled carve-out books them when they resolve.
        self.carried_off_slate: dict[str, Decimal] = {}
        self.resting: dict[tuple[str, str], RestingOrder] = {}
        # ── TIME AT PRICE, per (slug, side) → (price, since_ts) ──────────────────────────────
        # PURE INSTRUMENTATION: written by the quote path, read only by `_book_fill`. Nothing in
        # the quoting, sizing, gate or teardown paths reads it, and it must stay that way — a
        # clock that starts steering decisions is a behaviour change wearing an observability
        # label. The epoch is the SIDE's continuous presence at one price, NOT `self.resting`'s
        # identity: a same-price REPLACE mints a new order while the epoch continues, and a CANCEL
        # ends the epoch even though the price never moved. ABSENT means "no epoch we watched
        # begin" — the blank `price_rest_s` writes, never a zero.
        self._price_clock: dict[tuple[str, str], tuple[Decimal, float]] = {}
        # ⛔ EVERY cancelled order parks here for one delayed verify AFTER the lag horizon.
        # The venue's order reads lag reality two ways: a read can FAIL ("Order not found"), or it
        # can ANSWER with a stale cumQuantity — so discriminating "good read" from "stale read" at
        # cancel time is the wrong game (a readable-but-stale cum=0 passes any presence test). The
        # robust rule: trust the immediate read for fast booking, then VERIFY once more after
        # LAG_HORIZON_S regardless of what it said. cumQuantity is cumulative, so the delayed
        # verify books the exact delta.
        # Values: (order, due_ts, attempts, terminal_ts, origin). `terminal_ts` is stamped at PARK
        # time because the retry ladder rewrites `due_ts` and the recovery queue's verdict edge
        # must key on the TERMINAL moment. Bounded by max_parked; evictions route to the
        # ACTIVITIES RECOVERY queue, never a silent drop.
        self.pending_reconcile: dict[str, tuple[RestingOrder, float, int, float, str]] = {}
        self._teardown_pace_deadline = 0.0     # set by the teardown pass; 0 = unpaced
        # ── parked-alive exposure ─────────────────────────────────────────────────────────
        # ⛔ A CANCEL ECHO IS NOT A DEATH: orders cancelled with an `ok` echo have rested
        # CONCURRENTLY at the venue and been lifted in one sweep, while the pre-place cap check
        # sums INVENTORY alone — so the maker placed fresh quotes beside ghosts.
        # oid → WHY a PARKED order is believed alive SINCE its cancel. Present ⇒ the venue has
        # contradicted the echo and the order's REMAINING size counts toward its side's
        # worst-case exposure. Absent ⇒ unchanged behaviour (counting every parked order would
        # freeze the adding side for LAG_HORIZON_S after every reprice). The admission rule — and
        # why a stale `ORDER_STATE_NEW` is not evidence — is `_note_parked_sighting`.
        self.parked_alive: dict[str, str] = {}
        #: oid → the order's cumQuantity AT PARK TIME — the baseline the cum-advance test compares
        #: against: a cum past this means the order traded AFTER our cancel, which no store
        #: staleness can explain.
        self.parked_cum_at_park: dict[str, Decimal] = {}
        #: oids whose TERMINAL state a read-back has positively confirmed (canceled/filled/
        #: expired/rejected). A cancel ACK is not terminal — the venue acked the 2026-09-07 ghost
        #: and filled it 4 s later — so only a read of the ORDER's own state retires its exposure
        #: from `_unresolved_exposure`. A sighting takes the confirmation back.
        self.terminal_verified: set[str] = set()
        #: oid → when we last re-issued the cancel of a sighted-alive parked order.
        self._parked_recancel_ts: dict[str, float] = {}
        #: oids the (synchronous) WS drain sighted alive, awaiting their re-cancel at the top of
        #: the next `poll_fills` — the drain cannot await, and a sighting must not wait on a read.
        self._parked_recancel_due: list[str] = []
        #: re-cancels spent since this poll's budget was armed — see PARKED_RECANCEL_MAX_PER_POLL.
        self._recancels_this_poll = 0
        #: books whose adding side is refused THIS cycle by the parked-alive term. Cleared at the
        #: TOP of `_quote_market` and on the skip path, so a book that early-returns before the
        #: gate cannot carry a stale flag into its tape row.
        self._parked_alive_blocked: set[str] = set()
        #: (slug, side) the reach rail refused THIS cycle — either `_apply_action`'s resize found
        #: no lawful reach left or `_place`'s pre-send guard refused at the wire. Cleared at the
        #: TOP of `_quote_market`, read once before the tape row: the side tapes dark with
        #: `hold_cause=reach_guard`.
        self._reach_refused: set[tuple[str, str]] = set()
        #: (slug, side) refused THIS cycle where UNRESOLVED CANCEL exposure was what bound the
        #: size — a strict subset of `_reach_refused`, cleared with it, read once to name the
        #: tape's `hold_cause=unresolved_cancel`.
        self._unresolved_refused: set[tuple[str, str]] = set()
        #: (slug, side) → the size the reach rail actually SENT this cycle, when it clamped the
        #: decided one. The tape row must report the size that went to the wire, not the size the
        #: cycle decided before the cancel moved the belief.
        self._reach_sized: dict[tuple[str, str], int] = {}
        #: (slug, side) spells already announced, so the hold logs once per spell, not per cycle.
        self._parked_alive_announced: set[tuple[str, str]] = set()
        self.parked_recancels = 0
        self.parked_alive_holds = 0
        # ── in-run ORPHAN rail ─────────────────────────────────────────────────
        # An ORPHAN is a venue-listed order on one of THIS RUN's slugs whose id is in NEITHER
        # `self.resting` NOR `pending_reconcile` NOR `pending_activity_recovery` — the venue is
        # holding an order no structure in this process knows about. The canonical cause is a
        # create whose response was LOST (the order rests anyway). Without this rail an orphan is
        # invisible until `_teardown_sweep` — a whole run of unmodelled exposure quoting beside it.
        #: oid → consecutive `poll_fills` listings it has appeared in as an unknown order. Reset
        #: the moment the id is absent from a listing — see ORPHAN_MIN_SIGHTINGS.
        self._orphan_sightings: dict[str, int] = {}
        self.orphans_seen = 0          # adopted (debounce satisfied, slug ours, attributable)
        self.orphans_cancelled = 0     # …and a cancel was actually issued for it
        #: ⛔ SETS OF OIDS, NOT COUNTERS. Both of these classes
        #: are re-encountered on EVERY poll for as long as the order rests, so an increment per
        #: sighting counted POLLS and reported "4 orphans" for one order seen four times. The
        #: reported set is also what gates the unattributable branch's direct cancel, so that
        #: order is cancelled ONCE rather than re-cancelled every poll forever, and what dedupes
        #: the operator line (it replaced a separate `_orphan_announced` set that deduped only
        #: the log, leaving the count and the cancel unbounded).
        #: `_orphan_reported`: listed orders we could NOT act on and must not guess about — no
        #: readable slug (the `_teardown_sweep` rule — REPORTED, never cancelled), or ours but
        #: with no attributable side/price/quantity, which would mean fabricating the accounting
        #: for a real order.
        self._orphan_reported: set[str] = set()
        #: listed orders on slugs that are not this run's — LEFT ALONE (the account carries other
        #: processes' orders; a maker may only ever undo what it could have done).
        self._orphan_foreign: set[str] = set()
        #: creates whose response was an ERROR — "may have rested", the one path that produces a
        #: real resting order with no `self.resting` entry. Nonzero keeps `poll_fills` reading it.
        self._orphan_suspects = 0
        #: times the parked-alive term WOULD have refused a side another rail had already refused.
        #: Counted apart from `parked_alive_holds` because it changed no decision.
        self.parked_alive_masked = 0
        # ── belief recovery ────────────────────────────────────────────────────
        # Orders whose venue order-store record is gone but whose TRADE-LEDGER record survives:
        # the walk reads portfolio.activities (1 req/cycle) and answers FOUND / NO-FILL /
        # OVER-STATEMENT per entry. Holds the SAME RestingOrder objects as everything else.
        self.pending_activity_recovery: dict[str, RecoveryEntry] = {}
        # The NAMED terminal failure list (evictions, price-sanity refusals, over-statements,
        # coverage timeouts): order_id -> reason. Entries here may carry UNBOOKED fills, so their
        # durable order records are deliberately NOT cleared and the crash record stays open.
        self.unresolved_recovery: dict[str, str] = {}
        # Consecutive poll not_founds per RESTING order — the cancel-probe's streak gate.
        self.not_found_streak: dict[str, int] = {}
        # Breaker: >NOT_FOUND_BREAKER distinct not_founds in ONE poll = client/route
        # fault; probes, retirements and the walk refuse until a quieter poll clears it.
        self._nf_breaker_tripped = False
        # ⛔ RETIREMENT IS GATED OFF BY DEFAULT: the structured not_found cancel verdict has never
        # been observed on a real purge, and the chain's three legs all read the SAME order store
        # — the one documented lagging hours. Until the instrumentation read is on record, a
        # not_found probe answer is LOGGED and QUEUED FOR READ-ONLY RECOVERY (recovery is
        # separable from retirement), the order STAYS in `self.resting`, and nothing is placed
        # over it.
        self.probe_retirement = bool(probe_retirement)
        # Orders whose probe already answered not_found under DISABLED retirement — logged
        # once, not re-probed every cycle (the budget is for undecided zombies).
        self._probe_confirmed_gone: set[str] = set()
        # Walk coverage state. Matched executions per queued order (exec_id-deduped) persist
        # across traversals. ⛔ COVERAGE is a property of ONE TRAVERSAL, NEVER of the process:
        # pages are newest-first, so depth earned at T₁ says nothing about rows created after T₁
        # and since pushed off page 1. An entry may only credit a traversal STARTED at or after
        # its own QUEUED_TS, and a NEGATIVE verdict additionally requires the start past
        # terminal + LAG_HORIZON_S (the store publishes executions late).
        self._recovery_execs: dict[str, dict[str, dict]] = {}
        self._walk_started_ts: float = 0.0          # page-1 time of the CURRENT traversal
        self._walk_oldest_ts: Optional[float] = None  # oldest createTime IN this traversal
        self._walk_exhausted = False                # this traversal hit the ledger's end
        self._recovery_cursor: str = ""             # this traversal's deepening cursor
        # Operator-visible counters: through-zero re-basings on recovered fills (the loss-cap's
        # known interim window), the cancel-probe verdict distribution, and orders whose
        # activities legs carried NO commission snapshot (BLANK, never 0).
        self.recovered_through_zero = 0
        self.recovery_probe_verdicts: dict[str, int] = {}
        self.recovery_commission_absent: list[str] = []
        # Newest tape-row ts per slug — the out-of-order detector's reference: a recovered booking
        # corrupts pairing only when a LATER fill was already booked, not when it lands on a
        # virgin/quiet book.
        self._last_row_ts: dict[str, float] = {}
        self._empty_ledger_logged = False
        self.last_read_ts: dict[str, float] = {}
        # ⛔ VENUE TRUTH, refreshed on a slow cadence and used ONLY TO RESTRICT.
        # Belief has fallen far behind the venue on one book (past the cap) while every
        # rail keyed on belief — per-book cap, global cap, cooldown, loss cap — evaluated a
        # fiction the private design notes.
        # One hard rule: the venue number may BLOCK a side, never SIZE an order. The endpoint is
        # known to serve divergent replicas, and a stale read that caused a trade could open the
        # very position it thinks it is closing. Blocking on a stale read costs fills; trading on
        # one costs money.
        self.venue_inventory: dict[str, Decimal] = {}
        self.venue_inventory_ts: float = 0.0
        #: Slugs CURRENTLY in venue-flat-vs-belief-holds. MEMBERSHIP is the condition, so the
        #: warning fires on entering it and the slug is discarded on leaving — an add-only set let
        #: a benign first-fill race consume the one warning a real divergence needed.
        self._venue_flat_divergence: set[str] = set()
        #: Slugs whose live-belief tie-break has been announced once this run.
        self._venue_tiebreak_announced: set[str] = set()
        #: `self.inventory` copied at the instant the last venue read succeeded. Never read
        #: live belief for the divergence test — see `_refresh_venue_inventory`.
        self.venue_belief_at_read: dict[str, Decimal] = {}
        # Slugs whose LAST venue row was unparseable — they keep their previous value (never
        # silently read as flat) and the teardown flatten treats them as uncorroborated. Per-slug,
        # because one junk row on a market this run never touches must not fail the whole read.
        self.venue_stale_rows: set[str] = set()
        #: slug → venue status, for every belief the teardown retired as a SETTLED book
        #: (`carve_out_settled_books`). Read by the shim's certification note: a run whose only
        #: disagreement was settled books certifies, and the note must name them.
        self.settled_carved: dict[str, str] = {}
        # Consecutive un-quotable cycles per market; reset by any quotable one.
        self.unquotable_streak: dict[str, int] = {}
        # slug → (best_bid, best_ask, read_ts) from the most recent successful book read. Stamped
        # onto each fill so the tape carries a MARK, not only the rebate.
        self.last_touch: dict[str, tuple[Decimal, Decimal, float]] = {}
        #: slug → (net_bid, net_ask) STAMPED IN THE SAME BREATH AS `last_touch`, from the SAME
        #: cycle's single `net_of_self_touch` call. `_marks` reads this, never a second
        #: derivation. Unlike `_last_net` it NEVER carries a side forward: a stale net side is
        #: exactly the value the mark must refuse.
        self.last_touch_net: dict[str, tuple[Optional[Decimal], Optional[Decimal]]] = {}
        # slug → (net_bid, net_ask) from the last QUOTED cycle — `quote_reason`'s only state. NET,
        # not raw: the raw touch contains our own resting order, so a raw comparison calls our own
        # place/cancel a market move.
        self._last_net: dict[str, tuple[Optional[Decimal], Optional[Decimal]]] = {}
        #: slug → the client crossing guard's own book from THIS cycle's last place/replace,
        #: POPPED by `_write_quote`. Instrumentation only. Empty ⇒ this cycle placed nothing,
        #: which is why the guard columns are blank on hold/skip rows; both sides placing in one
        #: cycle leaves the LAST one.
        self._guard_seen: dict[str, dict] = {}
        #: ── I1/I2 TAPE-ONLY STATE. Nothing in this module BRANCHES on any of the four; they
        #: exist so `_write_quote` can name what already happened. All three POPPED maps follow
        #: `_guard_seen`'s rule exactly: absent ⇒ the event did not happen since the last row,
        #: which tapes BLANK, never 0. ⛔ Every read and write goes through `_tape_map`, because
        #: a maker built by `__new__` (tests/test_park_seat.py) never runs this block.
        #: slug → the `booked_via` of the last fill that moved `self.inventory[slug]`. Absent ⇒
        #: this run has booked nothing on the book, so the belief is the launch CARRY.
        self._inv_src: dict[str, str] = {}
        #: slug → the client's create ACK span (ms) for the last placement on this book.
        self._ack_lag_ms: dict[str, float] = {}
        #: slug → the verdict of the last cancel read-back on this book (`_cancel`'s own terms).
        self._cancel_readback: dict[str, str] = {}
        #: slug → the `_unresolved_exposure` term the last `_reach_permits` GROWTH branch used.
        self._unresolved_seen: dict[str, Decimal] = {}
        # Round-trip accounting for the adverse cooldown: average entry of the CURRENT position,
        # realized P&L and closed quantity of the current round trip (reset at flat), and the
        # per-book stand-down deadline.
        self.avg_entry: dict[str, Decimal] = {}
        #: Books whose basis is NOT this run's own fills — a CARRY seeded at launch.
        #: `fill_accounting` resets `avg_entry` to the latest OPENING fill's price on any position
        #: it inherited without a basis, so the number on such a book can price a position it
        #: never opened. `poly_settle_book --auto` REFUSES to price a "reset" row.
        self.basis_carried: set[str] = set()
        self.rt_realized: dict[str, Decimal] = {}
        self.rt_closed: dict[str, Decimal] = {}
        self.cooldown_until: dict[str, float] = {}
        #: ⛔ RUN-LIFETIME latch, set by the adverse ROUND-TRIP rail only (the mark tripwire keeps
        #: its wall-clock). A book in here quotes reduce-only for the REMAINDER OF THE RUN. The
        #: A real loss is why a TIMED stand-down is not enough: the stand-down expired, the
        #: maker resumed two-sided quoting into the SAME one-way flow and re-covered at a further
        #: loss. A timed stand-down re-arms on the clock; the FLOW that tripped the rail is what
        #: has to change, and nothing in this process can observe that it has.
        #:
        #: slug → the WALL-CLOCK time the latch was (re-)set, which a `clear_adverse_latch` ack is
        #: compared against. DURABLE ACROSS CRASHES, restored below from the per-lane ledger, so
        #: ⚠️ a crash-RESTART is NOT a re-arm. ⛔ SCOPE: the latch EXPIRES at a venue-confirmed
        #: CLEAN run end — within-run + crash-durable, NOT carried across clean runs. Mid-run the
        #: one clear is the explicit dated operator ack.
        self.adverse_latched: dict[str, float] = {}
        #: slug → wall-clock DEADLINE at which a PROBE-LANE timed ban re-admits the book. EMPTY on
        #: every other lane (the incumbent run-lifetime latch) and ⛔ EMPTY ON THE PROBE LANE TOO
        #: under the shipped `latch.rule = kill_reverted_lifetime`. A latched slug with NO entry
        #: here is a LIFETIME latch, so absence is meaningful. Restored from the durable ledger:
        #: a crash-restart mid-ban must neither clear the ban nor promote it to a lifetime one.
        self.adverse_ban_until: dict[str, float] = {}
        #: slug → `(episode_ban_s, armed_at)` for the RANDOMIZED-rule ban currently running on
        #: that book. It carries the two facts the deadline alone cannot:
        #:   · `episode_ban_s` — the arm DRAWN when this episode opened. A re-trip while the ban
        #:     still runs restarts the clock with THIS number instead of drawing again (confound
        #:     A: a re-draw would make the tape's arm not the treatment).
        #:   · `armed_at` — when the clock last (re)started. On expiry a mark-tripwire deadline
        #:     stamped at or before it is cleared, so the drawn ban is the WHOLE re-entry delay
        #:     rather than the ban plus the tail of a stand-down (confound B).
        #: ⛔ NOT DURABLE, unlike `adverse_ban_until`: a crash-restart restores the deadline with
        #: no episode record, and inventing an arm would mislabel the tape.
        self.adverse_ban_episode: dict[str, tuple[float, float]] = {}
        #: Per-book carriers of the qty-weighted mean entry half-spread over OPEN inventory and
        #: the RESOLVED open quantity it is a mean over. Deliberately NOT durable: an inherited
        #: position has no entry width of ours at all, which resolves to the absolute fallback.
        self._entry_hs_mean: dict[str, Decimal] = {}
        self._entry_hs_qty: dict[str, Decimal] = {}
        if state_store is not None:
            try:
                _snap = state_store.snapshot()
                restored = dict(_snap.adverse_latched)
                # Only deadlines whose slug is actually latched — an orphan deadline is
                # meaningless and would otherwise sit in the ledger forever.
                self.adverse_ban_until.update(
                    {s: t for s, t in _snap.adverse_ban_until.items() if s in restored})
            except Exception as exc:                     # a ledger read must never block a start
                log.error(f"⚠️ could not restore adverse latches ({exc!r}) — treating as none; "
                          f"a book latched by a PRIOR run may quote two-sided")
                restored = {}
            if self.adverse_ban_until:
                log.warning(
                    "⏳ adverse TIMED BANS restored from the durable ledger: "
                    + ", ".join(f"{s} until {_utc_iso(t)}"
                                for s, t in sorted(self.adverse_ban_until.items()))
                    + " — a restart does NOT clear a ban [cycle-4 P1]")
            if restored:
                self.adverse_latched.update(restored)
                # EVERY restored entry is kept — an off-slate latch must survive in case that
                # book is quoted again — but only the ones THIS run can act on are headlined.
                mine = sorted(s for s in restored if s in self.slugs)
                others = len(restored) - len(mine)
                if mine:
                    log.warning(f"🔒 adverse latch RESTORED from the durable ledger for {mine} "
                                f"— reduce-only until a dated operator ack; a restart does NOT "
                                f"clear it"
                                + (f" (plus {others} off-slate latch(es) carried)"
                                   if others else ""))
                else:
                    log.info(f"adverse latch: {others} carried latch(es) for books not on this "
                             f"slate — kept, not applicable to this run")
        # ⛔ AMENDMENT 19c — THE CONTROL-ARM / RESTORED-LATCH COLLISION. A latch restored above is
        # DURABLE, so a slug drawn into the control arm can start the night ALREADY reduce-only:
        # it would quote nothing, produce no counterfactual, and tape `latch_arm=off` — the exact
        # CENSORING this arm exists to end, arriving inside the arm and labelled as its evidence.
        # ⛔ REFUSE THE CELL, NEVER THE RUN, and fall back to `ban`. ⚠️ Deliberately NOT "clear the
        # latch so the cell can run": the latch is a durable safety record, and an experiment may
        # not clear one.
        _collide = sorted(self.latch_off_slugs & set(self.adverse_latched))
        if _collide:
            self.latch_off_slugs -= set(_collide)
            log.warning(
                f"⚗️ CONTROL ARM COLLISION on {_collide}: these books were drawn into the "
                f"LATCH-ONLY control arm (--latch-off-slugs) but carry an adverse latch RESTORED "
                f"from a prior run, so they are already reduce-only. Running them as control "
                f"cells would censor the arm — a held book produces no counterfactual, and it "
                f"would be taped as the treatment arm's own evidence. They fall back to `ban` "
                f"for this run; the rest of the control arm is unaffected"
                + (f" ({sorted(self.latch_off_slugs)} still unlatched)"
                   if self.latch_off_slugs else " (no control cells remain this run)"))
        self.mark_trip_per_ct = mark_trip_per_ct
        #: slug → resolution epoch (--resolves-at). Feeds ONLY the ADVEXIT decision LINE at the
        #: mark tripwire — no behaviour reads it. It accumulates the counterfactual n; arming is
        #: a future, separately-reviewed taker-path design (this maker is post_only always).
        self.resolves_at: dict[str, float] = dict(resolves_at or {})
        # P-c presence workdown: opt-in per-book set — in the plain cap-disabled one-sided state
        # the REDUCING side sizes min(|inv|, 2×size). Default empty = byte-identical quoting.
        self.presence_workdown: set[str] = set(presence_workdown or ())
        # ── , OPT-IN, DEFAULT OFF ───────────────────────────────────────
        # A book quoted at extreme prices traded through many fills and collected nothing: at those
        # prices `0.0125·p·(1−p)·size` is under the half-cent rounding boundary for the whole
        # book. This raises the ADDING side to `min_rebate_size(px)` at the price it is about to
        # post at, or refuses to post it. ⛔ NECESSARY, NOT SUFFICIENT: the venue rounds PER FILL
        # INCREMENT and the increment is the TAKER's choice, so a nibbler still forfeits it. What
        # the rule guarantees is the CONVERSE — a side resting BELOW the step earns $0 with
        # certainty while carrying the full adverse and inventory risk.
        self.rebate_step = rebate_step
        self.rebate_step_max = rebate_step_max
        if rebate_step_mode not in REBATE_STEP_MODES:
            raise ValueError(f"unknown rebate_step_mode {rebate_step_mode!r}; expected one of "
                             f"{REBATE_STEP_MODES}")
        self.rebate_step_mode = rebate_step_mode
        #: (slug, side) → the last taped rebate-step state, so the log line fires once per STATE
        #: CHANGE and not once per cycle.
        self._rebate_step_state: dict[tuple[str, str], str] = {}
        # ── , OPT-IN, DEFAULT OFF ────────────────────────────────────────────────
        # A reducing quote sized to a sub-contract residue forfeits its rebate to the venue's
        # nearest-cent-per-increment rounding while carrying the full adverse risk of resting.
        self.dust_hold = dust_hold
        # ──  [operator-decided 2026-09-04], the QUOTE MODE, default `improve` ────
        # ⛔ REFUSED, NOT COERCED: an unrecognised mode is a typo in an argv the operator armed,
        # and silently falling back to `improve` would tape a run under an arm it did not run.
        if quote_mode not in QUOTE_MODES:
            raise ValueError(f"unknown quote_mode {quote_mode!r}; expected one of {QUOTE_MODES}")
        self.quote_mode = quote_mode
        self.mark_trips: dict[str, int] = {}     # per-book tripwire fire count — never silent
        self.width_shielded: set[str] = set()    # books in a width-only breach spell (log once)
        # Tripwire-attributed stand-down deadlines, SEPARATE from `cooldown_until`'s shared clock:
        # the tripwire arms the SAME reduce-only machinery arm B measures, so its cycles must be
        # attributable on the tape (`status=mark_trip`) or the mechanism ratio is confounded.
        self.trip_until: dict[str, float] = {}
        # THIS RUN's realized, starting at zero. The durable ledger is a LIFETIME number carrying
        # PROFIT as well as loss, so a good session silently doubled the next session's loss
        # budget. The cap now binds on BOTH: this run's own loss AND the lifetime loss-to-date
        # (which floors at zero, so profit never buys budget on either axis).
        self.session_realized = _ZERO
        # Account-ledger degrade latch: ERROR on the TRANSITION into the degraded state only, so
        # per-cycle repeats do not become stderr noise. ⚠️ The OPERATIONAL gap is the opposite —
        # a degraded ledger read has no push alert at all (the private design notes).
        self._account_read_degraded = False
        self.cycle_index = 0
        self.should_stop = False
        self.halt_reason: Optional[str] = None
        #: WIND-DOWN mode [set ONLY by the shim's --passive-exit-s loop after a NORMAL clock
        #: expiry — never on kill/memory/loss halts]: every book quotes reduce-only at a
        #: clamped size until flat or the wind-down deadline; the normal teardown follows.
        self.winddown: bool = False
        #: PER-BOOK CALENDAR PULL (`--pull-at slug:epoch`): slug → wall-clock epoch after which
        #: that ONE book goes reduce-only. Wall-clock, not monotonic, because the deadlines are
        #: venue calendar events — the same basis the operator reads them in.
        self.pull_at: dict[str, float] = {}
        self._pull_announced: set[str] = set()
        #: PARK SEATS (`--park-seat slug:side:ticks_off`): slug → (side, ticks_off). A park book
        #: quotes ONE side, `ticks_off` ticks off ITS OWN touch, re-derived every cycle, with no
        #: improve logic — reward score is `d^ticks × size` and ignores queue position, so
        #: presence off the touch is the product. ⛔ EVERY safety rail still applies: per-book cap,
        #: global gross cap, venue truth, mark tripwire, adverse cooldown, the loss cap, the
        #: position guard, and `hot_settings` reduce-only.
        self.park: dict[str, tuple[str, int]] = {}
        #: Park books already announced DARK — the warning is once per slug per run; the
        #: `park_dark` tape status carries every subsequent cycle.
        self._park_dark_announced: set[str] = set()
        # Set ONLY at the END of `_teardown_sweep`, on the venue's own evidence — so a sweep phase
        # that raises ANYWHERE leaves this False and the crash record open. A FAILED phase can
        # only under-close, never over-close.
        self._swept_clean = False
        # None = the sweep never read the venue (listing raised / never ran) — cannot-verify,
        # NOT zero. An int is the venue's actual refused-cancel count from the last sweep.
        self._sweep_refused: Optional[int] = None
        #: What the teardown's flatten phase DID, as a value rather than as prose to grep
        #:: `filled` (nothing left) ·
        #: `resting_swept` (a residual remains, offered back passively) · `refused` (the venue did
        #: not corroborate belief — NOTHING was placed) · `failed` (the phase raised) · `shadow`.
        #: ⛔ Set to `failed` on ENTRY to `_teardown_flatten` and narrowed at each return, the
        #: `_swept_clean` discipline: a phase that raises anywhere leaves the WORSE value, and the
        #: unresolved-exposure record keys off it. None = the phase never ran.
        self.flatten_outcome: Optional[str] = None
        self._seq = 0
        # Venue requests spent OUTSIDE our own book reads — placements (2 each: the guard's book
        # read plus the create) and cancels. Folded into the cycle's request count.
        self._extra_requests = 0

        # ⛔ A NON-REAL maker's DEFAULT tapes are its OWN (.dry.csv): dry makers constructed
        # OUTSIDE pytest wrote the real default tapes, and the schema rotation then renamed the
        # LIVE real-money tape out from under a running session. The pytest sandbox is
        # defence-in-depth now, not the only wall.
        def _default(path: str) -> str:
            if real:
                return path
            stem, ext = os.path.splitext(path)
            return f"{stem}.dry{ext}"
        # ⛔ …and LANE-scoped, for the same displacement reason one axis over. TWO maker processes
        # shared these three files, and `_open_writer`'s schema rotation is an `os.replace`: a
        # probe launch moved the tape out from under the RUNNING main maker, whose open handle
        # followed the INODE into logs/rotated/. Per-lane paths remove the class rather than
        # narrowing the window: one lane's rotation can never name another lane's file.
        # ⚠️ Lane stamps BEFORE mode, so the `.dry` substring test in every real-money fold still
        # excludes a dry probe. ⚠️ The bare `logs/poly_live_mm_*.csv` paths are FROZEN LEGACY;
        # `bot.core.tape_paths.fold_patterns` is how a consumer still sees them as one tape.
        def _lane(path: str) -> str:
            return _default(lane_tape_path(path, self.lane))
        self._quote_path = quote_csv or _lane(DEFAULT_QUOTE_CSV)
        self._cycle_path = cycle_csv or _lane(DEFAULT_CYCLE_CSV)
        self._fill_path = fill_csv or _lane(DEFAULT_FILL_CSV)
        # Same lane/dry scoping as the other three. The FILE is only ever created when TTL mode
        # is armed (see `prepare`), so an OFF-mode run leaves the log tree exactly as it found it.
        self._ttl_path = ttl_csv or _lane(DEFAULT_TTL_CSV)
        # Same lane/dry scoping. ⚠️ CREATED LAZILY, on the FIRST BREACH — not header-only like the
        # three tapes above, because nothing reads this tape IN-RUN (it is an offline
        # counterfactual bank), so a run that never trips leaves the log tree as it found it and
        # the folding reader counts BREACHES, for which absent and empty mean the same thing.
        self._advexit_path = advexit_csv or _lane(DEFAULT_ADVEXIT_CSV)
        self._writers: dict[str, Any] = {}

    # ── lifecycle ────────────────────────────────────────────────────────────────────────────

    async def prepare(self) -> None:
        """Read every market's tick ONCE, and drop any market whose tick is unreadable.

        ⛔ NEVER DEFAULT A TICK. It varies per market on this venue (0.01 on ~88%, 0.005 and 0.001
        elsewhere) and it varies WITHIN a series, so a default is right often enough to look
        correct while silently mispricing exactly the markets that differ.
        """
        # ⛔ THE SLATE'S NORMALIZATION MUST BE UNAMBIGUOUS. `_on_slate` falls back to a normalized
        # comparison so a case/whitespace difference in a VENUE row cannot route our own book into
        # the "somebody else's position" branch. That is only sound if the mapping is 1:1 — two
        # slate strings collapsing to one key would make the guard's answer depend on dict order.
        if len(self._slate_norm) != len(set(self.slugs)):
            collisions = {}
            for s in self.slugs:
                collisions.setdefault(norm_slug(s), []).append(s)
            dupes = {k: v for k, v in collisions.items() if len(v) > 1}
            raise ValueError(
                f"REFUSING to start: slate slugs collide under normalization {dupes} — the "
                f"venue-inventory guard's membership test cannot tell them apart, so one "
                f"book's position could be judged against the other's reach.")
        for index, slug in enumerate(self.slugs):
            if index:
                await asyncio.sleep(self._gap_s)
            # ⛔ ONE REQUEST, BOTH FIELDS: the tick and `minimumTradeQty` come out of the same
            # payload, so reading the minimum costs no extra request.
            tick, min_qty = await self._read_book_meta(slug)
            if tick is None or tick <= _ZERO:
                log.warning(f"DROPPING {slug}: tick unreadable ({tick!r}) — a defaulted tick "
                            f"misprices silently, so this market is not quoted at all")
                continue
            # ⛔ THE SAME ADD TABLE A HOT ADD RUNS [continuous maker P1]. On this path every write
            # is idempotent (the constructor already built the maps from argv), which is the
            # point: a field only one of the two paths sets is a field a re-added seat is missing.
            await self._admit_book(slug, self._launch_entry(slug), tick, min_qty, launch=True)
        # ⛔ CREATE THIS LANE'S THREE TAPES, HEADER-ONLY, BEFORE ANY ORDER INTENT EXISTS. Until the
        # first ROW landed (fills possibly NEVER) a lane's tape did not exist, and three rails
        # cannot tell a not-yet-created file from a dead one: `poly_pos_guard`'s `tape_belief`
        # reads cannot-verify through the pre-first-fill window (when a carried position is most
        # exposed); the tier-1 backup finds nothing to copy; every folding reader sees only the
        # frozen legacy era. A header-only tape is a REAL ZERO, which is the distinction those
        # rails are built on. It goes through `_writer`, so a restart still EXTENDS the tape and a
        # schema mismatch still freeze-rotates.
        for _path, _hdr, _tag in ((self._quote_path, _QUOTE_HDR, "epoch"),
                                  (self._cycle_path, _CYCLE_HDR, "epoch"),
                                  (self._fill_path, _FILL_HDR, "epoch")):
            self._writer(_path, _hdr, rotate_tag=_tag)
        if self.order_ttl_s > 0:
            # ⛔ ONLY when armed. A header-only TTL tape on an OFF-mode run would be a new file
            # in the live log tree for a feature that run does not use — and the OFF path's whole
            # contract is that it is indistinguishable from the maker that shipped before it.
            self._writer(self._ttl_path, _TTL_HDR, rotate_tag="ttl")
        # ⛔ BEFORE `begin_run`. Nothing in the seed needs the durable record, and running it after
        # would open that record — run_id, clean_exit=False, the armed window opswatch reads — up
        # to `ws_seed_timeout_s` before the first order intent exists.
        await self._seed_ws_cache()
        if self.state is not None and self.real:
            # `self.inventory` at this moment IS the recovery-verified view [design v2 §B]:
            # the shim seeds accepted carries into it BEFORE prepare() and nothing else has
            # run — so a carry-start records the declared quantities and a clean start
            # records {}. The prior run's map never rides in by copy.
            self.state.begin_run(self.run_id, mode="real", loss_cap=self.loss_cap,
                                 tickers=list(self.ticks), inventory=dict(self.inventory),
                                 carried_off_slate=dict(self.carried_off_slate))
            # THE EMPTY-BOOK SEED's memory, restored [P1b]. ⛔ AFTER `begin_run` — it replaces
            # `_raw` wholesale (carrying this map forward itself), so reading before it would
            # load the same bytes and reading a field it dropped would load nothing. Scoped to
            # the seed-eligible slugs: a record can outlive a seat's declaration, and a slug the
            # launcher no longer lists must not be seedable through a stale row.
            self._durable_last_touch = {
                s: t for s, t in self.state.snapshot().last_touch.items()
                if s in self.seed_last_touch_slugs}
            self._durable_last_touch_written = dict(self._durable_last_touch)
            if self._durable_last_touch:
                log.warning(f"empty-book seed: restored last two-sided touch for "
                            f"{sorted(self._durable_last_touch)}")

    async def _seed_ws_cache(self) -> None:
        """Wait — BOUNDED — until the seam would SERVE every quoted book from the ws cache.

        ⛔ WHAT THIS IS FOR, measured [see WS_SEED_TIMEOUT_S for the tapes]. It is NOT "cycle 1 is
        always cold" — `prepare()`'s paced per-book tick sweep usually gives the socket seconds to
        deliver. The class this covers is the measured MINORITY where first frames took 15–105 s.

        ⛔ THE EXIT CONDITION IS SERVABILITY, NOT PRESENCE: a present-but-stale frame on a quiet
        socket is exactly what `_ws_try_serve` declines, so exiting on presence would declare
        victory over the population this wait exists to outlast. Free and side-effect-free by
        construction (cache reads only), so DRY, shadow and real starts behave identically.

        ⛔ FAIL OPEN. A feed that RAISES on a cache read degrades to the cold start with a warning
        — never a refusal, never a stall. This runs BEFORE `begin_run`, so a failure here cannot
        leave a durable run record open.

        ⚠️ NOT A GUARANTEE OF ZERO DEATHS: a book still unservable at the bound falls to the
        fresh-REST backstop, and at `WS_STALE_REREAD_MAX`+ the C-a cap escalates and books one.
        The correct diagnosis for that death is SLOW FIRST FRAMES, not a missing seed.

        The wait is TOTAL across the slate, not per book, and the loop yields between polls — the
        feed's own task fills the cache and cannot run if this blocks the loop. The seed set is
        `self.ticks`, NOT `self.slugs`: unreadable-tick books are already dropped."""
        if (self.book_source != "ws" or self.book_feed is None
                or self.ws_seed_timeout_s <= 0.0 or not self.ticks):
            return
        started = time.time()
        deadline = started + self.ws_seed_timeout_s
        framed = 0
        while True:
            try:
                # Presence is REPORTING ONLY (the message below); servability is the decision.
                framed = sum(1 for slug in self.ticks
                             if self.book_feed.get_book_md(slug) is not None)
                self.ws_seed_servable = self._ws_slate_servable()
            except Exception as exc:
                # FAIL OPEN: an optimization's failure must degrade to the old cold start, never
                # block a launch.
                log.warning(f"WS cache seed abandoned — the feed raised on a cache read: "
                            f"{exc!r}. Starting on the cold cache (cycle 1 falls to the "
                            f"fresh-REST backstop), exactly as before seeding existed.")
                break
            if self.ws_seed_servable:
                break
            remaining = deadline - time.time()
            if remaining <= 0.0:
                break
            await asyncio.sleep(min(WS_SEED_POLL_S, remaining))
        # Servability IMPLIES presence for every book, so that is the honest count; `framed` is
        # the diagnostic for the unservable case.
        self.ws_seeded_books = len(self.ticks) if self.ws_seed_servable else framed
        self.ws_seed_unframed = len(self.ticks) - self.ws_seeded_books
        waited = time.time() - started
        if self.ws_seed_servable:
            log.info(f"WS cache seed: the whole {len(self.ticks)}-book slate is servable from "
                     f"the ws cache after {waited:.1f}s — cycle 1 quotes off it")
        else:
            log.warning(
                f"WS cache seed: {self.ws_seed_unframed} of {len(self.ticks)} book(s) still "
                f"unservable after {waited:.1f}s ({framed} cached) — they start REST-served. "
                f"Up to {WS_STALE_REREAD_MAX} cost one backstop read each; at "
                f"{WS_STALE_REREAD_MAX + 1}+ the C-a cap escalates and cycle 1 books a feed "
                f"death (slow first frames, not a seeding failure)")

    async def read_touches(self) -> dict[str, tuple[Decimal, Decimal]]:
        """Each market's touch, quoting NOTHING — from the WS cache where it is servable, over a
        paced cache-busted ORIGIN read where it is not.

        Exists so the startup rebate check can run BEFORE any order is placed — doing it with a
        real `run_cycle` would print "these markets earn nothing" after the money was committed.

        ⛔ CACHE FIRST BECAUSE THE SWEEP IS WALL TIME ON THE LAUNCH PATH: paced REST for a whole
        slate cost a minute before cycle 1 and blew the launch child's drain-ack budget, which
        SIGTERM'd the maker. `prepare()` has
        already seeded the WS cache for every seated slug, and the read-only rebate warning this
        feeds is not a money rail — the quote rail's own OR-rule (`_ws_book_servable`) is ample
        evidence for it.

        ⛔ THE REST FALLBACK STAYS PACED AT `STARTUP_TOUCH_PACE_S`, NOT AT `_pace`'s per-cycle
        ceiling: every read here is a cache-busted ORIGIN read. At the ceiling an unpaced startup
        sweep drew a 429 before cycle 1; that observation establishes no threshold. `_pace` is left in place and is a no-op at this gap.
        """
        out: dict[str, tuple[Decimal, Decimal]] = {}
        start = time.time()
        slugs = sorted(self.ticks)
        rest: list[str] = []
        now = time.time()
        for slug in slugs:
            md = self._startup_touch_md(slug, now)
            bid, ask, _ = touch_from_md(md)
            if bid is not None and ask is not None:
                out[slug] = (bid, ask)
            else:
                rest.append(slug)
        log.warning(f"startup touches: {len(out)} from the WS cache, {len(rest)} over REST at "
                    f"1/{STARTUP_TOUCH_PACE_S}s (~{len(rest) * STARTUP_TOUCH_PACE_S:.0f}s)")
        for index, slug in enumerate(rest):
            if index:
                await asyncio.sleep(STARTUP_TOUCH_PACE_S)
            await self._pace(index, start)
            try:
                book = await self.client._fetch_book(slug, fresh=True)
            except Exception as exc:
                log.warning(f"touch read failed for {slug}: {exc!r}")
                continue
            bid, ask, _ = touch_from_md(book.get("marketData") if isinstance(book, dict) else None)
            if bid is not None and ask is not None:
                out[slug] = (bid, ask)
        # The dict is consumed sorted (`zero_rebate_markets`), but keep the slate order anyway so
        # the shape does not depend on which arm served which book.
        return {slug: out[slug] for slug in slugs if slug in out}

    def _startup_touch_md(self, slug: str, now: float) -> Optional[dict]:
        """The cached book `read_touches` may serve this slug's touch from, or None to read REST.

        The rule is the quote rail's, one copy: `_ws_book_servable`. REST transport (no
        `book_feed`) serves nothing here, so that path is unchanged.
        """
        if self.book_source != "ws" or self.book_feed is None:
            return None
        md = self.book_feed.get_book_md(slug)
        if md is None or not self._ws_book_servable(slug, md, now):
            return None
        return md

    def zero_rebate_markets(self, touches: dict[str, tuple[Decimal, Decimal]]) -> list[str]:
        """Markets where a fill at the current mid would earn NOTHING at this size.

        ⛔ The size floor is a floor on SIZE; this is the per-PRICE rule. `p(1−p)` collapses toward
        the ends of the grid, so size 5 clears the nearest-cent boundary only across roughly
        p∈[0.10,0.90]. Point `--slugs` at a longshot and the maker will quote it, collect $0.0000,
        and carry the full adverse and inventory risk — deleting the only positive term in the
        lane's economics while every other guard reports healthy.

        Reported rather than refused: the price moves, and the decision stays the operator's.
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

    def _rebate_step_size(self, slug: str, side: str, px: Decimal, base: int,
                          inv: Decimal) -> tuple[int, str, int]:
        """The size ONE ADDING side should rest at `px` under the rebate step, its state, and
        the ceiling that bounded it. `(size, state, ceiling)`; state ∈ {ok, sized:N, skipped}.

        ⛔ THE CALLER MUST HAVE ESTABLISHED THAT THIS SIDE ADDS. A REDUCING side never reaches
        here: exiting inventory does not wait on a rebate, and a raised exit is a flip through
        flat into a fresh opposite position — the hazard every other lane in `_quote_one`
        clamps `min(size, |inv|)` for.

        ⛔ CAP INTERPLAY — THE ONE QUESTION THIS RULE HAS TO ANSWER. `sides_allowed` flips the
        adding side off at `|inv| >= size × cap_fills` measured on the UNRAISED size, so a
        raised order resting at the cap boundary could fill past `cap + size`, which is the
        registered `_launch_reach` the venue-breach halt and the parked-alive gate both bound
        exposure with. The ceiling therefore carries the FULL worst case as a term:

            ceiling <= reach − |inv| − unresolved_exposure(slug, side) − REBATE_STEP_HEADROOM_CT

        so that `|inv| + ghosts + size <= reach` by construction — the same arithmetic the
        parked-alive gate at the end of `_quote_one` performs, and the same `_launch_reach`
        `_venue_breach` halts the run on.

        ⛔ THE GHOST TERM IS NOT OPTIONAL. That gate votes on the UNRAISED `book_size` and runs
        BEFORE this rule, so without the term a side it passed at size 5 could be raised to 14 and
        carry the ghost worst case straight past the reach — a rail that has already fired for
        real. Sighted-alive orders only, exactly as that gate counts them: with no contradicting
        read the term is 0 and the ceiling is unchanged.

        `int()` truncates, rounding every headroom DOWN. The near-cap notional halt and the
        durable loss cap read INVENTORY and marks rather than order size, so bounding inventory
        bounds them too; `--max-total-contracts` is bounded explicitly below.
        ⛔ `reserved` IS NOT `skipped`. `ceiling == base`
        means the headroom reserve (or the reach) leaves NO room above the base size, i.e. there
        is no raise to judge. Both modes answer `base, "reserved"` — a POSTED cycle at the
        configured size — because `skipped` withholds the side and cancels what rests, and
        darking the favourite side of every sports book is not what a sizing reserve may do.
        `skipped` keeps its own meaning: a raise WAS searchable and no size in range pays.
        """
        ceiling = self._rebate_step_ceiling(slug, side, base, inv)
        if ceiling <= base:
            return base, "reserved", ceiling
        if self.rebate_step_mode == REBATE_STEP_MODE_OPTIMAL:
            return self._rebate_step_optimal(slug, side, px, base, inv)
        if not rebate_rounds_to_zero(px, base):
            return base, "ok", base
        need = min_rebate_size(px)
        # ⛔ STRUCTURAL: `min_rebate_size` returns 0 when the per-contract credit is 0, i.e. at
        # p == 0 or p == 1, where NO size ever clears the boundary. `need <= 0` must read as
        # "unreachable", never as "any size will do" — a 0 would otherwise satisfy `need <=
        # ceiling` and tape `sized:0` on the one price where the rebate is provably dead.
        if need <= 0 or need > ceiling:
            return base, "skipped", ceiling
        return need, f"sized:{need}", ceiling

    def _rebate_step_ceiling(self, slug: str, side: str, base: int, inv: Decimal) -> int:
        """The largest size the rebate step may raise this ADDING side to. ⛔ THE SINGLE
        EXPRESSION BOTH MODES READ — see `_rebate_step_size` for what each term bounds and why
        the ghost term is not optional."""
        ceiling = self.rebate_step_max if self.rebate_step_max is not None else 2 * base
        reach = self._launch_reach.get(slug, self._launch_reach_unknown)
        # ⛔ ONE EXPRESSION, ALL THREE EXPOSURE TERMS. Splitting the ghost term into its own
        # `min` invites the next reader to drop one and keep the other; the worst case is the
        # SUM and it is written as the sum.
        # ⛔ THE SAME HEADROOM THE GUARD USES: `_unresolved_exposure`, not
        # `_parked_alive_size`. The sighted-alive set is a SUBSET of the unresolved one, so this
        # term binds strictly earlier: parked-alive binds only after a read-back caught the ghost
        # trading, and the 2026-09-07 ghost filled before any read saw it. The sighted-alive gate
        # at the end of `_quote_one` is kept as it is — it still owns the `parked_alive_exposure`
        # stand-down, which is about orders the venue has CONTRADICTED us on, not merely unknown.
        # ⛔ THE RESERVE IS PART OF THE SAME EXPRESSION [2026-09-08]. Sizing to the EXACT bound
        # means any ghost fill breaches; every `_venue_breach` book since 09-01 was sized this
        # way. `REBATE_STEP_HEADROOM_CT` (default: the cell's base) keeps one ghost's worth of
        # reach free.
        # ⛔ AND IT NEVER FALLS BELOW `base`. At the
        # deployed lane an unclamped reserve puts the ceiling AT or BELOW base, which both modes read as
        # "no size in range pays" and tape `skipped` — and `skipped` at the caller withholds BOTH adding
        # sides and cancels what rests, darking the book for the run (measured on the favourite side of
        # sports books). The reserve bounds the RAISE, never PRESENCE: `ceiling == base` means "no
        # raise available", `_rebate_step_size` answers `reserved` at base, and the base order
        # goes out judged by `_reach_permits` exactly as before.
        reserve = base if REBATE_STEP_HEADROOM_CT is None else REBATE_STEP_HEADROOM_CT
        ceiling = max(base, min(ceiling, int(Decimal(reach) - inv.copy_abs()
                                             - self._unresolved_exposure(slug, side)) - reserve))
        if self.max_total_contracts > 0:
            # Only the EXTRA contracts need room: `global_sides_allowed` already decided the base
            # size may rest, and this rule may not loosen that decision.
            ceiling = min(ceiling,
                          base + max(0, int(Decimal(self.max_total_contracts)
                                            - self.gross_exposure)))
        # ⛔ THE BOOK'S OWN CAP ROOM IS A TERM TOO [incident]: `_quote_one` clamps every
        # ADDING side to `cap − |inv|` (whole contracts held, the same sub-contract carve-out), so
        # a raise above that room would be cut back afterwards and the step would tape a size it
        # never rested. Answering it here means the state is `reserved`, not a silent trim.
        # ⛔ AND IT SITS OUTSIDE THE `max(base, ...)` FLOOR DELIBERATELY: a room UNDER `base` must
        # read as "no raise available" — `_rebate_step_size` returns `base, "reserved"` on
        # `ceiling <= base` and the base order still goes out, cut to the room by that clamp.
        # `skipped` would dark the side, which no sizing reserve may do (the 2026-09-08 BLOCKING).
        return min(ceiling, self.caps.get(slug, self.cap) - int(inv.copy_abs()))

    def _rebate_step_optimal(self, slug: str, side: str, px: Decimal, base: int,
                             inv: Decimal) -> tuple[int, str, int]:
        """`optimal` mode [operator decision 2026-09-04]: RECOVER THE ROUNDED-DOWN CENT, and
        nothing else. The SMALLEST size in `(base, ceiling]` whose paid cents reach
        `paid(base) + 1`, or `base` unchanged.

        The venue rounds the credit to the nearest cent per fill increment, so a base whose
        credit rounds DOWN forfeits its remainder outright: at p=0.35 a 5-lot's <n> collects
        as <n> and a 6-lot's <n> collects as <n>, and the floor rule leaves the 5-lot alone
        because it is not rounding to ZERO. Three clauses, in this order:

        ⛔ (1) THE GUARD — ACT ONLY WHERE THE BASE PAYS AT MOST ONE CENT. The rule is "fix the
        round-down loss on SMALL credits". A base already paying <n> or more is left alone
        (returns `ok`): at p=0.50 a 10-lot's <n> collects as <n>, and chasing the fourth cent
        would post 12 — +11% per contract for +20% exposure. Without this the size-10 arm creeps
        on every mid-band book, which is a size change the A/B never registered.

        (2) ROUNDS DOWN, OR NOTHING TO RECOVER. `paid(base) <= credit(base)` is exactly "the
        sub-cent remainder was discarded" (half-up rounds UP only when the remainder is at or
        above half a cent, and then `paid > credit`). A base that already rounds UP is being
        paid more than it earns — there is no loss to fix, so it stands.

        (3) THE TARGET IS THE NEXT CENT, NOT THE BEST RATE. The smallest qualifying size wins by
        construction, so ties are moot; a rate objective would instead pick the largest size that
        happens to sit on a favourable tooth and buy exposure for it.

        ⛔ THE INCREMENT IS THE TAKER'S CHOICE, NOT OUR ORDER SIZE (venue-reference skill
        § maker rebate; `predicted_rebate`'s ⛔ CORRECTED block). This objective is EXACT only
        for a full-clip fill — a nibble takes its own rounding, and the raised size buys the
        extra exposure whether or not the clip that lands earns the extra cent. Measured on the
        fills tape: most fills, mid-band and edge alike, took the WHOLE resting size
        (`filled_qty >= size`). So the gain is realised on most fills and
        the exposure is carried on 100% of them — size the ceiling on that, never on the
        sawtooth alone.

        Same ceiling, same both-or-neither contract, same caller contract as `floor` — this
        side ADDS, and a reducing side never reaches here. `base` is always a candidate, so a
        book with no headroom (`ceiling < base`) answers exactly as `floor` does.
        """
        ceiling = self._rebate_step_ceiling(slug, side, base, inv)
        # ⛔ TOTAL FUNCTION, like `floor`: a non-positive base has nothing to raise and no credit
        # to round. Answer the caller unchanged rather than raise inside a quote cycle.
        if base <= 0:
            return base, "ok", ceiling
        if all(paid_rebate_cents(px, candidate) == 0
               for candidate in range(base, max(ceiling, base) + 1)):
            # Nothing inside the ceiling pays a cent — identical to `floor`'s unreachable arm, and
            # it must stay identical: the both-or-neither coercion on a flat book keys on it.
            return base, "skipped", ceiling
        paid_base = paid_rebate_cents(px, base)
        if paid_base > 1:
            return base, "ok", ceiling  # (1) the guard — a <n>+ base is not this rule's business
        if Decimal(paid_base) / _CENTS_PER_DOLLAR > predicted_rebate(px, base):
            return base, "ok", ceiling  # (2) the base rounds UP; there is no discarded remainder
        for candidate in range(base + 1, ceiling + 1):
            if paid_rebate_cents(px, candidate) >= paid_base + 1:
                return candidate, f"opt:{candidate}", ceiling  # (3) the next cent, smallest size
        return base, "ok", ceiling

    @property
    def _gap_s(self) -> float:
        """The minimum spacing for ONE request. Used only where a single request is about to be
        spent outside the cycle's pacer (the startup tick sweep)."""
        return 1.0 / self.max_req_per_s if self.max_req_per_s > 0 else 0.0

    async def _pace(self, requests_spent: int, cycle_start: float) -> None:
        """Hold the cycle's cumulative request rate at or under `max_req_per_s`.

        ⛔ PACED ON REQUESTS SPENT, NOT ON MARKETS VISITED. A fixed gap between markets assumes
        every market costs one request; a QUOTING market costs five (its book read plus two
        placements, each carrying the client's own crossing-guard book read). Measured: two
        markets spent 10 requests in 0.28 s — a 35 req/s burst against a 20 req/s ceiling.
        Over-limit is THROTTLED rather than rejected, so the symptom is a late, stale book.
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

    def _note_commission(self, order_id: Optional[str],
                         total: Optional[Decimal]) -> Optional[Decimal]:
        """Record the LATEST per-order commission total and pass it through unchanged —
        the tape column and the session context stay byte-identical by construction.

        ⛔ AND BOOK THE DURABLE DELTA [AMENDMENT 30]: the cap binds on OVERALL, so the venue's
        own read-back has to reach the ledger the same way a price fill does. Per-order totals
        are CUMULATIVE and last-per-order wins, so the delta is `new − old` (sign-flipped: the
        venue sends a rebate negative). A fill whose read-back is missing (`total is None`) books
        NOTHING — a missing credit must never inflate loss headroom."""
        if order_id is not None and total is not None:
            delta = total - self._order_commissions.get(order_id, _ZERO)
            self._order_commissions[order_id] = total
            if delta != _ZERO and self.state is not None and self.real:
                self.state.add_rebate(-delta)
        return total

    @property
    def session_rebate(self) -> Decimal:
        """Σ of the LAST commission total per order, sign-flipped (venue sends rebates
        negative). ⛔ SINCE AMENDMENT 30 THIS IS A RAIL INPUT: it is the session axis of
        `_loss_cap_breach` (OVERALL = price + realized rebate), not merely message context."""
        return -sum(self._order_commissions.values(), _ZERO)

    def _marks(self, *, net: bool = True) -> dict[str, Decimal]:
        """Per-book mark price = the midpoint of the last touch WE READ, by default **NET OF OUR
        OWN RESTING ORDERS**, for books read recently enough for that to mean anything.

        ⛔ `net=False` IS THE THRESHOLD MAP, AND IT IS NOT AN OPTION. `ws_near_cap_notional` has
        TWO price terms: held value (numerator) and the cap-weighted p̄ that sets the bar. Only the
        NUMERATOR may see our own quote removed — netting the denominator too made the rail
        LOOSER. So: `net=True` → held value and the fallback exposure ranking; `net=False` → p̄ and
        the operator's printed trip point. **Our own quote never moves the threshold.**

        ⛔ NET, NOT GROSS, in the numerator: on a book we are the touch of, the gross mid is partly
        OUR OWN QUOTE, and this mark SIZES — it inflated the headroom the cap believed it had. The
        values are the SAME cycle's `net_of_self_touch` output stamped in `last_touch_net`, never
        a second derivation. DRY/shadow is unchanged: net IS raw there by construction.

        ⛔ AN EMPTIED NET SIDE DROPS THE BOOK FROM THE **NET** MAP — it does not fall back to the
        gross mid and it does not become $0. Dropped means UNMARKED, and every consumer prices an
        unmarked book at **<n>/contract**: the held position reads at its maximum possible value
        and the fallback set ranks the book EARLIER. A $0/absent held value — the direction that
        hides exposure from the cap — is unreachable.

        ⚠️ A BELIEF ABOUT PRICE, not a venue quote: both consumers want a magnitude ("is this book
        expensive?"), never a price to trade at. Zero requests.

        ⛔ AND IT EXPIRES. Anything older than `MARK_STALE_CYCLES × requote_s` is dropped, because
        a book skipped for hours would otherwise feed an HOURS-OLD price into a HALT rail. Dropped
        means unmarked = <n>/ct, the CONSERVATIVE direction; a stale mark has no such guarantee
        and can silently un-arm the axis.
        """
        cutoff = time.time() - MARK_STALE_CYCLES * max(self.requote_s, 1.0)
        out: dict[str, Decimal] = {}
        for slug, t in self.last_touch.items():
            if not t or t[0] is None or t[1] is None or t[2] < cutoff:
                continue
            if not net:
                out[slug] = (t[0] + t[1]) / 2            # THRESHOLD map: gross, always priced
                continue
            pair = self.last_touch_net.get(slug)
            if pair is None or pair[0] is None or pair[1] is None:
                continue                      # unmarked ⇒ <n>/ct in the held-value numerator
            out[slug] = (pair[0] + pair[1]) / 2
        return out

    @property
    def gross_exposure(self) -> Decimal:
        """Σ|inventory|. Not netted: a long in one market and a short in another do not settle
        against each other."""
        return sum((v.copy_abs() for v in self.inventory.values()), _ZERO)

    def _record_min_trade_qty(self, slug: str, min_qty: Optional[Decimal]) -> None:
        """File one venue-read `minimumTradeQty`, and SAY which number the book will use.

        ⛔ WHICH MINIMUM WAS USED IS LOGGED PER BOOK. A silent fallback is how one book's 0.01
        became "the venue's minimum" for the futures whose minimum is 1.
        ⚠️ A non-positive value is NOT a minimum — it is treated as ABSENT (fallback), never as
        "no minimum", which would let a 0.0001 dust order go out. The slug is marked PROBED
        either way: we asked, and even "unreadable" is a fact about this book.
        """
        self._min_qty_probed.add(slug)
        if min_qty is not None and min_qty > _ZERO:
            self.min_qty[slug] = min_qty
            log.info(f"{slug}: minimumTradeQty {min_qty} (venue metadata)")
            return
        log.warning(f"{slug}: minimumTradeQty unreadable ({min_qty!r}) — falling back to the "
                    f"documented constant {MIN_TRADE_QTY}. It is PER-MARKET on this venue "
                    f"(it differs per market by orders of magnitude), so this fallback may be wrong "
                    f"in either direction for this book.")

    async def _ensure_min_trade_qty(self, slug: str) -> Decimal:
        """`min_trade_qty(slug)`, but FETCHING first if this book was never asked.

        ⛔ FOR THE NON-HOT ORDER PATHS ONLY — today the teardown flatten. A book can reach an order
        path without `prepare()` ever having asked about it (a declared `--carry`, a hot slate
        change); `min_trade_qty` would then answer the fallback silently, which on a `tec-*` book
        sends a guaranteed-refusal dust order.
        ⛔ ONE ATTEMPT PER SLUG PER PROCESS: `_record_min_trade_qty` marks it PROBED whatever the
        venue said, so a flaky endpoint costs one request, not one per teardown. The quote path
        deliberately does NOT call this — a fetch there would spend a request every cycle.
        """
        if slug not in self._min_qty_probed:
            _meta = getattr(self.client, "get_market_meta", None)
            if _meta is not None:
                try:
                    _tick, _mq = await _meta(slug)
                except Exception as exc:
                    log.warning(f"{slug}: minimumTradeQty fetch-on-miss FAILED ({safe_exc(exc)})")
                    _mq = None
                self._record_min_trade_qty(slug, _mq)
        return self.min_trade_qty(slug)

    def min_trade_qty(self, slug: str) -> Decimal:
        """THE smallest quantity this ONE book will accept — the venue's own `minimumTradeQty`,
        read in `prepare()`, or the documented `MIN_TRADE_QTY` fallback when it was unreadable.

        ⛔ ONE READER for the per-market value, so no order path can go back to the global constant
        by accident. It differs per market by three orders of
        magnitude, and the wrong one is either a guaranteed venue refusal every teardown or a dust
        order that never had to be sent.

        ⛔ AN UNPROBED SLUG WARNS, ONCE: silently, the fallback is a GUESS wearing a measurement's
        clothes. Non-hot callers should use `_ensure_min_trade_qty`, which fetches instead of
        guessing; this stays sync and cheap because the quote path calls it every cycle.
        """
        if slug not in self._min_qty_probed and slug not in self._min_qty_unprobed_announced:
            self._min_qty_unprobed_announced.add(slug)
            log.warning(f"{slug}: minimumTradeQty was NEVER READ for this book (it entered after "
                        f"`prepare()` — a carry or a hot slate change) — using the fallback "
                        f"{MIN_TRADE_QTY}, which is a GUESS, not this book's measured minimum "
                        f"(it differs per market by orders of magnitude).")
        return self.min_qty.get(slug, MIN_TRADE_QTY)

    def dust_close_size(self, slug: str, inv: Decimal) -> Optional[Decimal]:
        """The EXACT size of a placeable sub-contract reducing order, or None when there is none.

        None has three distinct causes, all meaning "do not place, report instead":
          · `|inv|` is not sub-contract dust (0 < |inv| < 1 is the whole domain);
          · the fraction is BELOW this book's own minimum trade quantity, so the order is a
            guaranteed venue refusal — `min_trade_qty`, never the global constant;
          · `lane.fractional_close` is `off` (or this is not the probe lane). ⚠️ Not repo-wide:
            the per-market minimum is UNGATED, so `off` is not byte-for-byte on a book whose
            venue minimum differs from 0.01.

        ⛔ THE SIZE IS THE HELD FRACTION, NEVER ROUNDED. Rounding a 0.53 close up to 1 is not a
        close: it fills through flat and opens a 0.47 position the other way — which is why every
        clamp in this file TRUNCATES. The caller is responsible for having sized `inv` against
        the venue's own number where one is available.
        """
        if not self.fractional_close:
            return None
        mag = inv.copy_abs()
        if not (_ZERO < mag < _ONE):
            return None
        if mag < self.min_trade_qty(slug):
            return None
        return mag

    def _clock_guard_books(self) -> Optional[dict[str, Any]]:
        """The game-state snapshot (`latest.json` `books`), cached on the file's mtime.

        ⛔ `None` IS "NO SNAPSHOT AT ALL", DISTINCT FROM AN EMPTY ONE. The caller prints a
        per-slug UNGUARDED line for a book the snapshot does not carry, and a missing FILE must not
        produce one line per guarded slug on top of the file-level line that already said it.

        ⛔ EVERY FAILURE ANSWERS `None`, i.e. GUARD OFF, and says so ONCE. This is a research-tape
        sidecar on an undocumented third-party feed: it can be absent (no sidecar this run),
        half-written (it is not — `write_latest` is tmp + `os.replace` — but a truncated file must
        still not raise here) or owned by a process that died. None of that may raise inside a
        quote cycle, and none of it may stand a book down: the guard is an ADDITION to the rails
        that already govern, so its absence is the pre-amendment behaviour.
        """
        path = self.clock_guard_file or CLOCK_GUARD_FILE
        try:
            mtime = os.stat(path).st_mtime
            if self._clock_guard_cache is not None and self._clock_guard_cache[0] == mtime:
                return self._clock_guard_cache[1]
            with open(path) as fh:
                books = json.load(fh)["books"]
            if not isinstance(books, dict):
                raise ValueError(f"books is {type(books).__name__}, not an object")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            if not self._clock_guard_unreadable_announced:
                self._clock_guard_unreadable_announced = True
                log.warning(f"clock guard: {path} unreadable ({safe_exc(exc)}) — the guard is OFF "
                            f"for {sorted(self.clock_guard_slugs)}; every other rail unchanged")
            return None
        self._clock_guard_cache = (mtime, books)
        return books

    def _clock_guard_unguarded(self, slug: str, why: str) -> bool:
        """One operator line per slug per run: this GUARDED book is running UNGUARDED. Always False.

        ⛔ IT IS NOT THE SAME EVENT AS AN UNREADABLE FILE. The snapshot can be present, fresh and
        simply not carry this book — a dead sidecar leaving its last file behind, or the recorder's
        own `MAX_BOOKS` = 10 cap silently dropping the eleventh game — and in that shape the
        file-level line never fires while the guard is off on a live seat. Once per slug, on the
        `_min_qty_unprobed_announced` pattern: this is asked per market per cycle.
        """
        if slug not in self._clock_guard_unguarded_announced:
            self._clock_guard_unguarded_announced.add(slug)
            log.warning(f"clock guard OFF {slug}: {why} — this book runs UNGUARDED "
                        f"(every other rail unchanged)")
        # ⛔ AND IT COUNTS AS A DISARM: a book that armed and then went stale must not keep its
        # armed flag, or the next fresh `post` row prints no ARM line and the tape's transitions
        # stop matching the log.
        self._clock_guard_armed.discard(slug)
        return False

    def clock_guard_active(self, slug: str) -> bool:
        """Is this book in its game's FINAL STRETCH — reduce-only on the GAME's clock?
        [AMENDMENT 41, operator 2026-09-10; see the constants for the run that bought it.]

        Answers False for every book `--clock-guard-slugs` does not name, and False on ANY
        uncertainty: no snapshot, no row for this slug, a row older than `CLOCK_GUARD_STALE_S`, a
        blank period or clock (a failed poll writes the row with them blank — `state` blank too),
        or a sport with no clock rule and a game that is not yet `post`.

        ⛔ UNKNOWN IS NEVER A STAND-DOWN — the alternative arms on a state nobody has observed.
        ⛔ …EXCEPT that `post`, ONCE OBSERVED, LATCHES for the rest of the run [mm-review
        2026-09-10]. It is the one monotone state: a game that is over does not become not-over, so
        a snapshot that afterwards goes stale, drops the book past the recorder's `MAX_BOOKS` cap
        or disappears entirely must not re-admit a resolved book to full quoting. The staleness
        rule governs every state we have NOT yet seen.

        The rule, on the slug's SECOND token (`asc-nfl-…` → `nfl`), never on a venue category:
          · `state == post` — over, on EVERY sport;
          · football (`nfl`/`cfb`) — period ≥ 5 (overtime) or period 4 with ≤
            `CLOCK_GUARD_FOOTBALL_CLOCK_S` on the clock;
          · `mlb` — inning ≥ `CLOCK_GUARD_MLB_INNING` (baseball has no clock; the recorder writes
            `0:00`, so the inning is the whole rule);
          · anything else — `post` only, because no other rule has been registered for it.
        """
        if slug not in self.clock_guard_slugs:
            return False
        # ⛔ POST IS LATCHED, AND IT IS CHECKED BEFORE THE FILE [mm-review 2026-09-10]: `post` is
        # the one monotone state, so a snapshot that later goes stale, loses the book or vanishes
        # must NOT re-admit a resolved book to full quoting. No re-read, no per-slug line.
        if slug in self._clock_guard_post_latched:
            return True
        books = self._clock_guard_books()
        if books is None:
            return False                      # the file-level line already named it, once
        row = books.get(slug)
        if not isinstance(row, dict):
            return self._clock_guard_unguarded(slug, "no row in the snapshot")
        try:
            row_ts = float(row.get("ts") or 0.0)
        except (TypeError, ValueError):
            return self._clock_guard_unguarded(slug, "unparseable row ts")
        age = time.time() - row_ts
        if age > CLOCK_GUARD_STALE_S:
            return self._clock_guard_unguarded(slug, f"row age {age:.0f}s > {CLOCK_GUARD_STALE_S:g}s")
        state = str(row.get("state") or "")
        period_raw, clock_raw = str(row.get("period") or ""), str(row.get("clock") or "")
        if state == CLOCK_GUARD_STATE_POST:
            self._clock_guard_post_latched.add(slug)
            active = True
        else:
            rule = clock_guard_rule_for(slug)
            period = _int_or_none(period_raw)
            if period is None:
                active = False
            elif rule == CLOCK_GUARD_RULE_FOOTBALL:
                clock_s = _clock_seconds(clock_raw)
                active = (period > CLOCK_GUARD_FOOTBALL_PERIOD
                          or (period == CLOCK_GUARD_FOOTBALL_PERIOD and clock_s is not None
                              and clock_s <= CLOCK_GUARD_FOOTBALL_CLOCK_S))
            elif rule == CLOCK_GUARD_RULE_MLB:
                active = period >= CLOCK_GUARD_MLB_INNING
            else:
                active = False
        # ⛔ ONE LINE PER TRANSITION, and the numbers that decided it are ON the line: an operator
        # reading a book that stopped adding must not have to join two tapes to see why.
        if active and slug not in self._clock_guard_armed:
            self._clock_guard_armed.add(slug)
            log.warning(f"clock guard ARMED {slug}: reduce-only — state={state or '?'} "
                        f"period={period_raw or '?'} clock={clock_raw or '?'} "
                        f"row_age={time.time() - row_ts:.0f}s")
        elif not active and slug in self._clock_guard_armed:
            self._clock_guard_armed.discard(slug)
            log.warning(f"clock guard DISARMED {slug}: quoting both sides — state={state or '?'} "
                        f"period={period_raw or '?'} clock={clock_raw or '?'} "
                        f"row_age={time.time() - row_ts:.0f}s")
        return active

    def _maintenance_now(self, now: Optional[float] = None) -> bool:
        """Is the venue inside its RESERVED weekly maintenance window RIGHT NOW? Venue-wide, book
        independent — the 503-wall HOLD asks this one (`bot/core/maintenance.py` owns the terms).
        """
        return in_maintenance_window(time.time() if now is None else now)

    def maintenance_guard_active(self, slug: str, now: Optional[float] = None) -> bool:
        """Is this book in the DEFENSIVE MAINTENANCE POSTURE — reduce-only because the venue is
        inside its weekly maintenance window? [OPERATOR POLICY 2026-09-10.]

        The window is RESERVED, not always used: when it IS used the venue cancels every open
        order, 503s every request and reopens with EMPTY books, and a fresh position opened into
        that is unmanageable for up to four hours. The policy is therefore to keep QUOTING but
        with minimal exposure on the books whose price can move inside the window — the
        game-clock-guarded classes plus every in-play sports class, `--maintenance-guard-slugs`.
        Declared non-sports fixed seats are deliberately NOT named: they
        are the seats whose income is the point of running through the window at all.

        ⛔ WINDOW-ONLY AND NEVER LATCHED, unlike the clock guard's `post`: maintenance ENDS, and a
        book whose game is still live afterwards goes back to full quoting. The clock guard is
        what stands a resolving book down, on its own terms.
        ⛔ NOTHING ELSE IS RELAXED inside the window: every cap, the tripwire, the width shield
        and both loss caps still govern.
        """
        return slug in self.maintenance_guard_slugs and self._maintenance_now(now)

    def reduce_only_now(self, slug: str, *, pulled: Optional[bool] = None,
                        clock_guard: Optional[bool] = None,
                        maintenance_guard: Optional[bool] = None) -> bool:
        """THE reduce-only LANE predicate: wind-down, this book's calendar pull, the hot
        `quote: reduce_only` lane, the GAME-CLOCK GUARD, the MAINTENANCE POSTURE, the hot slate's
        RETIRE, or a periodic-VERIFY MISMATCH. These seven share the wind-down TREATMENT (clamped
        size, reducing side only, a flat book quotes nothing); the stand-downs in `in_cooldown`
        clamp SIDES only, which is why they are a separate question and not an eighth term here.

        ⛔ THE CLOCK GUARD BELONGS HERE AND NOT IN `in_cooldown` [AMENDMENT 41]: `in_cooldown`'s
        flat-book predicate deliberately does NOT stand a FLAT book down (no mark left to protect,
        and its one writer is the mark tripwire), while the whole point of this rail is to stop a
        flat book OPENING a position into a final whistle. Putting it there would have made it
        inert on exactly the books that lost the money.

        `pulled` and `clock_guard` are passed in by the quote path, which has already asked.
        ⛔ `clock_guard` IS AN ARGUMENT FOR A REASON: the quote path
        also needs the answer for `hold_cause`, and the snapshot cache re-reads on an mtime
        change — a second call could land on the OTHER side of the recorder's write and tape a
        cause the sizing decision did not use. ONE read per book per cycle, threaded.
        """
        if pulled is None:
            pulled = self.book_pulled(slug)
        if clock_guard is None:
            clock_guard = self.clock_guard_active(slug)
        if maintenance_guard is None:
            maintenance_guard = self.maintenance_guard_active(slug)
        return bool(self.winddown or pulled
                    or self.hot_quote.get(slug) in ("reduce_only", "gated")
                    # ⛔ `gated` IS THE DRAINED LANE AT ADMIT [2026-09-11]: a reseated
                    # burst-gated book rests reduce-only until the gate's own hot_settings
                    # `quote` write replaces it — before this it quoted two-sided for the
                    # ~15 s between the WS frame and the gate's claim.
                    or clock_guard or maintenance_guard
                    # ⛔ THE SIXTH TERM [continuous maker P1]: a book the hot slate RETIRED. Same
                    # clamp path as wind-down — reducing side only, size clamped to inventory, a
                    # flat book quotes nothing — and deliberately NOT a cancel: the retiring book
                    # still needs its queue seniority to work the inventory down.
                    # `getattr`: a maker is not always built by `__init__` (see `_tape_map`), and
                    # a rail may never raise on a map only the constructor seeds.
                    or slug in getattr(self, "_slate_retiring", ())
                    # ⛔ THE SEVENTH TERM [continuous maker P1b]: the periodic independent verify
                    # found the VENUE disagreeing with belief on this book. Same clamp path as
                    # wind-down, and it clears only on the NEXT verify that reads it clean — not
                    # on a timer, because nothing but a fresh venue read is evidence. A FAILED
                    # verify never puts a slug in here (cannot-verify ≠ mismatch).
                    or slug in getattr(self, "_verify_reduce_only", ()))

    def in_cooldown(self, slug: str) -> bool:
        """Is this book STOOD DOWN — reduce-only until a deadline or an ack?

        Two arming conditions, one machinery: the adverse LATCH (no deadline, cleared only by a
        dated ack) and `cooldown_until`.

        ⛔ `cooldown_until` HAS EXACTLY ONE WRITER — the unrealized-MARK TRIPWIRE — and the
        flat-book predicate below is only SAFE while that holds. It refuses to stand a FLAT book
        down on the argument that every tripwire arm requires inventory; a second writer whose
        stand-down is about something other than the mark would have that argument silently
        applied to it, and its stand-down would evaporate the moment the book flattened. Anything
        adding a second writer must revisit the predicate in the same change. (`adverse_cooldown_s`
        is the shared DURATION, not a second writer.)

        ⛔ THE TRIPWIRE ARM IS INVENTORY-CONDITIONAL — a FLAT book is not stood down by it
        [F5(a)]. Once the book is flat there is no mark left to protect, while the stand-down
        still costs 100% of both income channels (`_reduce_only(0)` is `(False, False)`, so the
        book quotes NEITHER side and prevents ZERO loss). The deadline is deliberately NOT erased
        here — it is the tape's own record, and a book that re-accumulates inside the window is
        stood down again, the conservative direction.
        ⛔ ONE PLACE ERASES IT, AND IT IS NOT A WRITER: `_expire_adverse_bans` deletes a deadline
        stamped at or before the arming of the randomized ban whose expiry it is processing —
        same adverse event — or a short ban would measure the tripwire instead of its drawn arm.
        ⚠️ The ADVERSE LATCH is untouched by any of this: a flat latched book still waits out its
        ban, because that rail is about the NEXT entry, not the mark.
        """
        return bool(slug in self.adverse_latched
                    or (time.time() < self.cooldown_until.get(slug, 0.0)
                        and self.inventory.get(slug, _ZERO) != _ZERO))

    def _reduce_only_books(self) -> set[str]:
        """Slate books that cannot lawfully ADD right now — the hot-settings raise rail's input.
        A raise on one of these buys bigger exits while widening the book's breach threshold
        permanently, so it is refused. BOTH senses count: the reduce-only lane and any
        stand-down."""
        return {slug for slug in self.sizes
                if self.reduce_only_now(slug) or self.in_cooldown(slug)}

    def book_pulled(self, slug: str) -> bool:
        """Has this ONE book passed its `--pull-at` calendar deadline? Announces once.

        A pulled book takes the wind-down treatment (reduce-only, clamped, sub-contract skip)
        while the rest of the slate keeps quoting normally — one book's exit must never stand the
        others down. Absent from `pull_at` = never pulled.
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

    def _latch_rule_for(self, slug: str) -> str:
        """This BOOK's adverse-latch rule this run [AMENDMENT 19c].

        ⛔ PER-CELL, BECAUSE THE CONTROL ARM IS PER-CELL. `self._probe_latch_rule` is the run's
        registered rule; a slug drawn into the control arm at pick time answers
        `LATCH_RULE_CONTROL_OFF` INSTEAD, and every other slug on the same run keeps the
        registered rule. That WITHIN-NIGHT contrast IS the experiment — a whole-run off switch
        would compare nights, which is the confound the prereg exists to avoid.

        ⛔ OFF-PROBE it answers `off` only for a slug the LINE named [AMENDMENT 33]:
        `_probe_latch_rule` is `""` there, so every other book keeps the main lane's own
        run-lifetime rail, unchanged.
        """
        if slug in self.latch_off_slugs:
            return LATCH_RULE_CONTROL_OFF
        return self._probe_latch_rule

    def _latch_adverse(self, slug: str, ban_s: Optional[float] = None, *,
                       defer_notify: Optional[list[tuple[str, str, str]]] = None) -> None:
        """Latch (or RE-latch) a book reduce-only, durably.

        The stamp is refreshed on every fire on purpose: an ack written before a re-fire is then
        OLDER than the latch it would clear, so the file-level ack cannot acknowledge an event
        that had not happened when it was written.

        `ban_s` is the PROBE-LANE timed scope; `None` — every other lane always, and the probe
        lane too under the shipped `latch.rule = kill_reverted_lifetime` — is the incumbent
        RUN-LIFETIME latch and writes no deadline. A re-trip re-stamps both, so a second ban is
        measured from the second trip. ⛔ AND IT IS THE SAME BAN [confound A]: the caller does not
        re-draw while a ban is running, so the clock restarts on the arm this EPISODE was
        assigned — otherwise the taped arm would not be the treatment the book received.

        `defer_notify` moves ONLY the probe-channel enqueue — never the latch itself. The latch
        fires mid-`_book_fill`, i.e. BEFORE the fill that armed it has been announced, so an
        un-deferred enqueue puts the latch on the channel ahead of its own cause.
        """
        first = slug not in self.adverse_latched
        now = time.time()
        # A RE-TRIP is a second arming inside a ban that is still running — the clock restarts but
        # the caller has already resolved `ban_s` to THIS episode's own drawn arm [confound A].
        # Read before the stamp overwrites the deadline this tests.
        retrip = (ban_s is not None and slug in self.adverse_ban_episode
                  and now < self.adverse_ban_until.get(slug, 0.0))
        self.adverse_latched[slug] = now
        if ban_s is None:
            # ⛔ A lifetime re-latch must ERASE any earlier deadline, or a stale ban would
            # re-admit a book that has just been latched for the run.
            self.adverse_ban_until.pop(slug, None)
            self.adverse_ban_episode.pop(slug, None)
        else:
            self.adverse_ban_until[slug] = now + ban_s
            # ⛔ `armed_at` MOVES ON A RE-TRIP, `episode_ban_s` DOES NOT. The clock restarted, so
            # the re-entry instant this episode is measured from is this trip; the arm it was
            # assigned is still the one drawn when the episode opened.
            self.adverse_ban_episode[slug] = (ban_s, now)
        self._persist_adverse_latches()
        # ⛔ The lifetime branch's text is UNCHANGED, character for character — the non-probe
        # lanes' observable output must not move at all.
        if ban_s is None:
            detail = ("reduce-only for the rest of the run" if first
                      else "RE-LATCHED (any ack already written for the earlier latch is now "
                           "stale)")
        else:
            detail = (f"reduce-only until {_utc_iso(now + ban_s)} ({ban_s:.0f}s ban)"
                      + (" — RE-TRIP inside the running ban: the clock restarts on THIS "
                         f"episode's own {ban_s:.0f}s arm, NOT a fresh draw" if retrip
                         else "" if first
                         else " — RE-LATCHED, ban re-measured from this trip"))
        if defer_notify is None:
            self._probe_notes.event(probe_notify.event_line, "ADVERSE LATCH", slug, detail)
        else:
            defer_notify.append(("ADVERSE LATCH", slug, detail))

    def _persist_adverse_latches(self) -> None:
        """Mirror the latch map AND its ban-deadline companion into the durable ledger. A ledger
        failure must not take the run down — the in-memory latch still holds for this process.

        `self.real` gates the write for consistency with every sibling persist site: a shadow run
        must not leave latches behind for the next real one.

        ⛔ ONE write for BOTH maps: `set_adverse_latched` takes the deadline map positionally so
        that no future caller can persist half the record."""
        if self.state is None or not self.real:
            return
        try:
            self.state.set_adverse_latched(self.adverse_latched, self.adverse_ban_until)
        except Exception as exc:
            log.error(f"⚠️ adverse latch NOT persisted ({exc!r}) — it holds for this process "
                      f"only; a crash-restart would quote {sorted(self.adverse_latched)} "
                      f"two-sided again")

    def _expire_adverse_bans(self) -> None:
        """Re-admit every book whose PROBE-LANE timed ban has run out [cycle-4 P1].

        ⛔ Keyed on the DEADLINE MAP, not on the lane: only the probe lane ever writes a deadline,
        so a book with none is a run-lifetime latch and is never touched here. That is what makes
        the incumbent rail bit-identical on every other lane.

        Re-admission returns the book to NORMAL quoting (it leaves `adverse_latched`, the one
        predicate `in_cooldown` reads); it is NOT a reduce-only half-state.
        ⛔ AND IT CLEARS A SAME-EPISODE TRIPWIRE DEADLINE [confound B] — the ONE delete site for
        `cooldown_until`/`trip_until`, gated on the deadline having been stamped at or before this
        episode's arming instant. A later fire is untouched and `mark_trips` never moves.
        A re-admitted book that still HOLDS inventory under a live tripwire deadline stays
        reduce-only, which is the tripwire legitimately holding it.

        Called once per cycle from `run_cycle`, BEFORE any quote decision reads the map, so a
        re-admitted book quotes on the very cycle it is re-admitted rather than one later.
        """
        if not self.adverse_ban_until:
            return
        now = time.time()
        due = sorted(s for s, deadline in self.adverse_ban_until.items() if now >= deadline)
        if not due:
            return
        cleared_trip: list[str] = []
        for slug in due:
            del self.adverse_ban_until[slug]
            # ⛔ CONFOUND B: THE ONE DELETE SITE for `cooldown_until`, and it is a DELETE, never a
            # write — the mark tripwire stays the map's single WRITER, which is what
            # `in_cooldown`'s flat-book predicate rests on.
            # A tripwire deadline stamped at or before this episode's arming instant belongs to
            # the SAME adverse event. Left standing it survives a short ban by up to 900 s, so the
            # book's FIRST post-ban fill flips it reduce-only and the short arms would measure
            # "one fill, then reduce-only" instead of their drawn ban.
            # ⛔ A deadline stamped AFTER the arming instant is a LATER breach and is untouched.
            # The write instant is DERIVED (`deadline − adverse_cooldown_s`) because the tripwire
            # writes only the deadline, which keeps that block byte-identical.
            episode = self.adverse_ban_episode.pop(slug, None)
            deadline = self.cooldown_until.get(slug)
            if (episode is not None and deadline is not None
                    and deadline - self.adverse_cooldown_s <= episode[1]):
                del self.cooldown_until[slug]
                self.trip_until.pop(slug, None)   # same writer, same deadline — they go together
                cleared_trip.append(slug)
            # A deadline whose latch is already gone (an operator ack cleared it) leaves
            # nothing to re-admit — drop the orphan quietly rather than announce a
            # re-admission that did not happen.
            self.adverse_latched.pop(slug, None)
        self._persist_adverse_latches()
        log.warning(
            f"🔓 adverse TIMED BAN expired for {due} — re-admitted to NORMAL two-sided "
            f"quoting [cycle-4 P1 probe lane]. A fresh qualifying adverse trip re-latches "
            f"and re-measures the ban from that trip.")
        if cleared_trip:
            # ⛔ `mark_trips` IS NOT TOUCHED — the trip happened and stays counted.
            log.warning(
                f"🔓 …and with it the mark-tripwire stand-down stamped at/before the ban's "
                f"arming, for {cleared_trip} [confound B 2026-09-02] — the drawn ban is the "
                f"WHOLE re-entry delay. A tripwire fire AFTER re-entry stands the book down "
                f"exactly as before.")
        for slug in due:
            self._probe_notes.event(
                probe_notify.event_line, "ADVERSE RE-ADMIT", slug,
                "timed ban expired — quoting normally again"
                + (" (its same-episode mark-tripwire stand-down cleared with it)"
                   if slug in cleared_trip else ""))

    def _rederive_anchors(self) -> None:
        """⛔ THE ONE PLACE every downstream anchor is re-derived from the running config.

        Called after a hot file applies, and again beside every venue-inventory refresh. The
         hazard is a raise that moves one consumer and leaves the rest sized for the launch
        config — a guard halting on lawful inventory. What moves here, or nowhere:

          · `self.caps` / `self.cap` / `self.size` — the per-book reduce-only trigger and the
            documented-unreachable scalar fallbacks. `self.caps` also feeds the near-cap
            cap-weighted p̄, so a raise moves the WS near-cap warning line too, which is intended.
          · `self._launch_reach[book]` — the venue-inventory BREACH threshold and the parser's own
            per-book ceiling. It RISES on a rail-checked raise and is RELEASED back once nothing
            uses the raised band, floored at `_launch_reach0` so a hot LOWERING cannot tighten it
            (inventory acquired lawfully under the launch config stays lawful). ⛔ It used to be
            pure HIGH-WATER, which was a RAIL-FREE BAND: rails arm only on `reach_new > anchor`,
            so after raise→lower an unstamped file re-raised into the band with NO rail run, and
            `_venue_breach` kept the raised threshold for the rest of the process.
          · `self._launch_reach_unknown` — the bound on FOREIGN rows (the slate MINIMUM per-book
            reach since 2026-09-09), left frozen on purpose: our own raise says nothing about
            somebody else's position.
          · `self.max_total_contracts` — NOT re-derived: it is never hot.

        The last consumer is OUT OF PROCESS and cannot be re-derived from here at all — the
        per-book `poly_pos_guard` watchers hold the `--pause-threshold` they were started with, so
        every lift prints the operator the exact restart line.
        """
        self.caps = {slug: cap_contracts(sz, self.cap_fills_by_slug[slug])
                     for slug, sz in self.sizes.items()}
        self.cap = max(self.caps.values(), default=self.cap)
        self.size = max(self.sizes.values(), default=self.size)
        for slug, sz in self.sizes.items():
            live_reach = sz * (self.cap_fills_by_slug[slug] + 1)
            anchor = self._launch_reach.get(slug, live_reach)
            if live_reach > anchor:
                self._launch_reach[slug] = live_reach
                # `guard` is never None here: a reach past the anchor only got through the parser
                # via rail 2, which requires this book's threshold.
                guard = self._guard_thresholds[slug]
                margin = self._guard_margin[slug]
                log.warning(
                    f"🚨 STALE GUARD after a hot RAISE on {slug}: lawful reach {anchor} → "
                    f"{live_reach} (size {sz} × cap_fills {self.cap_fills_by_slug[slug]}+1). "
                    f"Its out-of-process watcher is still holding |net| > {guard}, the LAUNCH "
                    f"threshold — headroom above the new reach is now {guard - live_reach} "
                    f"contracts. The maker CANNOT restart it, and thresholds are start-only. "
                    f"⚠️ Under the DEFAULT fleet watcher (one process "
                    f"for the whole slate) do NOT start a per-book guard beside it — "
                    f"the fleet keeps its stale TIGHTER bound and would still trip pause.json "
                    f"on a lawful position, halting every book. Instead: kill the fleet pid, "
                    f"respawn it with EVERY book's current threshold, this one at "
                    f"{live_reach + margin} (its log names the full --book list). Only under "
                    f"--per-book-guards is the single-book restart correct: .venv/bin/python "
                    f"the per-book guard for {slug} at threshold "
                    f"{live_reach + margin} — see the operator runbook")
                continue
            self._maybe_release_anchor(slug, live_reach)

    def _release_anchors(self) -> None:
        """The RELEASE half of `_rederive_anchors`, on its own so the cheap, frequent seams
        (cycle top, venue refresh) can re-attempt a deferred release without recomputing
        `self.caps` every cycle."""
        for slug, sz in self.sizes.items():
            self._maybe_release_anchor(slug, sz * (self.cap_fills_by_slug[slug] + 1))

    def _defer_release(self, slug: str, anchor: int, why: str) -> None:
        """Say ONCE why a raised anchor is still wide. A rail that goes quiet is
        indistinguishable from a rail that is not there, and this one is checked every cycle — so
        it announces on the REASON changing, never per cycle."""
        if self._release_defer_announced.get(slug) == why:
            return
        self._release_defer_announced[slug] = why
        log.warning(f"reach anchor on {slug} stays at {anchor} (release deferred): {why}. The "
                    f"venue-breach halt keeps the wider bound until it clears.")

    def _maybe_release_anchor(self, slug: str, live_reach: int) -> None:
        """Let a RAISED reach anchor fall back once nothing is standing in the raised band.

        ⛔ THE CONDITION IS INVENTORY, NOT TIME. Releasing while real inventory sits above the
        lower band would turn a hot LOWERING into a false halt on a position that was lawful when
        acquired. So BOTH readings must already be inside the target: the BELIEF, and every VENUE
        row that maps to this book — and only when the venue read is CURRENT (a stale read is not
        evidence of flat, so the anchor stays wide; the wide bound is the safe side of blind).

        The floor is `_launch_reach0`, so this can only give back what a RAISE took.
        """
        floor = self._launch_reach0.get(slug, live_reach)
        target = max(floor, live_reach)
        anchor = self._launch_reach.get(slug, target)
        if target >= anchor:
            return
        # ⛔ NOT ON THE CYCLE THE LOWERING LANDS. From FLAT: raise 10→22 (anchor 44, a 22-lot
        # resting), hot lower to 10 → both inventory reads are 0, so a same-cycle release drops
        # the anchor to 20 → the 22-lot fills inside the cancel/replace window → the venue-breach
        # halt fires on a fill its own rails authorised. `reach = cap + size` exists to absorb
        # exactly that in-flight overshoot, so the release must wait for it to drain.
        # ⛔ THE INVARIANT IS "A REQUOTE HAS HAPPENED SINCE THE CHANGE", NOT "A CYCLE HAS PASSED":
        # `_resize_cycle` is re-stamped by every `_skip_market`, because a skipped cycle cancels
        # nothing and the raised-size order is still live while the counter advances.
        # ⚠️ Deliberately NOT a `self.resting` check — that map is a BELIEF and documented
        # stale-capable, so it is the weaker gate of the two.
        if self.cycle_index <= self._resize_cycle.get(slug, -1):
            self._defer_release(slug, anchor, "the book has not been REQUOTED at the new size "
                                              "yet (a change or a skipped cycle this cycle), "
                                              "so an order at the previous size may rest")
            return
        if not self._venue_read_is_current():
            self._defer_release(slug, anchor, "the venue read is STALE, and absence of "
                                              "evidence is not evidence of flat")
            return
        if self.inventory.get(slug, _ZERO).copy_abs() > target:
            self._defer_release(slug, anchor, f"belief {self.inventory.get(slug, _ZERO)} is "
                                              f"still inside the raised band")
            return
        key = norm_slug(slug)
        venue_worst = max((qty.copy_abs() for row, qty in self.venue_inventory.items()
                           if norm_slug(row) == key), default=_ZERO)
        if venue_worst > target:
            self._defer_release(slug, anchor, f"the venue still holds {venue_worst}")
            return
        self._release_defer_announced.pop(slug, None)
        self._launch_reach[slug] = target
        log.warning(
            f"🔻 reach anchor RELEASED on {slug}: {anchor} → {target} (live reach "
            f"{live_reach}, launch floor {floor}); venue {venue_worst} and belief "
            f"{self.inventory.get(slug, _ZERO)} are both inside it. The venue-breach halt "
            f"tightens back, and any FURTHER raise must re-clear every rail — a raised band "
            f"is authorisation for the raise that opened it, not standing permission.")

    def _hot_file(self) -> str:
        """THIS lane's hot-settings path — the per-lane override, else the shared default.

        Resolved on every call, never cached: the default reads live config, which the launcher
        and the suite both set after this object exists.
        """
        return self.hot_settings_file or _hot_settings_path()

    def _stamp_hot_ack(self, status: str, digest: str, why: str) -> None:
        """Record the hot_settings verdict the next beat will carry. Never raises.

        `hot_file` is stamped with every verdict, not once at startup: it is what tells a writer
        polling a SHARED heartbeat file which lane's file this verdict is about.
        """
        self._hot_ack = {"hot_hash": digest, "hot_status": status, "hot_refused_why": why,
                         "hot_file": self._hot_file()}

    def maybe_apply_hot_settings(self) -> None:
        """The M0.8 apply seam, called at cycle top beside the kill switch. No file = no
        overrides = today's behavior; file REMOVAL is not a revert (the running config
        persists — reverting would be a config change nobody wrote down).

        Change detection keys on `(st_mtime_ns, st_size)` — two writes in one mtime tick are
        invisible forever — and the announce-once cache keys on the same tuple, so an ignored
        file complains exactly once per edit. Whole-file-or-nothing lives in the parser."""
        import os as _os
        from bot.core.hot_settings import parse_hot_settings
        path = self._hot_file()
        try:
            st = _os.stat(path)
        except OSError:
            # ⛔ ABSENT, not "applied" and not "refused". File REMOVAL is documented as
            # not-a-revert, so the running config persists — but a writer polling the beat must
            # see there are no bytes to hold a verdict on, or it waits forever.
            self._stamp_hot_ack("absent", "", "")
            return
        sig = (st.st_mtime_ns, st.st_size)
        if sig == self._hot_sig:
            return
        self._hot_sig = sig
        try:
            with open(path) as fh:
                raw = fh.read()
        except OSError as exc:
            # The hash stays EMPTY: we never got the bytes, so there is nothing to name. A
            # writer reads this as refused-and-unidentified, which is the honest answer.
            self._stamp_hot_ack("refused", "", f"unreadable ({type(exc).__name__})")
            if sig != self._hot_announced:
                self._hot_announced = sig
                log.warning(f"⚠️ hot_settings UNREADABLE ({type(exc).__name__}) — running "
                            f"config unchanged")
            return
        # Hashed off the bytes ALREADY READ — never a second open. A re-read could hash a
        # different revision than the one that was parsed.
        digest = _hashlib.sha256(raw.encode("utf-8", "surrogateescape")).hexdigest()
        changes, why = parse_hot_settings(
            raw, slate_sizes=self.sizes, launch_cap_fills=self._launch_cap_fills,
            launch_reach=self._launch_reach, min_size=MIN_SIZE, run_id=self.run_id,
            # ──  v1.1: everything a RAISE is judged against ────────────────────
            # Each one is LIVE at the moment of the read, not a launch snapshot — a raise judged
            # on stale inputs is the whole hazard.
            running_cap_fills=self.cap_fills_by_slug,
            guard_thresholds=self._guard_thresholds or None,
            max_total_contracts=self.max_total_contracts,
            latched=set(self.adverse_latched),
            reduce_only=self._reduce_only_books(),
            # The engine's OWN answer, from the one implementation of the rail — never a headroom
            # this module re-derives. ⚠️ It performs FILE I/O, acceptable here because this seam
            # only runs when the hot file's (mtime, size) changed. Do not move it into the quote
            # loop.
            loss_cap_ok=self._loss_cap_breach() is None,
            # ⛔ THE DROPPED BOOKS [continuous maker P1]. Ever-quoted minus still-registered: a
            # level-triggered hot file still names them, and without this the FIRST drop of the
            # night would void every later hot_settings edit whole.
            retired=self._ever_slugs - set(self.sizes))
        if changes is None:
            self._stamp_hot_ack("refused", digest, why or "refused")
            if sig != self._hot_announced:
                self._hot_announced = sig
                log.warning(f"⚠️ hot_settings IGNORED ({why}) — running config unchanged, "
                            f"whole-file-or-nothing")
            return
        # ⛔ APPLIED is stamped HERE, before the per-key loop, and deliberately covers the "every
        # value already current" case: the file was legal and adopted whole. Gating this on
        # `applied` being non-empty would report a legal no-op file as unacknowledged.
        self._stamp_hot_ack("applied", digest, "")
        self._hot_announced = sig
        import csv as _csv
        applied = []
        for slug, spec in changes.items():
            for key, new in spec.items():
                if key == "clear_adverse_latch":
                    # ⛔ EDGE-TRIGGERED ON A DATED ACK, NOT ON FILE CONTENT. This file is
                    # LEVEL-triggered — stale entries are never pruned and the mtime gate only
                    # suppresses a STEADY file — so an undated ack left behind would be re-applied
                    # by the next unrelated edit and clear a latch that had since RE-FIRED. The
                    # ack must NAME the event: it clears only a latch OLDER than the
                    # acknowledgement. `_latch_adverse` re-stamps on every fire, the other half.
                    latched_at = self.adverse_latched.get(slug)
                    if latched_at is None:
                        continue                       # nothing to acknowledge — silent no-op
                    if new > time.time() + ACK_MAX_SKEW_S:
                        # ⛔ A FUTURE ACK IS A PERMANENT CLEAR. One mistyped year clears once and
                        # then keeps clearing: the stamp stays in the level-triggered file and
                        # every later re-fire is by construction older than it. Refused in the
                        # same direction as a stale one — the latch stays.
                        log.warning(
                            f"⚠️ hot_settings: FUTURE-DATED clear_adverse_latch on {slug} "
                            f"({_utc_iso(new)} is more than {ACK_MAX_SKEW_S:.0f}s ahead of now) "
                            f"— IGNORED, the book stays reduce-only. A future stamp would clear "
                            f"every FUTURE re-fire too; check the year. Current: "
                            f'"{_utc_iso(time.time() + 1)}".')
                        continue
                    if new <= latched_at:
                        log.warning(
                            f"⚠️ hot_settings: STALE clear_adverse_latch on {slug} "
                            f"({_utc_iso(new)} ≤ latched {_utc_iso(latched_at)}) — IGNORED, the "
                            f"book stays reduce-only. Write a current timestamp "
                            f'("{_utc_iso(time.time())}") if you mean to clear it now.')
                        continue
                    del self.adverse_latched[slug]
                    # The ack clears the LATCH, so its ban deadline goes with it — a deadline
                    # left behind would be an orphan describing a book that is already quoting
                    # [cycle-4 P1].
                    self.adverse_ban_until.pop(slug, None)
                    # Two maps, ONE record: the episode goes with its deadline, so the arming
                    # site's reuse guard never meets a dangling half.
                    self.adverse_ban_episode.pop(slug, None)
                    self._persist_adverse_latches()
                    log.warning(f"🔓 hot_settings: adverse latch CLEARED on {slug} — acked "
                                f"{_utc_iso(new)} > latched {_utc_iso(latched_at)}; the book "
                                f"quotes two-sided again")
                    applied.append((slug, key, f"latched@{_utc_iso(latched_at)}",
                                    _utc_iso(new)))
                    continue
                old = (self.sizes.get(slug) if key == "size"
                       else self.cap_fills_by_slug.get(slug) if key == "cap_fills"
                       else self.hot_quote.get(slug, "normal"))
                if old == new:
                    continue
                if key == "size":
                    self.sizes[slug] = new
                    self._resize_cycle[slug] = self.cycle_index
                elif key == "cap_fills":
                    self.cap_fills_by_slug[slug] = new
                    self._resize_cycle[slug] = self.cycle_index
                else:
                    # ⚠️ `quote` is a LANE setting and deliberately does NOT clear the adverse
                    # latch — a lane flip is not an acknowledgement of a specific loss event.
                    self.hot_quote[slug] = new
                applied.append((slug, key, old, new))
        if not applied:
            log.info("hot_settings: file changed but every value already current")
            return
        self._rederive_anchors()
        try:
            new_file = not _os.path.exists("logs/hot_settings_changes.csv")
            with open("logs/hot_settings_changes.csv", "a", newline="") as fh:
                w = _csv.writer(fh)
                if new_file:
                    w.writerow(["ts", "run_id", "slug", "key", "old", "new"])
                for slug, key, old, new in applied:
                    # ⛔ MILLISECOND precision, matching the fill/quote tapes: a whole-second ts
                    # made the segment boundary coarser than the rows it splits. Module-level
                    # clock, same source as every other tape.
                    w.writerow([f"{time.time():.3f}", self.run_id, slug, key, old, new])
        except OSError as exc:
            log.warning(f"hot_settings change-tape write FAILED ({exc!r}) — changes applied "
                        f"but UNRECORDED; the run scores [config-changed, unscored]")
        for slug, key, old, new in applied:
            # The REPLACE note is a SIZE-change fact only: a cap_fills or quote change leaves the
            # resting order untouched, and claiming otherwise teaches the wrong cost model.
            note = (" (a size change REPLACES the resting order next cycle — queue "
                    "seniority forfeited for that book, the operator's explicit trade)"
                    if key == "size" else "")
            log.warning(f"🔧 HOT {slug} {key}: {old} → {new}{note}")

    # ── hot slate [continuous maker P1] ──────────────────────────────────────────────────────

    def _slate_file(self) -> str:
        """THIS lane's hot-slate path — resolved on every call, like `_hot_file()`."""
        return self.hot_slate_file or _hot_slate_path()

    def _stamp_slate_ack(self, status: str, digest: str, *, retryable_reason: str = "") -> None:
        """The verdict the next beat carries. The WRITER waits on `slate_hash`: a reseater must
        be able to tell "my bytes are live" from "my bytes were voided" without reading this
        process's log, and neither the change tape nor the epoch alone can answer it — a
        byte-identical rewrite at the same epoch is `applied` and writes no row and moves no
        epoch, which is indistinguishable from a refusal by those two channels alone.

        ⛔ `slate_epoch` IS THE FILE'S OWN EPOCH once one has applied, never a count of applies: a
        refused file moves nothing, so a maker-side counter would drift from the reseater's
        journal at the first refusal. The beat also carries live `sizes` (`_beat`), which is what
        lets a writer whose file was refused on a size disagreement re-read the running seat
        rather than re-writing the same refusal all day."""
        self._slate_ack = {"slate_hash": digest, "slate_status": status,
                           "slate_epoch": str(self.slate_epoch)}
        if retryable_reason:
            self._slate_ack["slate_retryable_reason"] = retryable_reason

    def _tape_slate_change(self, action: str, slug: str, *, size: Any = "",
                           mode: str = "", requote_s: Any = "", qty: Any = "",
                           basis: Any = "") -> None:
        """ONE row per slate DECISION. Never raises — a tape failure must not stop trading, and
        the run scores [slate-changed, unscored] exactly as the hot_settings tape's does."""
        try:
            path = HOT_SLATE_CHANGES_CSV
            new_file = not os.path.exists(path)
            os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
            with open(path, "a", newline="") as fh:
                writer = csv.writer(fh)
                if new_file:
                    writer.writerow(_SLATE_CHANGES_HDR)
                # ⛔ MILLISECOND ts, matching the quote/fill tapes: this tape's rows are the
                # segment boundaries of those, and a whole-second stamp is coarser than the rows
                # it splits.
                writer.writerow([f"{time.time():.3f}", self.run_id, self.slate_epoch, slug,
                                 action, size, mode, requote_s, qty, basis])
        except OSError as exc:
            log.warning(f"hot_slate change-tape write FAILED ({exc!r}) — {action} {slug} APPLIED "
                        f"but UNRECORDED; the run scores [slate-changed, unscored]")

    def _refuse_slate(self, digest: str, why: str, sig: tuple, *, slug: str = "",
                       retryable_reason: str = "") -> None:
        """Void the WHOLE file: nothing applied, the epoch unmoved, one line per edit."""
        self._stamp_slate_ack("refused", digest, retryable_reason=retryable_reason)
        self._tape_slate_change("refused", slug)
        if sig != self._slate_announced:
            self._slate_announced = sig
            log.warning(f"⚠️ hot_slate IGNORED ({why}) — the running slate is unchanged, "
                        f"whole-file-or-nothing")

    async def _read_book_meta(self, slug: str) -> tuple[Optional[Decimal], Optional[Decimal]]:
        """(tick, min_qty) for ONE book in ONE request. Shared by `prepare()` and the hot-slate
        add, so a re-added book's tick and venue minimum are read exactly as a launch seat's are.

        ⛔ NEVER DEFAULT A TICK — `prepare()`'s docstring owns the reason; the caller drops or
        refuses the book on an unreadable one.
        """
        _meta = getattr(self.client, "get_market_meta", None)
        if _meta is not None:
            return await _meta(slug)
        return await self.client.get_market_tick(slug), None

    def _launch_entry(self, slug: str) -> dict:
        """The launch seat AS A SLATE ENTRY, so `prepare()` and a hot add run the SAME add table.

        ⛔ This is what makes "a re-add is byte-identical to a launch seat" checkable rather than
        asserted: every field of the add table is written from an entry, and the launch path's
        entry is derived from the argv-built maps here. A new add-table field that is not read out
        of an entry will be missing on exactly one of the two paths.
        """
        entry: dict = {"size": self.sizes[slug],
                       "mode": self.hot_quote.get(slug, "normal"),
                       "pinned": slug in self._slate_pinned,
                       "latch_off": slug in self.latch_off_slugs,
                       "clock_guard": slug in self.clock_guard_slugs,
                       "maintenance_guard": slug in self.maintenance_guard_slugs}
        if slug in self.resolves_at:
            entry["resolves_at"] = self.resolves_at[slug]
        if slug in self.park:
            entry["park"] = self.park[slug]
        return entry

    async def _admit_book(self, slug: str, entry: Mapping[str, Any], tick: Decimal,
                          min_qty: Optional[Decimal], *, launch: bool = False) -> None:
        """THE ADD TABLE — register ONE book as a quoted seat. Shared by `prepare()` and the hot
        slate; every slate-keyed structure a launch seat gets is written HERE and nowhere else.

        `launch=True` is `prepare()`'s call: the constructor already built this book's sizes,
        caps and anchors from argv (so the writes below are idempotent), the shim owns the WS
        prime and `begin_run` owns the durable ticker list — and `max_total_contracts` must NOT be
        re-derived, because an operator-tightened global cap is an argv decision.

        ⛔ THE HOT PATH DOES NOT PUT THE BOOK IN `self.ticks`. It STAGES it: `quote_slugs` is
        `sorted(self.ticks)`, so a straight insert would have cycle 1 of the add fresh-REST an
        unframed book — and `WS_STALE_REREAD_MAX`+ unframed books in one cycle books a partial
        feed death on a socket that is perfectly healthy. `_promote_pending_books` graduates it.
        """
        size = int(entry["size"])
        # A RE-STAGE of a still-registered seat (the WS-reject refusal, re-listed before the drop
        # loop released it) already counts its cap in `max_total_contracts`; the bottom of this
        # method adds the DELTA so the invariant stays `launch + Σadds − Σdrops`.
        prev_cap = self.caps.get(slug, 0) if slug in self.sizes else 0
        cap_fills = (self.cap_fills_by_slug[slug] if launch else self._slate_cap_fills)
        if slug not in self.slugs:
            self.slugs.append(slug)
        self._slate_norm[norm_slug(slug)] = slug
        self.sizes[slug] = size
        self.cap_fills_by_slug[slug] = cap_fills
        self.caps[slug] = cap_contracts(size, cap_fills)
        # ⛔ `setdefault`, never a write: the launch anchors are the INTERLOCK references the
        # external per-book guards were sized against, and a re-add may not move them. The
        # pre-flight refuses an add whose reach would exceed an anchor that already exists.
        self._launch_cap_fills.setdefault(slug, cap_fills)
        self._launch_reach.setdefault(slug, size * (cap_fills + 1))
        self._launch_reach0.setdefault(slug, size * (cap_fills + 1))
        # The reach anchor may not be RELEASED until a LATER cycle has run — same rule as a hot
        # resize, for the same reason (an order placed under the old config can still be resting).
        self._resize_cycle[slug] = self.cycle_index
        mode = str(entry.get("mode", "normal"))
        if mode == "normal":
            self.hot_quote.pop(slug, None)
        else:
            self.hot_quote[slug] = mode
        for flag, target in (("latch_off", self.latch_off_slugs),
                             ("clock_guard", self.clock_guard_slugs),
                             ("maintenance_guard", self.maintenance_guard_slugs)):
            if entry.get(flag):
                target.add(slug)
            else:
                target.discard(slug)
        if entry.get("pinned"):
            self._slate_pinned.add(slug)
        else:
            self._slate_pinned.discard(slug)
        if entry.get("resolves_at") is not None:
            self.resolves_at[slug] = float(entry["resolves_at"])
        if entry.get("park") is not None:
            side, ticks_off = entry["park"]
            self.park[slug] = (str(side), int(ticks_off))
        self._ever_slugs.add(slug)
        # A re-add UN-RETIRES: the book is on the slate again, so the sixth reduce-only term
        # lapses. Its durable latch, ban and cooldown are untouched — a drop never cleared them.
        self._slate_retiring.discard(slug)
        self._record_min_trade_qty(slug, min_qty)
        if launch:
            self.ticks[slug] = tick
            return
        self._slate_pending[slug] = (tick, min_qty, time.time())
        # ⛔ `+= this book's cap`, NOT `= Σ caps`: on the default (non-binding) cap the two are
        # identical, but an operator who TIGHTENED `--max-total-contracts` made a risk decision a
        # slate add may not undo — re-deriving the sum would hand the tightened rail back.
        self.max_total_contracts += self.caps[slug] - prev_cap
        # NEVER LOWERED: the parked-verify bound is a request-leak rail, and a wider slate can
        # only need more room.
        self.max_parked = max(self.max_parked,
                              2 * max(1, len(self.slugs))
                              * int(LAG_HORIZON_S / max(1.0, self.requote_s)))
        self._rederive_anchors()
        if self.state is not None and self.real:
            # The recovery path reads `tickers` to know whose position to look for; a book added
            # mid-run and absent from it is a position no relaunch would go looking for.
            self.state.set_tickers(self.ticks | self._slate_pending)
        if self.book_feed is not None:
            try:
                self.book_feed.prime(slug, 1.0)     # 1.0 sentinel, the shim's own launch value
            except Exception as exc:                # defensive — the REST seam still serves
                log.warning(f"hot_slate: WS prime for {slug} failed "
                            f"({safe_exc(exc)}) — the book is staged and will serve from REST")
        # ⛔ NO `resubscribe()` HERE [2026-09-12 live]: the venue caps subscribe REQUESTS per
        # connection (~10 observed), not just slugs, so one request per added slug spent the
        # socket's whole budget by mid-evening and every later reseat add was REFUSED. The
        # caller PRIMES every add first and resubscribes ONCE — one request per apply.

    def _promote_pending_books(self) -> None:
        """Graduate staged admits into `self.ticks` — the moment they become QUOTED.

        A book graduates when the WS feed has framed it, or when `WS_SEED_TIMEOUT_S` has passed
        since the admit (the same bound `_seed_ws_cache` waits at launch). ⛔ At most
        `WS_STALE_REREAD_MAX` UNFRAMED books graduate per cycle: each one costs a fresh-REST
        backstop read on its first cycle, and the cap is exactly where the C-a escalation books a
        partial feed death.
        """
        if not self._slate_pending:
            return
        now = time.time()
        unframed = 0
        for slug, (tick, _min_qty, admitted) in sorted(self._slate_pending.items()):
            framed = False
            if self.book_feed is not None:
                try:
                    framed = self.book_feed.get_book_md(slug) is not None
                except Exception:
                    framed = False
                rejected = getattr(self.book_feed, "subscribe_rejected", set())
                if framed:
                    # A later resubscribe can be ACCEPTED for a slug the reject set still names
                    # (the set clears only on reconnect): a framed book is a watched book.
                    rejected.discard(slug)
                elif slug in rejected:
                    # ⛔ REFUSED, never graduated [2026-09-11 live]: the venue rejected the
                    # book's subscription ('max subscriptions per connection reached' — the
                    # socket's subscribe budget is spent), so it will not frame.
                    # Graduating it to REST made it stale past the re-read cap every cycle,
                    # and the partial-freeze rail halted a healthy maker over it. Nothing was
                    # quoted, so retiring it is the whole unwind: the drop path releases the
                    # registration once the venue read confirms flat. The next reseat may
                    # re-add it; it is refused again unless the socket has framed it by then.
                    self._slate_pending.pop(slug)
                    self._slate_retiring.add(slug)
                    log.error(f"⛔ hot_slate ADD REFUSED {slug}: the venue rejected its WS "
                              f"subscription (the socket's subscribe budget is spent; the feed "
                              f"reconnects once per {RECONNECT_ON_REJECT_MIN_S:.0f}s "
                              f"and the next reseat re-adds it); not quoting a book the feed "
                              f"cannot frame. Retired, dropped once the venue read confirms "
                              f"flat.")
                    self._tape_slate_change("refused", slug, size=self.sizes.get(slug, ""),
                                            mode=self.hot_quote.get(slug, "normal"))
                    continue
            elif self.book_source != "ws":
                framed = True            # no feed to wait for; REST serves from cycle one
            if not framed:
                if now - admitted < self.ws_seed_timeout_s:
                    continue
                if unframed >= WS_STALE_REREAD_MAX:
                    continue
                unframed += 1
            self._slate_pending.pop(slug)
            self.ticks[slug] = tick
            log.warning(f"➕ hot_slate ADD LIVE {slug}: size {self.sizes[slug]} cap "
                        f"{self.caps[slug]} mode {self.hot_quote.get(slug, 'normal')} "
                        f"(epoch {self.slate_epoch}, "
                        f"{'WS framed' if framed else 'seed bound lapsed — first cycle RESTs'})")
            self._tape_slate_change("add", slug, size=self.sizes[slug],
                                    mode=self.hot_quote.get(slug, "normal"))

    async def maybe_apply_hot_slate(self, stats: Optional[CycleStats] = None) -> None:
        """The slate seam, one cycle above `maybe_apply_hot_settings` (which reads `self.sizes`,
        so an add must be registered before the parser judges the file against it).

        Whole-file-or-nothing, and EVERY refusal is decided in the pre-flight, before the first
        mutation. Change detection and announce-once key on `(st_mtime_ns, st_size)`, hot_settings'
        rule verbatim.
        """
        # Unconditional: a staged admit graduates on its own clock, not on a file edit.
        self._promote_pending_books()
        path = self._slate_file()
        try:
            st = os.stat(path)
        except OSError:
            # ⛔ ABSENT, not refused. No file = no hot slate = the launch slate, forever; file
            # REMOVAL is not a retire-all (that would be a book-set change nobody wrote down).
            self._stamp_slate_ack("absent", "")
            return
        sig = (st.st_mtime_ns, st.st_size)
        if sig == self._slate_sig:
            return
        self._slate_sig = sig
        try:
            with open(path) as fh:
                raw = fh.read()
        except OSError as exc:
            self._refuse_slate("", f"unreadable ({type(exc).__name__})", sig)
            return
        # Hashed off the bytes ALREADY READ — a second open could hash a different revision than
        # the one that was parsed.
        digest = _hashlib.sha256(raw.encode("utf-8", "surrogateescape")).hexdigest()
        slate, why = parse_hot_slate(
            raw, run_id=self.run_id, min_size=MIN_SIZE,
            # ⛔ THE WS-GRACE PRODUCT RAIL ONLY APPLIES ON THE WS BOOK PATH, exactly as the
            # launcher's own refusal does — on the REST path there is no near-cap grace to
            # shorten, and a bound that refuses a lawful cadence there is a bound nobody armed.
            requote_max_s=(WS_NEARCAP_GRACE_S / 3.0 if self.book_source == "ws"
                           else float("inf")))
        if slate is None:
            self._refuse_slate(digest, why or "refused", sig)
            return
        # ── THE EPOCH IS THE FILE'S ──────────────────────────
        file_epoch = int(slate["epoch"])
        if file_epoch < self.slate_epoch:
            self._refuse_slate(digest, f"epoch {file_epoch} is BEHIND the running "
                               f"{self.slate_epoch} — a generation never goes backwards, and an "
                               f"older file replayed over a newer slate is a stale reseat", sig)
            return
        if file_epoch == self.slate_epoch:
            if slate == self._slate_applied:
                # ⛔ A BYTE-IDENTICAL REWRITE IS A NO-OP, not an apply: the mtime gate cannot see
                # it (a `cp` gives the same bytes a new mtime), and bumping here would stamp the
                # tapes with a generation whose book set never changed.
                self._stamp_slate_ack("applied", digest)
                self._slate_announced = sig
                log.info(f"hot_slate: file rewritten with no change at epoch {file_epoch} — "
                         f"no-op")
                return
            self._refuse_slate(digest, f"epoch {file_epoch} is unchanged but the CONTENT is not "
                               f"— a new book set must carry a new epoch, or its rows are "
                               f"indistinguishable from the previous generation's", sig)
            return
        apply_deadline = slate.get("apply_deadline_ts", float("inf"))
        if time.time() >= apply_deadline:
            self._refuse_slate(digest, "apply_deadline_ts expired", sig)
            return
        books: dict[str, dict] = slate["books"]
        # ⛔ A RE-LISTED book that is RETIRING and was NEVER QUOTED is an ADD, not a keep [MPR
        # 2026-09-12]: the WS-reject refusal retires the seat but leaves it in `self.sizes` until
        # the drop loop clears it (which needs a current venue read), so a reseat inside that
        # window took the keep branch — un-retired, never primed, never staged, never quoted,
        # never dropped, its cap still inside `max_total_contracts`. A DEAD SEAT holding cap room.
        adds = [s for s in books
                if s not in self.sizes
                or (s in self._slate_retiring and s not in self.ticks)]
        # ── PRE-FLIGHT. Every refusal below happens before anything mutates ─────────────────
        new_requote = slate["requote_s"]
        if (new_requote is not None and new_requote != self.requote_s
                and self.requote_ab_armed):
            # ⛔ requote_ab_armed [mm-review 2026-09-10]. The cadence A/B's reader keys its arm on
            # the RUN (the supervisor journal's `requote_s_arm`), so a mid-run change re-labels
            # the WHOLE day with the launch arm and pools two cadences into one arm — a silent
            # contamination of a registered trial. Refused whole until the reader is re-keyed on
            # `epoch` (P4).
            self._refuse_slate(digest, f"requote_ab_armed: this run was launched inside the "
                               f"cadence A/B, so its cadence is the trial's ARM — changing it "
                               f"from {self.requote_s:g}s to {new_requote:g}s would re-label the "
                               f"whole day with the launch arm", sig)
            return
        for slug in adds:
            owner = self._slate_norm.get(norm_slug(slug))
            if owner is not None and owner != slug:
                self._refuse_slate(digest, f"{slug!r} collides with slate book {owner!r} under "
                                   f"normalization — the venue-inventory guard's membership test "
                                   f"cannot tell them apart", sig, slug=slug)
                return
            anchor = self._launch_reach.get(slug)
            reach = books[slug]["size"] * (self._slate_cap_fills + 1)
            if anchor is not None and reach > anchor:
                self._refuse_slate(digest, f"{slug}: re-add at size {books[slug]['size']} reaches "
                                   f"{reach}, past the launch anchor {anchor} its external guard "
                                   f"was sized for — removal-direction only", sig, slug=slug)
                return
        for slug, entry in books.items():
            if slug in adds:
                continue
            # ⛔ A KEEP IS NOT A RESIZE SEAM. `size` and `mode` on a live book belong to
            # hot_settings, which judges a raise against the real external guards; a slate that
            # disagrees is a writer collision, not an instruction.
            if entry["size"] != self.sizes[slug]:
                self._refuse_slate(digest, f"{slug}: slate size {entry['size']} disagrees with "
                                   f"the live seat {self.sizes[slug]} — a live book's size is "
                                   f"hot_settings' lever, the slate carries it forward", sig,
                                   slug=slug)
                return
            if entry["mode"] != self.hot_quote.get(slug, "normal"):
                self._refuse_slate(digest, f"{slug}: slate mode {entry['mode']!r} disagrees with "
                                   f"the live {self.hot_quote.get(slug, 'normal')!r} — a live "
                                   f"book's quote mode is hot_settings' lever", sig, slug=slug)
                return
        # ⛔ VENUE ADOPTION ON ADD. A book we are about to quote may already be HELD — a prior
        # epoch's residue, or an out-of-process close that never booked. One forced read, and a
        # quantity is adopted ONLY when the lane's own durable record holds exactly it: anything
        # else is a position of unknown provenance, and quoting a book whose true position we are
        # guessing at is what the reach rails exist to prevent.
        adopt: dict[str, tuple[Decimal, Decimal]] = {}
        if adds and self.real:
            # ⛔ THE BOOL IS THE POINT.
            # `_refresh_venue_inventory` KEEPS the previous answer on a failure, so a missing row
            # after a failed read is CANNOT-VERIFY, not `[]` — seating a book on it is exactly the
            # `reconcile.py` fiction. ⚠️ The bool ALONE: a read that just succeeded is current by
            # construction, and an `_venue_read_is_current()` conjunct here would be dead code
            # dressed as a second rail [round-2 review].
            if not await self._refresh_venue_inventory():
                self._refuse_slate(
                    digest, f"venue_unverified: the position read for the added book(s) "
                    f"{sorted(adds)} did not succeed — an absent row is then CANNOT-VERIFY, "
                    f"never flat, and no book is seated on a guess",
                    sig, slug=sorted(adds)[0])
                return
            record: dict = {}
            snapshot = None
            if self.state is not None:
                try:
                    snapshot = self.state.snapshot()
                    record = dict(snapshot.inventory)
                except Exception as exc:            # a corrupt record adopts NOTHING
                    log.warning(f"hot_slate: durable record unreadable ({safe_exc(exc)}) — no "
                                f"add may adopt a venue position this cycle")
            for slug in adds:
                held = self.venue_inventory.get(slug, _ZERO)
                if held == _ZERO and slug not in self.venue_stale_rows:
                    continue
                recorded = record.get(slug)
                bound = books[slug]["size"] * (self._slate_cap_fills + 1)
                if (slug in self.venue_stale_rows or recorded is None
                        or Decimal(str(recorded)) != held or abs(held) > bound):
                    self._refuse_slate(
                        digest, f"{slug}: the venue holds {held} on a book this slate ADDS, and "
                        f"the lane's durable record says {recorded!r} (bound {bound}) — refusing "
                        f"the whole file rather than quoting a book whose position is a guess",
                        sig, slug=slug)
                    return
                # ⛔ AND IT NEEDS A COST BASIS [mm-review 2026-09-10]. Seated with `avg_entry`
                # unset, the FIRST reducing fill books its GROSS PROCEEDS as realized into the
                # DURABLE loss cap (a large gain against a true small loss), and
                # the mark tripwire prices the book against 0. The basis is REPLAYED through the
                # one expression the launch carry uses — `carried_basis_run` names the run whose
                # fills hold it, `replay_run_fills` walks them through the shipped
                # `fill_accounting` under the derive phase's whole guard set. No usable basis ⇒
                # refuse the file: a fabricated basis is a fabricated realized P&L.
                basis = None
                if snapshot is not None:
                    basis_run, basis_why = venue_close.carried_basis_run(
                        snapshot, slug, held, fills_paths=fold_paths(self._fill_path))
                    if basis_run:
                        try:
                            basis, basis_why = venue_close.replayed_basis(
                                basis_run, slug, held,
                                fills_paths=fold_paths(self._fill_path))
                        except Exception as exc:
                            basis, basis_why = None, f"replay raised ({safe_exc(exc)})"
                else:
                    basis_why = "no durable record on this run"
                if basis is None:
                    self._refuse_slate(
                        digest, f"{slug}: the venue and the record agree on {held}, but NO cost "
                        f"basis could be replayed for it ({basis_why}) — refusing the whole file "
                        f"rather than seating a position whose first reducing fill would book "
                        f"gross proceeds into the durable cap", sig, slug=slug)
                    return
                adopt[slug] = (held, basis)
        # ── APPLY ──────────────────────────────────────────────────────────────────────────
        metas: dict[str, tuple[Decimal, Optional[Decimal]]] = {}
        for slug in adds:
            self._extra_requests += 1
            try:
                tick, min_qty = await self._read_book_meta(slug)
            except Exception as exc:
                self._refuse_slate(digest, f"{slug}: market meta read failed "
                                   f"({safe_exc(exc)})", sig, slug=slug,
                                   retryable_reason=SLATE_RETRYABLE_METADATA)
                return
            if tick is None or tick <= _ZERO:
                self._refuse_slate(digest, f"{slug}: tick unreadable ({tick!r}) — a defaulted "
                                   f"tick misprices silently", sig, slug=slug,
                                   retryable_reason=SLATE_RETRYABLE_METADATA if tick is None else "")
                return
            metas[slug] = (tick, min_qty)
        # Metadata awaits may outlive or supersede the original snapshot. Revalidate before
        # changing the epoch, inventory, caps or retiring set; never restore stale bytes.
        try:
            with open(path, "rb") as fh:
                current = os.fstat(fh.fileno())
                unchanged = (os.path.samestat(st, current)
                             and _hashlib.sha256(fh.read()).hexdigest() == digest
                             and os.path.samestat(current, os.stat(path)))
        except OSError:
            unchanged = False
        if not unchanged:
            self._refuse_slate(digest, "slate changed during preflight", sig)
            return
        if time.time() >= apply_deadline:
            self._refuse_slate(digest, "apply_deadline_ts expired during preflight", sig)
            return
        was_epoch, self.slate_epoch = self.slate_epoch, file_epoch
        self._slate_applied = slate
        self._slate_announced = sig
        changed = False
        for slug in adds:
            if slug in adopt:
                held, basis = adopt[slug]
                self.inventory[slug] = held
                self.avg_entry[slug] = basis
                # ⛔ THE BASIS IS NOT THIS RUN'S OWN FILLS, so the teardown cross must not price
                # against it and the settled carve-out stamps `basis_source=reset` — the same
                # treatment a launch carry gets.
                self.basis_carried.add(slug)
                log.warning(f"🧾 hot_slate ADOPTED {held} on {slug} @ basis {basis} — the venue "
                            f"and this lane's durable record agree exactly, and the basis was "
                            f"REPLAYED from the run that traded it; the book is seated holding "
                            f"it (basis CARRIED, not this run's)")
                self._tape_slate_change("adopt", slug, size=books[slug]["size"],
                                        qty=str(held), basis=str(basis))
                changed = True
            await self._admit_book(slug, books[slug], *metas[slug])
            changed = True
        for slug, entry in books.items():
            if slug not in adds:
                # ⛔ THE SLATE OWNS EVERY PER-BOOK FLAG ON A KEEP TOO
                # 2026-09-10]. Applying them at ADD only made the slate the source of truth for
                # exactly one cycle of a book's life: a reseater that turned a clock guard ON for
                # a live seat would be silently ignored, which is worse than either answer. Size
                # and mode still refuse on a mismatch — those are hot_settings' levers.
                for flag, target in (("latch_off", self.latch_off_slugs),
                                     ("clock_guard", self.clock_guard_slugs),
                                     ("maintenance_guard", self.maintenance_guard_slugs),
                                     ("pinned", self._slate_pinned)):
                    if entry.get(flag):
                        if slug not in target:
                            changed = True
                        target.add(slug)
                    else:
                        if slug in target:
                            changed = True
                        target.discard(slug)
                if entry.get("resolves_at") is not None:
                    self.resolves_at[slug] = float(entry["resolves_at"])
                else:
                    self.resolves_at.pop(slug, None)
                if entry.get("park") is not None:
                    side, ticks_off = entry["park"]
                    self.park[slug] = (str(side), int(ticks_off))
                else:
                    self.park.pop(slug, None)
                if slug in self._slate_retiring:
                    changed = True
                self._slate_retiring.discard(slug)
        # ── RETIRE. Absent from `books` ⇒ reduce-only until flat, then dropped. NEVER a cancel:
        # a retiring book needs the queue seniority it already has to work its inventory down.
        for slug in sorted(self.sizes):
            if slug in books or slug in self._slate_retiring:
                continue
            if slug in self._slate_pinned:
                continue                 # ⛔ a pinned seat is never retired by a slate diff
            self._slate_retiring.add(slug)
            changed = True
            log.warning(f"➖ hot_slate RETIRE {slug}: reduce-only from now (inventory "
                        f"{self.inventory.get(slug, _ZERO)}); it is DROPPED once flat, the venue "
                        f"agrees and nothing of ours is resting. No cancel — the resting order "
                        f"keeps its queue position to work the position down.")
            self._tape_slate_change("retire", slug, size=self.sizes.get(slug, ""),
                                    mode=self.hot_quote.get(slug, "normal"))
        # ── CADENCE (the A/B interlock already refused this in the pre-flight) ─────────────
        if new_requote is not None and new_requote != self.requote_s:
            old = self.requote_s
            changed = True
            self.requote_s = new_requote
            self.max_parked = max(self.max_parked,
                                  2 * max(1, len(self.slugs))
                                  * int(LAG_HORIZON_S / max(1.0, self.requote_s)))
            if self.heartbeat is not None:
                # The declared cadence moves; the STALE budget is never lowered — it covers a
                # teardown that legitimately blocks, and the launcher already sized it for the
                # slowest registered arm.
                self.heartbeat.interval_s = float(self.requote_s)
                self.heartbeat.stale_after_s = max(
                    self.heartbeat.stale_after_s,
                    heartbeat_stale_after_s(self.requote_s, self.flatten_wait_s))
            log.warning(f"🔧 hot_slate REQUOTE {old:g}s → {self.requote_s:g}s (epoch "
                        f"{self.slate_epoch})")
            self._tape_slate_change("requote", "", requote_s=f"{self.requote_s:g}")
        if not changed:
            # ⛔ AN EPOCH ADVANCE THAT CHANGED NOTHING ELSE STILL GETS A ROW: the tapes are
            # already stamped with the new generation, so a reader folding this tape must be able
            # to date the boundary. `add` rows are written when a staged book goes LIVE, which is
            # a LATER cycle — so this row is not "nothing happened", it is "the generation
            # advanced here".
            self._tape_slate_change("epoch", "")
            log.info(f"hot_slate: epoch {was_epoch} → {file_epoch} with no book-set or cadence "
                     f"change (staged adds, if any, tape when they go live)")
        # ⛔ RESET THE hot_settings SIGNATURE. That reader is mtime-gated, and a book added this
        # cycle makes an UNCHANGED hot_settings file mean something different — its entries for
        # the new slug were slate ADDITIONS (a whole-file refusal) and are now lawful.
        self._hot_sig = None

        self._stamp_slate_ack("applied", digest)
        if stats is not None:
            self._beat(stats, slate_applied=True)
        if adds and self.book_feed is not None:
            # Publish the complete commit before this transport await can delay its receipt.
            await self.book_feed.resubscribe()   # one request; swallows transport errors

    def _drop_retired_books(self) -> None:
        """Retire → reduce-only → DROP. The drop is the only step that removes a book from the
        quote set, and it is conditioned on FOUR things at once: our belief is flat, nothing of
        ours is resting or pending on it, the venue read is CURRENT, and the venue says flat.

        ⛔ THE VENUE CLAUSE IS NOT REDUNDANT. Dropping on belief alone abandons exactly the
        position class this run cannot see — an out-of-process close that never booked, a fill the
        feed dropped — and a dropped book is off `slugs`, so no later poll, orphan scan, breach
        check or teardown flatten would ever look at it again. On a non-real run there is no venue
        to read and nothing to hold, so belief is the whole truth there.
        """
        if not self._slate_retiring:
            return
        for slug in sorted(self._slate_retiring):
            if self.inventory.get(slug, _ZERO) != _ZERO:
                continue
            if any(key[0] == slug for key in self.resting):
                continue
            if any(entry[0].slug == slug for entry in self.pending_reconcile.values()):
                continue
            if any(entry.order.slug == slug
                   for entry in self.pending_activity_recovery.values()):
                continue
            if self.real:
                if not self._venue_read_is_current():
                    continue
                if slug in self.venue_stale_rows:
                    continue
                if self.venue_inventory.get(slug, _ZERO) != _ZERO:
                    continue
            self._slate_retiring.discard(slug)
            self.slugs = [s for s in self.slugs if s != slug]
            self._slate_norm.pop(norm_slug(slug), None)
            self.ticks.pop(slug, None)
            self._slate_pending.pop(slug, None)
            self.sizes.pop(slug, None)
            # ⛔ GIVE THE GLOBAL CAP'S ROOM BACK. `_admit_book`
            # adds this book's cap to `max_total_contracts`; without the matching subtraction
            # slate CHURN ratchets the gross rail upward for the rest of the day (add/drop the
            # same book ten times and it is ten caps wider), and every consumer of it — the
            # global sides gate, the WS near-cap grace, the notional trip — loosens with it.
            # ⛔ THE INVARIANT: `max_total == launch + Σ(hot-add caps) − Σ(dropped caps)`. Taken
            # BEFORE the cap is popped, and floored at 1, NOT 0 — at 0 the rail reads as OFF
            # (`ws_near_cap` False, `global_sides_allowed` (True, True), the near-cap trip None),
            # so an arithmetic slip would DISABLE the gross cap rather than tighten it.
            self.max_total_contracts = max(1, self.max_total_contracts
                                           - self.caps.get(slug, 0))
            self.caps.pop(slug, None)
            self.cap_fills_by_slug.pop(slug, None)
            self.hot_quote.pop(slug, None)
            self._slate_pinned.discard(slug)
            self.park.pop(slug, None)
            self.inventory.pop(slug, None)
            # ⛔ SLATE-SCOPED ONLY. `adverse_latched`, `adverse_ban_until`, `cooldown_until`,
            # `mark_trips`, `_clock_guard_post_latched`, `avg_entry` (the basis) and
            # `_launch_reach*` all SURVIVE: a re-add of this book must inherit the ban it earned
            # and be judged against the anchor its external guard was sized for.
            for name in _SLATE_SCOPED_MAPS:
                store = getattr(self, name, None)
                if not isinstance(store, dict):
                    continue
                for key in [k for k in store
                            if k == slug or (isinstance(k, tuple) and k and k[0] == slug)]:
                    store.pop(key, None)
            log.warning(f"🗑️ hot_slate DROP {slug}: flat on belief and on the venue, nothing "
                        f"resting or pending — off the quote set at epoch {self.slate_epoch}. "
                        f"Its latch, ban and cooldown survive for a re-add.")
            self._tape_slate_change("drop", slug)
            if self.book_feed is not None:
                try:
                    self.book_feed.forget(slug)     # or every resubscribe re-sends it
                except Exception as exc:            # defensive — a feed seam never stops a drop
                    log.warning(f"hot_slate DROP {slug}: feed forget failed ({safe_exc(exc)})")
        if self.state is not None and self.real:
            self.state.set_tickers(self.ticks | self._slate_pending)

    # ── one cycle ────────────────────────────────────────────────────────────────────────────

    async def _refresh_venue_inventory(self) -> bool:
        """Ask the VENUE what we hold. True iff a read SUCCEEDED and replaced the answer.

        Failure keeps the previous answer: this number only ever RESTRICTS, so a stale
        restriction costs fills while a cleared one would restore exactly the blindness this
        exists to remove. ⛔ The bool matters — returning None either way left a caller unable to
        tell a confirmed re-read from two failed ones, and the halt claimed "(confirmed by a fresh
        read)" over a 503."""
        fresh: dict[str, Decimal] = {}
        stale_rows: set[str] = set()

        def _keep_previous(slug: str, why: str) -> None:
            # ⛔ PER-SLUG, not per-read: one junk row must not fail the whole refresh. The slug
            # keeps its previous value — never read as flat — and is flagged so the flatten
            # treats it as uncorroborated.
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
                # ⛔ `netPositionDecimal` ONLY — the venue's EXACT holding beside the ROUNDED
                # display `netPosition`. The breach threshold compares this map, and a rounded
                # read mis-sizes it by up to half a contract per book. No `qtyAvailable` fallback
                # (sign convention never verified) and no rounded-field fallback: missing = keep
                # previous.
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
            # Announced ONCE per refresh with its age, not per-slug per-cycle (which buried the
            # signal).
            log.warning(f"venue inventory was {time.time() - self.venue_inventory_ts:.0f}s stale "
                        f"before this read — restrictions were running on old evidence")
        self.venue_inventory = fresh
        self.venue_stale_rows = stale_rows
        self.venue_inventory_ts = time.time()
        # ⛔ BELIEF AS OF THIS READ — the discriminator the divergence rail keys on. The map
        # refreshes every RECONCILE_EVERY_CYCLES; belief moves on EVERY booked fill, so "map is
        # current AND slug absent AND belief non-zero" is produced routinely and benignly by the
        # first fill into any flat book. Comparing against belief *at read time* asks the only
        # question that means anything: when the venue last spoke, did it disagree with us?
        self.venue_belief_at_read = dict(self.inventory)
        # ⛔ ANNOUNCE EVERY SUCCESS. A rail that logs only on failure is indistinguishable from a
        # rail that never ran — and this one's entire parse was mutable to `{}` without a single
        # test noticing. Cycle 1 of a live run must tell the operator whether the rail exists.
        held = {s: str(q) for s, q in fresh.items() if q != _ZERO}
        # ⛔ WARNING, not INFO. This module has no handler of its own and inherits root's WARNING,
        # so an INFO announce is dropped entirely in production and the rail stays as unobservable
        # as the review said it was. This is a safety announcement.
        log.warning(f"venue inventory ({pages} page(s)): {held or 'flat'}")
        # ⛔ The reach anchor's RELEASE rests on a current venue read, so this is where the
        # evidence for it arrives: after a raise → flatten → lower, the breach threshold tightens
        # back at this refresh instead of staying wide for the rest of the process.
        self._release_anchors()
        return True

    async def _periodic_venue_verify(self) -> None:
        """Re-run the LAUNCH-TIME verify over the WHOLE slate, every `VERIFY_S` [P1b].

        The hourly-window maker started each window by asking the venue what it held and what was
        resting, and refusing to start on a disagreement. A day-long child never restarts, so the
        same two reads go on a clock: positions through `bot.poly_us.venue_positions._verify_positions`
        (the launcher's own reader — imported, never re-implemented, so `netPositionDecimal`-only
        parsing, pagination-to-the-end and None-is-cannot-verify are one behaviour) and the
        account-wide open-orders listing, filtered client-side to our own slugs.

        ⛔ THE TWO OUTCOMES ARE NOT THE SAME OUTCOME. A MISMATCH is evidence and stands the book
        down (`_verify_reduce_only`, cleared only by the next verify that reads it clean); an
        UNREADABLE venue is CANNOT-VERIFY and changes NOTHING — no stand-down, no clear, one line.
        Standing books down on an unreadable venue would hand every rate-limit episode a
        slate-wide halt to quoting, and clearing on one would erase a real disagreement.

        ⛔ AND NO SINGLE READ STANDS A BOOK DOWN — `VERIFY_CONFIRM_READS` consecutive reads must
        say the same thing, the second one on the NEXT CYCLE, not the next hour
       . Same discipline as `_venue_breach`'s re-read before a
        halt, and for the same measured reason: this endpoint serves divergent replicas (belief
        and the venue often disagree), and the ORDER listing lags a reprice by up to
        `LAG_HORIZON_S`. A one-read rail would hold a healthy book reduce-only for a whole hour
        on an ordinary requote.

        ⛔ ORDERS ARE COMPARED BY ID against everything the maker KNOWS ABOUT — `resting`,
        `pending_reconcile` (a cancel issued but not terminal) and `parked_alive` (sighted alive
        after its cancel echoed). A count comparison read every reprice as a mismatch: the old
        order is listed for up to `LAG_HORIZON_S` beside the new one, and a parked-alive order is
        listed indefinitely, which would have latched the stand-down permanently. The question
        this asks is "is the venue holding an order we have NO record of", which is the one an
        independent verify can answer.

        The reader lives in `bot.poly_us.venue_positions` (moved out of `scripts.poly_live_mm`,
        which re-exports it) so this module's import closure stays inside `bot/`.
        """
        from bot.poly_us.venue_positions import _verify_positions, venue_qty
        self._verify_ts = time.time()
        # ⛔ ONE, not the page count: `_venue_positions` paginates INSIDE the launcher's reader and
        # exposes no page total, so this under-counts a multi-page account by one request per
        # extra page. Charged rather than skipped — the pacer must see the floor.
        self._extra_requests += 1
        # ⛔ `record=False`: that reader's refusal sink is drained into the TEARDOWN CERTIFICATION
        # note, and a transient 03:00 read refusal is not a teardown finding [review CONCERN 4].
        positions = await _verify_positions(self.client, record=False)
        orders: Optional[list] = None
        if positions is not None:
            try:
                self._extra_requests += 1
                # ⛔ ACCOUNT-WIDE, filtered here — `_teardown_sweep`'s argument: this venue is
                # documented to accept-and-silently-ignore a `slugs` filter, and a filter that
                # returned [] would read as "nothing of ours is resting", which is the direction
                # that must never be guessed.
                orders = await self.client.get_open_orders()
            except Exception as exc:
                log.warning(f"verify_unavailable {self.run_id}: open-orders read {exc!r}")
                orders = None
        if positions is None or orders is None:
            if not self._verify_unavailable_logged:
                self._verify_unavailable_logged = True
                log.warning(f"verify_unavailable {self.run_id}: venue could not be read — "
                            f"no book stood down, no stand-down cleared")
            # ⛔ THE STAND-DOWN STATE IS UNTOUCHED (that is what "changes nothing" protects), but
            # the RECHECK falls back to the hourly clock: a pending confirmation that re-read
            # every cycle through an outage is a request leak, and the suspicion survives in
            # `_verify_suspect` either way.
            self._verify_recheck = False
            return
        self._verify_unavailable_logged = False
        # EVERY ORDER ID THIS MAKER KNOWS ABOUT. ⛔ All three sets, not `resting` alone: a replace
        # leaves the old id listed for up to `LAG_HORIZON_S` (`pending_reconcile`) and a
        # parked-alive order is listed until the venue stops serving it (`parked_alive`) — both
        # are orders we placed and are tracking, so neither is an independent disagreement.
        known_ids = ({o.order_id for o in self.resting.values() if o.order_id}
                     | set(self.pending_reconcile) | set(self.parked_alive))
        unknown_orders: dict[str, int] = {}
        for entry in orders:
            if not isinstance(entry, dict):
                continue
            _slug = entry.get("marketSlug") or entry.get("market_slug")
            _owned = self._slate_key(str(_slug)) if _slug else None
            if _owned is None or venue_order_id(entry) in known_ids:
                continue
            unknown_orders[_owned] = unknown_orders.get(_owned, 0) + 1
        venue_pos: dict[str, Decimal] = {}
        for _slug, _row in positions.items():
            _owned = self._slate_key(str(_slug))
            if _owned is not None:
                venue_pos[_owned] = venue_qty(_row)
        # slug → CONSECUTIVE mismatched reads, rebuilt from scratch each read: a book absent here
        # was just read clean, and its streak is gone rather than decremented.
        streak: dict[str, int] = {}
        for slug in sorted(self.ticks):
            venue_q = venue_pos.get(slug, _ZERO)
            belief_q = self.inventory.get(slug, _ZERO)
            stray = unknown_orders.get(slug, 0)
            if venue_q != belief_q or stray:
                streak[slug] = self._verify_streak.get(slug, 0) + 1
                # ⛔ PAGE-CLASS, ISSUE ONLY, id on the line — no rationale, no remedy per line.
                # The read count says whether this line armed the rail or is still confirming.
                log.error(f"verify_mismatch {slug} venue={venue_q} belief={belief_q} "
                          f"unknown_orders={stray} "
                          f"reads={streak[slug]}/{VERIFY_CONFIRM_READS:g} run={self.run_id}")
        # ⛔ `VERIFY_CONFIRM_READS` CONSECUTIVE READS ARM IT — one read never does.
        # ⛔ REPLACE, never union: this read is the ONLY thing that can clear a stand-down, and a
        # book absent from `streak` was just read clean.
        confirmed = {s for s, n in streak.items() if n >= VERIFY_CONFIRM_READS}
        cleared = self._verify_reduce_only - confirmed
        if cleared:
            log.warning(f"verify clean {self.run_id}: {sorted(cleared)} re-admitted")
        self._verify_reduce_only = confirmed
        self._verify_streak = streak
        # The confirming read runs on the NEXT CYCLE, not the next hour: an hour of a real
        # disagreement is an hour of quoting against a position nobody has reconciled.
        self._verify_recheck = bool(set(streak) - confirmed)

    @staticmethod
    def _derive_lane_scope(state_store: Optional[MakerStateStore]) -> bool:
        """Is a SIBLING run possible? — the same attribution the start-time gate makes
       .

        True iff this run is on a non-default lane, or a LIVE sibling lane exists (unclean exit,
        maybe-live orders, or recorded inventory — `live_sibling_lanes`, never mere lane-key
        existence: lane records are never deleted, and keying on existence latched scoping ON
        forever after one park probe).

        ⛔ EVERY failure path returns False, which is the ACCOUNT-GLOBAL guard — the STRICTER one.
        A ledger we cannot read is not evidence that somebody else owns a position."""
        if state_store is None:
            return False
        try:
            if str(getattr(state_store, "lane", DEFAULT_LANE)) != DEFAULT_LANE:
                return True
            return bool(live_sibling_lanes(getattr(state_store, "path", None),
                                           str(getattr(state_store, "lane", DEFAULT_LANE))))
        except Exception as exc:
            log.warning(f"lane-scope derivation failed ({exc!r}) — falling back to the "
                        f"ACCOUNT-GLOBAL venue-inventory guard (the stricter one)")
            return False

    def _slate_key(self, slug: str) -> Optional[str]:
        """The SLATE STRING this venue row belongs to, or None if the row is not ours.

        ⛔ RETURNS THE KEY, NOT A BOOLEAN. Every per-book threshold — `self.caps`, `self.sizes`,
        `self._launch_reach` — is keyed by the SLATE STRING, so a caller that merely learned "yes,
        it is ours" and then looked those up with the RAW venue string got the unknown-book
        fallbacks: a case-mismatched row judged against the cross-book launch MAX instead of its
        own book's frozen reach. Looser, on the one row we just proved is ours.

        ⛔ A normalized-only match is JUDGED, not skipped, and announced at ERROR: letting the
        difference route our own book into the "somebody else's position" branch would disarm the
        guard silently. Judging is the fail-closed direction."""
        if slug in self.ticks or slug in self.slugs:
            return slug
        key = norm_slug(slug)
        owned = self._slate_norm.get(key)
        if owned is not None:
            if slug not in self._slug_norm_warned:
                self._slug_norm_warned.add(slug)
                log.error(
                    f"⚠️ VENUE KEY {slug!r} matches slate book {owned!r} only after "
                    f"normalization (case/whitespace). Judging it as OURS against THAT book's "
                    f"own frozen reach — the safe reading — but the mismatch is real: fix the "
                    f"slate string, and check any other tool that joins venue rows to slugs by "
                    f"exact string.")
            return owned
        return None

    def _maybe_rederive_lane_scope(self) -> None:
        """Re-run the scope derivation beside the periodic venue refresh, and ANNOUNCE a flip.

        A sibling can be launched (or exit) at any point in a run, so a construction-time answer
        expires. Silent when nothing changed; an explicit `lane_scoped=` is never overridden."""
        if self._lane_scope_pinned:
            return
        fresh = self._derive_lane_scope(self.state)
        if fresh == self.lane_scoped:
            return
        self.lane_scoped = fresh
        log.warning(
            f"lane scope CHANGED mid-run → {'LANE-SCOPED' if fresh else 'ACCOUNT-GLOBAL'}: "
            + ("a live sibling lane now exists, so off-slate venue rows are attributed to it "
               "rather than judged against this run's reach"
               if fresh else
               "no live sibling lane remains, so the account-global venue-inventory guard is "
               "back in force (the stricter side) — an off-slate position can halt this run "
               "again"))

    def _foreign_owner(self, slug: str) -> Optional[str]:
        """The LIVE sibling lane that records this book, or None = UNATTRIBUTED.

        Attribution, not assumption. A lane owns the book if its record lists it as a ticker or
        carries inventory in it — the same record the start gate reads. ⛔ `live_sibling_lanes`
        skips OUR OWN lane by construction, so a previous run's orphan on this lane is
        UNATTRIBUTED and says so; with both gates lane-scoped nothing else is watching it.

        Any read failure returns None (UNATTRIBUTED) — an unreadable ledger must never be
        reported to the operator as "a sibling owns it"."""
        if self.state is None:
            return None
        try:
            sibs = live_sibling_lanes(getattr(self.state, "path", None),
                                      str(getattr(self.state, "lane", DEFAULT_LANE)))
        except Exception as exc:
            log.warning(f"sibling-lane attribution unreadable ({exc!r}) — reporting {slug} as "
                        f"UNATTRIBUTED, which is the honest answer, not a diagnosis")
            return None
        for lane, st in sorted(sibs.items()):
            if slug in (st.tickers or ()) or st.inventory.get(slug):
                return lane
        return None

    def _venue_breach(self) -> Optional[str]:
        """A halt reason if VENUE truth says our position is out of control, else None.

        ONE condition: MAGNITUDE — |venue| beyond `cap + size`, i.e. past any position the caps
        can produce even allowing for a full fill on an already-resting order after the adding
        side stopped being placed. Strict `>`: `cap + size` itself is reachable lawfully through
        cancel latency.

        ⛔ A SIGN-DISAGREEMENT CONDITION WAS TRIED AND REMOVED — do not re-add it. In simulated
        phase-runs against the REAL venue series it false-halted every run within the hour: belief and
        the venue frequently disagree and genuine opposite-sign states are rare but persistent enough
        that a halt keyed on them stopped every phase offset. Disagreement
        is the NORMAL state of a fast book and no reordering fixes it; the divergence is
        structural the private design notes.

        ⚠️ Honest scope: this in-process copy is defence in depth with a faster read cadence, not
        a replacement for the external position guard — run one guard per book. It has fired in real
        money, each time confirmed by a fresh re-read.

        ⛔ SCOPE: on a LANE-SCOPED run, rows for books OFF this run's slate are skipped
        and announced, not judged; a solo main run keeps the historical ACCOUNT-GLOBAL sweep.

        ⛔ Runs over EVERY venue row from run_cycle — not inside the quote path. A book whose book
        read failed, whose touch went one-sided, or that this run no longer quotes still holds
        whatever it holds, and divergence correlates with exactly the venue stress that makes
        reads fail.
        """
        for slug, venue_inv in sorted(self.venue_inventory.items()):
            if venue_inv == _ZERO:
                continue
            # ⛔ RESOLVE ONCE, THEN KEY EVERY PER-BOOK LOOKUP ON THE RESOLVED NAME.
            # `caps`/`sizes`/`_launch_reach` are keyed by the SLATE string; looking them up with
            # the raw venue string after a normalized match silently fell back to the unknown-book
            # values, judging one of our OWN books against the cross-book launch MAX. `None` =
            # genuinely off-slate.
            owned = self._slate_key(slug)
            if owned is None and slug in self.carried_off_slate:
                # ⛔ CARRIED OFF-SLATE — SKIPPED INDEPENDENT OF LANE SCOPE. The recovery gate
                # carried this row because our record and the venue AGREE on it; this run never
                # quotes it, never sizes it and must never flatten it, so judging it against THIS
                # run's lawful reach is the wrong subject. ⛔ It must NOT hang off
                # `self.lane_scoped`: that flips to False the moment the last live sibling exits,
                # and a lawful carry would then halt the run mid-evening. The exclusion is keyed
                # on the carried set itself, which only the start gate writes.
                # ⚠️ ONE LINE PER RUN, and it does not claim a guard: nothing is quoting this book.
                if slug not in self._carried_announced:
                    self._carried_announced.add(slug)
                    log.warning(f"carried off-slate: {slug} {venue_inv} (unmanaged; settles or "
                                f"is closed by hand)")
                continue
            if self.lane_scoped and owned is None:
                # ⛔ LANE-SCOPED: OFF-SLATE INVENTORY IS NOT THIS RUN'S TO JUDGE. The
                # first real+ws probe halted during startup reconcile — before cycle 1 — because
                # this guard is ACCOUNT-scoped and it measured a LIVE SIBLING's inventory against the
                # probe's own lawful reach. Correct arithmetic, wrong subject.
                # Attribution is the same one the START gate makes, and OVERLAP is policed there,
                # not here: the adopt/carry gates refuse a start whose slate collides with a
                # sibling's position.
                # ⛔ THE GUARD IS UNCHANGED FOR EVERY BOOK THIS RUN OWNS — same frozen launch
                # reach, same strict `>`, same halt. What is removed is a halt over a position
                # this process never placed, cannot size, and must not flatten.
                # ⚠️ SKIPPED, not silently ignored, and the announcement NAMES THE OWNER or says
                # it cannot. ⛔ `live_sibling_lanes` EXCLUDES OUR OWN LANE, so a PRIOR run's orphan
                # on THIS lane attributes to nothing and prints UNATTRIBUTED — correctly: no other
                # guard is watching it.
                owner = self._foreign_owner(slug)
                prev = self._foreign_inventory_announced.get(slug)
                mag = venue_inv.copy_abs()
                # Re-announce on a MATERIAL change (doubling), not once and forever: a foreign
                # position that grows while we quote beside it is new information.
                # ⛔ THE STORED VALUE **IS** THE PEAK, with no `max()` needed: the store is inside
                # the growth branch, so the recorded value is monotone by construction and an
                # oscillating 40 → 20 → 40 cannot re-announce.
                if prev is None or mag >= prev * 2:
                    self._foreign_inventory_announced[slug] = mag
                    log.warning(
                        f"venue inventory {venue_inv} on {slug} is OFF THIS RUN'S SLATE — "
                        f"lane-scoped, so this guard does NOT judge it"
                        + (f"; attributed to LIVE sibling lane {owner!r}, whose own run's "
                           f"guard covers it" if owner else
                           "; ⚠️ UNATTRIBUTED — no live sibling lane records this book, so "
                           "NOTHING is guarding it (a prior run's orphan on this lane, an "
                           "out-of-process position, or a lane nobody is running). Reconcile "
                           "it by hand")
                        + (f" [re-announced: magnitude grew from {prev} to {mag}]"
                           if prev is not None else ""))
                continue
            # `owned or slug`: on an UNSCOPED run an off-slate row resolves to None and keeps the
            # documented unknown-book fallbacks; a row that IS ours — however it was spelled — is
            # keyed by its slate string and gets its own frozen per-book threshold.
            book_key = owned or slug
            book_cap = self.caps.get(book_key, self.cap)
            book_size = self.sizes.get(book_key, self.size)
            belief = self.inventory.get(book_key, _ZERO)
            # ⛔ STRICT `>`, because `cap + size` is reachable lawfully: at belief == cap the
            # adding side stops being PLACED, but an already-resting full-size order keeps filling
            # until the next cycle cancels it. ⚠️ Positions are NOT quantised to multiples of the
            # quote size, so the quote size is not a ceiling.
            # ⛔ FROZEN launch anchor: this threshold must NOT move with a hot LOWERING. The
            # external guard is process-fixed so RAISES are its hazard; this guard is
            # live-recomputed, so LOWERINGS are the hazard HERE — a `size 85→28` cut turned the
            # venue's lawful 85 into a whole-run halt-and-teardown. Inventory acquired lawfully
            # under the launch config stays lawful; a hot lowering shrinks the QUOTES, never this
            # bound. The anchor ALONE — deliberately NOT max(anchor, live): for any lawful config
            # the live cap+size can never exceed the anchor, so a live term that would win the max
            # describes a config no gate approved, and widening to match it is concealment.
            reach = self._launch_reach.get(book_key, self._launch_reach_unknown)
            if venue_inv.copy_abs() > reach:
                basis = (f"live cap {book_cap} + overshoot {book_size}"
                         if book_key in self._launch_reach
                         else "off-slate: tightest slate reach")
                return (f"VENUE INVENTORY {venue_inv} on {slug} is beyond the launch lawful "
                        f"reach {reach} ({basis}"
                        + (f"; matched to slate book {book_key!r}" if book_key != slug else "")
                        + f") — belief said {belief}; halting into teardown")
        return None

    def _venue_read_is_current(self) -> bool:
        """Has a venue read SUCCEEDED recently enough that its silence is evidence?

        `venue_inventory_ts` is set ONLY on a complete, successfully-paginated read, and starts at
        0.0 — so this separates never read (0.0), read but stale, and read and current.
        ⛔ Absence is evidence of zero in exactly ONE of them.
        """
        return (self.venue_inventory_ts > 0.0
                and (time.time() - self.venue_inventory_ts) <= VENUE_STALE_AFTER_S)

    def _venue_restriction(self, slug: str, cap: int) -> tuple[bool, bool]:
        """(bid ok, ask ok) from VENUE truth alone — ANDed with the belief-side decision by the
        caller, so it can only ever narrow what we quote.

        ⛔ A VENUE ZERO UNDER A CURRENT READ, AGAINST A BELIEF THAT HELD AT THAT MOMENT, IS A
        DIVERGENCE — AND WE STOP QUOTING THAT BOOK. Reading an absent row as *unknown book ⇒ no
        restriction* is how the live phantom happened: the operator closed a book out of process,
        the venue went flat, belief kept a short, nothing fired, and the *reducing* bid filled —
        opening a real LONG FROM FLAT in a book the operator had deliberately exited.

        **Zero and absent are treated identically**, deliberately: whether this venue omits a zero
        row or serves it explicitly is NOT established anywhere in this repo, and covering both
        makes the question stop mattering.

        **The comparison is against belief AS OF THE READ, never live belief.** The map refreshes
        every ~60 s while belief moves on every booked fill, so live belief makes the first fill
        into any flat book look like a divergence. A rail that fired there would cancel BOTH sides
        of a freshly-opened book.

        Four states are kept apart, and conflating any two is the `[] must never mean "confirmed
        flat"` confusion the reconciler already paid for:
          · **unparseable row** ⇒ the venue DID answer and we could not read it ⇒ no restriction;
          · **never read** (`ts == 0.0`) ⇒ unknown ⇒ no restriction — the startup case;
          · **stale read** ⇒ unknown ⇒ no restriction — a book could have opened since;
          · **current read** ⇒ zero IS zero ⇒ restrict iff belief disagreed *at that read* AND
            still disagrees now (the live-belief clause is a TIE-BREAK, not a second safety
            condition).

        Direction: the only new restriction returned is `(False, False)` and the caller ANDs, so
        this can only ever narrow. ⚠️ That is a claim about PERMISSIONS, not about risk: blocking a
        book parks its inventory with nothing working it off. It is right here only because the
        position of record is wrong, so any "reducing" order is aimed at something that may not
        exist.
        """
        inv = self.venue_inventory.get(slug)
        if inv is not None and inv != _ZERO:
            self._venue_flat_divergence.discard(slug)
            return sides_allowed(inv, cap)
        # The venue says zero — either by omitting the row or by serving one at zero.
        if (slug in self.venue_stale_rows
                or not self._venue_read_is_current()
                or self.venue_belief_at_read.get(slug, _ZERO) == _ZERO
                # ⚠️ ...and belief must ALSO be non-zero NOW. The snapshot is taken before
                # `poll_fills`, so a CLOSING fill the venue already reflects but our poll has not
                # booked reads as `belief_at_read=−5 vs venue=0` — a divergence warning on a book
                # that is genuinely flat. This is the mirror of the first-fill race.
                # ⛔ A TIE-BREAK, NOT A PROOF OF SAFETY. The corner it suppresses: the venue is
                # TRULY flat and a resting *reducing* order fills in the same cycle, so live
                # belief returns to zero having actually opened a real position from flat. It is
                # narrow, and the resulting position is picked up by the next refresh. We take
                # the tie-break because the benign case is common, and we ANNOUNCE it below.
                or self.inventory.get(slug, _ZERO) == _ZERO):
            if (inv is None or inv == _ZERO) and self._venue_read_is_current() \
                    and slug not in self.venue_stale_rows \
                    and self.venue_belief_at_read.get(slug, _ZERO) != _ZERO \
                    and slug not in self._venue_tiebreak_announced:
                # The live-belief tie-break resolved an ambiguity in the permissive direction.
                # Behaviour is unchanged by this log; it tells the operator they disagreed at all.
                self._venue_tiebreak_announced.add(slug)
                log.warning(
                    f"⚠️ VENUE/BELIEF TIE-BREAK on {slug}: the last venue read held nothing while "
                    f"belief said {self.venue_belief_at_read.get(slug, _ZERO)} at that moment, but "
                    f"belief has since gone flat — NOT blocking. Usually a close we booked late; "
                    f"verify the venue before trusting the ledger on this book")
            self._venue_flat_divergence.discard(slug)
            # ⛔ A PRESENT zero row still goes through `sides_allowed`, never a bare (True, True).
            # At `cap == 0` — the deliberate reduce-only mode — `sides_allowed(0, 0)` is
            # (False, False), so short-circuiting here would WIDEN in exactly the corner this
            # rail claims it cannot.
            return sides_allowed(inv, cap) if inv is not None else (True, True)
        if slug not in self._venue_flat_divergence:
            self._venue_flat_divergence.add(slug)
            # WARNING, not INFO: this module inherits root's WARNING, so an INFO announcement is
            # invisible in production.
            log.warning(
                f"⛔ VENUE-FLAT DIVERGENCE on {slug}: the last venue read held NOTHING while "
                f"belief said {self.venue_belief_at_read.get(slug, _ZERO)} at that moment. Not "
                f"quoting this book. The venue is right — reconcile the durable ledger (an "
                f"out-of-process close does not book) before relaunching")
        return False, False

    def _loss_cap_breach(self) -> Optional[str]:
        """The halt reason if the loss cap is breached, else None. TWO axes, both floored at zero
        so profit never buys budget on either:

          SESSION — this run's own realized loss. What an operator means by "this run may lose
          <n>": it does not silently grow because last night went well.
          LIFETIME — the durable loss-to-date, the crash-restart ratchet: a loop of runs each
          losing under the session bound still halts once their sum passes it.

        ⛔ BOTH AXES BIND ON **OVERALL** [AMENDMENT 30, operator-decided 2026-09-08]: price
        realized (incl. booked settlements) PLUS the venue's REALIZED rebate read-back. Rewards
        stay excluded — venue-paid D+1 against the whole account, unattributable to a run or a
        lane. Marks never enter. The 2026-09-04 trip fired at price −<n> with this run's
        rebates unseen; the operator decides on the all-in number, so the rail enforces it.
        """
        if self.loss_cap <= _ZERO:
            return None
        session_overall = self.session_realized + self.session_rebate
        session_loss = max(_ZERO, -session_overall)
        if session_loss > self.loss_cap:
            # BOTH NUMBERS, ALWAYS LABELLED: the enforced one is OVERALL, and the price channel
            # beside it is what says whether the book is bleeding price under rebate volume.
            return (f"LOSS CAP: this run has realized price-channel {self.session_realized:+} · "
                    f"rebate {self.session_rebate:+} (last-per-order, "
                    f"{len(self._order_commissions)} orders) · OVERALL {session_overall:+} "
                    f"against a −{self.loss_cap} cap — halting into teardown. Rewards are NOT "
                    f"counted here at all (venue-paid D+1 — read capital_rewards.csv before "
                    f"judging this run).")
        if self.state is not None and self.real:
            # ⛔ ACCOUNT-WIDE and LIVE, not a startup snapshot: two lanes launched together each
            # priced a full cap at startup, so the account could lose N×cap. The lifetime axis
            # reads Σ per-lane loss_to_date from the SHARED ledger every check, so a sibling's
            # in-run losses tighten THIS run within one cycle. MAX of the live account read and
            # the own-lane snapshot — never WEAKER than the old own-lane check, because a
            # missing/unreadable/foreign-path file reads as Σ=0 rather than raising.
            # OVERALL on this axis too [AMENDMENT 30] — the durable ratchet and the session
            # bound must measure the same money, or a run halts on one definition and is
            # refused a restart on the other.
            _snap = self.state.snapshot()
            own_loss = _snap.overall_loss_to_date
            account_read_ok = True
            try:
                lifetime_loss = max(own_loss, account_loss(self.state.path, overall=True))
            except Exception as exc:
                # Designed degrade, but LOUD — a silent except here hid every route into THIS
                # refactor would have disabled the account-wide axis forever with no trace).
                # ERROR fires on the TRANSITION only.
                # ⚠️ NOT covered here: a state path that yields ZERO lanes returns a silent Σ=0
                # WITHOUT raising, so a path mismatch between sibling lanes still degrades
                # invisibly — the private design notes § account-ledger zero-lane read.
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
                        f"beyond the −{self.loss_cap} cap. Own lane: {cap_line(_snap)}. "
                        f"(the ratchet carries across processes "
                        f"AND lanes; see the loss-cap design note to clear it "
                        f"deliberately)")
        return None

    async def _halt_cycle(self, stats: CycleStats, started: float) -> CycleStats:
        """End a cycle that halted mid-flight: PULL THE QUOTES, attribute, tape it, return.

        ⛔ One implementation for every halt path. The loss cap shipped twice with a halt that
        stopped quoting but left the resting orders live — the second time because a `break`
        reached the cycle's NORMAL exit, which cancels nothing and writes the tape as a healthy
        cycle. Cancelling is the safe direction and the flatten is left for teardown.
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
        # request and already decided a quote. ⛔ AND THE HOT SEAMS MOVED BELOW IT [continuous
        # maker P1]: a slate apply PLACES nothing but it does register books and spend venue
        # reads, and doing that on the cycle the kill switch fires would seat a book into a
        # teardown. Both hot readers now run after the pause block, where their reads are also
        # counted and paced.
        # `self.lane` opts into the lane-scoped pause file so halting one lane's run cannot tear
        # down a concurrent maker in another lane. Any other file shape — including a bare
        # `touch pause.json` — is still a GLOBAL halt (ambiguity halts more, never less).
        if is_paused(self.lane):
            stats.paused = True
            # ⛔ A PAUSE PULLS THE QUOTES. Returning here without cancelling leaves resting orders
            # live and still filling, while skipping the cap and reduce-only checks entirely — so
            # the kill switch would produce a maker that keeps trading with NO cap in force.
            # Cancelling is always the safe direction; the flatten is left for teardown.
            for slug, side in list(self.resting):
                await self._cancel(slug, side)
                stats.actions[CANCEL] = stats.actions.get(CANCEL, 0) + 1
            # ⛔ AND THE RUN ENDS — same pattern as the memguard breach below. Without this the
            # kill switch pulled the quotes and left the process alive and IDLE for the rest of
            # --seconds, holding whatever inventory it had. The shim breaks on should_stop and
            # runs cancel-all → flatten → sweep.
            self.should_stop = True
            self.halt_reason = stats.halt_reason = "kill switch (pause.json)"
            stats.requests += self._extra_requests
            self._extra_requests = 0
            stats.wall_s = time.time() - started
            self._beat(stats)
            self._write_cycle(stats)
            return stats

        # PER-LANE hard RAM floor: a SHADOW run places nothing — an OOM SIGKILL costs only
        # unwritten tape rows — and the host's kill history shows the kernel only kills at swap
        # exhaustion, which the COMBINED floor still guards. The residency floor exists for a REAL
        # maker's TEARDOWN (venue HTTP cancel-all/flatten must not run from a swapfile).
        _lim = memguard.limits_from_config()
        if self.shadow and _lim.hard_ram_floor_mb:
            import dataclasses as _dc
            _lim = _dc.replace(_lim, hard_ram_floor_mb=0.0)
        mem = memguard.check(limits=_lim, label="poly_maker")
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
        # ⛔ THE SLATE BEFORE hot_settings [continuous maker P1]: `parse_hot_settings` is
        # whole-file-or-nothing against `self.sizes`, so a book added by THIS cycle's slate must
        # already be registered or every hot_settings entry naming it voids that file whole. After
        # the stats reset, so the add's market-meta and venue reads are counted and paced.
        await self.maybe_apply_hot_slate(stats)
        self.maybe_apply_hot_settings()   # M0.8 — same seam, same cadence; no file = no-op
        # Repair the inventory belief BEFORE the quote decisions below, and AFTER the stats reset
        # so the verify reads are COUNTED and paced — run before the reset, their reads were
        # erased from `stats.requests`, a request leak the rate line could never show.
        await self._retry_pending_reconciles()
        # The activities walk rides the same seam: after the stats reset (its read is
        # counted and paced), before `_venue_breach` (the breach line's "belief said" must be
        # the healed number, not the pre-recovery one).
        await self._recovery_walk()
        if self.real and self.cycle_index % RECONCILE_EVERY_CYCLES == 1:
            # Venue truth, on a slow cadence. Real runs only: in shadow there is nothing to hold.
            await self._refresh_venue_inventory()
            # ⛔ AND RE-DERIVE THE LANE SCOPE ON THE SAME CADENCE. Derived once at construction,
            # the attribution is a function of LAUNCH ORDER: a solo main run starts unscoped, a
            # probe lane is launched an hour later, and the main run keeps judging the probe's
            # position against its own reach. The predicate is a small ledger read already paid
            # for beside a venue read, and it moves in BOTH directions.
            self._maybe_rederive_lane_scope()
        # ⛔ FILLS FIRST, before any quote decision. The cap and the reduce-only rule are functions
        # of inventory, so polling afterwards would size every cycle's quotes off the PREVIOUS
        # cycle's position — the side that should have stopped adding keeps adding for one more
        # requote. Its requests are counted and paced here rather than spent invisibly by the
        # caller: at 33 markets a full poll is ~67 requests, three times the book-read cost.
        await self.poll_fills()
        # ⛔ AFTER the fill poll, BEFORE the QUOTE DECISIONS [cycle-4 P1]. After, so a trip
        # booked this cycle arms its ban before the sweep can look at it; before, so
        # `in_cooldown` / `_reduce_only_books` / the quote-tape status all see one consistent
        # answer within a cycle. ⚠️ NOT before every reader FULL STOP: `maybe_apply_hot_settings`
        # at the top of the cycle reads the latch map one sweep earlier, so an ack landing in the
        # same cycle a ban expires is judged against a latch that is about to lapse anyway —
        # safe direction (the book re-admits either way) and at most one requote of latency.
        # A no-op on every lane but `probe` (empty deadline map).
        self._expire_adverse_bans()
        # ⛔ THE DROP RUNS HERE [continuous maker P1]: after the fill poll and the venue refresh
        # (so "flat" is this cycle's belief against this cycle's venue read), and before the
        # quote decisions — a book dropped this cycle must not also be quoted this cycle.
        self._drop_retired_books()
        # Drain the probe-stats queue once a cycle. Without this a trailing event (the last
        # fill of a burst) would sit queued until teardown; the debounce still caps the post
        # rate, and the call is a no-op on every lane but `probe`.
        self._probe_notes.tick()
        # ⛔ THE PERIODIC INDEPENDENT VERIFY [P1b] — REAL runs only (in shadow there is nothing to
        # hold and nothing rests). Placed AFTER `poll_fills` and the drop, so belief is this
        # cycle's and a dropped book is not judged, and BEFORE the quote decisions, so a book the
        # venue disagrees about goes reduce-only on the SAME cycle the disagreement was found.
        # `_verify_recheck` short-circuits the clock for the CONFIRMING read only — set solely by
        # a verify that found an UNCONFIRMED mismatch, and cleared by the read it schedules.
        if self.real and (self._verify_recheck or time.time() - self._verify_ts >= VERIFY_S):
            await self._periodic_venue_verify()
        # ⛔ AND THE VENUE CHECK COMES AFTER THEM, for the same reason. Run BEFORE the poll it
        # compared a fresh venue row against a belief one requote stale, so an ordinary
        # buy-back-through-zero read as a SIGN DISAGREEMENT the rescan could not clear (the rescan
        # re-reads the VENUE and belief was the stale side) — most simulated phase-runs false-halted
        # within the hour. Still before any book read, so an unreadable book is still checked.
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
                    # ⛔ Only claim confirmation when a read actually SUCCEEDED. Returning the
                    # same None on success and failure let a 503 keep the stale row while the
                    # halt asserted "(confirmed by a fresh read)" — a false confirmation that
                    # feeds an operator hand-flatten of a position that may not exist.
                    self.halt_reason = venue_breach + (
                        " (confirmed by a fresh read)" if confirmed_read
                        else " (RE-READ FAILED — halting on the stale row, NOT confirmed)")
                log.error(self.halt_reason)
        breach = self._loss_cap_breach()
        if not self.should_stop and breach is not None:
            # Evaluated EVERY cycle, not only when a round trip completes: a carried ledger can
            # already breach (a relaunch with a tightened cap), and many quoted book-hours
            # complete no trip at all.
            self.should_stop = True
            if self.halt_reason is None:
                self.halt_reason = breach
        if self.should_stop:
            # A halt raised during the fill polls ends the cycle here.
            return await self._halt_cycle(stats, started)

        # ── WS whole-connection guard [brick 2, design §4] ──
        quote_slugs = sorted(self.ticks)
        # ⛔ THE 503 WALL IS A HOLD, NOT A HALT. Inside the maintenance
        # window a dark socket is the EXPECTED state, not a feed defect: the venue 503s every
        # route for up to four hours and reopens with empty books. The escalation below would
        # burn the bounded fallback window on REST reads that cannot succeed and then HALT — and
        # a halt inside the window means a teardown whose sweep and verify 503 too, i.e. an
        # UNCERTIFIED run and a refused next launch. So the cycle IDLES instead: no placements,
        # no cancels (the venue has either cleared our orders or it has not), no fallback burst,
        # and the next cycle's fill poll is the one paced read that tests whether routes are back.
        # ⛔ NOT LATCHED — a connected socket inside the window drops straight through to the
        # normal guard, so recovery costs no extra cycle.
        _maint_now = self._maintenance_now()
        if not _maint_now:
            self._maint_hold_announced = False
        # ⛔ DARK-NOW IS EVIDENCE FROM THE LAST CYCLE, NEVER `_ws_down_since` (which stays set
        # across the recovery cycle): keying on the watermark would idle the whole window even
        # after the routes came back. A held cycle reads nothing, so it writes capped=outage=0
        # and the NEXT cycle tries the normal path — the alternation IS the paced retry.
        if (_maint_now and self.book_source == "ws" and self.book_feed is not None
                and (not self.book_feed.connected or self._last_ws_capped > 0
                     or self._last_ws_outage > 0)):
            if not self._maint_hold_announced:
                self._maint_hold_announced = True
                log.warning("maintenance window: ws feed DARK — HOLDING this cycle (no "
                            "placements, no cancels, no halt). The venue cancels every open "
                            "order itself if it is using the window; re-reading next cycle.")
            quote_slugs = []
        elif self.book_source == "ws" and self.book_feed is not None:
            connected = self.book_feed.connected
            if not connected:
                # The outage watermark for `_cache_frozen`: a graced cycle may serve the ws
                # cache again only once a frame POST-DATES this stamp [round-2 BLOCKING].
                self._ws_last_down_ts = time.time()
            # DECAY, not reset: a flapping socket (alternating cycles) zeroed an integer streak
            # forever and disabled the guard entirely. Decay by 0.5 per healthy cycle lets
            # sustained flapping accumulate to the threshold while a lone blip fades.
            self._ws_disconnect_streak = (max(0.0, self._ws_disconnect_streak - 0.5) if connected
                                          else min(4.0, self._ws_disconnect_streak + 1.0))
            # NEAR-CAP is hoisted ABOVE the blip tolerance: a maker at >=0.7x of its contract cap
            # never gets the blip cycles (they serve the possibly-stale ws cache, the exact
            # exposure this rail forbids). The response to a dark cycle here is a bounded
            # FORCED-REST grace, not an instant halt.
            # Dark THIS CYCLE — `_ws_down_since is not None` is true at the start of the RECOVERY
            # cycle too, so keying on it preempted recovery and blamed a "dark" feed on the cycle
            # that proved it healthy; once near cap, recovery became structurally unreachable.
            clean_last = self._last_ws_capped == 0 and self._last_ws_outage == 0
            dark_now = (not connected
                        or (self._ws_down_since is not None and not clean_last))
            # TWO AXES, ORed [scale-hardening 2026-08-20]: contracts, and the notional axis
            # that reads the same position in dollars where the inventory actually sits. The
            # OR can only tighten (see `ws_near_cap_notional`); which axis fired is named in
            # the halt reason because the operator's next question is always "near WHAT cap".
            self._announce_near_cap_trip()
            _near_ct = ws_near_cap(self.gross_exposure, self.max_total_contracts)
            # NET marks price what we HOLD; GROSS marks set the bar [, MPR
            # BLOCKING]: netting the threshold too would let our own quote raise the bar.
            _near_nt = ws_near_cap_notional(self.inventory, self.caps, self._marks(),
                                            self.max_total_contracts,
                                            cap_marks=self._marks(net=False))
            self._ws_nearcap_grace = False
            if dark_now and (_near_ct or _near_nt):
                # GRACE, not instant halt [see WS_NEARCAP_GRACE_S]. The venue's routine socket
                # bounces reconnect in <10 s and the zero-window rail turned one into a 12 h halt.
                # Within the grace the feed rides the bounded fallback — force-entered NOW — and
                # `_ws_nearcap_grace` makes `_read_book_md` FORCE REST while the socket is down,
                # so no graced cycle serves the frozen ws cache (`_ws_try_serve(degraded=True)`
                # treats a ≤ws_stale_s-old frozen frame as young, which is the exact exposure the
                # instant halt existed to forbid). A reconnected socket's serves stay allowed:
                # they are the recovery evidence. Past the grace — or the per-run episode budget —
                # the same halt as before.
                _down_for = (0.0 if self._ws_down_since is None
                             else time.time() - self._ws_down_since)
                if not self._ws_grace_episode_counted:
                    self._ws_nearcap_grace_episodes += 1
                    self._ws_grace_episode_counted = True
                _budget_ok = self._ws_nearcap_grace_episodes <= WS_NEARCAP_GRACE_EPISODES
                # ⛔ NO MAINTENANCE TERM HERE, AND THAT IS DELIBERATE
                # 2026-09-10]: the in-window HOLD above is a strict superset of `dark_now`, so
                # this branch is UNREACHABLE with the window open. A `_maint_now` disjunct here
                # would be a dead-armed rail on the near-cap halt.
                if _down_for <= WS_NEARCAP_GRACE_S and _budget_ok:
                    self._ws_nearcap_grace = True
                    self._ws_disconnect_streak = max(self._ws_disconnect_streak, 2.0)
                    log.warning(
                        f"ws dark NEAR CAP (gross {self.gross_exposure} / "
                        f"{self.max_total_contracts}) — grace: forced-REST fallback for "
                        f"≤{WS_NEARCAP_GRACE_S:.0f}s (dark {_down_for:.0f}s so far; episode "
                        f"{self._ws_nearcap_grace_episodes}/{WS_NEARCAP_GRACE_EPISODES}), "
                        f"then halt")
                else:
                    self.should_stop = True
                    self.halt_reason = stats.halt_reason = (
                        f"ws feed UNVERIFIED near cap (connected={connected}, last cycle "
                        f"capped={self._last_ws_capped}/outage={self._last_ws_outage}; gross "
                        f"{self.gross_exposure} / {self.max_total_contracts}; axis="
                        f"{'contracts' if _near_ct else ''}"
                        f"{'+' if _near_ct and _near_nt else ''}"
                        f"{'notional' if _near_nt else ''}) — "
                        + (f"dark {_down_for:.0f}s, past the {WS_NEARCAP_GRACE_S:.0f}s "
                           f"near-cap grace — halt" if _budget_ok else
                           f"grace episode budget spent "
                           f"({self._ws_nearcap_grace_episodes} episodes > "
                           f"{WS_NEARCAP_GRACE_EPISODES}/run — flapping socket) — halt"))
                    log.error(self.halt_reason)
                    return await self._halt_cycle(stats, started)
            # ⛔ RECOVERY MUST STAY REACHABLE PAST THE PROBATION BUDGET. True recovery is gated on
            # `not _last_cycle_reduced`, and the ONLY producer of a full-slate cycle inside an
            # episode is probation — so a hard relapse budget made recovery STRUCTURALLY
            # UNREACHABLE: a feed that fully heals at t=75 s still halts at the 120 s expiry, and
            # the teardown crosses the spread to flatten. The churn bound must not cost a run its
            # recovery.
            # The escape is EVIDENCE, not another placement: past the budget a clean reduced cycle
            # is promoted to recovery only when the WHOLE SLATE is servable RIGHT NOW (a free
            # cache read). That is strictly STRONGER than what a probation cycle proves, so
            # reduced-set evidence alone still clears nothing.
            budget_spent = self._ws_probation_relapses >= WS_PROBATION_MAX_RELAPSES
            slate_evidence = (budget_spent and self._last_cycle_reduced
                              and self._ws_slate_servable())
            if connected and self._ws_down_since is not None and clean_last \
                    and (not self._last_cycle_reduced or slate_evidence):
                # TRUE RECOVERY: a clean FULL-SLATE cycle — every book read fresh from WS with
                # zero capped and zero forced-REST reads. Reduced-set evidence never clears the
                # window (see probation below) [re-review B1].
                # ⛔ SAY ONLY WHAT IS KNOWN. "Full slate confirmed" is a VENUE claim this cycle
                # never made: every book was SERVED from the ws cache with zero capped and zero
                # forced-REST reads, and on the quiet arm that means "the socket had frames" —
                # not that any book's own stream was re-confirmed against the venue. Per-book
                # venue confirmation is the periodic re-verify's job, on its own clock.
                log.warning(
                    f"WS book feed RECOVERED after "
                    f"{time.time() - self._ws_down_since:.0f}s — "
                    + ("the reduced set came back clean AND every book in the full slate is "
                       "servable from the ws cache right now (probation budget was spent, so "
                       "this cache-evidence path is what ended the episode)" if slate_evidence
                       else "socket alive and every book in the full slate served from the ws "
                            "cache (0 capped, 0 forced-REST)")
                    + "; NOT a per-book venue re-confirmation")
                self._ws_down_since = None
                self._ws_cancel_done = False
                self._ws_probation_relapses = 0   # the episode ended; the churn budget resets
                self._ws_grace_episode_counted = False   # next episode counts against the budget
            probation = (connected and self._ws_down_since is not None and clean_last
                         and self._last_cycle_reduced
                         # ⛔ THE CHURN BOUND. Past the relapse budget the full-slate try has
                         # already failed WS_PROBATION_MAX_RELAPSES times in THIS episode, so
                         # re-trying costs another place/cancel pair plus one un-C-a-bounded REST
                         # read per dark book for evidence we already have. ⛔ It does NOT mean
                         # "the same outcome anyway": the un-bounded path could still exit via
                         # RECOVERY, so a hard stop here would convert a healing feed into a
                         # window-expiry halt. Recovery past the budget runs on
                         # `_ws_slate_servable` evidence instead.
                         and self._ws_probation_relapses < WS_PROBATION_MAX_RELAPSES)
            if (connected or self._ws_disconnect_streak < 2) and self._ws_down_since is None:
                self._last_cycle_reduced = False    # healthy
            elif probation:
                # PROBATION: the reduced set came back all-WS-fresh, so TRY the full slate —
                # but the WINDOW KEEPS RUNNING (monotonic to expiry) and the cancel flag is NOT
                # reset: if the dropped books are still dark, this cycle caps (C-a bounds the
                # REST cost), the next cycle is fallback again, and the episode continues on
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
                # ⛔ AND THE FALLBACK WINDOW DOES NOT EXPIRE INTO A HALT INSIDE THE MAINTENANCE
                # WINDOW, for the same reason: "fallback is never steady
                # state" is a claim about a healthy venue.
                elif now - self._ws_down_since > WS_FALLBACK_WINDOW_S and not _maint_now:
                    self.should_stop = True
                    self.halt_reason = stats.halt_reason = (
                        f"ws feed degraded >{WS_FALLBACK_WINDOW_S:.0f}s — fallback window "
                        f"expired, halting (fallback is never steady state)")
                    log.error(self.halt_reason)
                    return await self._halt_cycle(stats, started)
                elif self._last_cycle_reduced is False and self._ws_cancel_done:
                    # PROBATION RELAPSE: the full-slate try failed — orders it placed on
                    # still-dark books get cancelled again. ⚠️ HONEST COST [round-4 review]: a
                    # persistent partial freeze therefore alternates probation-place / relapse-
                    # cancel every 2 cycles until the window expires (~120s) — dropped books'
                    # queue position resets each pair, and probation's REST reads are NOT C-a
                    # bounded (one read per dark book). Bounded by the window, safe-direction
                    # (no blind quoting), but it is churn, not free. ⛔ AND NOW BOUNDED BY A
                    # COUNT TOO [scale-hardening 2026-08-20]: this is the relapse the probation
                    # budget spends — at `WS_PROBATION_MAX_RELAPSES` the predicate above stops
                    # re-trying the full slate and the alternation stops.
                    self._ws_cancel_done = False
                    self._ws_probation_relapses += 1
                    if self._ws_probation_relapses >= WS_PROBATION_MAX_RELAPSES:
                        log.warning(
                            f"WS probation budget SPENT ({self._ws_probation_relapses}/"
                            f"{WS_PROBATION_MAX_RELAPSES} relapses this episode) — no further "
                            f"full-slate retries; riding the reduced set to the "
                            f"{WS_FALLBACK_WINDOW_S:.0f}s window expiry (i.e. to the halt). The "
                            f"place/cancel churn on the dropped books stops here.")
                # Reduced set limits ONLY this iteration — never self.ticks/self.inventory
                # (the reconciler + teardown flatten iterate those; pruning blinds teardown).
                quote_slugs = ws_fallback_slugs(self.inventory, self.ticks,
                                                sizes=self.sizes, marks=self._marks())
                self._last_cycle_reduced = True
                if len(quote_slugs) > WS_FALLBACK_MAX_BOOKS:
                    # ⛔ THE SET IS OVER ITS REST BUDGET AND SAYS SO [scale-hardening
                    # 2026-08-20]. `ws_fallback_slugs` never drops a held book (an unquoted
                    # held book cannot work down), so `held > cap` produces a REST burst
                    # bigger than the ~8–12 the pace is sized for — a real cost that used to
                    # be visible only as a stretched cycle. The books are ordered
                    # most-exposed-first so a cycle cut short still covers the exposure that
                    # matters; this line is the operator's notice that the cycle will be slow.
                    log.error(
                        f"WS fallback set is OVER BUDGET: {len(quote_slugs)} book(s) vs the "
                        f"REST-safe {WS_FALLBACK_MAX_BOOKS} — every one of them is HELD and a "
                        f"held book is never dropped, so this cycle's REST reads exceed the "
                        f"pace budget and the cycle will stretch. Ordered most-exposed-first.")
                # ⛔ CANCEL the dropped books' resting orders ONCE PER EPISODE-SEGMENT (the flag
                # resets on true recovery or a probation relapse).
                # ⛔ …BUT NOT INSIDE THE MAINTENANCE WINDOW: we never cancel
                # our own orders in there (the venue does it if it uses the window), and the
                # cancel would ride the 503ing routes anyway.
                if not self._ws_cancel_done and not _maint_now:
                    self._ws_cancel_done = True
                    reduced = set(quote_slugs)
                    for r_slug, r_side in list(self.resting):
                        if r_slug not in reduced:
                            await self._cancel(r_slug, r_side)
                            stats.actions[CANCEL] = stats.actions.get(CANCEL, 0) + 1
                log.warning(f"WS down {time.time() - self._ws_down_since:.0f}s — quoting "
                            f"reduced set ({len(quote_slugs)} of {len(self.ticks)}) over REST")

        if self.book_source == "ws" and self.book_feed is not None:
            # Choose this cycle's re-verify drain OLDEST-FIRST, before any book is read — the
            # per-cycle bound is only a bound if the queue drains by age [r2 CONCERN-5].
            self._plan_reverifies(time.time(), quote_slugs)
        await self._pace(stats.requests + self._extra_requests, started)
        for slug in quote_slugs:
            if self.should_stop:
                # ⛔ `_book_fill` is ALSO reachable from inside this loop — a requote's cancel
                # read-back books the fill that breaches the cap. A `break` here reaches the
                # NORMAL cycle exit, which cancels nothing, so a breach left four orders resting
                # and wrote the tape as a healthy cycle. Both paths run the SAME teardown-lite.
                return await self._halt_cycle(stats, started)
            await self._quote_market(slug, stats)
            if self.should_stop:
                # A halt raised BY this market — checking only at the top of the loop misses it
                # entirely when the breaching book is the last one, and the cycle would exit
                # normally, cancelling nothing.
                return await self._halt_cycle(stats, started)
            # Pace AFTER each market on everything spent so far, including the placements this
            # market just made. Pacing before the read, on a market count, is what produced the
            # 35 req/s burst — see `_pace`.
            await self._pace(stats.requests + self._extra_requests, started)
        stats.requests += self._extra_requests

        # PARTIAL-FREEZE ESCALATION [design §4 C-a]: a cycle that hit the C-a cap AND still had
        # stale books left ran partially blind — over consecutive cycles that is a partial feed
        # freeze, and skipping forever was the gap. Escalate into the SAME
        # bounded-fallback-then-halt path as a connection loss.
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
        # during the cycle scores NEGATIVE, which cannot detect a market whose reads have stopped
        # — the one thing this number exists for. A market never read at all counts from the cycle
        # start, so it is stale rather than absent from the maximum.
        ages = [finished - self.last_read_ts.get(slug, started) for slug in self.ticks]
        stats.max_staleness_s = max(ages) if ages else None
        warning = rate_budget_warning(stats.requests, self.requote_s, self.max_req_per_s)
        if warning:
            log.warning(warning)
        # ⛔ THE DEFERRED REACH RELEASE RE-ATTEMPTS HERE, AT THE TAIL — not at the cycle top. The
        # apply seam that lowers a size is by construction the one cycle that may not release, so
        # something later must try; but a TOP-of-cycle attempt could release on the strength of a
        # requote that had not happened yet. At the tail, "the book was requoted at the new size"
        # is a fact about a cycle that has finished.
        # ⚠️ RELEASE ONLY — deliberately not the whole of `_rederive_anchors`. Keeping the seams
        # split is narrowness: a per-cycle rebuild of a money-path map that only changes on an
        # operator edit would make any future out-of-band cap an invisible one-cycle revert.
        self._release_anchors()
        # THE EMPTY-BOOK SEED's durable mirror [P1b]. It has to survive the process: the venue
        # documents that a book reopens EMPTY after maintenance, and a maker restarted inside the
        # window has no in-memory touch to seed from. ⛔ REAL runs only — a shadow run seeds no
        # prices and must not write the live lane's record.
        # ⛔ WRITE ON CHANGE, NOT PER CYCLE [mm-review 2026-09-10]: `set_last_touch` rewrites the
        # SHARED multi-lane state file under a lock, and an unconditional write spent that on
        # every quoted cycle of every book. A row is due when its PRICES moved, or when the one on
        # disk has aged a quarter of the seed's own bound — so a long-stable touch stays fresh
        # enough to be usable without a write per requote. ⚠️ Derived from
        # `EMPTY_BOOK_TOUCH_MAX_S`, never a second number for the same fact.
        if self._durable_last_touch and self.state is not None and self.real:
            _refresh_after = EMPTY_BOOK_TOUCH_MAX_S / 4
            _due = {}
            for _s, _t in self._durable_last_touch.items():
                _w = self._durable_last_touch_written.get(_s)
                if _w is None or _w[:2] != _t[:2] or _t[2] - _w[2] > _refresh_after:
                    _due[_s] = _t
            if _due:
                self.state.set_last_touch(_due)
                self._durable_last_touch_written.update(_due)
        self._beat(stats)
        self._write_cycle(stats)
        return stats

    async def _read_book_md(self, slug: str,
                            stats: "CycleStats") -> tuple[Optional[dict], float, Optional[str]]:
        """(marketData, read_ms, error_reason) — the ONE book-read seam [WS-maker book feed].

        `rest` (default): today's cache-busted `_fetch_book` — behaviour-identical, no change.
        `ws`: read the injected feed's `get_book_md` and serve it under the OR-RULE — young
        CONTENT (transactTime age ≤ `ws_stale_s`) **or** a provably LIVE SOCKET (any frame
        within `ws_conn_live_s`) — with a periodic fresh-REST re-verify (`ws_reverify_s`) as
        the hard bound under both. Anything else fresh-REST-re-reads THAT ONE book (the
        backstop the arb had via its pre-fire REST re-check and a naive WS maker would lose).
        WS absent / no feed / unverifiable freshness / silent socket all degrade to a fresh
        REST read — never to a stale or empty book. ⛔ `error_reason` non-None means SKIP this
        market this cycle (a failed REST read); `md` is None only alongside an error.
        `fresh=True` is MANDATORY on every REST read here (CDN max-age=30 would stamp our
        clock onto a ≤30 s-old book).

        ⛔ `stats.requests` is incremented ONLY when a REST read is actually issued — a WS-fresh
        cycle spends NO request, and counting one would make the rate budget blind to the exact
        saving WS exists to produce. A scheduled re-verify and the §7-4 audit ARE real HTTP."""
        read_start = time.time()
        self.book_reads += 1
        book_src = "rest"
        if self._ws_down_since is not None:
            # FALLBACK STILL TRIES WS FIRST: an early return bypassed the WS read entirely, so
            # `ws_capped` was 0 by construction and the guard read that as "recovered" every other
            # cycle. A fresh WS book costs no request and IS the recovery evidence.
            # ⛔ NEAR-CAP GRACE → the cache is FROZEN until a frame POST-DATES the outage.
            # `_ws_try_serve`'s young-OR-socket-live rule reads `transactTime` age, which stops
            # advancing the moment the socket dies, so a pre-death frame stays "young" for up to
            # ws_stale_s while the venue book moves unseen — at >=0.7x cap, the forbidden exposure.
            # And `connected` is NOT the release signal: it goes True at handshake before any
            # frame arrives and the pre-death cache is never cleared, so keying on it served the
            # frozen cache on the first post-reconnect cycle. Release = the feed's newest frame is
            # NEWER than `_ws_last_down_ts`. A feed without `last_frame_age` stays frozen for the
            # whole grace: conservative direction.
            # ⚠️ ACCEPTED RESIDUAL: a SECOND bounce falling entirely between two cycle
            # observations during an active grace releases on a frame that pre-dates the second
            # gap. Bounded by the grace, the episode budget and the cycle gap; closing it exactly
            # needs the feed's frames-LOST semantics.
            _now_ts = time.time()
            _lfa = getattr(self.book_feed, "last_frame_age", None)
            try:
                # Same guard as `_ws_socket_live`: a feed stand-in / partially-built cache
                # must not throw — an escape here leaves run_cycle mid-read beside live
                # inventory. Unknown age → frozen, the conservative side. [r3 review NIT 4]
                _age = _lfa(_now_ts) if _lfa is not None else None
            except Exception:
                _age = None
            _cache_frozen = (self._ws_nearcap_grace
                             and not (_age is not None
                                      and _now_ts - _age > self._ws_last_down_ts))
            if self.book_source == "ws" and self.book_feed is not None and not _cache_frozen:
                # The SAME OR-rule as the healthy branch: an episode entered on quiet books must
                # be able to END on quiet books, or the window runs to expiry and halts.
                served = await self._ws_try_serve(slug, stats, degraded=True)
                if served is not None:
                    md, src, content_fresh = served
                    self.last_book_src[slug] = src
                    if src != "rest_reverify":
                        self._count_ws_serve(content_fresh)
                    return md, (time.time() - read_start) * 1000.0, None
            # Still dark → forced REST. The C-a cap does NOT apply (the bound is the REDUCED quote
            # set, and skipping held books during an outage is the harmful direction).
            # "ws_outage", NOT "rest_fallback": a shared label hid outages from the tape.
            self.last_book_src[slug] = "ws_outage"
            stats.ws_outage += 1
            stats.requests += 1
            try:
                book = await self.client._fetch_book(slug, fresh=True)
            except Exception as exc:
                log.warning(f"book read failed for {slug}: {exc!r}")
                return None, (time.time() - read_start) * 1000.0, "book_read_failed"
            self._last_verified_ts[slug] = time.time()   # a REST read IS a venue confirmation
            md = book.get("marketData") if isinstance(book, dict) else None
            return md, (time.time() - read_start) * 1000.0, None
        if self.book_source == "ws" and self.book_feed is not None:
            served = await self._ws_try_serve(slug, stats, degraded=False)
            if served is not None:
                md, src, content_fresh = served
                self.last_book_src[slug] = src
                if src == "rest_reverify":
                    # A re-verify caught a stale/lying cache: the REST value we already paid
                    # for serves, and this is NOT a ws read.
                    return md, (time.time() - read_start) * 1000.0, None
                self._count_ws_serve(content_fresh)
                if slug == self.ws_audit_slug:
                    await self._ws_audit(slug, md, stats)
                return md, (time.time() - read_start) * 1000.0, None
            # WS book absent/stale/unverifiable → fresh-REST backstop — but CAP the re-reads per
            # cycle [C-a]: past the cap a partial feed freeze would re-read the whole slate. Over
            # the cap, SKIP this book (no quote on an unverified book).
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
        self._last_verified_ts[slug] = time.time()   # a REST read IS a venue confirmation
        md = book.get("marketData") if isinstance(book, dict) else None
        return md, (time.time() - read_start) * 1000.0, None

    def _count_ws_serve(self, content_fresh: bool) -> None:
        """Book a ws serve to the arm that produced it [r2 CONCERN-2].

        ⛔ The N1 marker's `ws_fraction` is the ONE number standing between a shadow and a
        real-money ws flip, and its claim is "the WS path carried the run". A QUIET serve does
        not support that claim — it says the socket was alive, which the marker's other fields
        already say. Counting both in one bucket would let an all-quiet slate certify at
        ws_fraction 1.00 without a single book's stream ever being shown to work."""
        if content_fresh:
            self.ws_reads += 1
        else:
            self.ws_quiet_reads += 1

    def _ws_socket_live(self, now: float) -> bool:
        """Is the venue PUBLISHER provably still delivering on this socket? — the second arm
        of the OR-rule [receipt freshness 2026-08-13].

        ⛔ WHAT THIS DOES **NOT** PROVE: that any particular book's stream is still being served.
        Poly has no per-book sequence number, so a book we were silently unsubscribed from looks
        identical to a book nobody is trading. `_reverify_due` bounds that gap; this only removes
        the false-freeze reading of a correct, untraded book.

        A feed that cannot answer is NOT live: unknown must never serve, because the failure this
        guards is a maker quoting a book it cannot verify, not a maker spending a request.
        quoting a book it cannot verify, not a maker spending a request."""
        getter = getattr(self.book_feed, "last_frame_age", None)
        if getter is None:
            return False
        try:
            age = getter(now)
        except Exception as exc:      # a feed stand-in / partially-built cache must not throw
            log.warning(f"ws last_frame_age unreadable: {exc!r} — treating socket as NOT live")
            return False
        return age is not None and age <= self.ws_conn_live_s

    def near_cap_trip_contracts(self, marks: Mapping[str, Decimal]) -> Optional[Decimal]:
        """Where the NOTIONAL axis trips, expressed in CONTRACTS at the slate's own prices — the
        number a run plan needs. `marks` is the THRESHOLD map, i.e. `_marks(net=False)`.

        Dividing the trip notional by the cap-weighted average price gives back a contract count
        directly comparable to `max_total · frac` (the contract axis). They differ whenever the
        held books are priced away from p̄. None when no cap or no priced caps.

        ⛔ p̄ comes from `_cap_weighted_mark`, the SAME call the axis makes: pricing an unmarked
        capped book at <n> here while the axis prices it at p̄ tells the operator a bar the rail
        does not use. The printed number and the trip are one formula, pinned against each other.
        """
        if not self.max_total_contracts or self.max_total_contracts <= 0:
            return None
        cap_qty = sum((Decimal(c) for c in self.caps.values() if c > 0), _ZERO)
        if cap_qty <= _ZERO:
            return None
        p_bar = _cap_weighted_mark(self.caps, marks)     # the axis's OWN p̄, never a second copy
        if p_bar <= _ZERO:
            return None
        return Decimal(self.max_total_contracts) * WS_NEAR_CAP_F * p_bar

    def _announce_near_cap_trip(self) -> None:
        """Print both near-cap trip points ONCE per run, the first cycle marks exist.

        ⛔ Not in `prepare()` deliberately: prices are not known there, so a prepare-time line
        could only report the unpriced case and would be exactly wrong for the mixed slate this
        exists to warn about. First quoted cycle is the earliest HONEST moment.
        """
        if self._near_cap_announced:
            return
        # The printed number is the THRESHOLD (p̄ over every capped book), so it takes the GROSS
        # marks the axis's denominator takes: our own quote must not move the trip point.
        marks = self._marks(net=False)
        if not marks:
            return
        trip_nt = self.near_cap_trip_contracts(marks)
        if trip_nt is None:
            return
        self._near_cap_announced = True
        trip_ct = Decimal(self.max_total_contracts) * WS_NEAR_CAP_F
        log.warning(
            f"WS near-cap trip points for this slate: CONTRACTS axis at {trip_ct} of "
            f"{self.max_total_contracts}; NOTIONAL axis at ${trip_nt:.2f} of held value — "
            f"which is {trip_ct} contracts ONLY if the inventory is priced at the "
            f"cap-weighted average. Held dollars are what count, so inventory concentrated in "
            f"the expensive books trips it sooner (a mid+longshot slate can trip near ~0.37 "
            f"of the contract cap).")

    def _ws_slate_servable(self) -> bool:
        """Would the seam serve EVERY book on the slate from the ws cache right now?

        The escape hatch that keeps recovery reachable once the probation budget is spent. It asks
        the seam's own question — the book exists in the cache, it is not WS-DEAD, and its content
        is young **or** the socket is provably live — for the WHOLE slate, not the reduced set. A
        book that would produce `ws_capped`/`ws_outage` fails it, which is exactly the population
        a partial freeze consists of.

        ⛔ FREE AND SIDE-EFFECT-FREE: cache reads only. No REST, no request, no order, no clock
        refresh, no ws-serve counted — the alternative escape costs a place/cancel pair on every
        dropped book, which is the churn the budget exists to stop.

        ⛔ NOT "reduced-set evidence": it is a direct statement about every book the run would
        quote, and the caller ANDs it with `clean_last`.

        False on an empty slate or a feed that cannot answer: unknown never recovers.
        """
        if self.book_feed is None or not self.ticks:
            return False
        now = time.time()
        socket_live = self._ws_socket_live(now)
        for slug in self.ticks:
            if slug in self._ws_dead_books:
                return False
            try:
                md = self.book_feed.get_book_md(slug)
            except Exception as exc:
                log.warning(f"ws slate-servability read failed for {slug}: {exc!r} — treating "
                            f"the slate as NOT recovered")
                return False
            if md is None:
                return False
            age = transact_age_s(md.get("transactTime"), now)
            young = age is not None and age <= self.ws_stale_s
            if not (young or socket_live):
                return False
        return True

    def _reverify_due(self, slug: str, now: float) -> bool:
        """Is this book past its fresh-REST re-confirmation period?

        ⛔ An UNSEEDED clock is NOT due. Treating "never verified" as due would fresh-REST the
        WHOLE slate on cycle 1 (95 requests in one cycle on the shadow slate) — precisely the
        poll burst the WS feed exists to delete. The first ws serve seeds the clock one full
        period in the PAST, so the book is due on the cycle it is first served and the burst is
        bounded instead by `WS_REVERIFY_MAX_PER_CYCLE` (see `_ws_try_serve`)."""
        ts = self._last_verified_ts.get(slug)
        return ts is not None and (now - ts) > self.ws_reverify_s

    def _plan_reverifies(self, now: float, slugs: Optional[list[str]] = None) -> None:
        """Choose which books this cycle may re-verify — OLDEST CLOCK FIRST [r2 CONCERN-5].

        ⛔ NOT iteration order. `WS_REVERIFY_MAX_PER_CYCLE` creates a queue by construction (a
        whole slate seeds its clocks in one cycle and comes due in one cycle), and a queue
        drained in `sorted(self.ticks)` order is not a queue at all: the alphabetically-early
        books re-verify on schedule forever while the tail's interval stretches without limit —
        and the tail is exactly where a silently-unsubscribed book would hide. Draining by age
        makes the worst-case interval a function of slate size (`N·requote_s/MAX`), which is
        the bound the constant's own comment claims.

        Also emits the `_reverify_backlog` warning that comment promised and never wrote: when
        the OLDEST book has gone more than 2× its period unconfirmed, the drain rate is losing
        to the arrival rate and the per-book hard bound is no longer being honoured. Warned,
        not halted — the books are still being served under the OR-rule and the honest
        response is to shrink the slate or shorten the period, not to kill a live run.

        The §7-4 audit slug is excluded on a healthy cycle: its own fresh-REST read runs every
        cycle and refreshes the same clock, so scheduling a second one would spend a request to
        re-learn what the audit just proved (`_ws_audit` says so in its docstring — this makes
        it true). It is NOT excluded during a fallback episode, where the audit does not run."""
        self._reverify_plan_cycle = self.cycle_index
        pool = list(self._last_verified_ts) if slugs is None else list(slugs)
        if self._ws_down_since is None and self.ws_audit_slug is not None:
            pool = [s for s in pool if s != self.ws_audit_slug]
        due = sorted((ts, s) for s in pool
                     if (ts := self._last_verified_ts.get(s)) is not None
                     and (now - ts) > self.ws_reverify_s)
        # Permission prefix WIDER than the per-cycle work cap: sized to the cap, two undrainable
        # books permanently occupied both slots and silently disabled the per-book bound for the
        # whole slate. `stats.ws_reverifies` does the actual bounding at the drain site.
        self._reverify_allowed = {s for _ts, s in due[:WS_REVERIFY_MAX_PER_CYCLE * 4]}
        if due:
            oldest_ts, oldest_slug = due[0]
            if (now - oldest_ts) > 2.0 * self.ws_reverify_s:
                log.warning(
                    f"WS re-verify BACKLOG: {len(due)} book(s) overdue, oldest {oldest_slug} "
                    f"unconfirmed for {now - oldest_ts:.0f}s (> 2x the {self.ws_reverify_s:.0f}s "
                    f"period) — the {WS_REVERIFY_MAX_PER_CYCLE}/cycle drain is losing to the "
                    f"arrival rate, so the per-book hard bound is NOT being honoured. Shrink "
                    f"the slate or shorten --ws-reverify-s.")

    def _mark_ws_book_dead(self, slug: str, why: str) -> None:
        """REST-ONLY for THIS book until a genuinely fresh WS frame arrives for it.

        ⛔ ONE OWNER OF THE TRANSITION [2026-09-04]. Two detectors reach it — the
        `WS_STALE_CACHE_DEAD_N` re-verify streak, and the fresh-guard-refusal evidence in
        `_ws_dead_by_guard` — and a second copy of the mark is how two rails come to disagree
        about what "dead" means. `_ws_book_or_none` owns the RECOVERY (young content clears it).
        Idempotent: a book already marked logs nothing, so the line is one per SPELL.
        """
        if slug in self._ws_dead_books:
            return
        self._ws_dead_books.add(slug)
        log.error(
            f"⚠️ WS BOOK DEAD: {slug} {why}. REST-ONLY for this book (it no longer counts as "
            f"ws-served) until a fresh frame arrives for it.")

    def _ws_dead_by_guard(self, slug: str) -> None:
        """A crossing-guard refusal on a FRESH read is proof the served WS frame is stale
        [2026-09-04, the private design notes].

        THE FOUR CONJUNCTS, and each one is load-bearing:
          · the guard's own read was `rest_fresh` and it REFUSED — an `unread` refusal saw no
            book at all and proves nothing about ours;
          · that read had BOTH sides readable — an unreadable-side refusal never compared a
            price, so it carries no evidence about our frame;
          · that read's `transact_age_s` is within `ws_stale_s`. ⛔ A refusal on a FROZEN guard
            read must NOT kill the WS book — that is the frozen-ORIGIN class the newer-witness
            rule handles, and killing the cache there falls back onto the frozen body;
          · the maker SERVED this book from the WS cache this cycle — a `rest_*` serve has no
            WS claim to retract.

        ⛔ INSTRUMENTATION-SAFE, like `_capture_guard_book`: it runs inside `_place`'s refusal
        branch and may never raise there.
        """
        try:
            seen = self._guard_seen.get(slug)
            if not seen or not seen.get("refused") or seen.get("src") != "rest_fresh":
                return
            if seen.get("bid") is None or seen.get("ask") is None:
                return
            age = seen.get("age_s")
            if age is None or age > self.ws_stale_s:
                return
            if self.last_book_src.get(slug) not in ("ws", "ws_quiet"):
                return
            self._mark_ws_book_dead(
                slug, f"the crossing guard refused a placement against its OWN fresh read "
                      f"(age {age:.1f}s) while this book was served from the WS cache "
                      f"({self.last_book_src.get(slug)}) — the served frame is stale")
        except Exception as exc:                            # never fatal
            log.debug(f"_ws_dead_by_guard({slug}) skipped: {safe_exc(exc)}")

    def _ws_book_servable(self, slug: str, md: dict, now: float) -> bool:
        """The OR-rule, ONE copy: this cached book may be believed iff it is not WS-dead and
        (its content is young OR the socket is provably live) [receipt freshness 2026-08-13].

        The two callers are `_ws_try_serve` (may we QUOTE off this book) and
        `_ws_guard_witness` (may this book witness against a frozen REST origin). They ask the
        same question, so they read the same rule — a second copy is how the guard rail drifted
        stricter than the quote rail.

        ⚠️ NO SIDE EFFECTS and no request: a pure read of the feed's own cache. The ws-dead
        RECOVERY (clearing the mark on a genuinely fresh frame) stays with `_ws_try_serve`,
        which owns that transition.
        ⚠️ An UNKNOWN content age (no `transactTime`) is not refused here — `_ws_try_serve`
        serves such a frame on a live socket and that behaviour is unchanged. The witness
        refuses it itself, fail closed, because its stamp IS the comparison."""
        if slug in self._ws_dead_books:
            return False
        age = transact_age_s(md.get("transactTime"), now)
        young = age is not None and age <= self.ws_stale_s
        return young or self._ws_socket_live(now)

    async def _ws_try_serve(self, slug: str, stats: "CycleStats",
                            *, degraded: bool) -> Optional[tuple[dict, str, bool]]:
        """The OR-rule + its hard bound. Returns `(md, book_src, content_fresh)` to serve, or
        None to fall through to this branch's REST path. NEVER returns a None `md`.

        Servable iff the cached book EXISTS and (content is young OR the socket is live).
        Content-young is the strong evidence and tapes as `ws`; socket-live-only is the QUIET
        case and tapes as `ws_quiet` — a weaker claim. Underneath both: every `ws_reverify_s`
        the book is re-confirmed against ONE fresh REST read, charged to `stats.requests` and
        bounded to `WS_REVERIFY_MAX_PER_CYCLE` per cycle.

        ⛔ HOW A DISAGREEMENT IS READ DEPENDS ON WHICH ARM SERVED:
        • YOUNG arm, `MOVED` — BENIGN: the frame is younger than `ws_stale_s`, so the stream is
          demonstrably being delivered. The ws value serves and the clock refreshes.
        • QUIET arm, `MOVED` — a STALE CACHE, not motion: the premise of a quiet serve is
          "nothing changed" and the venue just said something DID. That is the
          silent-unsubscribe signature, so the fresh REST book SERVES (`rest_reverify`), it
          logs at ERROR, and the clock is NOT refreshed. `WS_STALE_CACHE_DEAD_N` in a row
          marks the book WS-DEAD until a fresh snapshot arrives.
        • `DIVERGED` — the fresh-but-WRONG class on EITHER arm, same handling plus its own
          louder counter.
        • `UNAVAILABLE` — no evidence either way: the ws book still serves and stays DUE."""
        md = self.book_feed.get_book_md(slug)
        if md is None:
            return None
        now = time.time()
        age = transact_age_s(md.get("transactTime"), now)
        young = age is not None and age <= self.ws_stale_s
        if slug in self._ws_dead_books:
            # REST-ONLY until a genuinely fresh snapshot arrives for THIS book. Young content is
            # the only thing that can produce that, so it is the only thing that clears the mark.
            # Falling through returns None, which puts the book on the C-a backstop and lets it
            # count toward the partial-freeze escalation — correctly: for this book the feed IS
            # frozen.
            if not young:
                return None
            self._ws_dead_books.discard(slug)
            self._ws_stale_cache_streak.pop(slug, None)
            log.warning(f"WS book {slug} RECOVERED — a fresh frame arrived (content age "
                        f"{age:.1f}s), clearing the ws-dead mark")
        if not self._ws_book_servable(slug, md, now):
            return None
        if self._reverify_plan_cycle != self.cycle_index:
            # Lazy plan for any path that reaches the seam outside the quote loop (and so the
            # ordering rule cannot be bypassed by a caller that forgot to plan).
            self._plan_reverifies(now)
        if (slug in self._reverify_allowed and self._reverify_due(slug, now)
                and stats.ws_reverifies < WS_REVERIFY_MAX_PER_CYCLE):
            stats.ws_reverifies += 1
            self.ws_reverifies += 1
            # ws_touch / ws_tt are the compare's SNAPSHOT of the LIVE feed frame, taken before its
            # awaited REST read; re-reading `md` here would print a different (later) frame.
            verdict, rest_md, ws_touch, ws_tt = await self._ws_rest_touch_compare(
                slug, md, stats, why="re-verify")
            stale_cache = verdict == _WS_CMP_DIVERGED or (verdict == _WS_CMP_MOVED and not young)
            if stale_cache and rest_md is not None:
                self.ws_reverify_stale_cache += 1
                stats.ws_reverify_stale_cache += 1
                streak = self._ws_stale_cache_streak.get(slug, 0) + 1
                self._ws_stale_cache_streak[slug] = streak
                if verdict == _WS_CMP_DIVERGED:
                    self.ws_reverify_mismatches += 1
                    log.error(
                        f"⚠️ WS RE-VERIFY MISMATCH on {slug}: cached ws touch "
                        f"{ws_touch} vs fresh REST {touch_from_md(rest_md)[:2]} at "
                        f"the SAME transactTime {ws_tt!r} — the cache is WRONG. "
                        f"Serving the REST value; this book re-checks EVERY cycle until it "
                        f"agrees. Mismatch {self.ws_reverify_mismatches}. If it repeats, halt "
                        f"and fall back to --book-source rest.")
                else:
                    # ⛔ The silent-unsubscribe signature, and with C-a unreachable on a
                    # frame-alive socket this re-verify is the ONLY per-book detector left. ERROR,
                    # not INFO: on the quiet arm this is the failure, not the normal case.
                    log.error(
                        f"⚠️ WS STALE CACHE on {slug}: served on socket-liveness alone (content "
                        f"age {age if age is None else round(age, 1)}s), but fresh REST shows "
                        f"the book MOVED — ws touch {ws_touch} @ "
                        f"{ws_tt!r} vs REST {touch_from_md(rest_md)[:2]} @ "
                        f"{rest_md.get('transactTime')!r}. We never received that frame on a "
                        f"socket we can prove is alive — the SILENT-UNSUBSCRIBE signature. "
                        f"Serving the REST value; streak {streak}/{WS_STALE_CACHE_DEAD_N}.")
                if streak >= WS_STALE_CACHE_DEAD_N:
                    self._mark_ws_book_dead(
                        slug, f"disagreed with fresh REST on {streak} consecutive re-verifies "
                              f"— its stream is gone even though the socket is not")
                # Clock deliberately NOT refreshed — a disagreeing cache is re-read every cycle.
                return rest_md, "rest_reverify", False
            if verdict != _WS_CMP_UNAVAILABLE:
                # Successful venue contact: the touch was verified correct NOW, so the clock
                # refreshes. But the STREAK clears only on evidence the stream is ALIVE: an AGREE
                # at a DIFFERING transactTime means the venue book advanced past our frame and the
                # prices merely coincide — a dropped book oscillating on the cent grid reads that
                # way every check, and a full clear would defeat WS_STALE_CACHE_DEAD_N forever.
                self._last_verified_ts[slug] = now
                if verdict == _WS_CMP_AGREE_STALE_TT and not young:
                    streak = self._ws_stale_cache_streak.get(slug, 0)
                    if streak > 1:
                        self._ws_stale_cache_streak[slug] = streak - 1
                    else:
                        self._ws_stale_cache_streak.pop(slug, None)
                else:
                    self._ws_stale_cache_streak.pop(slug, None)
        else:
            # ⛔ SEEDED **DUE**, NOT SEEDED FRESH. `setdefault(slug, now)` handed every book a
            # full `ws_reverify_s` of UNVERIFIED quoting at first serve — the only per-book freeze
            # detector there is, disarmed for the first fifteen minutes of every run, and books
            # were quoted off a cache whose `transactTime` had not advanced for 1,300+ s inside
            # exactly that window. Seeding one full period in the past makes the book due on the
            # cycle it is FIRST served; the drain is still bounded. Never moves an existing clock.
            self._last_verified_ts.setdefault(slug, now - self.ws_reverify_s)
        if degraded:
            # "ws_degraded", not "ws": a fill during a fallback episode must be separable from a
            # healthy-cycle fill on the fill tape alone.
            return md, "ws_degraded", young
        return md, ("ws" if young else "ws_quiet"), young

    async def _ws_rest_touch_compare(
        self, slug: str, ws_md: dict, stats: "CycleStats", *, why: str,
    ) -> tuple[str, Optional[dict], tuple[Optional[Decimal], Optional[Decimal]], Optional[str]]:
        """ONE fresh-REST read beside a ws book, classified.
        `(verdict, rest_md, ws_touch, ws_tt)`.

        Shared by the §7-4 audit and the periodic re-verify — two copies of this classification
        would drift.

        ⛔ A CALLER MUST HANDLE ALL FIVE verdicts: omitting `_WS_CMP_AGREE_STALE_TT` is exactly
        how `_ws_audit` came to test `verdict == _WS_CMP_AGREE` and let the fifth fall through
        into its divergence branch.

        • `_WS_CMP_UNAVAILABLE`    — the REST read failed or returned no book. No verdict.
        • `_WS_CMP_AGREE`          — touch PRICES match at the SAME transactTime.
        • `_WS_CMP_AGREE_STALE_TT` — PRICES match, transactTime DIFFERS: the prices are right,
          it is proof of the STREAM being alive that is missing. AGREEMENT for the audit;
          WITHHELD EVIDENCE for the re-verify's streak decay.
        • `_WS_CMP_MOVED`          — prices differ AND transactTime differs: a correct feed on
          an active book.
        • `_WS_CMP_DIVERGED`       — prices differ at the SAME (or undeterminable) transactTime:
          same book version, different content. The fresh-but-WRONG class.

        PRICES only — the third `touch_from_md` element is size/context, which legitimately
        differs between a cached frame and a fresh REST ladder. Charges one request.

        The ws touch is read exactly ONCE at entry and the snapshot is RETURNED so no caller
        re-derives it. ⚠️ DEFENSE-IN-DEPTH, not a fix for an observed race: it stops this
        function depending on a feed-internal aliasing guarantee it does not own."""
        ws_bid, ws_ask, _ws_tob = touch_from_md(ws_md)
        ws_touch = (ws_bid, ws_ask)
        ws_tt = ws_md.get("transactTime")
        stats.requests += 1
        try:
            book = await self.client._fetch_book(slug, fresh=True)
        except Exception as exc:
            log.warning(f"ws-{why} REST read failed for {slug}: {exc!r} — skipped")
            return _WS_CMP_UNAVAILABLE, None, ws_touch, ws_tt
        rest_md = book.get("marketData") if isinstance(book, dict) else None
        if rest_md is None:
            log.warning(f"ws-{why} REST book empty for {slug} — skipped")
            return _WS_CMP_UNAVAILABLE, None, ws_touch, ws_tt
        rest_tt = rest_md.get("transactTime")
        tt_differs = ws_tt is not None and rest_tt is not None and ws_tt != rest_tt
        if ws_touch == touch_from_md(rest_md)[:2]:
            verdict = _WS_CMP_AGREE_STALE_TT if tt_differs else _WS_CMP_AGREE
        elif tt_differs:
            verdict = _WS_CMP_MOVED
        else:
            verdict = _WS_CMP_DIVERGED
        return verdict, rest_md, ws_touch, ws_tt

    async def _ws_audit(self, slug: str, ws_md: dict, stats: "CycleStats") -> None:
        """§7-4's belt-and-braces: the cycle USES the ws book it just gated servable; this reads
        the SAME market from fresh REST and compares the touch. Divergence is the one failure
        class nothing else catches (a fresh-but-WRONG ws book). Audit only — the ws value still
        quotes; a failed REST read is a skipped audit, never a cycle error.

        Cost: ONE origin request per cycle on the `--ws-audit-slug` market. That read is also a
        venue confirmation, so it refreshes the book's re-verify clock — the audit slug never
        needs a separate periodic re-verify.

        The comparison itself lives in `_ws_rest_touch_compare`, shared with the periodic
        re-verify: the two differ in when they fire and what they do with the answer, never in
        how a disagreement is judged.

        ⛔ COUNTER ERA — `ws_audit_divergences` is only meaningful from the  fix
        (2026-08-20) forward, and the teardown line that prints it spans that break; pre-fix
        counts are UNJUDGEABLE and must not be compared against a post-fix one.

        ⛔ AND `ws_audit_agree_stale_tt` MUST BE PRINTED BESIDE THEM, not folded into agreement.
        The audit slug is EXEMPT from the periodic re-verify, so the stale-cache streak — the ws
        path's only per-book silent-unsubscribe detector — never runs on it, and a silently
        unsubscribed book with a STICKY touch lands every check in AGREE_STALE_TT while the
        teardown reports it CLEAN. A high count is a REVIEW trigger, not by itself a fault."""
        verdict, rest_md, ws_touch, ws_tt = await self._ws_rest_touch_compare(
            slug, ws_md, stats, why="audit")
        if verdict == _WS_CMP_UNAVAILABLE or rest_md is None:
            return
        self.ws_audit_checks += 1
        self._last_verified_ts[slug] = time.time()
        if verdict in (_WS_CMP_AGREE, _WS_CMP_AGREE_STALE_TT):
            # ⛔ BOTH agree-verdicts are agreement FOR THE AUDIT'S QUESTION: it asks "are the ws
            # touch PRICES the venue's?", and in both the answer is yes. Letting
            # `_WS_CMP_AGREE_STALE_TT` fall through a bare `== _WS_CMP_AGREE` check counted equal
            # tuples as a DIVERGENCE. But it is COUNTED, never silent: the audit slug has no
            # periodic re-verify, so this bucket is its only silent-unsubscribe tell.
            if verdict == _WS_CMP_AGREE_STALE_TT:
                self.ws_audit_agree_stale_tt += 1
            return
        # `ws_touch` / `ws_tt` are the compare's SNAPSHOT — every line below judges and prints the
        # instant that was actually compared, without re-deriving it from `ws_md`.
        rest_touch = touch_from_md(rest_md)[:2]
        if verdict == _WS_CMP_MOVED:
            # The transactTime DISCRIMINATOR: a differing REST transactTime means the book
            # legitimately MOVED between the frame and now — a correct feed on an active book.
            self.ws_audit_moved += 1
            log.info(f"ws-audit: {slug} moved between frame and check "
                     f"(ws {ws_touch} @ {ws_tt!r} vs REST {rest_touch} @ "
                     f"{rest_md.get('transactTime')!r}) — benign motion, "
                     f"{self.ws_audit_moved} moved / {self.ws_audit_checks} checks")
            return
        # EQUAL transactTime with different prices is the genuine fresh-but-WRONG class — same
        # book version, different content — and that alone earns the ERROR.
        self.ws_audit_divergences += 1
        log.error(f"⚠️ WS-AUDIT DIVERGENCE on {slug}: ws touch {ws_touch} vs REST touch "
                  f"{rest_touch} at the SAME transactTime {ws_tt!r} — "
                  f"divergence {self.ws_audit_divergences}/{self.ws_audit_checks} checks. "
                  f"A fresh-but-wrong ws book is the uncovered failure class; if this "
                  f"repeats, halt and fall back to --book-source rest.")

    def _seed_touch(self, slug: str, tick: Decimal) -> Optional[tuple[Decimal, Decimal]]:
        """The prices to quote this EMPTY book at, or None = keep skipping it [P1b].

        FOUR conditions, ALL required, and every one of them fails towards today's behaviour
        (skip). The CALLER owns the fifth and strictest one — the book must be EMPTY on BOTH
        sides.
          1. the book is on the launcher's `--seed-last-touch-slugs` list — a DECLARED NON-SPORTS
             fixed seat. ⛔ THE WHOLE ELIGIBILITY TEST, and it is not `_slate_pinned`
            : that set is written only from a hot-slate
             entry, so requiring it made the seed dead on every declared seat on a run with no
             slate file — while `_slate_pinned` on its own would let a declared SPORTS seat seed
             (the reseater pins those too). The argv list already carries both halves of the
             population, resolved where the class rules live
             (`poly_probe_night._seed_last_touch_argv`, the complement of
             `_maintenance_guard_argv`); the maker knows no class rules.
          2. a recorded two-sided touch exists, from this process or the durable record.
          3. it is younger than `EMPTY_BOOK_TOUCH_MAX_S`.
          4. BOTH prices land on THIS book's tick grid and are strictly inside (0, 1).
             ⛔ REJECTED, NEVER ROUNDED ONTO THE GRID: the record can predate a venue tick change
             or a re-seat of the same slug at a different tick, and moving a recorded price by up
             to half a tick to make it postable is inventing the number this rail exists to
             avoid. The venue rejects an off-grid price anyway; skipping is the honest answer.
        """
        if slug not in self.seed_last_touch_slugs:
            return None
        touch = self.last_touch.get(slug) or self._durable_last_touch.get(slug)
        if touch is None:
            return None
        seed_bid, seed_ask, touch_ts = touch
        if time.time() - touch_ts > EMPTY_BOOK_TOUCH_MAX_S:
            return None
        if seed_bid >= seed_ask:
            # A recorded crossed/locked touch is not a market to quote back into.
            return None
        if tick <= _ZERO:
            return None
        for _p in (seed_bid, seed_ask):
            if not (_ZERO < _p < _ONE) or _p % tick != _ZERO:
                return None
        return (seed_bid, seed_ask)

    async def _quote_market(self, slug: str, stats: CycleStats) -> None:
        tick = self.ticks[slug]
        # ⛔ CLEARED AT THE TOP, not only where the gate runs: this method has several early
        # returns that never reach the parked-alive gate, and a flag left set from a previous
        # cycle would tape a book `parked_alive_exposure` on a cycle nothing judged.
        self._parked_alive_blocked.discard(slug)
        # `getattr` because the park-seat tests drive this method on a `__new__`-built maker that
        # never ran `__init__` — initialise here, never a class-level mutable.
        self._reach_refused = getattr(self, "_reach_refused", set()) - {(slug, "bid"),
                                                                        (slug, "ask")}
        self._unresolved_refused = getattr(self, "_unresolved_refused", set()) - {
            (slug, "bid"), (slug, "ask")}
        self._reach_sized = {k: v for k, v in getattr(self, "_reach_sized", {}).items()
                             if k[0] != slug}
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
        # placements) puts last_trade_age_s on a different clock from read_ms [r7 nit].
        book_stats = parse_book_stats(md, time.time())
        # Net-of-self touch, computed BEFORE the quote choice [net-of-self forensics].
        # ⛔ Computed AFTER `pick_quotes` it ate the RAW touch — so once our own order was alone
        # at the best of a side, the quote re-derived from OUR OWN price and never followed the
        # market net of us (measured live: a bid pinned for hours, many ticks through the net
        # touch). Real only: a shadow/DRY maker's orders never rest, so the raw touch already IS
        # the market net of us. Belief remainder = size − filled_qty. ⚠️ Belief can OVERSTATE our
        # presence, and over-subtraction reports a WIDER market-behind-us than exists — the
        # flattering direction; the helper promotes past the emptied level rather than clamping.
        _own_b = self.resting.get((slug, "bid")) if self.real else None
        _own_a = self.resting.get((slug, "ask")) if self.real else None
        if self.real:
            net_bid, net_ask = net_of_self_touch(
                md,
                (_own_b.price, Decimal(_own_b.size) - _own_b.filled_qty) if _own_b else None,
                (_own_a.price, Decimal(_own_a.size) - _own_a.filled_qty) if _own_a else None)
        else:
            net_bid, net_ask = bid, ask
        # ⛔ THE PARK BRANCH IS TAKEN BEFORE THE TWO-SIDED REQUIREMENT. A park book is routinely
        # ONE-SIDED — its battery requires only the side it rests on — so `no_two_sided_touch`
        # would skip (and eventually PULL) the seat every cycle on exactly the books the class
        # exists for. It needs its OWN anchor side and nothing else.
        park = self.park.get(slug)
        park_side = park[0] if park is not None else None
        #: The three park-cycle tape statuses, kept APART [round-3 nit + concern 2]:
        #: `park_hold` = the anchor was unreadable and the resting order was KEPT (presence);
        #: `park_cancel` = it was stood down as out-of-position (the opposite of presence, so
        #: folding it into `park_hold` would inflate every presence count read off this tape);
        #: `park_dark` = the seat can neither park nor exit and quotes nothing at all.
        _park_why: Optional[str] = None
        _park_cancel = False
        _park_dark = False
        #: The EMPTY-BOOK SEED's prices for this cycle, or None [P1b]. Initialised HERE, above the
        #: park branch, because the row's `reason_*` cells read it after the two paths rejoin — and
        #: a PARK book never seeds: it is one-sided BY DESIGN, so `no_two_sided_touch` is not a
        #: state it reaches, and its anchor side is priced by the park assembly.
        seeded_touch: Optional[tuple[Decimal, Decimal]] = None
        if park is not None:
            # ⛔ THE PARK PRICE IS DERIVED FURTHER DOWN, NOT HERE. It needs the signed INVENTORY
            # (or a reducing placement gets priced at the park distance, where it never drains),
            # which is read below beside the allow flags. Here we take only the raw touch as the
            # default — the EXIT price on both sides — and record that a park never improves.
            my_bid, my_ask = bid, ask
            improved = False
        else:
            # ⛔ THE EMPTY-BOOK SEED [continuous maker P1b] — the ONLY prices in this method that
            # do not come from a book this cycle read. It exists for one population: a DECLARED
            # non-sports fixed seat whose book came back EMPTY, where `no_two_sided_touch` meant
            # the seat waited for somebody else to make a market before it could quote at all.
            # ⚠️ The venue DOCUMENTS that books reopen empty after maintenance; our own tape has
            # not yet observed a venue-side empty (four windows unused), so the book-time this
            # addresses is UNMEASURED — the first real night's `no_two_sided_touch` count on the
            # declared seats is the read.
            # ⛔ `and`, NOT `or` — the book must be EMPTY on BOTH sides
            # 2026-09-10 BLOCKING 1]. On a ONE-SIDED book the seed is a TAKER order: a book
            # reopening with a lone ask far above a recorded two-sided touch had us post an ask UNDER a
            # live offer and get lifted — a full-size fill there is a real loss, from a path
            # whose whole justification is passive presence. A one-sided book keeps skipping on
            # `no_two_sided_touch` exactly as it did before P1b.
            seeded_touch = (self._seed_touch(slug, tick)
                            if bid is None and ask is None else None)
            if (bid is None or ask is None) and seeded_touch is None:
                await self._skip_market(stats, slug, tick, None, "no_two_sided_touch",
                                        read_ms=read_ms, gap_s=gap_s)
                return
            # Quote off the touch NET of our own size wherever a net side exists. A side whose net
            # is None while WE have an order resting there is a side our own order is the whole of
            # — no market behind us to measure from, so that side HOLDS at our current resting
            # price. ⛔ The raw-touch fallback is NOT allowed there: raw includes us, and
            # pick_quotes would improve OVER OUR OWN PRICE, ratcheting a lone quote one tick per
            # cycle toward the far side. DRY/shadow: net == raw, nothing changes.
            q_bid = net_bid if net_bid is not None else bid
            q_ask = net_ask if net_ask is not None else ask
            try:
                if seeded_touch is not None:
                    # ⛔ AT THE RECORDED TOUCH, NEVER INSIDE IT [P1b]: `pick_quotes` is
                    # deliberately not called. Improving on prices nobody is showing INVENTS a
                    # market — the seed's only claim is "this is where this book last really had
                    # two sides", and a tick inside that is a claim about a book that has no other
                    # side to measure against. `improved=False` for the same reason.
                    my_bid, my_ask, improved = seeded_touch[0], seeded_touch[1], False
                else:
                    my_bid, my_ask, improved, _ = pick_quotes(q_bid, q_ask, tick)
                # ── THE JOIN ARM ──────────────────────────────────────────
                # ⛔ APPLIED TO THE RESULT, never inside `pick_quotes` — that function is pinned
                # byte-equal to the shadow collector's copy, and parameterising it would either
                # break the pin or silently fork the two.
                # `join` posts AT the touch on BOTH sides — never one, which is `pick_quotes`'s own
                # NEVER-ONE-SIDE invariant restated at the mode level. `improved` is False by
                # construction, because nothing was improved.
                # ⛔ IT TOUCHES PRICES ONLY: every cap, the reduce-only lanes, the latch, the mark
                # tripwire and the park assembly all run exactly as before.
                if self.quote_mode == QUOTE_MODE_JOIN and improved:
                    my_bid, my_ask, improved = q_bid, q_ask, False
            except ValueError:
                # Tape the RAW touch, matching every other row's bid/ask columns — the net
                # values have their own columns [review CONCERN-2].
                await self._skip_market(stats, slug, tick, (bid, ask), "unquotable_touch",
                                        read_ms=read_ms, gap_s=gap_s)
                return
            # ⛔ THE SEEDED CYCLE SKIPS THE None-NET HOLD [P1b]: on an EMPTY book both nets are
            # None, so this block would hold both sides at our own resting prices — which is the
            # ratchet it exists to prevent, applied to a book with no market at all. The seed's
            # prices are the whole point; the self-cross check below still runs.
            if self.real:
                if seeded_touch is None:
                    if net_bid is None and _own_b is not None:
                        my_bid, improved = _own_b.price, False
                    if net_ask is None and _own_a is not None:
                        my_ask, improved = _own_a.price, False
                if my_bid >= my_ask:
                    # ⛔ INVARIANT VIOLATION, believed UNREACHABLE: a None-net override can only
                    # move my_bid DOWN to the raw bid / my_ask UP to the raw ask, and pick_quotes
                    # guaranteed my_bid < my_ask. Reachable only if `net_of_self_touch`'s promotion
                    # or the resting-price/book-top identity changes. Loud, distinct status.
                    log.error(f"self_cross_hold on {slug}: held quotes cross "
                              f"({my_bid}/{my_ask}) — the None-net hold invariant is "
                              f"broken; both resting orders stand this cycle. Investigate "
                              f"net_of_self_touch before trusting quotes on this book.")
                    await self._skip_market(stats, slug, tick, (bid, ask),
                                            "self_cross_hold",
                                            read_ms=read_ms, gap_s=gap_s)
                    return

        self.unquotable_streak[slug] = 0
        # LAST cycle's net touch, read BEFORE this cycle overwrites it — the input
        # `quote_reason` needs to say whether the market moved. A skipped cycle
        # leaves the last QUOTED cycle's value, which is the right comparison: the question is
        # "has the touch we quoted against changed", not "has it changed in exactly 10s".
        prev_net = self._last_net.get(slug, (None, None))
        # ⛔ BOTH SIDES OR NOTHING [park lane]. `_marks()` averages this tuple and `mark_pnl` reads
        # its liquidation side; a one-sided park book would store a None and raise inside the
        # loss/mark machinery.
        if bid is not None and ask is not None:
            self.last_touch[slug] = (bid, ask, self.last_read_ts[slug])
            # THE EMPTY-BOOK SEED's memory [P1b], mirrored only for the seed-eligible books and
            # flushed durably once per cycle by `run_cycle`. ⛔ Two-sided by construction — it
            # hangs off this branch, which is the only place a two-sided touch is established.
            if slug in self.seed_last_touch_slugs:
                self._durable_last_touch[slug] = self.last_touch[slug]
            # THE SAME CYCLE'S NET SIDES, stamped together so one read_ts dates both
            #. No second derivation: these are the values computed at the
            # top of this cycle. In DRY/shadow they ARE the raw touch (net == raw above).
            self.last_touch_net[slug] = (net_bid, net_ask)
        # (net_bid/net_ask were computed at the top of the cycle, before the quote choice —
        # see the net-of-self forensics block above; one computation, all consumers below share it.)
        inv = self.inventory.get(slug, _ZERO)
        book_size = self.sizes.get(slug, self.size)
        book_cap = self.caps.get(slug, self.cap)
        pulled = self.book_pulled(slug)
        # ⛔ ONE reduce-only implementation, THREE flags. Both call sites below use THIS name, and
        # the predicate lives in `reduce_only_now` — the hot-settings raise rail is the third
        # reader, and a copy there is how a fourth condition gets missed.
        # ⛔ ONE clock-guard read per book per cycle, threaded into BOTH consumers (the lane
        # predicate and `hold_cause` below) — see `reduce_only_now`.
        clock_guard_now = self.clock_guard_active(slug)
        # ⛔ ONE clock read per book per cycle, threaded for the same reason: `hold_cause` below
        # must name the term the SIZING decision actually used, not a second look at the clock.
        maintenance_guard_now = self.maintenance_guard_active(slug)
        reduce_only_now = self.reduce_only_now(slug, pulled=pulled, clock_guard=clock_guard_now,
                                               maintenance_guard=maintenance_guard_now)
        if reduce_only_now:
            # WIND-DOWN and its PER-BOOK form, the calendar PULL (`--pull-at slug:epoch`): one
            # book reaches its own deadline and works its inventory off reduce-only while the rest
            # of the slate keeps quoting normally. Same machinery, same guards: cap-0 is the
            # DOCUMENTED deliberate reduce-only mode — a flat book quotes nothing, a positioned
            # book quotes only the reducing side. The size clamp is the FLIP GUARD: a full-size
            # reduce order can fill through flat into a NEW position, and min(size, |inv|) makes
            # the flip unrepresentable rather than unlikely.
            book_cap = 0
            if inv.copy_abs() >= 1:
                book_size = min(book_size, int(inv.copy_abs()))
        per_bid, per_ask = sides_allowed(inv, book_cap)
        # ── THE SUB-CONTRACT REDUCER ────────────────────────────────────────────────
        # `dust_close_size` is None unless 0 < |inv| < 1 AND the fraction clears THIS book's own
        # `minimumTradeQty` AND `lane.fractional_close` is on. When it is a number it is the EXACT
        # held fraction, and it overrides every `int()` clamp below — those clamps all TRUNCATE
        # (rounding a 0.53 exit up to 1 fills through flat into a fresh 0.47 opposite position),
        # so on dust they floor to 0 and the reducing side goes dark.
        # ⛔ IT ONLY EVER RELAXES THE **REDUCING** SIDE. Every stand-down above and below still
        # governs, and each ALREADY permits the reducer — that is what reduce-only means. What
        # changes is that the reducer can now be SIZED. The adding side is untouched.
        dust_size = self.dust_close_size(slug, inv)
        if reduce_only_now and _ZERO < inv.copy_abs() < 1 and dust_size is None:
            # No RUNNABLE close: the residue is under this book's minimum trade quantity, or the
            # lane is `off`. Quote NOTHING and let the teardown report it (the "no runnable
            # close" rule, poly_close's discipline) — the pre-2026-09-02 behaviour.
            per_bid = per_ask = False
        glob_bid, glob_ask = global_sides_allowed(inv, self.gross_exposure,
                                                  self.max_total_contracts)
        # ⛔ AND IN VENUE TRUTH — A PLAIN AND. Belief sizes the order; the venue may only ever
        # take a side away (belief said 0 while the venue held −36 against a cap of 12).
        # ⛔ Opposite-sign states DO reach this line and the AND deliberately goes (False,False):
        # belief blocks one side, the venue blocks the other, and the book quotes NOTHING until
        # they reconverge — a short pause, not a parked position.
        # ⛔ DO NOT re-add the `or _reduce_only(inv)` carve-out ("keep belief's reducing side"):
        # with belief long and the venue short, belief's "reducer" is an ASK, so it quotes a
        # full-size sell INTO a growing short — the exact order this feature exists to prevent.
        # [refuted.md, venue-truth AND]
        ven_bid, ven_ask = self._venue_restriction(slug, book_cap)
        allow_bid = per_bid and glob_bid and ven_bid
        allow_ask = per_ask and glob_ask and ven_ask
        # ⛔ AND IN ORDERS THE VENUE KEPT ALIVE AFTER WE CANCELLED THEM. Every gate above sums
        # INVENTORY only; the worst case for a side is inventory PLUS everything that could still
        # fill on it — the order about to be placed, and any parked order a read has since
        # contradicted. Bound: the SAME `_launch_reach` the venue-breach halt uses, because a
        # second constant for the same quantity is how two rails come to disagree.
        # ⛔ Only SIGHTED-alive parked orders count, on evidence a stale record cannot fake.
        # Counting every parked order would suspend the adding side for LAG_HORIZON_S after every
        # ordinary reprice; with no contradicting read this term is 0.
        # ⚠️ ACCEPTED UNDER-COUNT: the order RESTING on this side is not in the sum, so the true
        # worst case is one order-size higher. Including it double-counts the same quote on every
        # ordinary replace and refuses the adding side of any book with |inv| > 0 at cap == size.
        # ⛔ THE REDUCING SIDE IS EXEMPT: this is a rail about EXPOSURE GROWTH, so it may only
        # ever refuse growth. Refusing a short's only exit while the grower keeps quoting is the
        # failure `sides_allowed` exists to prevent. ⚠️ Residual, accepted and named: ghost
        # reducers can still fill through zero past the reach — the venue-breach halt is the
        # backstop there, as for every other flip-through-zero path.
        _red_bid_pk, _red_ask_pk = _reduce_only(inv)
        _reduces_pk = {"bid": _red_bid_pk, "ask": _red_ask_pk}
        _reach = self._launch_reach.get(slug, self._launch_reach_unknown)
        for _side, _sign in (("bid", 1), ("ask", -1)):
            _pk = self._parked_alive_size(slug, _side)
            if _pk <= _ZERO or _reduces_pk[_side]:
                self._parked_alive_announced.discard((slug, _side))
                continue
            _worst = (inv + _sign * (_pk + Decimal(book_size))).copy_abs()
            if _worst <= _reach:                     # AT the reach is lawful, as in `_venue_breach`
                self._parked_alive_announced.discard((slug, _side))
                continue
            # ⛔ THE STATUS IS ONLY CLAIMED WHEN THIS GATE IS THE BINDING REFUSAL. A side another
            # rail has already disallowed still gets the LOG LINE below — the operator must not
            # have to infer a ghost from silence — but the tape keeps the other rail's status.
            # Claiming `parked_alive_exposure` there would put it at the TOP of the precedence
            # chain over `mark_trip`/`adverse_latch`/`cooldown` and make those spells unreadable
            # to the episode readers that key on the status column. ⚠️ ACCEPTED, NAMED: while a
            # book is capped, `poly_presence` books the dead time as `capped_one_sided`, whose
            # remedy (flatten) does nothing about a ghost.
            _already_refused = not (allow_bid if _side == "bid" else allow_ask)
            if not _already_refused:
                if _side == "bid":
                    allow_bid = False
                else:
                    allow_ask = False
                self._parked_alive_blocked.add(slug)
                self.parked_alive_holds += 1
            else:
                self.parked_alive_masked += 1
            if (slug, _side) not in self._parked_alive_announced:
                # One line per SPELL, not per cycle — the same cardinality rule the ADVEXIT
                # line and the park-dark announce follow.
                self._parked_alive_announced.add((slug, _side))
                _oids = sorted(o for o, e in self.pending_reconcile.items()
                               if o in self.parked_alive
                               and e[0].slug == slug and e[0].side == _side)
                # ⛔ The reach is quoted with its SOURCE, never re-derived from the live cap:
                # `_launch_reach` is FROZEN at launch, while `book_cap`/`book_size` move with hot
                # settings and go to 0/clamped under reduce-only — printing "live cap 0" beside a
                # reach of 10 reads as an arithmetic error to the operator holding the incident.
                _why_alive = {o: self.parked_alive[o] for o in _oids}
                log.error(
                    f"⛔ parked_alive_exposure {slug} {_side}: "
                    + ("HOLDING" if not _already_refused else
                       "would HOLD (this side is ALREADY refused by another rail, so the tape "
                       "keeps that rail's status — but the ghost is real)")
                    + f" — inventory {inv} plus {_pk} contract(s) parked-but-sighted-alive on "
                    f"the {_side} plus a new {book_size} reaches {_worst}, beyond the lawful "
                    f"reach {_reach} (`_launch_reach`, frozen at launch; the book's LIVE "
                    f"cap/size are now {book_cap}/{book_size}). Orders: {_why_alive}")
        size_by_side: dict[str, int | Decimal] = {"bid": book_size, "ask": book_size}
        # ⛔ THE INTENT IS BOUND TO THE SIZE THAT ACTUALLY GOES OUT, NEVER TO "dust EXISTS".
        # `dust_size is not None` says only that a placeable fraction is HELD — not that this
        # cycle is placing it. On a healthy unrestricted book holding +0.53 nothing below writes
        # the fraction, so a `close_long` derived from the holding alone ships a full-size
        # SELL_LONG against it: either the venue takes it and we flip through flat, or it refuses
        # and the ask side is dark every cycle. So this name is written ONLY beside a
        # `size_by_side[...] = dust_size` assignment, and it is the sole input to `close_long`.
        dust_close_side: Optional[str] = None
        if reduce_only_now and dust_size is not None:
            # The reduce-only lane clamps `book_size` to `int(|inv|)` ABOVE, which is 0 on dust —
            # so the fraction has to be written in here.
            # ⛔ WRITING IT ON THE ADDING SIDE TOO WOULD CHANGE NOTHING, AND THAT IS THE POINT:
            # this line is a SIZE, and sizes do not decide which sides quote —
            # `sides_allowed(inv, book_cap=0)` already made the adding side False. The rail is
            # THERE, not here. It keys off the reducing side only so `dust_close_side` (and thus
            # the SELL_LONG intent) names one unambiguous side.
            dust_close_side = "ask" if inv > _ZERO else "bid"
            size_by_side[dust_close_side] = dust_size
        # ── P-c presence workdown the private design notes ──────────
        # ONLY in the plain cap-disabled state (|inv| >= cap made sides_allowed one-sided — NOT
        # a latch, trip, cooldown, venue restriction, OR the reduce-only lane): the reducing side
        # sizes min(|inv|, 2×size). A fill bounded by |inv| can never flip through zero.
        # Safety precedence has TWO mechanisms and both are needed: cooldown/latch/tripwire run
        # AFTER this block and OVERWRITE the size with their stricter min(size, |inv|), while the
        # reduce-only lane clamps `book_size` BEFORE it — so that lane is excluded by the
        # `not reduce_only_now` conjunct, not by ordering.
        # ⚠️ A deeply underwater capped book arms the mark tripwire every cycle, whose reducer
        # then overrides this boost — read the book's mark before expecting the flag to act.
        if (slug in self.presence_workdown and inv != _ZERO and not reduce_only_now
                and inv.copy_abs() >= book_cap and per_bid != per_ask):
            _wd_size = min(int(inv.copy_abs()), 2 * book_size)
            _wd_side = "ask" if inv > _ZERO else "bid"
            if _wd_size > size_by_side[_wd_side]:
                size_by_side[_wd_side] = _wd_size
        # ── the unrealized-mark tripwire ─────────────────────────────────────────────────
        # The durable cap is price-REALIZED-only and the adverse cooldown fires only after a
        # COMPLETED round trip, so both are blind to the accumulation phase inventory headroom
        # widens. The mark is inventory against the LIQUIDATION side of the touch just read (zero
        # extra requests); a breach rides the PROVEN cooldown/reduce-only machinery — the exit
        # keeps working, re-entry is blocked, and each cycle re-arms the stand-down.
        # ── SUB-CONTRACT DUST CARVE-OUT ─────────────────────────────────────────────────
        # The threshold SCALES with |inv|, so a fractional residue breaches on a ~2-cent marked
        # loss — and the arm it fires costs a full adverse cooldown of foreclosed re-entry on a
        # book that may be profitable. The dollars at risk on dust are negligible against the
        # profit the stand-down forecloses, so dust may arm NEITHER the breach nor the
        # unknown-basis arm. The |inv| < 1 boundary matches the recovery gate's own carve-out.
        dust = inv.copy_abs() < _ONE
        if self.mark_trip_per_ct <= _ZERO or inv == _ZERO or dust:
            # A flat book ends any width-shield spell [review CONCERN-1]: without this, a
            # book that shielded, exited, and re-accumulated into a fresh width breach
            # would log NOTHING for the rest of the process — and the shield log is the
            # sole remaining trace of the width-loss exposure class this redesign unwatches.
            # Dust takes the same exit — the shield is a sub-case of this mark machinery,
            # so a book that decays to dust must not linger in `width_shielded`.
            self.width_shielded.discard(slug)
        if self.mark_trip_per_ct > _ZERO and inv != _ZERO and not dust:
            # A one-sided park book has no liquidation side to mark against: that is
            # CANNOT-VERIFY and falls into the `unknown` arm below, never a clean pass.
            # ⛔ THE NET TOUCH, NOT THE RAW ONE: the raw touch can BE our own resting order, so a
            # long alone at the best bid would be marked against a price WE set — flattering in
            # the one direction that matters. Shadow/DRY is unaffected (there net IS raw).
            # ⛔ THE GUARD IS ON THE LIQUIDATION SIDE ONLY: a long liquidates into the BID and
            # never touches the ask, so requiring BOTH net sides would arm the unknown arm's
            # cooldown on a perfectly markable book. The RAW two-sidedness requirement stays —
            # it is what makes the net touch readable at all.
            liq_net = net_bid if inv > _ZERO else net_ask
            marked = (mark_pnl(inv, self.avg_entry.get(slug, _ZERO), liq_net, liq_net)
                      if liq_net is not None and bid is not None and ask is not None
                      else None)
            threshold = self.mark_trip_per_ct * inv.copy_abs()
            # The trip requires the mark breach AND drift-dominance — the episode bank's OWN
            # class boundary (`split_tally`: drift < 0 and |drift| > half_spread). The mark
            # threshold stays calibrated against MARKED P&L as documented at
            # DEFAULT_MARK_TRIP_PER_CONTRACT; only the width classes are shielded, and adding a
            # conjunct can only ever trip a SUBSET of the old rule's cycles.
            # ⛔ ONE PREDICATE, ONE TOUCH: `mid` and `half_spread` come from the same NET touch
            # the mark does. A mixed predicate prices the breach against the real market and
            # classifies it against our own quote (both sides ours narrows the RAW spread to our
            # own two-sided quote, so a width move reads as drift-dominated and over-trips).
            drift_per_ct: Decimal | None = None
            half_spread: Decimal | None = None
            if marked is not None and net_bid is not None and net_ask is not None:
                mid = (net_bid + net_ask) / Decimal("2")
                half_spread = (net_ask - net_bid) / Decimal("2")
                drift_per_ct = (mid - self.avg_entry[slug]) if inv > _ZERO \
                    else (self.avg_entry[slug] - mid)
            breach_mark = marked is not None and marked <= -threshold
            drift_dominated = (drift_per_ct is not None
                               and drift_per_ct < _ZERO
                               and drift_per_ct.copy_abs() > half_spread)
            breach = breach_mark and drift_dominated
            # A width-class breach is SHIELDED, not silent — and it must be MEASURABLE by
            # the same episode reader that justified this redesign, so the spell lands in
            # the quote tape as status=width_shield (below), not only in the log. One log
            # line per spell — the spell ends when the mark un-breaches or the book flats.
            # ⛔ A breach the net touch cannot CLASSIFY is not a shield [same build]. With one
            # net side emptied by our own order there is no net spread, so the width/drift
            # question has no answer — and answering it "width" would let our own quote
            # silence a real breach. It takes the unknown arm below instead, the same
            # cannot-verify path an emptied liquidation side already takes.
            unclassified = breach_mark and half_spread is None
            shielded = breach_mark and not breach and not unclassified
            if shielded and slug not in self.width_shielded:
                self.width_shielded.add(slug)
                log.warning(
                    f"🛡️ WIDTH SHIELD {slug}: marked P&L {marked:.3f} ≤ −{threshold:.3f} but "
                    f"the move is width-class (drift/ct {drift_per_ct:.3f} vs half-spread "
                    f"{half_spread:.3f}; inv {inv:.3f}, "
                    f"avg {self.avg_entry.get(slug, _ZERO):.3f}, "
                    f"touch {bid}/{ask}, net {net_bid}/{net_ask}) — holding both sides "
                    f"(bank: width class)")
                # ONE event per spell, on the same branch as the one log line — the spell
                # opens here and only here, so the channel cannot double-announce a book
                # that stays shielded across cycles.
                self._probe_notes.event(
                    probe_notify.event_line, "WIDTH SHIELD", slug,
                    f"marked P&L {marked:.3f} ≤ −{threshold:.3f} but the move is WIDTH-class "
                    f"(drift/ct {drift_per_ct:.3f} vs half-spread {half_spread:.3f}; "
                    f"inv {inv:.3f}) — holding both sides, NOT tripped", icon="🛡️")
            elif not shielded:
                self.width_shielded.discard(slug)
            # UNKNOWN BASIS arms too [review §2]: an inherited position has no avg_entry
            # from this run, mark_pnl returns None, and cannot-verify is not clean — the
            # rail built for accumulating positions must not pass silently on the one
            # position class nothing else watches. Effect: inherited inventory exits
            # (reduce-only) before its book re-enters.
            unknown = marked is None or unclassified
            if breach or unknown:
                already = time.time() < self.cooldown_until.get(slug, 0.0)
                deadline = time.time() + self.adverse_cooldown_s
                self.cooldown_until[slug] = deadline
                self.trip_until[slug] = deadline
                if not already:
                    self.mark_trips[slug] = self.mark_trips.get(slug, 0) + 1
                    if breach:
                        log.warning(
                            f"🛑 MARK TRIPWIRE {slug}: marked P&L {marked:.3f} ≤ "
                            f"−{threshold:.3f} AND drift-dominated (drift/ct "
                            f"{drift_per_ct:.3f} > half-spread {half_spread:.3f}; "
                            f"{self.mark_trip_per_ct}/contract × |{inv:.3f}|; "
                            f"avg {self.avg_entry.get(slug, _ZERO):.3f}, "
                            f"touch {bid}/{ask}, net {net_bid}/{net_ask}) — "
                            f"reduce-only; re-arms while the breach persists")
                        # ⛔ The notify line carries the DRIFT-DOMINANCE conjunct too, or an
                        # operator cannot tell a trip from a shielded width breach.
                        self._probe_notes.event(
                            probe_notify.event_line, "MARK TRIPWIRE", slug,
                            f"marked P&L {marked:.3f} ≤ −{threshold:.3f} AND drift-dominated "
                            f"(drift/ct {drift_per_ct:.3f} > half-spread "
                            f"{half_spread:.3f}; "
                            f"{self.mark_trip_per_ct}/ct × |{inv:.3f}|) — reduce-only",
                            icon="🛑")
                        # ── ADVEXIT decision line — DATA ONLY. A real cross needs a taker path
                        # this maker structurally lacks (post_only=True always). This accumulates
                        # the counterfactual inputs at every breach; NOTHING reads it in-run.
                        _ra = self.resolves_at.get(slug)
                        _now = time.time()
                        _ttr = (f"{(_ra - _now) / 3600.0:.1f}" if _ra is not None else "?")
                        # A long exits into the BID; a short covers at the ASK.
                        _x_side = "bid" if inv > _ZERO else "ask"
                        _x_px = bid if inv > _ZERO else ask
                        _depth = qty_at_price(md, _x_side, _x_px)
                        _fits = _depth is not None and _depth >= inv.copy_abs()
                        _near = _ra is not None and 0 < (_ra - _now) < 86400.0
                        _why = ("" if _ra is not None else " no_resolves_at")
                        # ⛔ THE TAPE IS THE RECORD. As a `log.warning` ALONE this went into a run
                        # log with no rotation discipline, so accumulating the registered n≥20
                        # meant grepping unrotated logs and could silently stop growing. The log
                        # line stays as the operator's live signal; `_write_advexit` is the record.
                        self._write_advexit(slug, inv=inv, side=_x_side,
                                            crossable_px=_x_px, depth=_depth, now=_now)
                        log.warning(
                            f"ADVEXIT {slug}: ttr_h={_ttr} crossable={_x_px} "
                            f"depth={_depth if _depth is not None else '?'} "
                            f"inv={inv.copy_abs()} "
                            f"would_cross={'YES' if (_near and _fits) else 'NO'}{_why}")
                    elif (self.avg_entry.get(slug, _ZERO) > _ZERO
                          and (liq_net is None or unclassified)):
                        # ⛔ TWO CAUSES, TWO LINES. The unknown arm fires both when there is no
                        # basis and when the basis is FINE but our own order was the whole
                        # liquidation side — an operator reading "inherited" there would go
                        # looking for a carry that does not exist. So the line names which side
                        # went and what that side was to us.
                        _liq_side = "bid" if inv > _ZERO else "ask"
                        _empty = (_liq_side if liq_net is None
                                  else ("bid" if net_bid is None else "ask"))
                        _role = "liquidation side" if _empty == _liq_side else "far side"
                        log.warning(
                            f"🛑 MARK TRIPWIRE {slug}: inventory {inv} is un-markable — the "
                            f"{_role} is our own order, nothing behind it (net "
                            f"{_empty} empty; touch {bid}/{ask}), arming reduce-only until the "
                            f"position exits")
                        self._probe_notes.event(
                            probe_notify.event_line, "MARK TRIPWIRE", slug,
                            f"inventory {inv} is un-markable — the {_role} is our "
                            f"own order, nothing behind it (net {_empty} empty) — reduce-only "
                            f"until it exits", icon="🛑")
                    else:
                        log.warning(
                            f"🛑 MARK TRIPWIRE {slug}: inventory {inv} has NO BASIS from "
                            f"this run (inherited) — un-markable, arming reduce-only until "
                            f"the position exits; absence is not a basis")
                        self._probe_notes.event(
                            probe_notify.event_line, "MARK TRIPWIRE", slug,
                            f"inventory {inv} has NO BASIS from this run (inherited) — "
                            f"un-markable, reduce-only until it exits", icon="🛑")
        # The adverse latch rides the SAME reduce-only machinery as the timed cooldown — one
        # implementation, two arming conditions — but has no deadline to expire. ⛔ The expression
        # lives in `in_cooldown()`: the hot-settings raise rail needs the same question answered,
        # and a copy there missed the tripwire arm entirely, so a book standing down under
        # accumulating unrealized loss could still be sized UP. One function, every reader.
        in_cooldown = self.in_cooldown(slug)
        if in_cooldown:
            # Post-adverse cooldown: only the REDUCING side may quote (flat → neither), so a
            # residual position keeps its exit working while re-entry is blocked.
            cool_bid, cool_ask = _reduce_only(inv)
            allow_bid, allow_ask = (allow_bid and cool_bid), (allow_ask and cool_ask)
            # ⛔ The reducer is sized to the INVENTORY, never to --size: a cooling short 5 quoted a
            # full-size bid 12 would fill through zero to long 7 — a brand-new position opened
            # DURING the stand-down that exists to prevent exactly that. A sub-contract residue
            # truncates to 0 and quotes nothing rather than rounding up into a flip.
            # ⛔ ON DUST THE EXACT FRACTION, NOT THE TRUNCATION: `dust_close_size` is already
            # bounded by |inv| (it IS |inv|), so it cannot fill through zero either.
            reduce_size: int | Decimal = (dust_size if dust_size is not None
                                          else min(book_size, int(inv.copy_abs())))
            if inv != _ZERO:
                _red = "ask" if inv > _ZERO else "bid"
                size_by_side[_red] = reduce_size
                if _red == "ask":
                    allow_ask = allow_ask and reduce_size > 0
                else:
                    allow_bid = allow_bid and reduce_size > 0
                # Set BESIDE the assignment, and only when the size written IS the fraction.
                if dust_size is not None:
                    dust_close_side = _red

        if park is not None:
            # ══ THE PARK ASSEMBLY — one rule, stated once, applied to both sides ═══════════
            # ⛔ ADD vs REDUCE IS DECIDED BY THE SIGN OF INVENTORY, NEVER BY WHICH SIDE WAS
            # DECLARED. The park DISTANCE buys reward score, so it belongs to the ADDING
            # placement only; an exit ten ticks off the touch never drains.
            # ⛔ EVERY reducing placement — either side, any lane — prices at the RAW TOUCH and
            # is sized `min(book_size, |inv|)`. That clamp is the FLIP GUARD: a full 10-lot exit
            # against 5 held fills through flat into a brand-new opposite position.
            # ⛔ ONE SIDE WHILE THE RAILS ARE CLEAR — but the moment ANY rail restricts the book,
            # the surviving non-park side is by construction the REDUCING one and must keep
            # working, or a real position is left with nothing to take it off.
            _red_bid, _red_ask = _reduce_only(inv)
            _reduces = {"bid": _red_bid, "ask": _red_ask}
            _rails_clear = allow_bid and allow_ask
            _park_add_px: Optional[Decimal] = None
            if not _reduces[park_side]:
                # The park side ADDS this cycle, so it wants the park distance — off the NET touch.
                try:
                    _park_add_px = park_price(net_bid if park_side == "bid" else net_ask,
                                              tick, park_side, park[1])
                except ValueError as exc:
                    # ⛔ UNPRICEABLE ANCHOR ⇒ HOLD IN PLACE, NOT CANCEL — presence IS the product.
                    # Re-declaring the resting order's OWN price makes `quote_action` return HOLD,
                    # so the order and its queue position survive a cycle we could not re-derive;
                    # cancelling would forfeit every one-second reward snapshot. The WHOLE cycle
                    # still runs: any owed exit quotes.
                    # ⛔ EXCEPT WHEN THE RESTING ORDER IS OUT OF POSITION — through the opposite
                    # touch, OR stale-through on its OWN side. `park_order_is_stale` owns both
                    # tests and the reason text; only then is the side cancelled below.
                    # stays None and the side is cancelled below.
                    _existing = self.resting.get((slug, park_side))
                    _stale = (park_order_is_stale(park_side, _existing.price, park[1], tick,
                                                  net_bid, net_ask, bid, ask)
                              if _existing is not None else "no resting order to hold")
                    if _existing is not None and _stale is None:
                        # ⛔ `_park_why` IS SET ONLY ON THE HOLD. It drives the tape's `park_hold`
                        # status, and a CANCEL cycle is the opposite of a hold — labelling it
                        # `park_hold` would inflate every presence count read off this tape.
                        _park_why = str(exc)
                        _park_add_px = _existing.price
                        log.info(f"🅿️ {slug}: park anchor unpriceable this cycle ({exc}) — "
                                 f"HOLDING the resting {park_side} at {_existing.price}. The "
                                 f"read succeeded; a cancel here forfeits reward snapshots.")
                    else:
                        _park_cancel = True
                        log.warning(
                            f"🅿️ {slug}: park anchor unpriceable ({exc}) and {_stale} "
                            f"(touch {bid}/{ask}, net {net_bid}/{net_ask}) — CANCELLING. An "
                            f"order the market has run past is a gift, not presence.")
            # ⚠️ `int()` TRUNCATES, so a fractional remainder leaves dust behind. Rounding UP is
            # forbidden — it opens a real new position out of dust. A residue that clears the
            # book's own `minimumTradeQty` is rested as an exact fractional reducer; `park_dark`
            # is reached only when it does not clear it, or `lane.fractional_close` is off.
            for _s in ("bid", "ask"):
                if _reduces[_s]:
                    # ⛔ DUST HAS A SIZE: a placeable fraction rests at the raw touch like
                    # any other reducer. When `dust_close_size` is None (under the book's minimum,
                    # or the lane is off) the truncation floor and `park_dark` behave as before.
                    size_by_side[_s] = (dust_size if dust_size is not None
                                        else min(size_by_side[_s], int(inv.copy_abs())))
                    if dust_size is not None:
                        # Beside the assignment, as everywhere else. `_reduces[_s]` guards this
                        # loop, so `_s` is the reducing side by construction.
                        dust_close_side = _s
                    if size_by_side[_s] <= 0:
                        if _s == "bid":
                            allow_bid = False
                        else:
                            allow_ask = False
            if park_side == "bid":
                if not _red_bid:
                    # ADDING: the park distance, or nothing (an out-of-position anchor stands the
                    # side down). REDUCING keeps the raw touch already in `my_bid`.
                    my_bid = _park_add_px
                    allow_bid = allow_bid and _park_add_px is not None
                allow_ask = allow_ask and _red_ask and not _rails_clear
            else:
                if not _red_ask:
                    my_ask = _park_add_px
                    allow_ask = allow_ask and _park_add_px is not None
                allow_bid = allow_bid and _red_bid and not _rails_clear
            # ⛔ THE DARK STATE, MADE VISIBLE. A SUB-CONTRACT residue of the sign OPPOSITE the
            # park makes the park side the reducer and the reducer's clamp floors to 0, so the
            # seat can neither park nor exit and quotes NOTHING, forever — a seat that has
            # silently stopped being a seat must not tape as a healthy one. Reachable only when
            # the fraction is under the book's own `minimumTradeQty` or `fractional_close` is off.
            if not allow_bid and not allow_ask and _ZERO < inv.copy_abs() < 1:
                _park_dark = True
                if slug not in self._park_dark_announced:
                    self._park_dark_announced.add(slug)
                    log.warning(
                        f"🅿️ {slug}: PARK SEAT IS DARK — inventory {inv} is a SUB-CONTRACT "
                        f"residue, so the {park_side} is owed to an exit that cannot be sized "
                        f"(under this book's minimum trade qty {self.min_trade_qty(slug)}, or "
                        f"lane.fractional_close={self.fractional_close_mode!r}) and the other "
                        f"side is suppressed. This book will quote NOTHING until it is cleared: "
                        f"declare it as a --carry on the next run, or hand-close the fraction. "
                        f"It is taped `park_dark`, not `ok`.")

        # ── THE REBATE STEP ──────────────────────────────────────
        # OPT-IN (`--rebate-step`), default OFF.
        # ⛔ LAST, AND DELIBERATELY SO. Every stand-down, clamp and reducer above has already
        # written its size, so the ONE thing this can still touch is a side that is allowed and
        # is not the reducer — a side genuinely ADDING at a price we chose. The park lane is
        # included on purpose: a park ADD rests at the park distance, which is exactly the price
        # the step must be judged at.
        _rebate_step_status: Optional[str] = None
        _rebate_step_dropped = False
        if self.rebate_step:
            _sr_bid, _sr_ask = _reduce_only(inv)
            _reduces_step = {"bid": _sr_bid, "ask": _sr_ask}
            _judged: dict[str, tuple[int, str, int]] = {}
            for _side, _px, _ok in (("bid", my_bid, allow_bid), ("ask", my_ask, allow_ask)):
                _base = size_by_side[_side]
                # A Decimal size is a `dust_close_size` fraction, a REDUCER by construction; the
                # isinstance keeps `rebate_rounds_to_zero`'s int contract honest.
                if (_px is None or not _ok or _reduces_step[_side]
                        or not isinstance(_base, int)):
                    self._rebate_step_state.pop((slug, _side), None)
                    continue
                _judged[_side] = self._rebate_step_size(slug, _side, _px, _base, inv)
            # ⛔ BOTH ADDING SIDES OR NEITHER. On a FLAT book both sides add, and dropping only
            # the side whose step does not fit would leave a ONE-SIDED, SIZE-BOOSTED seat — a
            # directional position this rule has no mandate to open. It is `pick_quotes`'s own
            # "NEVER ONE SIDE" invariant restated at the SIZE level. ⛔ ADDING sides only: with
            # inventory the reducer was never judged, so an exit is never withheld because a
            # rebate does not fit.
            _coerced: set[str] = set()
            if any(st == "skipped" for _, st, _c in _judged.values()):
                for _side in list(_judged):
                    _sz, _st, _c = _judged[_side]
                    if _st != "skipped":
                        _coerced.add(_side)
                        _judged[_side] = (_sz, "skipped", _c)
            for _side, (_new, _state, _ceil) in _judged.items():
                _base = size_by_side[_side]
                if _state == "skipped":
                    # NOT POSTED this cycle: at this price the side cannot reach a paid size
                    # inside the run's ceiling, so resting it would buy a guaranteed $0 with
                    # the full adverse and inventory risk attached. The existing
                    # disallowed-side machinery cancels any order already resting there.
                    _rebate_step_dropped = True
                    if _side == "bid":
                        allow_bid = False
                    else:
                        allow_ask = False
                else:
                    size_by_side[_side] = _new
                # `skipped` outranks `sized:N`/`opt:N` outranks `ok` — the strongest state the
                # rule reached on this book this cycle. Per-side truth is `my_*_size`/`allow_*`.
                if _state == "skipped" or (_state.startswith(("sized", "opt"))
                                           and _rebate_step_status != "skipped"):
                    _rebate_step_status = _state
                elif _rebate_step_status is None:
                    # `reserved` is a POSTED cycle like `ok`, but it is taped by NAME: the seat
                    # rested its BASE size because the ghost reserve held the headroom a raise
                    # would have used, and a reader joining on this column must be able to tell
                    # that apart from a book the rule never touched.
                    _rebate_step_status = _state
                if self._rebate_step_state.get((slug, _side)) != _state:
                    self._rebate_step_state[(slug, _side)] = _state
                    if _state == "skipped":
                        _px = my_bid if _side == "bid" else my_ask
                        log.warning(
                            f"rebate-step: {slug} {_side} skipped, "
                            + (f"the other ADDING side could not reach its step and this book "
                               f"is flat — both sides or neither, at p={_px}" if _side in _coerced
                               else f"needs size {min_rebate_size(_px)} > max {_ceil} "
                                    f"at p={_px}"))
                    elif _state == "reserved":
                        log.info(f"rebate-step: {slug} {_side} at base {_base} — no raise inside "
                                 f"the ghost reserve (ceiling {_ceil}), side still posted")
                    elif _state != "ok":
                        log.info(f"rebate-step: {slug} {_side} sized {_new} (from {_base}), "
                                 f"max {_ceil} at p={my_bid if _side == 'bid' else my_ask}")
        # ── DUST-AWARE REDUCING SIZE ──────────────────────────────────────
        # OPT-IN (`--dust-hold`), default OFF: absent the flag this block does nothing.
        # A SUB-CONTRACT residue (0 < |inv| < 1) has no reducing quote worth resting — the
        # reducing side's clamp is `int(|inv|)` = 0, and any sub-step size forfeits its rebate to
        # nearest-cent-PER-INCREMENT rounding while carrying the full adverse risk of resting.
        # ⛔ (a) FRACTIONAL CLOSE STILL WINS: when `dust_close_side` is set the residue is already
        # routed to the `fractional_close` lane at an exact size, and this rule does not touch it.
        # ⛔ (b) OTHERWISE HOLD, do not rest — the residue goes to the recorded-carry path and the
        # cycle tapes `status=dust_hold` so presence readers see a NAMED stand-down.
        # ⛔ WHOLE-CONTRACT REDUCERS ARE UNCHANGED: |inv| >= 1 never reaches this branch, so a
        # 1-lot exit below `min_rebate_size` keeps resting. Exits stay working.
        _dust_hold = False
        if self.dust_hold and _ZERO < inv.copy_abs() < 1 and dust_close_side is None:
            _dh_side = "ask" if inv > _ZERO else "bid"
            # ⛔ ONLY WHERE THE EXIT IS THE ONLY THING LEFT TO REST — i.e. a rail above has already
            # refused the ADDING side. On an UNRESTRICTED book holding a residue both sides quote
            # the book's ordinary whole-lot size: that is a two-sided seat, not a reducing quote
            # sized to dust, and killing one leg would make the seat one-sided.
            if not (allow_bid if _dh_side == "ask" else allow_ask):
                _dust_hold = True
                if _dh_side == "bid":
                    allow_bid = False
                else:
                    allow_ask = False
                # The residue itself, not a size we chose — `normalize()` drops trailing zeros.
                _rebate_step_status = f"dust:{inv.copy_abs().normalize():f}"
        # ── THE ADDING SIDE'S CAP ROOM ──
# `sides_allowed` flips a book to reduce-only only AT `|inv| >= cap` and never SIZED the
# adding side, so a book one contract under the cap still quoted a FULL size and each fill
# landed the position past it, overshooting to nearly 2× the cap. The room under the cap IS the ceiling for anything that
        # ADDS, and it is applied LAST — after the reduce-only lanes, the workdown boost, the
        # cooldown reducer, the park assembly and the rebate step's RAISE — so every size writer
        # above is bounded by it and no lane can route around it.
        # ⛔ THE REDUCING SIDE IS UNTOUCHED. Lanes 2/3, the park assembly and the dust close each
        # own their `min(size, |inv|)` flip guard; a second clamp here could only ever withhold an
        # exit, which is the failure `sides_allowed` exists to prevent.
        _cap_red_bid, _cap_red_ask = _reduce_only(inv)      # True = that side REDUCES
        # ⛔ THE ROOM IS MEASURED ON WHOLE CONTRACTS HELD — the SAME sub-contract carve-out the
        # tripwire and the reducing clamps take. Flooring `cap − |inv|` instead would refuse the
        # adding side of any dust book at `cap_fills 1` (room 4.47 < MIN_SIZE), which is the
        # healthy-unrestricted-dust state this maker deliberately leaves quoting two full sides.
        # Residual, bounded and named: a fractional residue can overshoot the cap by < 1 contract.
        _cap_room = book_cap - int(inv.copy_abs())
        _cap_room_off: set[str] = set()
        for _cs, _cs_reduces in (("bid", _cap_red_bid), ("ask", _cap_red_ask)):
            # A `Decimal` size is a `dust_close_size` fraction — a reducer by construction.
            # ⛔ AND A SIDE ANOTHER RAIL ALREADY REFUSED IS SKIPPED, so `cap_room` is claimed ONLY
            # where this clamp is the BINDING refusal: a flat book under the reduce-only lane has
            # `book_cap = 0` and no reducing side, and naming `cap_room` there would put it above
            # `reduce_only`/`clock_guard`/`maintenance_guard` in `hold_cause`'s precedence.
            if (_cs_reduces or not isinstance(size_by_side[_cs], int)
                    or not (allow_bid if _cs == "bid" else allow_ask)):
                continue
            if _cap_room < MIN_SIZE:
                # No lawful order fits in the room (0, or under the maker's minimum): post
                # NOTHING on this side rather than a sub-minimum quote whose rebate rounds to
                # zero at every increment. The reducing side keeps working — so this refusal
                # leaves an `ok` row at `allow=(N,Y)`, which `hold_cause` names below.
                _cap_room_off.add(_cs)
                if _cs == "bid":
                    allow_bid = False
                else:
                    allow_ask = False
            elif _cap_room < size_by_side[_cs]:
                size_by_side[_cs] = _cap_room
        # ── THE SEEDED CYCLE'S SIZE CEILING [P1b, mm-review 2026-09-10] ──────────────────
        # ⛔ LAST, AFTER EVERY OTHER CLAMP, AND IT ONLY EVER LOWERS: the cap room, the
        # reduce-only lanes and the rebate step have already voted, and this takes the min of
        # their answer and `SEED_SIZE_MAX`. A seeded quote is the one order in the book placed
        # against NO observed market, so its size is the only rail that bounds the mistake.
        # ⚠️ `int` only — a `Decimal` size is a dust-close fraction, already below the ceiling.
        if seeded_touch is not None:
            for _ss in ("bid", "ask"):
                if isinstance(size_by_side[_ss], int):
                    size_by_side[_ss] = min(size_by_side[_ss], SEED_SIZE_MAX)
        # `my_bid`/`my_ask` can be None on a park book's non-park side (a one-sided book): a
        # side we cannot price is never placed, whatever the allow flags say.
        desired = {"bid": my_bid if (allow_bid and my_bid is not None) else None,
                   "ask": my_ask if (allow_ask and my_ask is not None) else None}
        queue = {"bid": qty_at_price(md, "bid", my_bid) if my_bid is not None else None,
                 "ask": qty_at_price(md, "ask", my_ask) if my_ask is not None else None}
        actions: dict[str, str] = {}
        reasons: dict[str, str] = {}
        for side in ("bid", "ask"):
            existing = self.resting.get((slug, side))
            action = quote_action(existing.price if existing else None, desired[side])
            # ⛔ A SIZE THE REACH RAIL CLAMPED IS A MATCH, not a wrong size.
            # Without this, a 6 resting under a decided 8 is requoted every cycle and re-clamped
            # to 6 — an unbounded cancel/place churn whenever inventory sits inside the last
            # `MIN_SIZE` of the reach. ONE expression: the same `_reach_permits` the resize and
            # `_place`'s guard read, on this cycle's inventory.
            _clamped = (self._reach_permits(slug, side, Decimal(size_by_side[side]))
                        if size_by_side[side] is not None else None)   # whole contracts already
            if _clamped is not None and _clamped < Decimal(size_by_side[side]):
                # The intended-size cell reports the CLAMP, not the pre-clamp decision — on a HOLD
                # cycle nothing is sent and this is the size actually resting. `_apply_action`
                # overwrites it with what went to the wire when it places.
                self._reach_sized[(slug, side)] = int(_clamped)
            if (action == HOLD and existing is not None and desired[side] is not None
                    and existing.size != size_by_side[side]
                    and (_clamped is None or Decimal(existing.size) != _clamped)):
                # Same price but the wrong size: a full-size reducer resting from before the
                # cooldown must shrink to the inventory. Forfeits queue position — the one place
                # correctness outranks the queue rule.
                action = REPLACE
            # ⛔ THE INTENT IS NOT IN THIS PREDICATE, AND IT DOES NOT NEED TO BE. The
            # `sell` ↔ `sell_long` switch is ALWAYS accompanied by a size switch, and the two size
            # domains are DISJOINT: a dust close is a `Decimal` in (0, 1) while every ordinary
            # quote size is an `int` >= MIN_SIZE. So they can never compare equal, and the size
            # clause above already forces the REPLACE on both directions of the crossing.
            # ⚠️ THE CONDITION TO PRESERVE: if a dust close is ever sized >= 1, or an ordinary
            # quote ever sized < 1, this argument dies and the intent must join the predicate.
            actions[side] = action
            # Labelled BEFORE `_apply_action` runs — it mutates `self.resting`, so a reason
            # derived afterwards would compare the new order against itself.
            # ⛔ THE SEEDED ROW LABELS ITSELF [P1b]: `quote_reason`'s whole vocabulary describes a
            # decision taken against a book THIS CYCLE READ, and on a seeded cycle there was no
            # such book — `net_unreadable` would be true and useless. Both sides carry it, because
            # the seed is a per-BOOK decision and never one-sided. It decides nothing, like every
            # other value in this column.
            reasons[side] = "seeded_last_touch" if seeded_touch is not None else quote_reason(
                action, resting=existing.price if existing else None,
                desired=desired[side],
                resting_size=existing.size if existing else None,
                desired_size=size_by_side[side] if desired[side] is not None else None,
                prev_net=prev_net[0] if side == "bid" else prev_net[1],
                net=net_bid if side == "bid" else net_ask,
                improved=improved)
            stats.actions[action] = stats.actions.get(action, 0) + 1
            await self._apply_action(action, slug, side, desired[side], queue[side], improved,
                                     size=size_by_side[side], net=(net_bid, net_ask),
                                     # ⛔ A FRACTIONAL ASK CLOSING A LONG IS `SELL_LONG`, not the
                                     # ordinary ask's `BUY_SHORT` (which would OPEN a short beside
                                     # the dust). ⛔ AND IT IS KEYED ON THE SIZE THAT WAS ACTUALLY
                                     # WRITTEN, never on dust merely being HELD.
                                     close_long=(side == "ask" and side == dust_close_side))

        stats.markets_quoted += 1
        # ⛔ EVERY STAND-DOWN NEEDS ITS OWN `status` VALUE. `allow=(N,N)` alone is ambiguous with
        # the global gross cap, and the presence/capture reads derive uptime from
        # `status` + `allow_*` — an unnamed refusal is counted as legitimate no-quote time and
        # the denominators drift. Precedence (widest-blocking first) is load-bearing:
        # `parked_alive_exposure` (live orders we do not track — the narrowest, loudest) >
        # `venue_flat` > `mark_trip` (its cycles must stay excludable from f_B) > `adverse_latch`
        # > `width_shield` (a shielded cycle QUOTES, but must stay bankable by
        # poly_mark_trip_episodes) > the park trio > `dust_hold` > `rebate_step` > `ok`.
        # ⚠️ NEVER INFER DURATION FROM `adverse_latch`: on the PROBE lane the ban is TIMED, so
        # read the spell's END off the status column's own boundary, never as run-lifetime.
        # ⚠️ `mark_trip` cannot appear on a FLAT, un-latched book since 2026-09-02 — the
        # tripwire's stand-down is inventory-conditional in `in_cooldown`. The deadline still
        # exists for episode readers keyed on `trip_until`.
        _status = ("parked_alive_exposure" if slug in self._parked_alive_blocked
                   else ("venue_flat" if slug in self._venue_flat_divergence
                   else ("mark_trip" if (in_cooldown and time.time() < self.trip_until.get(slug, 0.0))
                         else ("adverse_latch" if slug in self.adverse_latched
                               else ("cooldown" if in_cooldown
                                     else ("width_shield" if slug in self.width_shielded
                                     # ⛔ BELOW every safety status, above `ok` [park lane]. A
                                     # park cycle that did not quote normally is not a normal
                                     # cycle — but it must never mask a stand-down, so all
                                     # three sit under `cooldown`/`mark_trip`/`adverse_latch`.
                                     else ("park_dark" if _park_dark
                                           else ("park_cancel" if _park_cancel
                                           else ("park_hold" if _park_why
                                     # ⛔ NEITHER `dust_hold` NOR `rebate_step` IS AN `ok`
                                     # CYCLE: both are REFUSALS to post, and presence/capture
                                     # readers derive uptime from `status` + `allow_*`, so an
                                     # unnamed refusal is banked as legitimate no-quote time
                                     # and the denominators drift. Both sit BELOW every safety
                                     # status and the park trio — a safety stand-down must
                                     # never be masked by an economics one.
                                                 else ("dust_hold" if _dust_hold
                                                 else ("rebate_step" if _rebate_step_dropped
                                                       else "ok")))))))))))
        # ── WHICH GATE SILENCED THIS CYCLE [hold_cause, TAPE-ONLY] ──────────────────────
        # ⛔ DERIVED, NEVER DECIDED: every name below reads a flag the path above already
        # computed, in the order the path applies them. Nothing here narrows `allow_*`.
        # Only an `ok` row with BOTH sides off is attributable — every other stand-down
        # (venue-flat divergence, the tripwire/latch/cooldown, the park trio, `dust_hold`,
        # `rebate_step`, `parked_alive_exposure`) already names itself in `status`, and
        # `_venue_restriction` cannot return (False, False) without claiming `venue_flat`.
        # `other` is the residual: it means this enumeration is incomplete, not "no cause".
        # ⛔ THE REACH RAIL OUTRANKS THE `ok`-AND-BOTH-SIDES-OFF DERIVATION BELOW: it refuses
        # AFTER the gates voted, so `allow_*` legitimately say Y on a side that never went to the
        # wire, and the derivation would name whatever gate happened to be nearest — or nothing.
        # A refused side is taped DARK three cells over (`my_*` price, `my_*_size`, `action_*`),
        # which is what makes it invisible to `bot.core.presence`: that state machine reads
        # `action_*` (`place`/`replace` → RESTING) and is overridden by `resting_*_qty`, which is
        # blank here because `_place` returned before `self.resting` was written.
        _reach_off = {_s for _s in ("bid", "ask") if (slug, _s) in self._reach_refused}
        for _s in _reach_off:
            self._reach_refused.discard((slug, _s))
            # A REPLACE's cancel DID reach the venue, so `cancel` is the truthful action; a
            # refused PLACE acted on nothing, and blank reads as UNCHANGED in every presence read.
            actions[_s] = CANCEL if actions[_s] == REPLACE else ""
            desired[_s] = None                       # blanks `sizes` on the write below
            # `reason_*` is left as decided: it says WHY the side was acted on, and its literal
            # set is a documented enum — the refusal itself is named by `hold_cause`.
        # ⛔ NAMED, NEVER SILENT: a side clamped by an UNRESOLVED CANCEL
        # (the old order's contracts still count — `_unresolved_exposure`) is a different refusal
        # from one clamped by inventory alone, and the tape must say which. The unresolved term
        # wins where both applied: it is the one that will clear on its own read-back.
        _unresolved_off = {_s for _s in _reach_off if (slug, _s) in self._unresolved_refused}
        for _s in _unresolved_off:
            self._unresolved_refused.discard((slug, _s))
        # ⛔ `cap_room` SITS UNDER THE REACH RAILS AND, LIKE THEM, IS NOT BOUND BY THE
        # BOTH-SIDES-OFF SHAPE [incident]: the cap-room clamp darkens the ADDING side
        # while the reducer keeps quoting, so the row is `status=ok` with `allow=(N,Y)` — a shape
        # the derivation below never reaches, and one no other gate would name.
        _hold_cause = ("unresolved_cancel" if _unresolved_off
                       else "reach_guard" if _reach_off
                       else "cap_room" if _cap_room_off else "")
        if not _hold_cause and _status == "ok" and not allow_bid and not allow_ask:
            # ⛔ THE CLOCK GUARD NAMES ITSELF [AMENDMENT 41]: it is one of `reduce_only_now`'s four
            # terms, and a run whose books all read `reduce_only` cannot tell a wind-down from a
            # final whistle — which is the only thing the guard's own read wants to count.
            _hold_cause = ("clock_guard" if reduce_only_now and clock_guard_now
                           # ⛔ AFTER the clock guard: a resolving book inside the window is
                           # stood down by the whistle first, and that is the read that wants it.
                           else "maintenance_guard" if reduce_only_now and maintenance_guard_now
                           # ⛔ THE VERIFY MISMATCH NAMES ITSELF TOO [P1b]: it is the SEVENTH
                           # `reduce_only_now` term and the only one produced by a venue
                           # DISAGREEMENT, so folding it into `reduce_only` would hide an
                           # unreconciled book inside the ordinary wind-down population.
                           else "verify_mismatch" if (
                               reduce_only_now
                               and slug in getattr(self, "_verify_reduce_only", ()))
                           else "reduce_only" if reduce_only_now
                           else "book_cap" if not (per_bid or per_ask)
                           else "max_total" if not (glob_bid or glob_ask)
                           else "other")
        # This cycle's net touch becomes next cycle's comparison point — stamped only on a QUOTED
        # cycle, so a skip never fabricates a "move" on the cycle after it.
        # ⛔ A None SIDE DOES NOT OVERWRITE: an unreadable net would otherwise store None and
        # LATCH, so every later cycle would read `prev_net is None` and label `no_prior_touch`
        # forever. Per side, so a thin bid does not blank a healthy ask.
        _prev_b, _prev_a = self._last_net.get(slug, (None, None))
        self._last_net[slug] = (net_bid if net_bid is not None else _prev_b,
                                net_ask if net_ask is not None else _prev_a)
        self._write_quote(stats, slug, tick, (bid, ask),
                          (None if "bid" in _reach_off else my_bid,
                           None if "ask" in _reach_off else my_ask), improved=improved,
                          actions=actions, reasons=reasons, queue=queue, inventory=inv,
                          allow=(allow_bid, allow_ask), read_ms=read_ms, gap_s=gap_s,
                          status=_status, book_stats=book_stats, net=(net_bid, net_ask),
                          # G4: the size actually intended per side — `size_by_side` AFTER the
                          # cooldown reducer, keyed off `desired` so a disallowed side tapes blank.
                          # Read-only: an argument to the tape writer, it decides nothing.
                          # ⛔ …and the size the reach rail SENT where it clamped that one
                          #: taping the decided 8 while a 6 rests
                          # misreports our own presence on exactly the cycles the rail bit.
                          sizes=tuple(self._reach_sized.get((slug, s), size_by_side[s])
                                      if desired[s] is not None else None
                                      for s in ("bid", "ask")),
                          rebate_step=_rebate_step_status, hold_cause=_hold_cause)

    async def _skip_market(self, stats: CycleStats, slug: str, tick: Decimal,
                           touch: Optional[tuple[Decimal, Decimal]], status: str, *,
                           read_ms: Optional[float], gap_s: Optional[float]) -> None:
        """One market could not be quoted this cycle: tape it, and decide whether to pull its
        resting quotes.

        The streak, not the single failure, is the signal — see `stale_cancel_cycles`. Every skip
        reason counts toward it, not just a failed HTTP read: a market that has gone one-sided or
        crossed for 30 s is exactly as unreadable, from a quoting point of view, as one timing out.
        """
        stats.markets_skipped += 1
        # A skipped cycle judged nothing, so it carries no parked-alive verdict [round-3 nit c].
        self._parked_alive_blocked.discard(slug)
        # ⛔ A SKIP IS NOT A REQUOTE: the reach-anchor release waits for the book to have been
        # quoted at the NEW size, and a skipped cycle quotes nothing — the order placed under the
        # previous (raised) size is still resting, because the pull below needs a STREAK. Counting
        # cycles instead of requotes let the release fire with that order live. Re-stamping here
        # is unconditional and conservative.
        self._resize_cycle[slug] = self.cycle_index
        # ⛔ THE STREAK AND THE PULL DECISION ARE COMPUTED **BEFORE** THE ROW IS WRITTEN. Computed
        # after, a cycle that cancelled BOTH sides taped `reason=hold` — under-counting pulls in
        # exactly the stress cycles an analyst would go looking for them in. The cancels
        # themselves still run after the write; only the LABEL moved.
        streak = self.unquotable_streak.get(slug, 0) + 1
        self.unquotable_streak[slug] = streak
        # ⛔ INSIDE THE MAINTENANCE WINDOW THE STREAK PULLS NOTHING. The
        # streak's premise is "a resting order on a market we can no longer read is unchecked
        # exposure"; inside the window the unreadability is the venue 503ing every route, and the
        # cancel would ride the same dead routes. If the venue is USING the window it has already
        # cancelled every open order itself; if it is not, the book is still ours and the orders
        # keep earning. Either way the pull is a cost with no counterparty. `poll_fills` retires
        # whatever vanished when the routes return.
        pulled = ([side for side in ("bid", "ask") if (slug, side) in self.resting]
                  if streak >= self.stale_cancel_cycles and not self._maintenance_now() else [])
        # Tape the REAL recorded inventory, not the signature default of 0: a skipped cycle 1 on a
        # carried book wrote inventory=0, which the session report's carry-baseline read as
        # "started flat".
        self._write_quote(stats, slug, tick, touch, None, status=status,
                          inventory=self.inventory.get(slug, _ZERO),
                          reasons={side: "pull" for side in pulled},
                          read_ms=read_ms, gap_s=gap_s)
        if not pulled:
            return
        log.warning(f"{slug}: un-quotable for {streak} consecutive cycles ({status}) — PULLING "
                    f"{len(pulled)} resting quote(s). A resting order on a market we can no "
                    f"longer read is exposure nothing is checking.")
        for side in pulled:
            await self._cancel(slug, side)
            stats.actions[CANCEL] = stats.actions.get(CANCEL, 0) + 1

    def _reach_permits(self, slug: str, side: str, count: Decimal) -> Decimal:
        """The largest size <= `count` this side may send against **LIVE** inventory.

        ⛔ ONE EXPRESSION, TWO CALL SITES: `_apply_action`
        RESIZES a replacement with it after the cancel's own `_reconcile_order` has moved the
        belief, and `_place` REFUSES with it at the wire — `_reach_permits(...) < count` is
        exactly the pre-send guard's admission test, so the two can never disagree.

        The rule, unchanged: a worst case at or under the book's registered `_launch_reach` is
        lawful (strict `>` matches `_venue_breach` and the parked-alive gate), and an order that
        SHRINKS `|inv|` is lawful whatever the bound says — that exemption is what keeps a
        position already past its reach able to exit (reducing quotes, the teardown flatten, the
        dust close). Anything else gives back exactly its overshoot, which lands the worst case
        ON the bound. Admissibility is a prefix in `count`, so this is the maximum, not a sample.

        ⛔ THREE TERMS ON THE GROWTH BRANCH:
        inventory, the order being sent, AND the contracts of every cancel on this side whose
        terminal state is not yet confirmed (`_unresolved_exposure`). A cancel ACK is not
        terminal — the venue acked one and filled it 4 s later, so a replacement sized against
        inventory alone can take the position past the reach.

        ⛔ AND THE SHRINK EXEMPTION IS TESTED ON THE BARE ORDER, BEFORE THAT TERM [money-path
        review 2026-09-07]. Adding unresolved contracts into the worst case the exemption reads
        makes a reducing order stop shrinking `|worst|` by construction, and a pile of unreadable
        parked entries would then REFUSE the flatten, the reduce-only quote and the dust close —
        stranding a position with no exit order ever sent. An order that shrinks `|inv|` is
        lawful, full stop; the unresolved term may only ever refuse GROWTH.
        """
        inv = self.inventory.get(slug, _ZERO)
        bound = Decimal(self._launch_reach.get(slug, self._launch_reach_unknown))
        bare = inv + (count if side == "bid" else -count)
        if bare.copy_abs() <= inv.copy_abs():
            return count                             # SHRINKS the position — always lawful
        _unresolved = self._unresolved_exposure(slug, side)
        # ⛔ TAPE ONLY, and DELIBERATELY BELOW THE SHRINK RETURN: a reducing order never
        # evaluates the term, so its row must say "not evaluated" (blank), not "0".
        self._tape_map("_unresolved_seen")[slug] = _unresolved
        exposed = count + _unresolved
        worst = inv + (exposed if side == "bid" else -exposed)
        if worst.copy_abs() <= bound:
            return count
        # ⛔ WHOLE CONTRACTS, and ONLY where the clamp bit: `int()` on an unclamped size would
        # floor the fractional dust close to 0 and refuse the one order that clears the residue.
        return Decimal(int(max(_ZERO, count - (worst.copy_abs() - bound))))

    async def _apply_action(self, action: str, slug: str, side: str,
                            price: Optional[Decimal], queue_ahead: Optional[Decimal],
                            improved: bool, size: Optional[int | Decimal] = None,
                            net: tuple[Optional[Decimal], Optional[Decimal]] = (None, None),
                            close_long: bool = False,
                            ) -> None:
        if action == HOLD:
            return                                   # ⛔ the queue position we are protecting
        if action in (CANCEL, REPLACE):
            # `for_replace` is read by the time-at-price clock ONLY: a REPLACE's cancel must not
            # end the price epoch, because the replacement may land at the same price.
            cancelled = await self._cancel(slug, side, for_replace=(action == REPLACE))
            # ⛔ NO PLACE OVER AN UNCONFIRMED CANCEL. Cannot-verify is not cancelled — discarding
            # this return and placing anyway overwrote `self.resting` and made the maybe-live old
            # order INVISIBLE: two 5-lots resting, inventory believing 5, the cap checked against
            # a wrong number, the flatten sized to half the real position. Refusing costs one
            # requote cycle; the teardown sweep is the last resort.
            if not cancelled and action == REPLACE:
                self.replace_blocked += 1
                return
            if self.should_stop:
                # ⛔ THE CANCEL'S OWN READ-BACK CAN HALT US. `_cancel` → `_reconcile_order` →
                # `_book_fill` books the fill that breaches the loss cap, and without this the
                # very next statement re-places the quote we just cancelled — new exposure taken
                # after the budget is known to be gone, on the book that spent it.
                #
                # The replacement is not coming, so the side is dark whatever the action was.
                self._end_price_epoch(slug, side)
                return
        if action in (PLACE, REPLACE) and price is not None:
            if self.shadow:
                # Shadow keeps a VIRTUAL resting book so the requote decision is genuinely
                # exercised and taped — that is the <run-id> measurement — without ever reaching
                # `_place`, which raises.
                _now = time.time()
                self.resting[(slug, side)] = RestingOrder(
                    slug=slug, side=side, price=price,
                    size=self.sizes.get(slug, self.size) if size is None else size, order_id=None,
                    intent_id="shadow", placed_ts=_now, queue_ahead=queue_ahead,
                    improved=improved, net_bid_place=net[0], net_ask_place=net[1],
                    rest_since_ts=_now)
                self._begin_price_epoch(slug, side, price)
                return
            # ⛔ EVERY QUOTE THE WS BOOK PRICED CARRIES THE WITNESS. The
            # 2026-09-03 adding-only carve-out (and its r2 refinement onto signed inventory) is
            # retired: on a frozen REST origin it withheld the witness from almost every refusal, all on REDUCING quotes
            # (the private design notes). The reducing side of an open position
            # is the side we most need resting, and the override is post-only against a NEWER
            # venue stamp that does not cross our price — the same exposure the teardown flatten
            # already accepts, and what the code ENFORCES is post-only on both rails (never a
            # taker fee). What is NOT bounded by code is the WS-vs-reality divergence: worst case
            # per order is that divergence × size (one tick on size 10 = <n>), unobserved.
            # `close_long` is not a second reason to
            # withhold it: a fractional dust close is a reducing quote priced off this same WS
            # book, and the flag only picks the venue intent (`SELL_LONG`), which
            # `_newer_witness_clears` already reads as a sell. See `_place`'s ws_witness comment.
            # ⛔ …AND ONLY IF THE WS BOOK ACTUALLY SERVED THIS CYCLE'S READ
            # 2026-09-07]. `book_source == "ws"` is the run's lane, not this book's: a cycle that
            # fell back to REST (`rest_fallback`, `ws_outage`, `rest_reverify`, `ws_capped`)
            # priced this quote off the REST body, and defending it with the retained WS frame is
            # pricing off one book while defending with the other. Same served-source set the
            # dead-marking rail reads (`_ws_dead_by_guard`), never a second one.
            # ⛔ RESIZE OFF **LIVE** INVENTORY, NOT THE CYCLE-TOP SNAPSHOT [root fix]. Everything above decided on `inv` as it was
            # at the top of the cycle, but a REPLACE's own cancel ran `_reconcile_order` five
            # statements ago and can have booked the outgoing order as FULLY FILLED. Placing the
            # pre-decided size then breaches the reach and halts the
            # evening on the next cycle top. The permissible size is therefore re-derived HERE,
            # from `_reach_permits` — the same expression `_place`'s pre-send guard refuses on,
            # so a resize can never leave a size that guard would then reject.
            # ⛔ REDUCING QUOTES ARE UNTOUCHED by construction: they shrink `|inv|`, which
            # `_reach_permits` admits at any size. The dust close (a fractional `close_long`)
            # rides the same exemption.
            # ⛔ AND A CLAMP MAY NOT LAND UNDER `MIN_SIZE`: below it the venue's minimum and the
            # rebate boundary are both missed, so the remaining reach buys inventory risk for no
            # credit. Dark for one cycle instead; the next cycle sizes off settled inventory.
            _want = Decimal(self.sizes.get(slug, self.size) if size is None else size)
            _permitted = self._reach_permits(slug, side, _want)
            if _permitted < _want:
                if _permitted < MIN_SIZE:
                    log.error(f"⛔ {action} REFUSED — no lawful reach left: {slug} {side} "
                              f"{_want} @ {price} (inventory {self.inventory.get(slug, _ZERO)}, "
                              f"reach {self._launch_reach.get(slug, self._launch_reach_unknown)}, "
                              f"permitted {_permitted})")
                    self._reach_refused.add((slug, side))
                    if self._unresolved_exposure(slug, side) > _ZERO:
                        self._unresolved_refused.add((slug, side))
                    self._end_price_epoch(slug, side)
                    return
                log.warning(f"⚠️ {action} RESIZED to the remaining reach: {slug} {side} "
                            f"{_want} → {_permitted} @ {price} (inventory "
                            f"{self.inventory.get(slug, _ZERO)}, reach "
                            f"{self._launch_reach.get(slug, self._launch_reach_unknown)})")
                size = int(_permitted)
                # The tape must report the size that went to the WIRE.
                self._reach_sized[(slug, side)] = size
            placed = await self._place(
                slug, side, price, improved=improved, queue_ahead=queue_ahead, size=size,
                net=net, close_long=close_long,
                ws_witness=self.last_book_src.get(slug) in ("ws", "ws_quiet"))
            # The epoch starts only on an order we believe RESTED: a stale epoch from the order we
            # just cancelled would date a future fill from a presence that had a gap in it.
            if placed is None:
                self._end_price_epoch(slug, side)
            else:
                self._begin_price_epoch(slug, side, price)

    def _ws_guard_witness(self, slug: str) -> Optional[tuple[Decimal, Decimal, float]]:
        """`(bid, ask, transact_epoch)` from OUR WS book for `slug`, for the client crossing
        guard's newer-witness rule — or None (guard behaves exactly as it always has).

        ⛔ THE FEED'S OWN PER-BOOK STATE, NEVER A READ: no request is spent and none may be.
        The stamp is the BOOK's own `transactTime` epoch, not our receipt time — the guard
        compares it against the REST body's `transactTime`, so both sides must be the venue's
        clock or the margin means nothing.

        ⛔ "NEWER" IS RELATIVE AND THAT IS NOT ENOUGH — a WS frame frozen at 400 s beats a REST
        body frozen at 410 s. The witness must ALSO pass this maker's own freshness rails,
        reused rather than copied: `_ws_book_servable` (the OR-rule — content age
        <= `self.ws_stale_s` OR `self._ws_socket_live(now)`, and not WS-dead), plus
        `self._ws_down_since is None`. Any failing = no witness. Fail CLOSED: an override is a
        placement through a touch we chose not to believe, and a content age we cannot READ
        (no `transactTime`) is refused here even though the quote rail serves it — the stamp IS
        the comparison the guard makes.

        ⛔ THE WITNESS RAIL IS THE QUOTE RAIL. It was
        content-age-only, so a QUIET book (a political/fixed seat unchanged for minutes) had no
        witness and every placement on it spent a nonced ORIGIN read, which under load held the
        key. But the maker QUOTES those books off the same cache under the OR-rule: `transactTime`
        is verified LAST-MUTATION time (refuted.md), so an old stamp on a live socket is a current,
        untraded book. The risk bound is therefore exactly quoting's — and ⛔ UNDER A KEY HOLD THERE
        IS NO BOUND: the `WS_REVERIFY_S` re-verify's only mechanism is a fresh ORIGIN read, which
        returns UNAVAILABLE while the key is held (observed: many books overdue). So
        a silently-unsubscribed quiet book can witness a post-only placement against a frozen
        touch for as long as the hold lasts. The maker already QUOTES off that same frame, so the
        guard adds no second transport there — this rail never held that risk; it only spent
        reads.

        ⛔ A BOOK `_ws_dead_by_guard` MARKED IS NOT A WITNESS: the
        guard already caught this cache lying against its own fresh read, so its retained frame
        may not then override that same guard. Fail closed for BOTH callers — the flatten's WS
        pricing included. ⚠️ The book's SERVE SOURCE is deliberately NOT checked here: the
        teardown flatten prices off this witness directly (its own `_fetch_book`, no
        `last_book_src` write), so gating on the quote cycle's serve source would silently return
        the 09-02 one-cent-behind flatten. The quote path asserts that precondition itself, at
        `_apply_action`, where "the WS book priced this quote" is a fact about the cycle."""
        if self.book_source != "ws" or self.book_feed is None:
            return None
        md = self.book_feed.get_book_md(slug)
        if md is None:
            return None
        bid, ask, _ = touch_from_md(md)
        if bid is None or ask is None:
            return None
        now = time.time()
        age = transact_age_s(md.get("transactTime"), now)
        if age is None:      # fail closed: no venue stamp, nothing to compare the REST body to
            return None
        if not self._ws_book_servable(slug, md, now):
            return None
        if self._ws_down_since is not None:
            return None
        return (bid, ask, now - age)

    async def _place(self, slug: str, side: str, price: Decimal, *,
                     improved: bool, queue_ahead: Optional[Decimal],
                     size: Optional[int | Decimal] = None,
                     net: tuple[Optional[Decimal], Optional[Decimal]] = (None, None),
                     ttl_s: Optional[float] = None,
                     close_long: bool = False,
                     ws_witness: bool = False,
                     ) -> Optional[RestingOrder]:
        """Send ONE resting quote. `post_only=True` always.

        ⛔ `close_long=True` ships `side="sell_long"` = `ORDER_INTENT_SELL_LONG` instead of the
        ordinary ask's `side="sell"` = `ORDER_INTENT_BUY_SHORT`, which OPENS a short. `SELL_LONG`
        is the safe ship under either reading of the venue's intent semantics
        (`PolyUSClient.place_limit_gtc` § `side="sell_long"`).
        ⚠️ ONLY the sub-contract reducing paths pass it — the ordinary quoting ask is UNCHANGED.
        ⛔ IT IS A STATEMENT ABOUT THE `size` IN THIS CALL, NOT ABOUT THE POSITION: deriving it
        from the holding ships a full-size `SELL_LONG` whenever a dust book quotes normally,
        which is the flip-through-flat it exists to prevent.
        ⛔ REDUCE-ONLY IS THE CALLER'S JOB (the venue has no `reduceOnly` field): a `close_long`
        order MUST be sized at or below the long actually held, or it flips through flat.

        `ws_witness=True` hands the client's crossing guard our WS touch as a newer witness
        against a frozen REST origin. ⛔ DEFAULT FALSE, and the order stays POST-ONLY either way:
        the witness buys a resting price the frozen REST body would have refused, never a taker
        fill — but the venue ACCEPTS AND RESTS a post-only order priced through the live touch
        (2026-07-19 A/B, `place_limit_gtc`), so a WS book ahead of reality RESTS the order through
        the true touch and pays that divergence. ⚠️ Post-only is the only bound the code enforces;
        the divergence itself is UNOBSERVED, and the worst case per order is divergence × size
        (one tick on size 10 = <n>).
        THE RULE AS OF 2026-09-07 [operator]: `_apply_action` passes it on EVERY quote — adding
        or reducing, dust close included — because the quote path prices off that same WS book;
        withholding it on reducing quotes cost almost every guard refusal and a long dark stretch over
        one evening. ⛔ The invariant is unchanged:
        pass it ONLY where the WS book PRICED the order — `_teardown_flatten` still passes it just
        on a flatten the WS book itself priced (2026-09-05). Pricing off one book and defending
        with the other is what left the 09-02 residual behind the touch.

        `ttl_s` overrides the run's `--order-ttl-s` for THIS order; `None` means "the run's TTL".

        ⛔ The durable intent is written BEFORE the order is sent — recording after the venue
        answers leaves "sent, then died before the response arrived" with no trace.

        `size` defaults to the quote size but the TEARDOWN passes the residual: sizing a reducing
        order at `--size` overshoots whenever the residual is smaller, opening a position in the
        opposite direction to the one it was clearing.
        """
        if self.shadow:
            raise ShadowViolation(
                f"shadow run reached _place({slug}, {side}, {price}) — a shadow run places "
                f"NOTHING, and that has to be structural rather than incidental.")
        count = self.sizes.get(slug, self.size) if size is None else size
        # ⛔ PRE-SEND REACH GUARD, ON **LIVE** INVENTORY. `_apply_action` decides off the cycle-top `inv`
        # snapshot, but a REPLACE's own cancel runs `_reconcile_order`, which can book the
        # outgoing order as FULLY FILLED between that snapshot and this send: belief jumped inside
        # the cancel, the place added the full size against the stale belief, and the sum breached the
        # reach and halted the run on the next cycle top. The cycle-top halt cannot see
        # an intra-cycle move, so the rail is repeated HERE, at the last point before the wire.
        # ⛔ ONE RULE, TWO CALL SITES: same `_launch_reach` / `_launch_reach_unknown` and the
        # same strict `>` as `_venue_breach` and the parked-alive gate — AT the reach is lawful.
        # ⛔ AND IT MAY ONLY REFUSE **GROWTH**: the shrink exemption is tested on the BARE order
        # (`|inv ± count| <= |inv|`), BEFORE any unresolved-cancel term, so every shrinking order
        # — a reducing quote, the teardown flatten, a dust close — is exempt whatever else is
        # outstanding, and a position already past its reach keeps its only exit. Refusing records NO intent and leaves the
        # side dark for one cycle; the next cycle sizes normally off the settled inventory.
        # ⛔ AND THE ROOT FIX SIZES OFF THE SAME EXPRESSION, one cycle earlier, in
        # `_apply_action` — this stays the LAST line, on the value actually going to the wire.
        _reach_inv = self.inventory.get(slug, _ZERO)
        _reach_bound = self._launch_reach.get(slug, self._launch_reach_unknown)
        if self._reach_permits(slug, side, Decimal(count)) < Decimal(count):
            log.error(f"⛔ place REFUSED — would breach lawful reach: {slug} {side} {count} "
                      f"@ {price} (inventory {_reach_inv}, reach {_reach_bound})")
            # The row for this cycle must not show the side as quoted.
            self._reach_refused.add((slug, side))
            if self._unresolved_exposure(slug, side) > _ZERO:
                self._unresolved_refused.add((slug, side))
            return None
        self._seq += 1
        intent_id = f"{slug}:{side}:{self._seq}"
        if self.state is not None and self.real:
            self.state.record_intent(intent_id, ticker=slug, side=side, price=price,
                                     count=count)
        # The deadline is stamped HERE so the value we record locally and the value on the wire
        # come from ONE computation. `None` (TTL off) keeps the payload byte-identical.
        # ⛔ A caller that passes `ttl_s` (the teardown flatten) gets a deadline whenever the rail
        # has not disabled TTL, INDEPENDENT of the lane's quote TTL: flatten closes have rested over an
        # hour under a venue ban because the probe lane ships no quote TTL and the
        # flatten's own budget was computed and ignored. Still a second net under our own cancels.
        deadline_ts: Optional[float] = None
        good_till_time: Optional[str] = None
        if self.ttl_on or (ttl_s is not None and self._ttl_disabled is None):
            deadline_ts = float(int(time.time() + (self.order_ttl_s if ttl_s is None
                                                   else float(ttl_s))))
            good_till_time = gtd_stamp(deadline_ts)
        try:
            # ⛔ Both kwargs are OMITTED, not passed as None, when off. OFF mode must be
            # byte-identical at the CALL SITE too, not merely on the wire.
            # The WS witness lets the guard tell a stale REST body from a real disagreement.
            # ⛔ THE CALLER MUST HAVE PRICED OFF THE SAME WITNESS. `ws_touch` never makes an order
            # aggressive (post_only stands); it stops the guard's own frozen REST body refusing a
            # price a NEWER venue stamp says rests. The quote path and the teardown flatten both
            # qualify; nothing may pass it while pricing off REST.
            _ws_witness = self._ws_guard_witness(slug) if ws_witness else None
            # TWO requests, not one: `place_limit_gtc` fetches the book itself for its crossing
            # guard before it sends. Counting only our own reads understates the live rate ~5x.
            # ⛔ ONE on the witness arm: a witness plus `post_only` (always
            # true here) makes the guard compare the WS frame and spend NO request, so charging 2
            # would overstate the live rate on exactly the path that exists to spend less.
            _charge = 1 if _ws_witness is not None else 2
            self._extra_requests += _charge
            resp = await self.client.place_limit_gtc(
                slug, price, count, label=f"polymm {side}",
                post_only=True,
                side=("buy" if side == "bid" else ("sell_long" if close_long else "sell")),
                **({"good_till_time": good_till_time} if good_till_time is not None else {}),
                **({"ws_touch": _ws_witness} if _ws_witness is not None else {}))
            self._capture_guard_book(slug)
        except PreSendRefusal as exc:
            # The client refused BEFORE any venue call — nothing was placed, KNOWN. The durable
            # intent is a phantom: left in place it reads as `maybe_live_orders` and blocks a
            # sibling lane's start. ONLY this type clears — a generic exception may have a resting
            # order behind it. And no requests happened: un-charge what was pre-charged above.
            self._extra_requests -= _charge
            log.error(f"place refused {slug} {side} @ {price}: {safe_exc(exc)}")
            if self.state is not None and self.real:
                self.state.clear_order(intent_id)
            return None
        except Exception as exc:
            # An error is NOT an outcome — we do not know whether the order rested. The intent
            # stays in the durable record so the teardown sweep and `maker_recover` both see it.
            # ⛔ AND IT ARMS THE ORPHAN RAIL FOR THE REST OF THE RUN: this is the one branch that
            # can leave a REAL resting order with NO entry in `self.resting`, and `poll_fills`
            # normally skips its listing read when nothing rests and nothing is parked — so the
            # orphan would be invisible for the whole run. Never cleared: "the order may have
            # rested" does not expire.
            self._orphan_suspects += 1
            # The guard RAN before the create was attempted, so its book is real evidence about
            # this failure. (Not on the `PreSendRefusal` branch: that refusal happens BEFORE the
            # guard's book read.)
            self._capture_guard_book(slug)
            log.error(f"place failed {slug} {side} @ {price}: {safe_exc(exc)}")
            return None
        if resp is None:
            # The client's crossing guard refused, or the touch was unreadable. Nothing rested. A
            # refusal on the guard's OWN FRESH read, against a book we served from the WS cache,
            # is evidence the cache is frozen — close it here.
            self._ws_dead_by_guard(slug)
            if self.state is not None and self.real:
                self.state.clear_order(intent_id)
            return None
        # Tape only [I2]. Read HERE, past the `resp is None` return, because that is the one
        # point where an ack is known to have arrived — the client writes its span only on the
        # acked path, so a refused or failed send can never hand us a stale number. Outside the
        # `try:` above on purpose: a raise in there is read as a CREATE FAILURE.
        # `getattr` because a stub client has no such attribute (same rule as the guard book).
        _lag = getattr(self.client, "last_ack_lag_ms", {}).get(slug)
        if _lag is not None:
            self._tape_map("_ack_lag_ms")[slug] = _lag
        order_id = venue_order_id(resp)
        if order_id is None and not self.client_is_dry:
            # LOUD. A silent None here is what disabled fills, cancels and both caps at once. The
            # order is still recorded (with a None id) so the sweep and `maker_recover` find it.
            log.error(f"⚠️ {slug} {side} @ {price}: the venue's create response carried NO `id` "
                      f"(keys={sorted(_order_body(resp))}). The order MAY BE RESTING and cannot "
                      f"be cancelled or polled by id — the teardown sweep is the only backstop.")
        if self.state is not None and self.real:
            self.state.record_placed(intent_id, order_id)
        if self.order_feed is not None and order_id is not None and self.real:
            # The echo watchdog's input: a real placement must echo on the private WS within its
            # deadline. Real only — a dry client's orders never reach the venue.
            self.order_feed.expect_echo(order_id)
        # ONE `time.time()` for both stamps: `placed_ts` and `rest_since_ts` are the same wall
        # moment by definition, and two calls would let them disagree by the call gap.
        _placed_ts = time.time()
        order = RestingOrder(slug=slug, side=side, price=price, size=count,
                             order_id=order_id, intent_id=intent_id,
                             placed_ts=_placed_ts, queue_ahead=queue_ahead, improved=improved,
                             net_bid_place=net[0], net_ask_place=net[1],
                             deadline_ts=deadline_ts, rest_since_ts=_placed_ts)
        self.resting[(slug, side)] = order
        if deadline_ts is not None:
            self.ttl_stamped += 1
            self._write_ttl("stamped", order, observed_ts=order.placed_ts,
                            verdict=f"tif GOOD_TILL_DATE, goodTillTime {good_till_time}")
        return order

    # ── time-at-price bookkeeping (instrumentation only) ────────────────────────────────────

    def _begin_price_epoch(self, slug: str, side: str, price: Decimal) -> None:
        """Record that this side is quoting `price`, KEEPING the existing start when the price is
        unchanged. Called after a placement is known to have rested.

        ⛔ THE `if` IS THE WHOLE COLUMN. Stamping unconditionally would make `price_rest_s` a
        second copy of `order_rest_s`, because a same-price REPLACE re-places exactly like a move
        does — and the pair exists precisely to tell those apart."""
        key = (slug, side)
        current = self._price_clock.get(key)
        if current is None or current[0] != price:
            self._price_clock[key] = (price, time.time())

    def _end_price_epoch(self, slug: str, side: str, *, for_replace: bool = False) -> None:
        """The side is no longer quoting — forget its epoch, so a later placement at the same
        price starts a fresh one (we were absent; the price was not ours during the gap).

        `for_replace=True` suppresses that: a REPLACE cancels only to re-place, and the epoch's
        fate is decided by the price the replacement lands at (`_begin_price_epoch`) or by the
        replacement failing to rest (the call sites end it explicitly)."""
        if not for_replace:
            self._price_clock.pop((slug, side), None)

    async def _cancel(self, slug: str, side: str, *, for_replace: bool = False) -> bool:
        """Cancel one resting quote. True if the venue confirmed it.

        `for_replace` is INSTRUMENTATION ONLY (see `_end_price_epoch`).

        ⛔ THE ORDER IS FORGOTTEN ONLY ON A CONFIRMED CANCEL. `client.cancel_order` returns False
        on failure and does NOT raise, so popping first turns a failed cancel into an invisible
        live order. Cannot-verify is not cancelled.

        ⛔ AND THE ORDER'S FINAL FILL IS RECONCILED FIRST — an order can fill in the requote
        window between two polls, and dropping it without a last read loses those contracts
        permanently, leaving the cap computed against a wrong number.
        """
        order = self.resting.get((slug, side))
        if order is None or self.shadow:
            self.resting.pop((slug, side), None)
            self._end_price_epoch(slug, side, for_replace=for_replace)
            return True
        if order.order_id is None:
            # Never had an id (a create whose response carried none). Nothing to cancel by id; the
            # teardown sweep is the backstop. Keep the durable intent so recovery still sees it.
            log.warning(f"{slug} {side}: no venue id — cannot cancel by id, leaving it for the "
                        f"teardown sweep.")
            self.resting.pop((slug, side), None)
            # Tape only [I2]: a cancel DID run on this book and produced NO read-back at all.
            # `none` and blank are different facts — blank is "no cancel this cycle".
            self._tape_map("_cancel_readback")[slug] = "none"
            # UNCONDITIONAL, even under a REPLACE: this order left our book with its fate unknown.
            # An epoch we cannot vouch for is worse than no epoch, so it ends here.
            self._end_price_epoch(slug, side)
            return False
        # ⛔ THE READ-BACK COMES AFTER THE ACK. Same one request, one RTT
        # later: read BEFORE the cancel and the answer is always a resting `ORDER_STATE_NEW`,
        # which confirms nothing terminal, so `_unresolved_exposure` would hold the side's
        # contracts against every ordinary replace for the whole verify horizon. Read AFTER and
        # the ordinary replace resolves on the spot (`ORDER_STATE_CANCELED`), while the ghost —
        # acked, still trading — answers non-terminal or unreadable and keeps its exposure
        # counted, which is exactly the case that cost a real loss. It still books any fill
        # taken in the requote window, later and therefore no worse.
        self._extra_requests += 1
        ok = await self.client.cancel_order(order.order_id, slug)
        _filled_before = order.filled_qty
        readable = await self._reconcile_order(order)
        # Tape only [I2]: the read-back's own verdict, in the terms this method and
        # `_reconcile_order` already use. `filled_beside` FIRST — an increment booked by this very
        # read-back is the ghost-replace shape, and it is the fact worth knowing even when the
        # order also came back terminal.
        self._tape_map("_cancel_readback")[slug] = (
            "filled_beside" if order.filled_qty > _filled_before
            else "terminal_verified" if order.order_id in self.terminal_verified
            else "blind" if not readable
            else "pending")
        if ok:
            # ⛔ EVERY confirmed cancel parks for a delayed verify — not just the blind ones. The
            # immediate read above can ANSWER with a stale cumQuantity and still miss a fill, so a
            # presence test on the read cannot decide safety. One extra read per cancel is the cost.
            self._park(order)
            # No expect_echo here: the watchdog is PLACEMENT-driven. Cancel echoes are observed on
            # the tape but no per-order cancel timestamps exist, so neither their rate nor their
            # latency can be computed — and an unvalidated expectation is a false-death storm.
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
        self._end_price_epoch(slug, side, for_replace=for_replace)
        # No clear_order here — the durable record retires at the parked VERIFY, same as the poll
        # path. Clearing at cancel-confirm erased the record 120 s before the verify could book a
        # stale-read fill. An order that never got a venue id has no parked entry.
        return True

    # ── GTD order TTL: the venue-side dead man, and the rail that refuses to trust it ────────

    @property
    def ttl_on(self) -> bool:
        """True while creates should carry a deadline. Goes False for the rest of the run the
        moment the rail catches the venue not honouring one — never back True."""
        return self.order_ttl_s > 0 and self._ttl_disabled is None

    def _flatten_ttl_s(self) -> float:
        """The deadline a TEARDOWN FLATTEN order is given, in seconds from its own placement.

        ⛔ It must outlive the wait it is given, and the margin is not a bare 60 s. Each flatten
        order is stamped at ITS OWN `_place` time, but the single `sleep(flatten_wait_s)` starts
        only after the LAST placement — so the first order of a wide slate has already burned the
        whole placement wall before the wait begins, and the poll and sweep are still to come. The
        budget is `wait + 60 s + 2 s per book`. The run's own TTL is the other floor; this
        expression exists so the guarantee does not silently depend on that dominating.
        """
        return max(self.order_ttl_s, self.flatten_wait_s + 60.0 + 2.0 * len(self.slugs))

    def _disable_ttl(self, reason: str) -> None:
        """Fall back to plain GTC for the rest of the run. Idempotent, and LOUD.

        Falling back is always safe: GTC + our own cancels is exactly what every run before this
        feature did. That asymmetry is why the rail may act on its own.
        """
        if self._ttl_disabled is not None:
            return
        self._ttl_disabled = reason
        log.error(f"⛔ ORDER TTL DISABLED for the rest of this run — {reason}. New quotes go out "
                  f"as plain GTC (tif GOOD_TILL_CANCEL, no goodTillTime); the maker's own cancels "
                  f"and the teardown sweep are the only order lifetime control from here.")
        self._write_ttl("disabled", None, observed_ts=time.time(), verdict=reason)

    def _write_ttl(self, event: str, order: Optional[RestingOrder], *,
                   observed_ts: float, state: str = "", verdict: str = "",
                   deadline_ts: Optional[float] = None) -> None:
        """One row on the TTL tape. Never raises — this is instrumentation, and instrumentation
        must not be able to stop trading (same rule as `_beat`)."""
        dl = deadline_ts if deadline_ts is not None else (
            order.deadline_ts if order is not None else None)
        # An order that CARRIES a deadline is taped whatever the lane's quote TTL — the teardown
        # flatten stamps one on a lane with `order_ttl_s` 0, and its `stamped`/`expired` rows are
        # the only evidence the venue enforces a deadline on that lane. Instrumentation only.
        if self.order_ttl_s <= 0 and dl is None:
            return
        try:
            _handle, writer = self._writer(self._ttl_path, _TTL_HDR, rotate_tag="ttl")
            lag = (observed_ts - dl) if dl is not None else None
            writer.writerow([
                f"{time.time():.3f}", event,
                order.slug if order is not None else "",
                order.side if order is not None else "",
                order.order_id or "" if order is not None else "",
                f"{dl:.3f}" if dl is not None else "",
                f"{observed_ts:.3f}",
                f"{lag:.3f}" if lag is not None else "",
                state, verdict, self.run_id,
            ])
            _handle.flush()
        except Exception as exc:                      # defensive
            log.error(f"TTL tape write failed ({exc!r}) — enforcement quality is UNMEASURED "
                      f"for this event; the counters in the run summary still hold")

    def _write_advexit(self, slug: str, *, inv: Decimal, side: str,
                       crossable_px: Optional[Decimal], depth: Optional[Decimal],
                       now: float) -> None:
        """One row on the ADVEXIT tape per mark-tripwire breach. Never raises.

        ⛔ INSTRUMENTATION, AND IT MUST NOT BE ABLE TO STOP TRADING — the `_write_ttl`/`_beat`
        rule. A counterfactual line is worth nothing if it can kill a live run.

        ⛔ THE RESOLVE-AT IS AN INPUT, NEVER DERIVED HERE. This maker does not import
        `poly_resolution`'s slug-date heuristic and must not: a second spelling of the horizon rule
        inside the money path is exactly the drift the one-owner rule prevents. What it CAN say is
        which absence it has — an empty map is a LAUNCH that never passed the flag, a missing key
        in a populated map is a BOOK the heuristic could not date.
        """
        try:
            ra = self.resolves_at.get(slug)
            if ra is not None:
                reason = ""
            elif not self.resolves_at:
                reason = ADVEXIT_NO_MAP
            else:
                reason = ADVEXIT_UNDATED
            ttr_h = (ra - now) / 3600.0 if ra is not None else None
            fits = depth is not None and depth >= inv.copy_abs()
            near = ra is not None and 0 < (ra - now) < 86400.0
            _handle, writer = self._writer(self._advexit_path, _ADVEXIT_HDR,
                                           rotate_tag="advexit")
            writer.writerow([
                f"{now:.3f}", slug,
                f"{ra:.3f}" if ra is not None else "",
                f"{ttr_h:.1f}" if ttr_h is not None else "",
                side,
                crossable_px if crossable_px is not None else "",
                depth if depth is not None else "",
                inv.copy_abs(),
                "YES" if (near and fits) else "NO",
                reason, self.run_id,
            ])
            _handle.flush()
        except Exception as exc:                      # defensive
            log.error(f"ADVEXIT tape write failed ({exc!r}) — this breach is missing from the "
                      f"the adverse-exit counterfactual n; the run is unaffected")

    def _ttl_skip_forbidden(self, order: RestingOrder) -> bool:
        """True when the conditional poll may NOT skip this order's read-back.

        ⛔ An order carrying a deadline is read EVERY poll while TTL mode is on. The conditional
        poll's safety argument is "the next poll catches it", which is about FILLS; the TTL rail
        asks whether the venue is HOLDING the deadline we sent, and a rail that only samples
        cannot answer it. Both features are flag-gated and off by default.
        the cost lands only on a run that armed both deliberately.
        """
        return self.ttl_on and order.deadline_ts is not None

    def _observe_ttl(self, order: RestingOrder, body: Mapping[str, Any], verdict: str, *,
                     now: float, state: str) -> Optional[str]:
        """ONE TTL observation on one order read-back. Returns a cancel REASON, or None.

        It never cancels and never places — the caller acts after the poll loop, so a TTL
        decision can never re-enter the loop that produced it. Three detected states:
          1. `ORDER_STATE_EXPIRED` — the venue DID enforce; counted, latency taped.
          2. a stale/absent `goodTillTime` on a resting order — the order has no dead man behind
             it, so cancel and let the next cycle re-place. ⚠️ A CREATE-time integrity check,
             not a renewal check — nothing renews a deadline.
          3. ⛔ PAST DEADLINE AND STILL RESTING — the venue accepted the field and does not act
             on it: cancel it ourselves and disable TTL for the run.

        ⚠️ It deliberately does NOT retire an order for being past its deadline — a deadline is
        not evidence the order is gone, and retiring on our own arithmetic is how a live order
        becomes invisible and gets quoted over.
        """
        if not self.ttl_on or order.deadline_ts is None:
            return None
        past = now - order.deadline_ts
        if state == "ORDER_STATE_EXPIRED":
            # ONCE PER ORDER, not once per observation — this read repeats for as many polls as
            # the open-orders listing keeps carrying the order.
            if order.order_id not in self._ttl_expired_ids:
                self._ttl_expired_ids.add(order.order_id or "")
                self.ttl_expired += 1
                self.ttl_expiry_lags.append(past)
                self._write_ttl("expired", order, observed_ts=now, state=state,
                                verdict="venue enforced the deadline")
            return None
        if verdict == "not_found" and past > 0:
            # Consistent with expiry, and NOT proof of it — a purge looks identical whatever
            # killed the order. The cancel-probe chain still decides retirement, unchanged.
            # ⚠️ SCOPE OF THE BREAKER CLAIM: while the venue still answers ORDER_STATE_EXPIRED the
            # read is structured and never not_found, so the breaker cannot see it. Once the order
            # is PURGED the read does answer not_found, and whether the listing drops it BEFORE
            # the read endpoint does is UNMEASURED. So "a night of expiries cannot trip the
            # breaker" is established for the EXPIRED phase only; what shrank the exposure is the
            # residence-scale TTL, not a proof.
            self.ttl_purged_past_deadline += 1
            self._write_ttl("purged_past_deadline", order, observed_ts=now, state=state,
                            verdict="consistent with expiry — NOT proof; retirement unchanged")
            return None
        if verdict != "ok" or not state or state in _TERMINAL_ORDER_STATES:
            # Cannot-verify says nothing about the deadline, and an order that died some other
            # way is not a TTL observation.
            return None
        if past > TTL_NOT_ENFORCED_GRACE_S:
            self.ttl_not_enforced += 1
            log.error(
                f"⛔ TTL NOT ENFORCED: {order.slug} {order.side} order {order.order_id} is "
                f"{past:.0f}s past its goodTillTime and still reads {state}. The venue ACCEPTED "
                f"the deadline and is not acting on it — an echoed field is not an enforced one. "
                f"Cancelling it ourselves and falling back to GTC.")
            self._write_ttl("not_enforced", order, observed_ts=now, state=state,
                            verdict=f"still resting {past:.0f}s past deadline")
            # ⛔ UNCONDITIONAL, regardless of `ttl_fallback`. That flag governs the softer
            # stale-deadline branch, where the venue is at least holding SOME deadline. This
            # branch means the dead man does not work at all.
            self._disable_ttl(f"venue did not expire {order.order_id} {past:.0f}s past its "
                              f"deadline (observed once — that is enough)")
            return "ttl_not_enforced"
        read_deadline = iso_to_ts(body.get("goodTillTime"))
        if read_deadline is None or read_deadline < order.deadline_ts - TTL_STALE_TOLERANCE_S:
            # A missing deadline gets the same treatment as an old one on purpose: "the venue did
            # not send goodTillTime" and "the venue is holding an older goodTillTime" are both
            # "this resting order is not protected by the deadline we think it has".
            self.ttl_stale_deadline += 1
            self._ttl_stale_this_poll += 1
            self._write_ttl("stale_deadline", order, observed_ts=now, state=state,
                            verdict=(f"read back {body.get('goodTillTime')!r}, sent "
                                     f"{gtd_stamp(order.deadline_ts)}"))
            return "ttl_stale_deadline"
        return None

    async def _apply_ttl_cancels(self, decisions: list[tuple[RestingOrder, str]]) -> None:
        """Act on `_observe_ttl`'s verdicts, AFTER the poll loop. Cancel only.

        The re-place is left to the next quote cycle: `quote_action` sees an empty side and
        PLACEs, with a fresh deadline. That keeps this rail on the one path the money-path review
        has already covered, so a rail defect can only cost quotes, never open exposure.
        """
        for order, reason in decisions:
            log.warning(f"{order.slug} {order.side}: hard-replacing {order.order_id} — {reason}. "
                        f"Cancelling now; the next quote cycle re-places with a fresh deadline.")
            await self._cancel(order.slug, order.side)

    def _drain_order_feed(self) -> None:
        """Book every order body the WS feed has delivered since last cycle. Zero requests.

        Runs BEFORE poll_fills so the poll's later read-back of the same order sees its cum
        already counted (delta 0) — WS accelerates, REST verifies, and cum-idempotency makes
        double-booking structurally impossible. An unknown order id is NOT booked (we cannot
        attribute a side/slug we never placed); the teardown sweep remains the net for those.
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
                # ⛔ THE INCIDENT'S OWN CADENCE: every fill in that sweep was
                # `booked_via=ws` and the ghosts lived 4–24 s — far inside both the parked
                # verify's horizon and the store's creation lag, so the WS event is the FIRST and
                # cheapest place a live ghost is visible. Recording is sync; the re-cancel it asks
                # for is issued at the top of `poll_fills`.
                if self._note_parked_sighting(
                        oid, str(body.get("state") or ""),
                        cum=_amount(body.get("cumQuantity"))):
                    if oid not in self._parked_recancel_due:
                        self._parked_recancel_due.append(oid)
            if ro is None and oid in self.pending_activity_recovery:
                # A recovery-queued order is still OURS — a pushed body for it books
                # normally and must never count as unattributed.
                ro = self.pending_activity_recovery[oid].order
            if ro is None:
                # Not ours to book (we cannot attribute a side/slug we never placed — usually a
                # different process). Counted, never silent.
                self.order_feed_unattributed += 1
                continue
            if self._book_fill(ro, _order_body({"order": body}), booked_via="ws") is not None:
                booked_any = True
        # ⛔ The DURABLE record must sync HERE, on the booking path itself: the poll's sync is
        # gated on its OWN fills list, and a ws-booked fill leaves that poll a zero delta — so
        # without this line no path ever wrote the record, and a state file carried the PRIOR
        # run's inventory at opposite sign. Once per drain, not per event.
        if booked_any and self.state is not None and self.real:
            self.state.set_inventory(self.inventory)
        if drained:
            log.info(f"order feed: drained {drained} event(s)")
        # The echo watchdog rides the same per-cycle seam as the drain — liveness was computed and
        # never read in-run. On death the feed has already torn itself down for its reconnect
        # ladder; booking degrades to the REST poll, which reads everything regardless.
        reason = self.order_feed.check_liveness()
        if reason:
            log.warning(f"⚠️ order feed DEAD ({reason}) — booking degrades to the REST poll "
                        f"until the reconnect ladder restores the subscription")

    async def _reconcile_order(self, order: RestingOrder) -> bool:
        """Read one order's final state and book any fill we have not already counted.
        Returns True if the read was USABLE (a cumQuantity was present — fill or no fill),
        False if the order could not be read.

        ⛔ NOT "silent on a read failure — the order stays in our book". `_cancel` pops the order
        after a confirmed cancel regardless, so an order that filled inside the venue's ~60–90 s
        create-lag blind window vanished with its fill unbooked. The caller now PARKS unreadable
        orders in `pending_reconcile` instead of letting them vanish.
        """
        if order.order_id is None:
            return False
        self._extra_requests += 1
        body = _order_body(await self.client.get_order(order.order_id))
        self._book_fill(order, body)
        # ⛔ THE ONLY PLACE A CANCEL'S EXPOSURE IS RETIRED. A terminal
        # state read off the ORDER retires it from `_unresolved_exposure`; anything else (a
        # resting `ORDER_STATE_NEW`, an unreadable body) leaves it counted, so the replacement
        # is sized against it. A later sighting takes this back (`_note_parked_sighting`).
        if str(body.get("state") or "") in _TERMINAL_ORDER_STATES:
            self.terminal_verified.add(order.order_id)
        else:
            self.terminal_verified.discard(order.order_id)
        return _amount(body.get("cumQuantity")) is not None

    def _park(self, order: RestingOrder, origin: str = "cancel") -> None:
        """Park a cancelled/terminal order for its delayed verify. Bounded: past the scaled bound
        the NEWEST entry is EVICTED into the activities-recovery queue rather than dropped (the
        ledger outlives the order store, so an eviction is a routing decision, not a loss)."""
        if order.order_id is None:
            return
        now = time.time()
        # ⛔ The bound SCALES WITH THE RUN and eviction removes the NEWEST entry: a fixed bound
        # binds at ~2.5 fully-repricing markets, and evicting by min(due_ts) removed the entry
        # CLOSEST to verifiable, i.e. the highest-information one. The victim keeps its shot at a
        # verdict through the recovery queue, carrying ITS OWN terminal_ts and origin path.
        # ⛔ A SIGHTED-ALIVE ENTRY IS NEVER EVICTED. Eviction drops the order out of
        # `pending_reconcile`, which is what `_parked_alive_size` iterates — so evicting a
        # known-live order RELEASES the adding side against exposure the venue is still holding.
        # If every entry is sighted alive the bound is BREACHED deliberately and loudly, because
        # the alternative is quoting over ghosts.
        if len(self.pending_reconcile) >= self.max_parked:
            _evictable = [k for k in self.pending_reconcile if k not in self.parked_alive]
            if not _evictable:
                log.error(f"⛔ parked set at bound {self.max_parked} and EVERY entry is sighted "
                          f"ALIVE at the venue — refusing to evict. The bound is exceeded on "
                          f"purpose: dropping a live order would release the adding side "
                          f"against exposure the venue is still holding.")
            else:
                victim = max(_evictable, key=lambda k: self.pending_reconcile[k][1])
                v_order, _due, _att, v_terminal, v_origin = self.pending_reconcile[victim]
                log.error(f"⚠️ parked set at bound {self.max_parked} — EVICTING NEWEST UNSIGHTED "
                          f"{victim} into activities recovery (older entries are closer to their "
                          f"verify and keep their place; sighted-alive entries are never "
                          f"evicted).")
                del self.pending_reconcile[victim]
                self._forget_parked(victim)
                self._queue_recovery(v_order, v_terminal, f"park_evicted_{v_origin}")
        # A re-park is a fresh cancel: the previous run of sightings says nothing about it, and
        # the cum baseline the sighting test compares against is re-stamped from THIS order.
        # ⛔ …EXCEPT a terminal state read for THIS cancel, which `_forget_parked` also clears:
        # `_cancel` reads back after the ack and parks immediately after, so dropping it would
        # dark the side after every ordinary replace. Carried across the forget, not around it.
        _terminal = order.order_id in self.terminal_verified
        self._forget_parked(order.order_id)
        self.pending_reconcile[order.order_id] = (order, now + LAG_HORIZON_S, 0, now, origin)
        self.parked_cum_at_park[order.order_id] = order.filled_qty
        if _terminal:
            self.terminal_verified.add(order.order_id)
        # Retired entries carry no exposure, so their terminal marks are dead weight — pruned
        # here rather than at each retirement site, which is where they would drift out of sync.
        self.terminal_verified &= set(self.pending_reconcile)

    def _forget_parked(self, oid: str) -> None:
        """Drop every parked-alive trace of `oid`. Called wherever an entry leaves
        `pending_reconcile`, and on a re-park, so the alive map cannot outlive the parked set."""
        self.parked_alive.pop(oid, None)
        self._parked_recancel_ts.pop(oid, None)
        self.parked_cum_at_park.pop(oid, None)
        self.terminal_verified.discard(oid)

    def _note_parked_sighting(self, oid: str, state: str, *, cum: Optional[Decimal] = None,
                              presence_is_alive: bool = False) -> bool:
        """Record what one venue read said about a PARKED order. True ⇒ it is NEWLY alive and
        its cancel must be re-issued (`_sight_parked` is the async wrapper; this half is sync so
        the WS drain, which is not a coroutine, can call it).

        ⛔ THE DISCRIMINATOR, and it is the whole design: any read may declare a parked order
        ALIVE, but only on evidence a STALE record cannot produce —
          · `ORDER_STATE_PARTIALLY_FILLED` (a cancelled order cannot acquire a partial fill);
          · `cumQuantity` ADVANCED past the cum recorded when we parked it;
          · presence in the OPEN-ORDERS listing (a second endpoint asserting it still works).

        ⛔ A PLAIN `ORDER_STATE_NEW` WITH AN UNMOVED CUM IS NOT A SIGHTING — that is what the
        order store serves for a genuinely-dead order inside its own creation lag, and treating
        it as evidence would refuse the adding side after every ordinary replace. A TERMINAL
        state retires the alive record from ANY read: that direction only releases quoting.

        ⚠️ On a REST-only run this is a HORIZON-cadence rail (`LAG_HORIZON_S`), not a
        seconds-cadence one — the WS order event is the only timely site and needs `--order-ws`,
        and a listing-absent ghost is invisible here. It bounds a repeat, not the first sweep.
        """
        entry = self.pending_reconcile.get(oid)
        if entry is None:
            return False
        if state in _TERMINAL_ORDER_STATES:
            self.parked_alive.pop(oid, None)
            self.terminal_verified.add(oid)
            return False
        advanced = cum is not None and cum > self.parked_cum_at_park.get(oid, _ZERO)
        # Named by the STRONGEST evidence present — "listed_open" understates a trading ghost.
        why = (state if state == "ORDER_STATE_PARTIALLY_FILLED"
               else (f"cum_advanced_to_{cum}" if advanced
                     else ("listed_open" if presence_is_alive else "")))
        if not why:
            return False
        first = oid not in self.parked_alive
        self.parked_alive[oid] = why
        # A sighting TAKES BACK any terminal confirmation: this order can still trade.
        self.terminal_verified.discard(oid)
        if first:
            order = entry[0]
            log.error(f"⚠️ {order.slug} {order.side}: PARKED order {oid} is SIGHTED ALIVE at the "
                      f"venue ({why}; state={state or '—'}, cum {cum} vs "
                      f"{self.parked_cum_at_park.get(oid, _ZERO)} at park) after its cancel "
                      f"echoed ok — its remaining contracts now count toward the {order.side} "
                      f"side's worst-case exposure, and the cancel is being re-issued.")
        return True

    async def _sight_parked(self, oid: str, state: str, *, cum: Optional[Decimal] = None,
                            presence_is_alive: bool = False) -> None:
        """`_note_parked_sighting` plus the re-cancel it may call for.

        Two consequences, and they are independent: the order's remaining size starts counting
        toward its side's worst-case exposure (`_parked_alive_size`), and the cancel is
        RE-ISSUED. Cancels are idempotent at the venue, and the order is already popped from
        `self.resting`, so a re-cancel can never lose a quote we meant to keep.
        """
        if self._note_parked_sighting(oid, state, cum=cum,
                                      presence_is_alive=presence_is_alive):
            await self._recancel_parked(oid, self.pending_reconcile[oid][0])

    async def _recancel_parked(self, oid: str, order: RestingOrder) -> None:
        """Re-issue the cancel of a sighted-alive parked order — at most once per
        `PARKED_RECANCEL_MIN_S` for THIS order, and at most `PARKED_RECANCEL_MAX_PER_POLL`
        across all of them per poll. Never raises: a failed or skipped re-cancel leaves the
        alive record — and therefore the exposure term — exactly where it was, which is the
        safe direction; the ghost simply waits its turn."""
        if self.shadow:
            return
        now = time.time()
        if now - self._parked_recancel_ts.get(oid, 0.0) < PARKED_RECANCEL_MIN_S:
            return
        if self._recancels_this_poll >= PARKED_RECANCEL_MAX_PER_POLL:
            if self._recancels_this_poll == PARKED_RECANCEL_MAX_PER_POLL:
                self._recancels_this_poll += 1        # announce once per poll, not per skip
                log.warning(
                    f"parked re-cancel budget spent ({PARKED_RECANCEL_MAX_PER_POLL}/poll) with "
                    f"{len(self.parked_alive)} order(s) sighted alive — the rest wait for the "
                    f"next poll (oldest first). Their exposure keeps counting meanwhile.")
            return
        self._recancels_this_poll += 1
        self._parked_recancel_ts[oid] = now
        self.parked_recancels += 1
        self._extra_requests += 1
        try:
            ok = await self.client.cancel_order(oid, order.slug)
        except Exception as exc:
            log.warning(f"{order.slug} {order.side}: re-cancel of sighted-alive parked order "
                        f"{oid} FAILED ({exc!r}) — it keeps counting toward exposure.")
            return
        if not ok:
            log.warning(f"{order.slug} {order.side}: re-cancel of sighted-alive parked order "
                        f"{oid} was REFUSED by the venue — it keeps counting toward exposure.")

    @property
    def orphans_reported(self) -> int:
        """DISTINCT listed orders reported and not adopted — set size, never a poll count."""
        return len(self._orphan_reported)

    @property
    def orphans_foreign(self) -> int:
        """DISTINCT off-slate listed orders left alone — set size, never a poll count."""
        return len(self._orphan_foreign)

    async def _scan_orphans(self, listing_bodies: dict[str, dict]) -> None:
        """THE FIFTH USE OF THE PER-POLL LISTING: which listed order is in
        NEITHER `self.resting` NOR `pending_reconcile` NOR `pending_activity_recovery`?

        ZERO ADDITIONAL REQUESTS — `poll_fills` has already read this listing.

        The three attribution rules are `_teardown_sweep`'s, deliberately UNCHANGED — a second,
        subtly different ownership rule for the same account is how two rails come to disagree
        about whose order a thing is:
          · no readable slug  → REPORTED, never cancelled (we cannot establish it is ours);
          · slug not in `self.ticks | self.slugs` → LEFT ALONE and counted (the account carries
            other processes' orders — a probe, a sibling lane, a hand-placed close);
          · slug ours → ours to cancel.

        An adopted orphan is PARKED (`origin="orphan"`) and marked alive, so its size counts
        toward the side's worst-case exposure and only a TERMINAL read retires it.

        FAIL DIRECTION: called only with a listing that ANSWERED (cannot-verify is not an empty
        account), and it rides the `slugs`-FILTERED listing — so a venue-side filter that omits
        an order hides it from this rail entirely. That is MISSED DETECTION with no signal, never
        a wrong cancel; `_teardown_sweep`'s UNFILTERED read is the backstop.
        """
        if self.shadow:
            return
        # ⛔ AN UNTRACKED-ID QUOTE OF OURS IS INDISTINGUISHABLE FROM AN ORPHAN. `_place` records a
        # RestingOrder with `order_id=None` when the create response carried no id, and its venue
        # id — which the listing DOES carry — is in no structure here, so it would read as an
        # orphan and be cancelled while `self.resting` still believes it is a live quote. The
        # whole pass is skipped rather than half-trusted.
        if any(o.order_id is None for o in self.resting.values()):
            log.warning("orphan scan SKIPPED this poll — a resting quote has no venue id, so a "
                        "listed order we do not recognise cannot be told apart from it. The "
                        "teardown sweep remains the backstop for both.")
            self._orphan_sightings.clear()
            return
        known = {o.order_id for o in self.resting.values() if o.order_id}
        mine = set(self.ticks) | set(self.slugs)
        seen_now: set[str] = set()
        # Deterministic order so a spent re-cancel budget delays the same orders every poll rather
        # than a random subset.
        for oid in sorted(listing_bodies):
            if (oid in known or oid in self.pending_reconcile
                    or oid in self.pending_activity_recovery):
                continue
            body = listing_bodies[oid]
            slug = body.get("marketSlug") or body.get("market_slug")
            if not slug:
                if oid not in self._orphan_reported:
                    self._orphan_reported.add(oid)
                    log.error(f"⛔ listed order {oid} has NO readable slug and is not in our "
                              f"book — REPORTED, never cancelled: cancelling on a guess is the "
                              f"failure the slug scoping exists to prevent. Inspect by hand.")
                continue
            slug = str(slug)
            if slug not in mine:
                self._orphan_foreign.add(oid)
                continue
            seen_now.add(oid)
            sightings = self._orphan_sightings.get(oid, 0) + 1
            self._orphan_sightings[oid] = sightings
            if sightings < ORPHAN_MIN_SIGHTINGS:
                log.warning(f"{slug}: listed order {oid} is on our slate and in NO structure of "
                            f"ours — first sighting, NOT cancelled. A create whose response has "
                            f"not returned yet looks exactly like this; if it is still here next "
                            f"poll it is an orphan and gets cancelled.")
                continue
            side = orphan_lane_side(body)
            qty = _amount(body.get("quantity"))
            price = _amount(body.get("price"))
            cum = _amount(body.get("cumQuantity")) or _ZERO
            state = str(body.get("state") or "")
            if side is None or qty is None or price is None:
                # Ours by slug, so cancelling it is lawful — but adopting it is not: a parked
                # entry needs a side to charge exposure to and a price to book a fill at, and
                # inventing either would write a fabricated number into the ledger the loss cap
                # reads. Cancel (the exposure-REDUCING direction), report, adopt nothing.
                # ⛔ ONCE PER ORDER, NOT ONCE PER POLL. This branch cannot adopt, so nothing here
                # removes the id from the unknown set — an ungated cancel re-fired on the SAME
                # order every poll for the rest of the run. The reported set is the gate.
                if oid in self._orphan_reported:
                    continue
                self._orphan_reported.add(oid)
                log.error(
                    f"⛔ {slug}: ORPHAN {oid} is on our slate but NOT attributable "
                    f"(intent={body.get('intent') or '—'} → side={side or '—'}, "
                    f"qty={qty if qty is not None else '—'}, "
                    f"price={price if price is not None else '—'}). Cancelling it — the slug "
                    f"makes it ours — but it is NOT adopted: its exposure is UNCOUNTED and a "
                    f"fill on it stays unattributed. Reconcile this run by hand.")
                self._extra_requests += 1
                # ⛔ `client.cancel_order` NEVER RAISES — it catches everything and returns False.
                # The failure must be loud, because this cancel is NOT retried in-run (the retry
                # would be the per-poll loop this gate exists to stop).
                if not await self.client.cancel_order(oid, slug):
                    log.warning(f"{slug}: cancel of unattributable orphan {oid} FAILED — it is "
                                f"still LIVE with UNCOUNTED exposure and will NOT be retried this "
                                f"run; the teardown sweep is the backstop.")
                continue
            order = RestingOrder(
                slug=slug, side=side, price=price, size=qty, order_id=oid,
                intent_id=f"orphan:{oid}", placed_ts=time.time(),
                # ⛔ BLANK, never 0 or a fabricated stamp: we did not watch this order being
                # placed, so its queue position, improve flag, placement-time net touch and BOTH
                # rest clocks are unknown, and a 0 would read as "filled the instant it was
                # placed".
                # ⛔ THE BASELINE IS ZERO, NOT `cum`. Adopting at `filled_qty=cum` declares the
                # pre-adoption fills already booked when nothing had ever booked them —
                # `_book_fill` books only INCREMENTS past this baseline, so those fills would stay
                # out of inventory, avg_entry, rt_realized and the durable loss cap. Zero + the
                # immediate booking below puts `cum` through the SAME path every other fill takes.
                queue_ahead=None, improved=False, filled_qty=_ZERO, rest_since_ts=None)
            if cum > _ZERO:
                # `late_booked=True` is the truth about this fill: it happened at an unknown
                # earlier moment, so the current touch is not a mark for it. Same booking site,
                # same tape, same loss-cap feed as a verify-recovered fill.
                if self._book_fill(order, body, late_booked=True) is not None:
                    if self.state is not None and self.real:
                        self.state.set_inventory(self.inventory)
                    log.error(f"⛔ {slug} {side}: orphan {oid} was ALREADY {cum} filled before we "
                              f"adopted it — booked now at {price}. Inventory, avg entry and the "
                              f"loss cap were blind to it until this moment.")
            self._park(order, origin="orphan")
            self.parked_alive[oid] = "orphan_listed"
            self.orphans_seen += 1
            log.error(
                f"⛔ {slug} {side}: ORPHAN order {oid} — the venue is holding it, this process "
                f"never recorded it (a lost create response is the known cause). qty {qty} "
                f"@ {price}, cum {cum}, state={state or '—'}. Adopting it: its remaining "
                f"{max(_ZERO, qty - cum)} contract(s) now count toward the {side} side's "
                f"worst-case exposure, any fill on it books, and its cancel is being issued.")
            before = self.parked_recancels
            await self._recancel_parked(oid, order)
            if self.parked_recancels > before:
                self.orphans_cancelled += 1
            self._orphan_sightings.pop(oid, None)
        # A debounce is CONSECUTIVE by construction: an id absent from this listing starts over.
        self._orphan_sightings = {o: n for o, n in self._orphan_sightings.items() if o in seen_now}

    def _keep_parked_alive(self, oid: str, order: RestingOrder, attempts: int,
                           terminal_ts: float, origin: str, now: float) -> bool:
        """THE ONE GUARD ABOVE BOTH RETIREMENT PATHS in `_retry_pending_reconciles`. True (and
        the entry's verify is re-armed) when the venue has positively shown this parked order
        ALIVE and no terminal read has taken that back.

        ⛔ Neither the horizon nor cannot-verify may retire such an entry. `_parked_alive_size`
        iterates `pending_reconcile` alone, so ANY retirement drops the exposure to zero and
        nothing can ever restore it — the activities-recovery queue settles the FILL verdict, it
        carries no exposure. Only a TERMINAL read, which clears `parked_alive`, retires the entry.

        `attempts` is passed through UNCHANGED: the exhaustion ladder must not advance on an entry
        that cannot exhaust.
        """
        if oid not in self.parked_alive:
            return False
        self.pending_reconcile[oid] = (order, now + VERIFY_RETRY_S, attempts, terminal_ts, origin)
        return True

    def _parked_alive_size(self, slug: str, side: str) -> Decimal:
        """Contracts on `side` of `slug` that are PARKED but sighted alive since their cancel.

        The REMAINING size, not the placed size: whatever a partial fill already took is in
        inventory, and counting it twice would refuse quotes against exposure we already hold.
        ⛔ EXACT Decimal — `int(filled_qty)` truncated a 4.39 partial to 4 and silently
        over-stated the remainder by 0.61 of a contract.
        """
        total = _ZERO
        for oid, entry in self.pending_reconcile.items():
            if oid not in self.parked_alive:
                continue
            order = entry[0]
            if order.slug != slug or order.side != side:
                continue
            total += max(_ZERO, Decimal(order.size) - order.filled_qty)
        return total

    def _unresolved_exposure(self, slug: str, side: str) -> Decimal:
        """THE ONE HEADROOM TERM: contracts
        on `side` of `slug` whose cancel was ISSUED but whose fate is not yet KNOWN.

        A cancel acknowledgement is not a terminal state. The venue once acked the cancel of an ask,
        `_apply_action` placed the replacement against inventory at the reach, and BOTH asks filled
        seconds later: a breach past the reach.
        `_parked_alive_size` could not see it — that gate counts a parked order only once a
        read-back has SIGHTED it alive, and nothing sighted this one before it filled.

        So the term is every entry in `pending_reconcile` on this side (the parked-alive set is a
        strict SUBSET of it), less those whose TERMINAL state a read-back has confirmed
        (`terminal_verified` — the cancel's own post-ack read-back is the normal, fast path). Only
        a non-terminal or unreadable read-back leaves the exposure counted.

        ⛔ THE REMAINING size, exact Decimal, exactly as `_parked_alive_size` computes it: a
        partial fill is already in inventory, and `int(filled_qty)` truncates a 4.39 fill to 4 and
        over-states the remainder by 0.61 of a contract.
        """
        total = _ZERO
        for oid, entry in self.pending_reconcile.items():
            if oid in self.terminal_verified:
                continue
            order = entry[0]
            if order.slug != slug or order.side != side:
                continue
            total += max(_ZERO, Decimal(order.size) - order.filled_qty)
        return total

    async def _retry_pending_reconciles(self, *, ignore_due: bool = False) -> None:
        """Run the DELAYED VERIFY on parked orders whose horizon has passed.

        Every cancelled order gets exactly one verify read after LAG_HORIZON_S — regardless of
        what its immediate pre-cancel read said, because that read can ANSWER with a stale
        cumQuantity and still be wrong. cumQuantity is cumulative, so the verify books the exact
        missed delta or nothing. Unreadable at verify time → retry in VERIFY_RETRY_S, up to
        VERIFY_MAX_ATTEMPTS, then a LOUD drop. A parked order is already cancelled, so this can
        never double-place — it only repairs the number every cap and flatten reads."""
        if self.shadow or not self.pending_reconcile:
            return
        now = time.time()
        for oid, (order, due, attempts, terminal_ts, origin) in list(
                self.pending_reconcile.items()):
            if now < due and not ignore_due:
                continue
            if ignore_due and time.monotonic() < self._teardown_pace_deadline:
                await asyncio.sleep(TEARDOWN_VERIFY_PACE_S)
            self._extra_requests += 1
            body = _order_body(await self.client.get_order(oid))
            if _amount(body.get("cumQuantity")) is None:
                # ⛔ CANNOT-VERIFY NEVER RETIRES A KNOWN-LIVE ORDER. An unreadable per-order read
                # is not an independent fault: five of them is ~2.5 min of exactly the order-store
                # degradation that MAKES ghosts. Running exhaustion before the horizon guard
                # dropped the entry into the recovery queue — which carries NO exposure — so the
                # release was PERMANENT and a fresh full-size order went out beside a live one.
                if self._keep_parked_alive(oid, order, attempts, terminal_ts, origin, now):
                    continue
                if attempts + 1 >= VERIFY_MAX_ATTEMPTS:
                    # ⛔ Not a drop: the order-store record may be purged while the TRADE ledger
                    # still carries its executions — queue for the activities walk.
                    log.error(f"⚠️ {order.slug} {order.side}: order {oid} UNREADABLE after "
                              f"{VERIFY_MAX_ATTEMPTS} verify attempts — queueing for "
                              f"activities recovery (the trade ledger outlives the order "
                              f"store).")
                    del self.pending_reconcile[oid]
                    self._forget_parked(oid)
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
            # The verify read is a SIGHTING under the same discriminator as every other read. This
            # site is LAG_HORIZON_S late by construction — the WS drain and the listing are the
            # timely ones — but it is the only one a run without a WS feed has.
            await self._sight_parked(oid, str(body.get("state") or ""),
                                     cum=_amount(body.get("cumQuantity")))
            # ⛔ A READABLE answer RETIRES the entry only past the horizon — "readable = safe" is
            # the rule this design abolishes (a stale cum=0 books nothing and hides a real fill).
            # Booking early is always safe (cum is monotone); retiring early is the bug.
            # ⛔ AND A SIGHTED-ALIVE ENTRY IS NEVER RETIRED: only a TERMINAL read may retire it,
            # or the exposure term vanishes and the maker quotes beside a live order. Both
            # retirement paths must go through `_keep_parked_alive`.
            if self._keep_parked_alive(oid, order, attempts, terminal_ts, origin, now):
                continue
            if now >= due:
                del self.pending_reconcile[oid]
                self._forget_parked(oid)
                # The durable order record retires HERE, with the verify — not at the pop.
                # Clearing at the pop erased the record before the verify ran; never clearing
                # accumulated verified-dead records until a crash-restart refused over them.
                if self.state is not None and self.real:
                    self.state.clear_order(oid)

    # ── fills ────────────────────────────────────────────────────────────────────────────────

    async def poll_fills(self, *, force_full: bool = False) -> list[Fill]:
        """Detect fills and read the COMMISSION back for each one.

        One `get_open_orders()` for the whole book, then a `get_order` read-back per resting
        order. The listing is a HINT for retirement, never the authority: it carries no pagination
        token, so a silent server-side cap cannot be ruled out, and the pop decision keys on the
        per-order read's STATE, which cannot be truncated.

        `cumQuantity` is CUMULATIVE, so the increment is `cum - already_seen`.
        """
        # The WS accelerator drains FIRST so every caller — the cycle loop AND each teardown
        # phase — books pushed fills before its own reads.
        self._drain_order_feed()
        self._recancels_this_poll = 0                  # this poll's global re-cancel budget
        # The WS drain's re-cancels, issued BEFORE the early return below: a book whose resting
        # set is empty is exactly the state a ghost survives in. OLDEST GHOST FIRST, so a spent
        # budget delays the newest rather than starving the one that has waited longest.
        self._parked_recancel_due.sort(
            key=lambda o: self.pending_reconcile[o][3] if o in self.pending_reconcile else 0.0)
        while self._parked_recancel_due:
            _due_oid = self._parked_recancel_due.pop(0)
            _entry = self.pending_reconcile.get(_due_oid)
            if _entry is not None:
                await self._recancel_parked(_due_oid, _entry[0])
        if self.shadow:
            self._nf_breaker_tripped = False
            return []
        if not self.resting:
            # An empty book cannot accumulate not_founds, so a tripped breaker here can only be
            # STALE — and leaving it latched walls the recovery walk off forever. Clearing is the
            # safe direction: a real ongoing outage just fails the walk's own reads.
            self._nf_breaker_tripped = False
            # ⛔ `_orphan_suspects` joins the parked set as a reason to spend the listing read
            #: after a create ERRORED, "nothing rests and nothing is parked" is the
            # state a lost create response leaves, not evidence the account is empty.
            if not self.pending_reconcile and not self._orphan_suspects:
                return []
        # ⛔ AN EMPTY RESTING SET NO LONGER SKIPS THE LISTING WHILE ORDERS ARE PARKED [round-3
        # review]: a ghost survives PRECISELY in that state — every quote cancelled, belief says
        # nothing is working, and the venue is still holding orders. The listing read below is
        # then spent on the parked scan alone and the poll returns before the resting loop.
        try:
            self._extra_requests += 1
            # Slate-scoped at the source [M0.5b-a]: the listing is a retirement HINT and a
            # truncation on an account-wide read is invisible; bounding it to our own slugs
            # shrinks that surface (the state authority still decides the pop either way).
            open_orders = await self.client.get_open_orders(slugs=self.slugs)
        except Exception as exc:
            # Cannot-verify is not "nothing filled" — skip this poll rather than invent a state.
            log.warning(f"open-orders read failed ({exc!r}) — fills UNVERIFIED this poll")
            return []
        listing_bodies: dict[str, dict] = {}
        for o in open_orders:
            oid = venue_order_id(o)
            if oid:
                listing_bodies[oid] = _order_body(o)
        still_open = set(listing_bodies)
        # ── PARKED orders in the OPEN-ORDERS listing [incident] ────────────────
        # The timely sighting, and it is FREE: the listing has already been read, and a
        # cancelled-and-parked order appearing in it is the venue contradicting our own cancel
        # echo. Waiting for the parked entry's own verify would be LAG_HORIZON_S late — the
        # incident's concurrent asks were lifted within a minute. The listing is only a HINT
        # for RETIREMENT (it can be truncated); as evidence that an order is ALIVE it is an
        # authority, which is the direction used here.
        # Oldest ghost first, so the poll's re-cancel budget is spent on the longest-standing.
        for _oid in sorted((o for o in listing_bodies if o in self.pending_reconcile),
                           key=lambda o: self.pending_reconcile[o][3]):
            await self._sight_parked(_oid, str(listing_bodies[_oid].get("state") or ""),
                                     cum=_amount(listing_bodies[_oid].get("cumQuantity")),
                                     presence_is_alive=True)
        # ── ORPHANS in the same listing ───────────────────────────────────────
        # The loop above asks "is a PARKED order still listed?"; this asks what nothing asked
        # in-run: "is a LISTED order in no structure of ours at all?" Same free listing, and
        # deliberately BEFORE the empty-resting early return — an orphan survives precisely in
        # the state where we believe nothing is working.
        await self._scan_orphans(listing_bodies)
        if not self.resting:
            # Parked-only poll: the listing was read for the scan above and there is nothing
            # resting to read back. Returning here keeps the conditional-poll slot, the
            # not_found breaker and the TTL rail on their own (resting-order) semantics.
            return []
        # Forced-FULL cadence for the conditional poll: slot 0 of every
        # CONDITIONAL_POLL_FULL_EVERY successful listings reads every order regardless, so a
        # listing staler than the per-order endpoint defers a fill by at most N polls. Incremented
        # only here — a failed listing never advances the slot.
        # `force_full=True` comes from the TEARDOWN call sites: the skip's whole safety argument
        # is "the next poll catches it", and at teardown there is no next poll — a skipped stale
        # listing there loses the flatten's own reducing fill and prints a phantom residual.
        force_full = force_full or (self._poll_seq % CONDITIONAL_POLL_FULL_EVERY == 0)
        self._poll_seq += 1

        fills: list[Fill] = []
        nf_this_poll: set[str] = set()
        probe_candidates: list[RestingOrder] = []
        # TTL verdicts are COLLECTED here and acted on after the loop — a cancel inside the loop
        # would re-enter the very structure the loop is iterating.
        ttl_cancels: list[tuple[RestingOrder, str]] = []
        self._ttl_stale_this_poll = 0
        for key, order in list(self.resting.items()):
            if order.order_id is None:
                continue
            if self.conditional_poll and not force_full and not self._ttl_skip_forbidden(order):
                lbody = listing_bodies.get(order.order_id)
                if lbody is not None:
                    lcum = _amount(lbody.get("cumQuantity"))
                    if lcum is not None and lcum == order.filled_qty:
                        # PRESENT in the listing (presence contradicts a purge) with an UNMOVED
                        # cumulative: the read-back would return what we already booked. The rule
                        # keys on cumQuantity DIRECTLY, never the action, and a missing/unparseable
                        # listing cum falls through — cannot-verify is not "unchanged". Skipped
                        # orders cannot reach the pop/park/probe branches (all require
                        # listing-absence). The not_found streak is deliberately NOT cleared here:
                        # only an `ok` READ clears evidence.
                        self.conditional_poll_skips += 1
                        continue
            self._extra_requests += 1
            raw, verdict = await self.client.get_order_ex(order.order_id)
            body = _order_body(raw)
            # The staleness-symmetry MEASUREMENT: whenever a read happens and the listing also
            # carried this order, compare the two cums. This decides whether a "listing unchanged"
            # skip defers fills 0-in-N or routinely — recorded on every run, at zero request cost.
            _lb = listing_bodies.get(order.order_id)
            if _lb is not None:
                _lcum, _rcum = _amount(_lb.get("cumQuantity")), _amount(body.get("cumQuantity"))
                if _lcum is not None and _rcum is not None and _lcum != _rcum:
                    if _lcum < _rcum:
                        self.poll_listing_behind += 1     # listing stale — the dangerous direction
                    else:
                        self.poll_listing_ahead += 1      # read stale (the documented cum-0 lie)
            # The not_found STREAK: reset only by an `ok` read — `error` is transport noise and
            # neither advances nor clears the evidence. The streak alone never retires anything
            # (store-lag has run 4.6 HOURS); it only makes the order a cancel-probe CANDIDATE.
            if verdict == "ok":
                self.not_found_streak.pop(order.order_id, None)
            elif verdict == "not_found":
                # ⛔ The breaker counts only CONTRADICTED not_founds. `_order_verdict` issues
                # not_found ONLY on a structured venue body — a transport/route fault resolves to
                # `error` — so a not_found the poll's own listing CORROBORATES is evidence of a
                # genuine purge, not a client fault. The any-not_found form measured 0% precision
                # live, and while tripped it refused the very probes and recovery drain that
                # would have cleared it — the breaker disabled its own remedy. Residual, accepted:
                # a wrong-base-URL client is consistent across BOTH endpoints and no longer trips.
                if order.order_id in still_open:
                    nf_this_poll.add(order.order_id)
                self.not_found_streak[order.order_id] = \
                    self.not_found_streak.get(order.order_id, 0) + 1
            fill = self._book_fill(order, body)
            if fill is not None:
                fills.append(fill)
            state = str(body.get("state") or "")
            missing_from_listing = order.order_id not in still_open
            # The TTL rail rides the read-back that already happened — zero extra requests. It
            # sits AFTER `_book_fill` so an expiring order's last fill is booked first, and it
            # only ever returns a decision.
            _ttl_reason = self._observe_ttl(order, body, verdict, now=time.time(), state=state)
            if _ttl_reason is not None:
                ttl_cancels.append((order, _ttl_reason))
            if (verdict == "not_found"
                    and self.not_found_streak.get(order.order_id, 0) >= 2
                    and time.time() - order.placed_ts > LAG_HORIZON_S
                    and missing_from_listing):
                # Streak >=2 AND past the create-lag horizon AND absent from the poll's OWN
                # listing (a different endpoint, free, NECESSARY — presence contradicts a purge).
                # Still only a candidate: the probe below is the decider.
                probe_candidates.append(order)
            if missing_from_listing and state in _TERMINAL_ORDER_STATES:
                # ⛔ PARK BEFORE POPPING — the read above can be served STALE (cum 0 on a filled
                # order), and a pop without a parked verify loses the fill FOREVER: nothing else
                # ever re-reads a popped order. Two buys were dropped here, driving belief to the opposite
                # sign of the venue. The parked verify is cum-idempotent, so a fresh read
                # that finds nothing new books nothing. origin="poll" so an eviction carries the
                # right recovery path.
                self._park(order, origin="poll")
                # The state this branch selected on IS a terminal read of the order's own body —
                # the same authority `_reconcile_order` trusts. Without the mark a TTL-expired or
                # venue-cancelled partially-filled order would hold `size − filled` against the
                # reach for the whole verify horizon.
                self.terminal_verified.add(order.order_id)
                self.resting.pop(key, None)
                # ⛔ THE SIDE IS DARK, AND THE VENUE — NOT US — MADE IT SO. A purge is exactly the
                # path a same-price re-place follows minutes later, and an epoch left running
                # across the gap reports `price_rest_s` spanning time we were ABSENT — the bias
                # points TOWARD the passive reading, corrupting the one discrimination it makes.
                self._end_price_epoch(order.slug, order.side)
            elif missing_from_listing and state:
                # A LIVE or unrecognised state contradicting the listing. Keep tracking — wrongly
                # keeping costs a log line and a retried cancel; wrongly popping leaves a live
                # order resting that no code path ever cancels. The old `"OPEN" not in state`
                # condition popped every one of these: the venue has NO state containing "OPEN".
                log.warning(f"{order.slug} {order.side}: {order.order_id} absent from the "
                            f"open-orders listing but state={state} — keeping it (listing "
                            f"stale or truncated; the state is the authority)")
            elif missing_from_listing:
                # Neither authority answered: absent from the listing AND the read-back carried NO
                # state. Keep (cannot-verify is not terminal) but say so — under a get_order
                # outage every resting order lands here at once.
                log.warning(f"{order.slug} {order.side}: {order.order_id} absent from the "
                            f"open-orders listing and its read-back carried NO state — kept; "
                            f"cannot-verify is not terminal")
        # ── the TTL rail's verdicts [gtd_ttl_rollout §4] ─────────────────────────────────
        # Disjoint from the probe chain by construction: a TTL cancel needs an `ok` read of a
        # RESTING order, a probe candidate needs a structured not_found.
        if self._ttl_stale_this_poll >= TTL_STALE_FALLBACK_PER_POLL:
            log.error(
                f"⛔ {self._ttl_stale_this_poll} resting orders read back a STALE or ABSENT "
                f"goodTillTime in ONE poll (≥{TTL_STALE_FALLBACK_PER_POLL}) — the venue is not "
                f"holding the deadlines we send. Quoting on deadlines that do not exist is worse "
                f"than quoting GTC and knowing it.")
            if self.ttl_fallback:
                self._disable_ttl(f"{self._ttl_stale_this_poll} stale/absent read-back deadlines "
                                  f"in one poll")
            else:
                log.error("--no-ttl-fallback is set: TTL mode STAYS ON and the stale deadlines "
                          "keep being replaced. This is an observation posture, not a safe one.")
        if ttl_cancels:
            await self._apply_ttl_cancels(ttl_cancels)
        # ── the breaker, then the cancel-probes ───────────────────────────────
        self._nf_breaker_tripped = len(nf_this_poll) > NOT_FOUND_BREAKER
        if self._nf_breaker_tripped:
            log.error(f"⛔ {len(nf_this_poll)} distinct orders answered not_found in ONE poll "
                      f"WHILE the open-orders listing still carried them (> "
                      f"{NOT_FOUND_BREAKER}) — two endpoints contradicting is a client/route "
                      f"fault signature, not a purge. Probes and retirements REFUSED; the "
                      f"recovery queue holds (same client, same fault). Suspect the order "
                      f"store or a truncated listing — base-URL drift cannot produce a "
                      f"structured not_found at all.")
        elif probe_candidates:
            await self._run_cancel_probes(probe_candidates)
        # Streak hygiene: entries for orders no longer resting are dead weight — prune, don't leak.
        alive = {o.order_id for o in self.resting.values() if o.order_id}
        self.not_found_streak = {k: v for k, v in self.not_found_streak.items()
                                 if k in alive}
        self._probe_confirmed_gone &= alive
        if self.state is not None and self.real and fills:
            self.state.set_inventory(self.inventory)
        return fills

    async def _run_cancel_probes(self, candidates: list[RestingOrder]) -> None:
        """ONE deliberate cancel per zombie candidate, oldest-first, <=PROBE_MAX_PER_CYCLE. The
        probe's structured not_found is the only path that retires a resting order the STORE
        disowns. A false retirement places a second untracked order over a live one, which is why
        the chain is this long — streak >=2 + age gate + listing-absence got the order HERE, and
        the venue's own cancel answer decides — and why retirement additionally gates on
        `probe_retirement`. ⚠️ In DRY the client short-circuits every cancel to `ok`, so the
        verdict distribution is meaningless there."""
        # ⛔ Confirmed-gone orders are excluded BEFORE the budget slice: they are the OLDEST
        # candidates, so slicing first let two of them occupy both slots forever under the
        # default retirement-off gate.
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
                # Retirement is UNPROVEN: the three corroborating reads share one (demonstrably
                # laggy) store, and a false retirement re-quotes over a live order. Keep it
                # resting — the mute side stays mute, which costs fills. ⛔ But RECOVERY is
                # separable from retirement: the activities walk is read-only and places nothing,
                # and skipping it left a PURGED order that had already FILLED unbooked, with the
                # teardown sweep certifying clean over it.
                self._probe_confirmed_gone.add(oid)
                self._queue_recovery(order, time.time(), "cancel_probe")
                log.error(f"⛔ {order.slug} {order.side}: cancel-probe answered structured "
                          f"not_found for {oid} but retirement is DISABLED "
                          f"(probe_retirement=False, the C1 evidence gate). Order kept "
                          f"(this side will not re-quote); activities recovery QUEUED — "
                          f"any fill it carries books from the ledger. Review the "
                          f"captured 404 body and relaunch with --enable-probe-retirement "
                          f"to act on the retirement half.")
            elif verdict == "not_found":
                # Terminal: the order store disowns it twice over (reads AND the cancel).
                # Retire so the mute side can speak again (quote_action(resting=None) PLACEs
                # next cycle) and queue for the activities walk.
                self.resting.pop((order.slug, order.side), None)
                self._end_price_epoch(order.slug, order.side)   # dark side — see the poll pop
                self.not_found_streak.pop(oid, None)
                log.warning(f"{order.slug} {order.side}: cancel-probe confirmed {oid} GONE "
                            f"from the order store — retired; activities recovery will "
                            f"answer whether it filled first.")
                self._queue_recovery(order, time.time(), "cancel_probe")
            elif ok:
                # WAS-LIVE (store lag served the reads a stale miss): the venue just accepted a
                # real cancel → the normal cancelled path, delayed verify included. ⚠️ If the venue
                # cancels idempotently this verdict is uninformative.
                self.resting.pop((order.slug, order.side), None)
                self._end_price_epoch(order.slug, order.side)   # dark side — see the poll pop
                self.not_found_streak.pop(oid, None)
                log.warning(f"{order.slug} {order.side}: cancel-probe found {oid} WAS-LIVE "
                            f"(store lag) — cancelled through the normal path.")
                self._park(order)
            # error → keep everything; the next poll retries the whole chain.

    # ── belief recovery: the activities walk ─────────────────────────────────────

    def _queue_recovery(self, order: RestingOrder, terminal_ts: float, path: str) -> None:
        """Enter `order` into the activities-recovery queue. Bounded by `self.max_parked`;
        past the bound the NEWEST entry is routed to the named UNRESOLVED list (the oldest is
        closest to a verdict and evicting it wastes the most walk progress) — never silent."""
        oid = order.order_id
        if oid is None or oid in self.pending_activity_recovery:
            return
        if len(self.pending_activity_recovery) >= self.max_parked:
            # REFUSE the incoming entry — it is by construction the newest and has zero walk
            # progress, so evicting an existing entry to admit it contradicts the stated policy
            # that progress is the thing worth keeping. Named UNRESOLVED either way.
            log.error(f"⚠️ recovery queue at bound {self.max_parked} — REFUSING incoming "
                      f"{oid} (existing entries keep their walk progress).")
            self.unresolved_recovery[oid] = "refused: recovery queue at bound"
            return
        self.pending_activity_recovery[oid] = RecoveryEntry(
            order=order, terminal_ts=terminal_ts, path=path, queued_ts=time.time())

    def _dequeue_recovery(self, oid: str) -> None:
        """A CLEAN exit (NO-FILL confirmed, or nothing left to learn): the durable order
        record retires here, exactly as the delayed verify's retirement does.

        ⛔ UNLESS the order is still in `self.resting`: the retirement-off probe path queues
        recovery while deliberately KEEPING the order tracked as possibly-live. A NO-FILL verdict
        answers "did it fill BEFORE terminal" — it says nothing about the order being dead, and
        clearing its durable record here would leave a SIGKILL with a live venue order and
        `maybe_live_orders == 0`: the invisible crash."""
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

        ⛔ `queued_ts`, not `terminal_ts`: the merge is queued-ids-only, so a traversal started
        before the entry JOINED filtered its rows out of every page it read — crediting it is
        coverage over deleted evidence. `queued_ts >= terminal_ts` on every entry path, so this
        gate subsumes the terminal one.

        For a NEGATIVE verdict (NO-FILL / OVER-STATEMENT) the start must ALSO clear
        `terminal_ts + LAG_HORIZON_S`: the activities store can publish an execution late, and
        deep pages read before it existed cover its createTime window without containing it.
        FOUND booking keeps the looser gate — positive evidence is monotone-safe."""
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
        """Fold one page's executions into the per-order store — QUEUED ids only, matched by
        our-id presence on EITHER leg, deduped by execution id. ⛔ NOT by isAggressor: the
        recorded population has ZERO aggressor trades, so its True behaviour is entirely
        unobserved — and a self-trade carries our id on BOTH legs.

        Quantity is the LEG's `lastShares` (the number that belongs to OUR order), falling back to
        the trade's `qtyDecimal`. ⛔ NEVER `trade.qty` — it is ROUNDED to the nearest integer.
        (Booking itself rides `cumQuantity`; the leg quantity feeds the partial-sight detector,
        where a rounded value fakes or hides partial sight.)"""
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
                    "price": _amount(trade.get("price")),   # yes-space on EVERY leg [§0]
                    "avg_px": _amount(order.get("avgPx")),  # the VENUE's own, 133/133 legs
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
        """At most ONE activities request per quote cycle while the queue is non-empty
       , charged to `_extra_requests` after the cycle's stats reset. Page-1 verdict
        edges take priority, then cursor deepening for the current traversal, then a
        traversal RESTART for entries the current traversal cannot cover (queued after its
        start; needing a verdict-capable start only now reachable; or the cursor died
        before the ledger's proven end). The walk keeps its OWN coverage state — never
        `page_poly_activities.reached`, which reads an empty page and a missing cursor as
        covered (the `[] means flat` shape). An EMPTY page is never coverage, and a FULL
        page with no cursor is not exhaustion either [mm-review C1.3] — only a short final
        page proves the ledger's end."""
        if self.shadow or not self.pending_activity_recovery:
            return
        if self._nf_breaker_tripped:
            return                        # the queue HOLDS during a client/route fault [§2]
        now = time.time()
        entries = self.pending_activity_recovery
        need_page1 = any(
            now >= e.terminal_ts + LAG_HORIZON_S
            and e.verdict_read_ts < e.terminal_ts + LAG_HORIZON_S
            for e in entries.values())
        # Deepening serves entries the CURRENT traversal can ever satisfy (started >= their
        # queued_ts); a restart serves entries it cannot, but only once a restart taken NOW would
        # clear their required start — a page-1 read before `terminal + LAG` can never ground a
        # negative verdict.
        coverable = [e for e in entries.values()
                     if self._walk_started_ts >= e.queued_ts]
        need_deepen = (not self._walk_exhausted and bool(self._recovery_cursor)
                       and any(not self._oldest_covered(e) for e in coverable))
        need_restart = any(
            self._walk_started_ts < e.queued_ts
            or (self._walk_started_ts < self._coverage_start_required(e, for_verdict=True)
                and now >= self._coverage_start_required(e, for_verdict=True))
            for e in entries.values()) or (
            # A traversal with NO cursor left and no proven end has nothing more to give but has
            # not covered everyone — without this the walk goes completely silent until the
            # timeout, holding unbooked venue evidence.
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
                # A genuinely empty ledger can never produce coverage (an empty page is not
                # evidence), so entries will age out UNRESOLVED — make the cause readable.
                self._empty_ledger_logged = True
                log.warning("recovery walk: the activities ledger returned EMPTY — no "
                            "coverage is derivable; queued entries will time out "
                            "UNRESOLVED unless trades appear.")
            if read_ts is not None and acts:
                self._merge_activities(acts)
                # A missing cursor proves exhaustion only on a SHORT page: a full page with no
                # cursor is indistinguishable from a truncated listing, and the safe misread
                # direction is "not exhausted".
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
        """The venue's own running total for the order: the max `order.cumQuantity` across matched
        executions. ⛔ NOT Σ of leg quantities — Σ equals cum only under COMPLETE execution sight
        and silently understates under partial sight, which is exactly when a derived quantity
        would freeze an under-booking as "NO-FILL". cum is the same field `get_order` serves, and
        partial sight can only UNDERSTATE it (monotone-safe). None = no matched execution carried
        the field."""
        cums = [x["cum"] for x in execs.values() if x["cum"] is not None]
        return max(cums) if cums else None

    def _evaluate_recovery(self, now: float) -> None:
        # VENUE-TIME order, not insertion order: two recovered fills on one slug booked
        # newest-first manufacture the very out-of-order re-basing the through-zero counter exists
        # to report. Keyed on the NEWEST exec ts — the value `_book_recovered` stamps the row with
        # (min-ts sorting re-created the inversion for multi-execution orders).
        def _entry_ts(item):
            execs = self._recovery_execs.get(item[0], {})
            stamps = [x["ts"] for x in execs.values() if x["ts"] is not None]
            return max(stamps) if stamps else float("inf")

        for oid, entry in sorted(self.pending_activity_recovery.items(), key=_entry_ts):
            execs = self._recovery_execs.get(oid, {})
            venue_cum = self._venue_cum(execs)
            booked = entry.order.filled_qty
            book_cov = self._oldest_covered(entry)
            # A NEGATIVE verdict needs the STRICTER coverage: a traversal started after the entry
            # joined AND after terminal + LAG (the store publishes executions late — deep pages
            # read earlier cover the createTime window without containing them).
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
                    # Verb keyed on the NUMBER beside it: an order that filled through the normal
                    # path and is merely confirmed here must not log "NO-FILL" next to a nonzero
                    # booked.
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
                             f"reconcile the tape against the venue after the run — see the operator runbook")
                    continue
            # Residence timeout: from the LATER of terminal and queue entry — the
            # verify-exhaustion path joins ~240 s after its terminal.
            if now - max(entry.terminal_ts, entry.queued_ts) > RECOVERY_MAX_S:
                booked_at_deadline = booked
                if venue_cum is not None and venue_cum > booked:
                    # ⛔ POSITIVE venue evidence in hand at the deadline is BOOKED, not discarded:
                    # `cumQuantity` is the venue's own total and booking it is monotone-safe
                    # regardless of coverage — throwing it away under-states inventory in the
                    # direction every cap reads. The entry still routes UNRESOLVED (the coverage
                    # failure is real), but the ledger's number reaches belief first.
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
            # Price sanity: resting post-only fills execute AT our limit — EXACT, 0 ticks, on
            # every recorded row. Sign-aware by construction: equality refuses a bid-side
            # "improvement" and the short-side trap (an "improvement" on an ask is a HIGHER yes
            # number) identically. Refusal ROUTES, it does not just log.
            self._resolve_unresolved(
                oid, f"price sanity: activity prices {sorted(str(p) for p in prices)} ≠ "
                     f"our limit {order.price} — refused, nothing booked")
            return
        newest = max(execs.values(),
                     key=lambda x: (x["cum"] if x["cum"] is not None else Decimal(-1),
                                    x["ts"] or 0.0))
        row_ts = newest["ts"]
        if row_ts is None:
            # An unreadable createTime must never fall through to `_book_fill`'s wall-clock
            # default — but refusing outright discards POSITIVE venue evidence. Fallback chain:
            # the newest execution WITH a readable ts, else the entry's `terminal_ts` (every
            # execution predates its terminal — a true upper bound, never a fresh-looking lie).
            readable = [x["ts"] for x in execs.values() if x["ts"] is not None]
            row_ts = max(readable) if readable else entry.terminal_ts
            log.warning(f"{order.slug} {order.side}: {oid} newest execution's createTime "
                        f"is UNREADABLE (venue shape drift?) — stamping the recovered row "
                        f"with {'the newest readable execution ts' if readable else 'the entry terminal_ts'} "
                        f"({row_ts:.0f}) instead of a wall-clock guess.")
        qty_sum = sum((x["qty"] for x in execs.values() if x["qty"] is not None), _ZERO)
        if all(x["qty"] is not None for x in execs.values()) and qty_sum != venue_cum:
            # Partial EXECUTION sight: cum is still safe to book (it is the venue's own total at
            # the newest seen execution), but the walk has not seen every leg — worth a line, not
            # a refusal. Gated on all-qtys-readable: with SOME legs unreadable an under-sum is a
            # field artifact.
            log.warning(f"{order.slug} {order.side}: {oid} partial or divergent execution "
                        f"sight — Σ leg qty {qty_sum} ≠ venue cum {venue_cum}.")
        # ⛔ avgPx is the VENUE's field or ABSENT — never synthesized from our limit: the tape
        # contract says an empty avg_px marks "fell back to our limit", and the <run-id> commission
        # gate evaluates at the venue's own avgPx. A derived value defeats the marker exactly on
        # the rows where it matters and makes any "avg_px == price" statistic self-confirming.
        body: dict = {"cumQuantity": str(venue_cum)}
        if newest["avg_px"] is not None:
            body["avgPx"] = str(newest["avg_px"])
        if newest["state"]:
            body["state"] = newest["state"]     # the venue's own token, never an invented one
        if newest["comm_total"] is not None:
            # The NEWEST matched execution's snapshot: the field is a running-total snapshot that
            # differs across executions on most multi-execution orders, and the oldest copy can
            # under-report by up to 96%.
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
            # all(), not any(): with SOME legs missing the field, an under-sum is a
            # missing-field artifact, not evidence of unseen executions.
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
            # ⛔ A recovered booking that touches or crosses flat AND arrives OUT OF ORDER (a later
            # fill was already booked on this slug). Both halves are load-bearing: a
            # sign-flip-only condition missed the from-flat case that actually corrupts the
            # ratchet, and a wider one fired on every CLEAN recovery onto a quiet book — a
            # corruption tell that cries on the happy path trains the operator to ignore it.
            # Order-invariance does NOT hold for re-basing on out-of-order arrival; ACCEPTED as
            # interim, but the corrupted case must be visible without forensics.
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

    def _booking_instant_touch(self, slug: str, *, late_booked: bool,
                               ) -> tuple[Optional[Decimal], Optional[Decimal], Optional[float]]:
        """(bid, ask, frame content age) from the WS book cache AT THIS INSTANT, for the fill
        tape's BOOKING-instant stamp.

        ⛔ ZERO REQUESTS AND ZERO SIDE EFFECTS — cache reads only: no fresh-REST, no C-a budget,
        no verification-clock refresh, no ws serve. Buying a fill-instant mark with a venue
        request would change the maker's request profile on the fill path.

        ⛔ `late_booked` returns blanks: the fill happened up to LAG_HORIZON_S ago, so a book read
        NOW is not its instant. Any feed exception is swallowed to blanks and logged — a tape
        column must never be able to kill the booking of a real fill."""
        if late_booked or self.book_feed is None:
            return None, None, None, None, None
        getter = getattr(self.book_feed, "get_book_md", None)
        if getter is None:
            return None, None, None, None, None
        try:
            md = getter(slug)
            if md is None:
                return None, None, None, None, None
            bid, ask, _tob = touch_from_md(md)
            age = transact_age_s(md.get("transactTime"), time.time())
            # NET-OF-SELF at the booking instant: same owner and same `self.real` gate as the
            # quote path — a shadow's orders never rest, so subtracting them fabricates a
            # thinner book.
            net_bid = net_ask = None
            if self.real:
                _ob = self.resting.get((slug, "bid"))
                _oa = self.resting.get((slug, "ask"))
                net_bid, net_ask = net_of_self_touch(
                    md,
                    (_ob.price, Decimal(_ob.size) - _ob.filled_qty) if _ob else None,
                    (_oa.price, Decimal(_oa.size) - _oa.filled_qty) if _oa else None)
        except Exception as exc:
            log.warning(f"booking-instant touch unreadable for {slug}: {exc!r} — the fill "
                        f"tape's booking_bid/booking_ask stay blank for this row")
            return None, None, None, None, None
        if bid is None or ask is None:
            return None, None, None, net_bid, net_ask   # one-sided raw touch: raw blanks, net may still read
        return bid, ask, age, net_bid, net_ask

    def _book_fill(self, order: RestingOrder, body: dict, *,
                   late_booked: bool = False, booked_via: str = "poll",
                   ts_override: Optional[float] = None) -> Optional[Fill]:
        """Book any NEW fill on `order` from a read-back body. None if nothing new filled.

        `cumQuantity` is CUMULATIVE, so the increment is `cum − already_seen`; adding the
        cumulative figure on every poll would double the position. An unreadable `cumQuantity`
        books nothing — cannot-verify is not a fill.

        `ts_override` stamps the row with the VENUE's own execution time, because `rebate_total`'s
        final-row selection keys on (cum, ts) and a recovered fill can land minutes after it
        happened. `late_booked=True` BLANKS the mid columns: the fill happened up to
        LAG_HORIZON_S ago, so the current touch is not a mark for it and a fresh-looking
        `mid_age_s` would poison every markout built on the tape."""
        cum = _amount(body.get("cumQuantity"))
        if cum is None:
            return None
        increment = cum - order.filled_qty
        if increment < _ZERO:
            # ⛔ `cumQuantity` is monotone by contract — a negative delta means one of the two
            # reads is WRONG. Which one is undecidable here, so booking is refused and belief
            # unchanged; the arbiter for the direction this hides (an OVER-stating earlier body)
            # is `scripts.poly_order_diff` against the venue's activities, after teardown.
            self.cum_regressions += 1
            log.warning(f"⚠️ {order.slug} {order.side} {order.order_id}: cumQuantity went "
                        f"BACKWARDS ({order.filled_qty} → {cum}) — one of the two reads is "
                        f"wrong; booking refused, belief unchanged. If the earlier read was "
                        f"a WS body this is the over-statement signature — check "
                        f"reconcile the tape against the venue after the run — see the operator runbook.")
            return None
        if increment == _ZERO:
            return None
        touch = None if late_booked else self.last_touch.get(order.slug)
        mid = ((touch[0] + touch[1]) / 2) if touch else None
        # The BOOKING-instant touch, read here rather than at the cycle's book read
        #. Free (WS cache only) and blank when there is no feed.
        f_bid, f_ask, f_age, f_nbid, f_nask = self._booking_instant_touch(
            order.slug, late_booked=late_booked)
        row_ts = ts_override if ts_override is not None else time.time()
        # ⛔ THE GAP THIS STAMP IS WORTH NOTHING WITHOUT: how long after the VENUE's own execution
        # stamp we booked. `lastTransactTime` is the order's last state transition, which for the
        # increment just detected is the execution that produced it [VERIFIED against recorded
        # live order bodies]. ⚠️ On a body read AFTER a cancel (every REPLACE) this stamps the
        # CANCEL transition, so an older fill reports ~0 — benign direction. ⚗️ Since 2026-09-07
        # `_cancel` reads back AFTER the ack, so a REPLACE-booked fill stamps the cancel
        # transition too, not only the poll path.
        _venue_ts = iso_to_ts(body.get("lastTransactTime"))
        f_lag = (row_ts - _venue_ts) if _venue_ts else None
        # This side's time-at-price epoch, matched to the FILLED order's price. A different
        # price means the side has moved on and this row cannot be dated from it.
        _epoch = self._price_clock.get((order.slug, order.side))
        fill = Fill(
            ts=row_ts, slug=order.slug, side=order.side, order_id=order.order_id,
            price=order.price, size=order.size, filled_qty=increment, cum_filled_qty=cum,
            commission_order_total=self._note_commission(
                order.order_id, _amount(body.get("commissionNotionalTotalCollected"))),
            queue_ahead=order.queue_ahead,
            time_to_fill_s=row_ts - order.placed_ts, improved=order.improved,
            order_state=str(body.get("state") or ""), mid_at_fill=mid,
            mid_age_s=(time.time() - touch[2]) if touch else None,
            best_bid=touch[0] if touch else None,
            best_ask=touch[1] if touch else None,
            net_bid_place=order.net_bid_place, net_ask_place=order.net_ask_place,
            booking_bid=f_bid, booking_ask=f_ask, booking_touch_age_s=f_age,
            booking_net_bid=f_nbid, booking_net_ask=f_nask,
            fill_stamp_lag_s=f_lag, venue_ts=_venue_ts,
            book_src="" if late_booked else self.last_book_src.get(order.slug, ""),
            avg_px=_amount(body.get("avgPx")), late_booked=late_booked,
            booked_via=booked_via,
            # Read HERE, at the booking instant, on every path (ws / poll / verify /
            # activities) — a poll-booked row inside a WS outage is exactly the row the
            # blind-window measurement needs, and stamping it at the one common booking site
            # is what makes the paths comparable.
            feed_subscribed=bool(self.order_feed is not None and self.order_feed.subscribed),
            # THE TWO REST CLOCKS, read at the booking instant from their own sources — the
            # ORDER for `order_rest_s`, the (book, side) price epoch for `price_rest_s`. See the
            # `Fill` field comments: crossing them is the mutation this pair exists to expose.
            order_rest_s=_elapsed_since(row_ts, order.rest_since_ts),
            price_rest_s=_elapsed_since(
                row_ts, _epoch[1] if (_epoch is not None and _epoch[0] == order.price) else None))
        order.filled_qty = cum
        slug = order.slug
        prev_rt = self.rt_realized.get(slug, _ZERO)
        prev_inv = self.inventory.get(slug, _ZERO)
        new_inv, new_avg, rt_r, rt_c, completed = fill_accounting(
            prev_inv, self.avg_entry.get(slug, _ZERO),
            prev_rt, self.rt_closed.get(slug, _ZERO),
            order.side, order.price, increment)
        # ⛔ The entry width comes from THIS ORDER's placement-time net touch, never from the fill
        # row's `best_*` (self-inclusive).
        # ⛔⛔ SNAPSHOT FIRST — the trigger reads the PRE-fill carrier: on a THROUGH-ZERO fill the
        # line below has already reset the carrier to the width of the position that just OPENED,
        # so reading the post-call value prices a completed trip off the NEXT trip's width.
        hs_at_entry = (self._entry_hs_mean.get(slug, _ZERO), self._entry_hs_qty.get(slug, _ZERO))
        self._entry_hs_mean[slug], self._entry_hs_qty[slug] = entry_width_accounting(
            prev_inv, hs_at_entry[0], hs_at_entry[1], order.side, increment,
            entry_half_spread(order.net_bid_place, order.net_ask_place))
        self.inventory[slug] = new_inv
        # Tape only [I1]: which path last moved the belief. Set beside the ONE assignment to
        # `self.inventory`, so the two can never disagree about provenance.
        self._tape_map("_inv_src")[slug] = booked_via
        self.avg_entry[slug] = new_avg
        self.rt_realized[slug] = rt_r
        self.rt_closed[slug] = rt_c
        # ⛔ THE LOSS CAP FEEDS PER REDUCING FILL, not per completed round trip. The old form
        # added `completed[0]` only when a trip closed to EXACTLY flat or crossed zero, so a
        # position unwinding 37 reducing fills to −0.64 banked −<n> of realized loss the cap
        # never saw — and reduce-only mode makes the never-flat branch the NORM (it sizes to
        # min(book, int(|inv|)), so it never crosses zero and a FRACTIONAL residue can never be
        # quoted away at all). Per-fill delta: on a completion `completed[0]` is prev_rt plus this
        # fill's realized, else the delta is rt_r − prev_rt; an ADDING fill moves neither. Σ
        # per-fill deltas equals the old per-trip sum when trips DO complete — the change is WHEN
        # the ledger learns, never how much. Late-booked fills still feed the cap (money is
        # order-invariant).
        fill_pnl = (completed[0] - prev_rt) if completed is not None else (rt_r - prev_rt)
        if fill_pnl != _ZERO and self.state is not None and self.real:
            self.session_realized += fill_pnl
            self.state.add_realized(fill_pnl)
            breach = self._loss_cap_breach()
            if breach is not None:
                self.should_stop = True
                if self.halt_reason is None:
                    # First cause wins: a breach during a kill-switch teardown must not rewrite
                    # the durable exit record's reason.
                    self.halt_reason = breach
                log.error(breach)
        # ⛔ Late-booked fills never ARM the cooldown: the delayed verify books up to
        # LAG_HORIZON_S after the fact, so arrival order ≠ fill order and the round trip a late
        # delta appears to complete can be fiction. Inventory stays correct either way; only the
        # TRIGGER declines to act on re-ordered history. The adverse-heavy subset is exactly the
        # late subset, so this errs toward missing a cooldown, never inventing one.
        prev_newest = self._last_row_ts.get(slug)
        self._last_row_ts[slug] = row_ts if prev_newest is None else max(prev_newest, row_ts)
        # ⛔ NOTIFY ORDER ONLY. The latch fires below, before this fill has been announced, so its
        # channel line is collected here and enqueued after the fill/round-trip lines — the
        # channel then reads FILL → ROUND TRIP → LATCH, the order the events happened in. Nothing
        # about the latch's own timing moves.
        latch_notes: list[tuple[str, str, str]] = []
        if completed is not None and not late_booked and self.adverse_cooldown_s > 0:
            realized, closed = completed
            # ── THE TRIGGER, AND THE ONE LANE CONDITION [cycle-4 P1] ────────────────────────
            # ⛔ `self.lane == PROBE_LANE` is the whole of it. On every other lane `hs` stays None,
            # `bar` stays the incumbent ADVERSE_RT_PER_CONTRACT and `ban_s` stays None, so the
            # comparison, the latch and its scope are the pre-existing ones.
            probe = self.lane == probe_notify.PROBE_LANE
            # ⛔ THE SNAPSHOT, not the live carrier — see the BLOCKING note at `hs_at_entry`.
            hs = hs_at_entry[0] if (probe and hs_at_entry[1] > _ZERO) else None
            # `hs is None` on the probe lane = no entry fill of ours carried a usable net pair.
            # The fallback is the INCUMBENT absolute bar, taped — never `best_bid/best_ask`, which
            # is self-inclusive and would hand the rule our own quoted width as the market's.
            fallback = probe and hs is None
            # ⛔ FLOORED: width-normalisation may only ever LOOSEN. The fallback branch resolves to
            # the floor by construction — the same number — but stays a distinct branch because
            # the TAPE must record which one a replay is looking at.
            bar = (max(ADVERSE_WIDTH_BAR_FLOOR, ADVERSE_WIDTH_K * hs) if hs is not None
                   else ADVERSE_RT_PER_CONTRACT)
            if closed > _ZERO and realized / closed <= -bar:
                per_ct = realized / closed
                # ⛔ STICKY, not a deadline: `adverse_cooldown_s > 0` is the rail's on/off switch,
                # but its duration no longer governs the adverse rail (the mark tripwire still
                # uses it). Scope is the config's to choose, and ⛔ THE TAG MOVES WITH THE SCOPE —
                # `latch_rule` is never a constant here, or the tape labels post-change armings
                # with the rule they are no longer under. `per_ct`/`bar` are taped in every case
                # as the COVARIATE, not the treatment. `_latch_rule_for` is the ONE place the
                # per-book control arm can enter — ON EVERY LANE [AMENDMENT 33, operator
                # 2026-09-09]: a book the LINE named must not latch, or the flag is a no-op the
                # operator reads as armed.
                book_rule = self._latch_rule_for(slug)
                control = book_rule == LATCH_RULE_CONTROL_OFF
                if not probe:
                    ban_s: Optional[float] = None
                    ban_rule = ""
                elif control:
                    ban_s, ban_rule = None, LATCH_RULE_CONTROL_OFF
                elif book_rule in probe_config.LATCH_BAN_ARMS_BY_RULE:
                    # ⛔ ONE DRAW PER EPISODE [confound A]. A trip while this book's ban is STILL
                    # RUNNING is a re-trip, not a new assignment: the clock restarts on the arm
                    # the episode already holds and the rng is NOT advanced. Re-drawing let an
                    # 1800 s book land on 60 s and be two-sided a minute later, so `latch_ban_s`
                    # on the tape was not the ban the book actually served. A trip AFTER the ban
                    # expired finds no episode and draws afresh — that IS a new assignment.
                    _episode = self.adverse_ban_episode.get(slug)
                    if (_episode is not None
                            and time.time() < self.adverse_ban_until.get(slug, 0.0)):
                        ban_s = _episode[0]
                    else:
                        # ⛔ THE ARMS COME FROM THE RULE THAT IS RUNNING, never from one module
                        # constant: each randomized rule draws its OWN registered set and tapes
                        # its own tag, and a single constant taped a four-arm draw under a tag
                        # that promises two.
                        ban_s = draw_ban_seconds(self._ban_rng, ban_arms_for_rule(book_rule))
                    # ⛔ THE TAG IS THE RULE THAT RAN, not a hardcoded one — the tape's whole
                    # purpose is to separate the eras row by row.
                    ban_rule = book_rule
                else:
                    ban_s, ban_rule = None, LATCH_RULE_LIFETIME_REVERTED
                first = slug not in self.adverse_latched
                # ⛔ AMENDMENT 19c — THE ONE THING THE CONTROL ARM SKIPS, AND NOTHING ELSE. The
                # trigger above was EVALUATED and the arming row below is TAPED under
                # `latch_rule=off` with `latch_ban_s` blank, so the would-have-armed instant is on
                # the fills tape — the counterfactual no replay could observe, because every book
                # a rule would have spared was held reduce-only and never re-quoted.
                # ⛔ EVERY OTHER RAIL STILL GOVERNS on a control cell. It is NOT
                # `--adverse-cooldown-s 0`, which would take the tripwire with it.
                if not control:
                    self._latch_adverse(slug, ban_s, defer_notify=latch_notes)
                # ── the rule's inputs, ON THE ARMING FILL'S OWN TAPE ROW ────────────────────
                # So the falsifier replay is a table read rather than a log parse. Blank on every
                # non-probe lane (no rule inputs exist).
                if probe:
                    fill.latch_hs = hs
                    fill.latch_hs_src = "abs" if fallback else "net"
                    fill.latch_ratio = per_ct.copy_abs() / bar
                    fill.latch_ban_s = ban_s
                    fill.latch_fallback = fallback
                    fill.latch_rule = ban_rule
                # ⛔ RE-FIRES ARE ANNOUNCED TOO: the re-stamp silently invalidates any ack already
                # in the file, so an operator who saw only the first line would meet a STALE
                # refusal for an event nobody told them about.
                # ⛔ TWO INDEPENDENT CLAUSES, KEYED ON DIFFERENT THINGS: the bar clause on
                # `probe`, the scope clause on `ban_s`. One nested conditional on `ban_s is None`
                # was correct only while "no deadline" and "not the probe lane" were the same
                # fact — under `kill_reverted_lifetime` the probe lane has NO deadline while still
                # firing on the width-normalised bar, so a single key printed the incumbent <n>
                # over a trigger that was nothing of the kind.
                if not probe:
                    bar_clause = f"(≤ −{ADVERSE_RT_PER_CONTRACT}/contract)"
                else:
                    # The probe lane's line names the rule that fired, because the operator's
                    # first question during a run is "why this book, and until when".
                    # ⚠️ "severity … (COVARIATE)" is deliberate on the DRAWN arm: an operator
                    # reading that line during a run must not infer the ban from it.
                    bar_clause = (
                        f"(≤ −{bar:.3f}/contract = max(floor {ADVERSE_WIDTH_BAR_FLOOR}, "
                        f"{ADVERSE_WIDTH_K}×hs, hs={hs if hs is None else f'{hs:.4f}'})"
                        f"{' [ABSOLUTE FALLBACK — no usable net entry width]' if fallback else ''}"
                        f"; severity {per_ct.copy_abs() / bar:.2f}×"
                        + (f" [{ban_rule}])" if ban_s is None else
                           f" (COVARIATE — the ban is DRAWN from "
                           f"{{{', '.join(f'{a:.0f}' for a in LATCH_BAN_ARMS_S)}}}s "
                           f"independently of it [{ban_rule}]))"))
                # ⛔ AMENDMENT 19c — THE CONTROL CELL'S LINE, AND IT CARRIES NO ACK. The remainder
                # of this block prints a pasteable `clear_adverse_latch` stanza; on a control cell
                # there is no latch to clear, and offering one would invite an operator to "fix" a
                # book that was never held. The trip is still announced.
                if control:
                    detail = (f"realized {realized:.3f} over {closed:.3f} contracts "
                              + bar_clause
                              + f" — NOT latched: this book was drawn into the LATCH-ONLY "
                                f"CONTROL arm at pick time (--latch-off-slugs, "
                                f"latch.control_share). It keeps quoting; the mark tripwire, "
                                f"width shield, caps and loss cap still govern it. The trip is "
                                f"taped with latch_rule={LATCH_RULE_CONTROL_OFF!r} and a blank "
                                f"latch_ban_s.")
                    log.warning(f"ADVERSE TRIP (control cell, NOT latched) on {slug}: {detail}")
                    latch_notes.append(("ADVERSE TRIP (control cell, NOT latched)", slug, detail))
                else:
                    # THIS lane's file, not the shared default: an ack pasted into the other
                    # lane's file gets a refusal, or voids that lane's file with a foreign slug.
                    if ban_s is None:
                        scope_clause = (f" — reduce-only for remainder of run. To clear, put a "
                                        f"CURRENT timestamp in {self._hot_file()}: ")
                    else:
                        scope_clause = (f" — reduce-only until {_utc_iso(time.time() + ban_s)} "
                                        f"({ban_s:.0f}s), then NORMAL quoting. An earlier clear "
                                        f"needs a CURRENT timestamp in {self._hot_file()}: ")
                    log.warning(
                        f"adverse round trip on {slug}"
                        f"{'' if first else ' (AGAIN — latch re-stamped, any ack already written '
                        'for the earlier latch is now stale)'}: realized "
                        f"{realized:.3f} over {closed:.3f} contracts "
                        + bar_clause + scope_clause
                        + f'{{"books": {{"{slug}": {{"clear_adverse_latch": '
                        # +1s: `_utc_iso` truncates DOWN, so stamping `now` prints an instant
                        # already older than the latch it would clear.
                        f'"{_utc_iso(time.time() + 1)}"}}}}}} — an ack older than the latch (or '
                        f"more than {ACK_MAX_SKEW_S:.0f}s in the future) is ignored, and "
                        f"quote:normal does NOT clear it")
        # ⛔ AFTER the trigger: a trip that closes to flat must be judged against the width it was
        # ENTERED at, and only then does the carrier stop describing an open position.
        if new_inv == _ZERO:
            self._entry_hs_mean.pop(slug, None)
            self._entry_hs_qty.pop(slug, None)
        self._write_fill(fill)
        # ── probe-stats channel: notify-only, debounced, cannot raise ────────────────────────
        # ⛔ The RENDERER is handed over, not its result — rendering at the call site would sit
        # outside `event()`'s swallow.
        self._probe_notes.event(
            probe_notify.fill_line,
            slug=slug, side=order.side, price=order.price, filled_qty=increment,
            cum_filled_qty=cum, commission_order_total=fill.commission_order_total)
        if completed is not None:
            self._probe_notes.event(
                probe_notify.round_trip_line,
                slug=slug, realized=completed[0], closed=completed[1])
        # LAST: a latch is a CONSEQUENCE of the trip above, so it is announced after it.
        for kind, latched_slug, detail in latch_notes:
            self._probe_notes.event(probe_notify.event_line, kind, latched_slug, detail)
        return fill

    # ── teardown ─────────────────────────────────────────────────────────────────────────────

    async def teardown(self) -> list[tuple[str, str]]:
        """Run the four phases IN ORDER and return (phase, outcome) for each.

        ⛔ cancel-all → reconcile-pending → flatten → sweep. Sweeping before flattening re-reads a
        book that the flatten is about to move. The order is driven off `TEARDOWN_PHASES` rather
        than written out as separate statements, so the invariant is one pinnable value rather
        than four places that can drift apart.
        """
        # Post anything still queued BEFORE the phases run: teardown can take ~70 s and a
        # SIGKILL during it would lose the queue entirely.
        self._probe_notes.flush()
        results: list[tuple[str, str]] = []
        for phase in TEARDOWN_PHASES:
            handler = getattr(self, f"_teardown_{phase}")
            try:
                if phase == "flatten" and hasattr(self.client, "recovery_public_reads"):
                    async with self.client.recovery_public_reads():
                        outcome = await handler()
                else:
                    outcome = await handler()
                results.append((phase, outcome))
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
                "sequence, in order: (1) reconcile the tape against the venue's "
                "activities and note any venue≠tape gap; (2) note realized/loss_to_date "
                "from the state file; (3) only THEN close the record by deleting the state "
                "file, carrying the noted loss forward via --loss-cap on the next launch. "
                "Do NOT delete before reconciling — deletion discards the durable loss "
                "ledger, and an understated ledger is the thing this flag exists to stop. "
                "[registered residue: a recovery-aware refusal cause in maker_state]")
        if self.state is not None and self.real:
            # ── CLOSE THE CRASH RECORD — but only on the VENUE's evidence ────────────────────
            # `_swept_clean` is True only when the sweep's listing succeeded and every order on
            # our markets cancelled cleanly. Order records outlive their parked verifies here
            # (the 120s horizon exceeds the ~80s teardown window), so without this clear every
            # CLEAN run left opswatch warning "N orders may be RESTING" — a false alarm that
            # trains the operator to ignore the true one. Writing "clean" off our own belief
            # instead of the venue's answer is the reconcile.py fiction, in the flag that tells
            # `scripts/maker_recover.py` there is nothing to do.
            if self._swept_clean and not recovery_open:
                for _iid in list(self.state.snapshot().orders):
                    self.state.clear_order(_iid)
                # ⚠️ LATCH RUN-END EXPIRY sits INSIDE the venue-confirmed clean close on purpose:
                # a SIGKILL/OOM never reaches this line, so a crash-restart KEEPS its latches
                # ("restart is not a re-arm" holds by construction, not by a flag). The
                # accounting-open branch below does NOT expire, and off-slate latches are never
                # touched. A mid-run clear still needs the dated operator ack.
                # ⛔ `_ever_slugs`, NOT `slugs` [continuous maker P1]: a book the hot slate DROPPED
                # is off `slugs`, and its latch is still a fact about THIS run — left standing it
                # would ban that book on the next run for a trip this one took.
                expired = sorted(s for s in self.adverse_latched if s in self._ever_slugs)
                if expired:
                    for _s in expired:
                        del self.adverse_latched[_s]
                        # The deadline dies with its latch — a probe-lane ban that outlived the
                        # run it was measured in would ban a book on the NEXT run for a trip that
                        # run never took.
                        self.adverse_ban_until.pop(_s, None)
                        self.adverse_ban_episode.pop(_s, None)
                    self.state.set_adverse_latched(self.adverse_latched,
                                                   self.adverse_ban_until)
                    log.info(f"🔓 adverse latch EXPIRED at clean run end for {expired} — "
                             f"within-run stickiness only; a bad book re-latches next run "
                             f"after one bounded round trip")
                self.state.end_run(self.halt_reason or "clean")
            elif self._swept_clean:
                # The C8 branch: the venue is swept clean of RESTING orders, but the ACCOUNTING is
                # not closed — a queued/unresolved recovery entry can carry a fill the realized
                # ledger never saw. The record stays open so the next start refuses rather than
                # trading on an understated loss ledger.
                left_record_open = True
                # ⛔ STAMP WHY [review B1]. A venue read cannot see an unbooked fill, so
                # `poly_cert_reconcile` must REFUSE this record — without the marker it reads
                # flat, closes, and writes clean_exit over an understated loss ratchet.
                self.state.set_record_open_reason("unreconciled_realized")
            else:
                left_record_open = True
                # The sweep/verify could NOT confirm nothing is resting — the one open-record
                # cause a fresh venue read actually answers, so this reason is the closable one.
                self.state.set_record_open_reason("sweep_unverified")
                # ⛔ Do NOT point this message at `scripts.maker_recover` — it is KALSHI-only and
                # has been demonstrated reading the wrong venue, printing "the venue confirms
                # flat" over a live POLY order and writing clean_exit=True.
                # ⛔ And confirmation is by API, not the UI: the tool runs with the SAME
                # credentials that placed the orders, where "nothing in the UI" is also what a
                # wrong-account login shows.
                msg = ("⚠️ the venue could not confirm that nothing is resting — leaving the "
                       "crash record OPEN (clean_exit stays False). The next start will "
                       "REFUSE cleanly (the preflight for a readable stray; begin_run for "
                       "an unattributable one) and its message carries the Poly remedy. "
                       "Resolve on the POLY venue: confirm what is actually resting with "
                       "a read-only listing (see the operator runbook; same "
                       "credentials that placed the orders); cancel strays by hand and "
                       "close positions by hand (see the operator runbook); once the venue is "
                       "confirmed clear, close the record with the reconcile tool (see the operator runbook) for run "
                       f"{self.run_id} — it re-reads "
                       "the lane's open orders and, on zero resting, records the exit while "
                       "KEEPING the loss ledger and the carries' basis. Deleting the state "
                       "file named in the refusal is the LAST RESORT — ⚠️ it also discards "
                       "the durable loss ledger (realized_pnl and the cap ratchet reset to "
                       "zero) and every carry's basis, so note realized/loss_to_date first. "
                       "Do NOT run the Kalshi recovery tool for "
                       "this — it is Kalshi-only and would certify a false clean from the "
                       "wrong venue.")
                if self._sweep_refused:
                    # Only honest when the venue ACTUALLY refused a cancel — the other two causes
                    # of an unswept exit are not ghosts and must not inherit this diagnosis.
                    msg += (f" The venue REFUSED {self._sweep_refused} cancel(s): if "
                            "a read-only listing shows nothing resting and positions flat, this "
                            "is the terminal-but-listed case (the sweep's listing carried "
                            "an already-dead order) — the record is open over a ghost and "
                            "the confirm-flat-then-reconcile path above applies; it is not a "
                            "wrong-account signal.")
                elif self._sweep_refused is None:
                    msg += (" ⛔ The sweep's LISTING FAILED — the venue was never read. "
                            "This is a cannot-verify, not a ghost: do NOT delete anything "
                            "until a read-only listing answers.")
                log.error(msg)
        if self.heartbeat is not None:
            _exit = "clean" if self.halt_reason is None else f"halted:{self.halt_reason}"
            if left_record_open:
                # The branch above just left the crash record OPEN — the heartbeat must carry the
                # same verdict, or the two shutdown artifacts disagree and the deadman reads
                # "clean" over a record the next start will refuse on. Any non-"clean" status
                # fails Deadman.ok. The flag is set IN the record-open branch itself.
                _exit = f"record_open:{_exit}"
            self.heartbeat.mark_exit(_exit)
        self._close_writers()
        return results

    async def _teardown_reconcile_pending(self) -> str:
        """READ every parked order before the flatten sizes itself (`ignore_due` schedules early
        READS — a teardown cannot wait out the horizon), booking any delta each pass. ⛔ Entries do
        NOT retire early: an early readable answer can be stale, so anything still inside its
        horizon stays parked, is NAMED below, and the venue-verify at exit remains the last word.
        The flatten therefore sizes off the best booked number the lag allows."""
        if not self.pending_reconcile and not self.pending_activity_recovery:
            return "nothing parked"
        if self.pending_reconcile:
            self._teardown_pace_deadline = time.monotonic() + TEARDOWN_VERIFY_PACE_BUDGET_S
            for _ in range(4):
                await self._retry_pending_reconciles(ignore_due=True)
                if not self.pending_reconcile:
                    break
                await asyncio.sleep(20)
        # ── the recovery drain: after reconcile-pending, before the flatten (which then
        # sizes off healed belief). HARD BUDGET: <=3 requests, <=5 s wall, NO sleeps — a
        # pause.json halt must not stretch by minutes. A call that spends nothing has nothing
        # left to read.
        drain_deadline = time.monotonic() + 5.0
        drain_reqs = 0
        if self.pending_activity_recovery and self._nf_breaker_tripped:
            # The flag is stamped by the last COMPLETED poll and `poll_fills` early-returns once
            # resting empties, so at teardown it can only be stale-TRIPPED, never stale-cleared —
            # say so rather than draining silently into zero requests.
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
        # ⛔ The success string is conditioned on BOTH queues: an unresolved recovery queue must
        # never read as "all parked orders reconciled".
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
        """ONE pass over `self.resting` ∪ `parked_alive`, nothing else.

        `_halt_cycle` has already `_cancel`led every resting quote before teardown starts, so
        `resting` holds only REFUSED cancels; the orders the venue contradicted after their ACK
        (`parked_alive`) were reached only by the sweep's listing. NOT `pending_reconcile`: those
        cancels the venue already ACKed, and re-DELETEing them is wasted requests on the burst
        that trips the edge. A ban ends the pass: the by-id leg reads `cancel_order_ex`'s
        `banned` verdict; `_cancel` collapses a ban into a refusal (`client.cancel_order` never
        raises), so a refusal there is followed by the zero-request read of THIS client's ban
        bucket (`state_path(source_ip)` — the default bucket is inert under `POLY_SOURCE_IP`).
        The ids not attempted are named. No retry: the sweep and the post-exit timer own the rest.
        """
        if self.shadow:
            return "shadow: nothing was ever placed"
        from bot.core import venue_budget
        bucket = venue_budget.state_path(getattr(self.client, "_source_ip", "") or "")
        resting_keys = list(self.resting)
        resting_ids = {o.order_id for o in self.resting.values() if o.order_id}
        parked = [oid for oid in self.parked_alive
                  if oid not in resting_ids and oid in self.pending_reconcile]
        todo: list[tuple[str, Any]] = ([("resting", k) for k in resting_keys]
                                       + [("parked", oid) for oid in parked])
        done = 0
        banned: Optional[str] = None
        for i, (kind, key) in enumerate(todo):
            try:
                if kind == "resting":
                    ok = await self._cancel(*key)
                    if not ok and venue_budget.ban_active(path=bucket) is not None:
                        banned = "venue ban active"
                else:
                    self._extra_requests += 1
                    self.parked_recancels += 1
                    ok, verdict = await self.client.cancel_order_ex(
                        key, self.pending_reconcile[key][0].slug)
                    if verdict == "banned":
                        banned = verdict
                done += int(ok)
            except venue_budget.VenueRefused as exc:
                banned = str(exc)
            if banned is not None:
                left = []
                for kind, k in todo[i + 1:]:
                    o = self.resting.get(k) if kind == "resting" else None
                    left.append(o.order_id if o is not None else k)
                log.error(f"⚠️ teardown cancel-all ENDED under a venue ban ({banned}) — "
                          f"{len(left)} not attempted: {left}; the sweep and the post-exit "
                          f"recovery own them.")
                return (f"cancelled {done}/{len(todo)} (resting {len(resting_keys)}, "
                        f"parked-alive {len(parked)}); not attempted under ban: {left}")
        return (f"cancelled {done}/{len(todo)} (resting {len(resting_keys)}, "
                f"parked-alive {len(parked)})")

    async def _teardown_flatten(self) -> str:
        """Passive, reducing-only, and it REPORTS what it could not clear.

        ⛔ The passive pass never crosses the spread, and OFF THE PROBE LANE nothing here does —
        see `_flatten_cross_residual` for the probe-lane cross that runs after the wait, on the
        corroborated remainder only. Cancelling is safe, flattening is not: a residual is left in
        the durable record for an operator. A maker that market-sells its own inventory to look
        tidy at exit is a maker that pays the taker fee it spent all day avoiding.
        """
        # ⛔ WORST VALUE FIRST, narrowed at every return below (see `flatten_outcome`): a raise
        # anywhere in this method must not read as "the flatten was offered and simply rested".
        self.flatten_outcome = "failed"
        residual = {s: q for s, q in self.inventory.items() if q != _ZERO}
        if not residual:
            self.flatten_outcome = "filled"
            return "flat (LOCAL BELIEF — the verify line below is the certification)"
        if self.shadow:
            self.flatten_outcome = "shadow"
            return f"shadow: would flatten {residual}"
        # ⛔ READ THE VENUE FRESH, HERE. The gate below decides whether to TRADE, and the scheduled
        # refresh is up to RECONCILE_EVERY_CYCLES old before teardown even starts — then
        # `_teardown_reconcile_pending` sleeps 4×20 s ahead of us. A 60–140 s-old row corroborates
        # nothing. One request.
        venue_fresh = await self._refresh_venue_inventory() if self.real else False
        # ⛔ CLASSIFY BEFORE REFUSING. A SETTLED market is dropped from the positions endpoint, so
        # its belief presents as an uncorroborated divergence and the refusal below then withheld
        # the WHOLE flatten — including books the venue did corroborate. Only a CONFIRMED-FRESH
        # read may be used to say "no row". Fails closed per book; moves no money.
        settled_status_seen: dict[str, str] = {}
        if venue_fresh:
            # ⛔ `venue_stale_rows` IS PART OF "THE VENUE ANSWERED". An unparseable row is a row:
            # it is absent from `venue_inventory` by construction, so passing that map alone would
            # read a junk row as "no row" and could retire a belief over a LIVE position.
            self.settled_carved = await carve_out_settled_books(
                self.client, self.inventory,
                set(self.venue_inventory) | self.venue_stale_rows,
                store=self.state, avg_entry_yes=self.avg_entry,
                basis_carried=self.basis_carried,
                observed=settled_status_seen, run_id=self.run_id)
            if self.settled_carved:
                residual = {s: q for s, q in self.inventory.items() if q != _ZERO}
                if not residual:
                    return (f"flat after the settled carve-out ({sorted(self.settled_carved)}) "
                            f"— the verify line below is the certification")
        unsafe: dict[str, str] = {}
        #: slug → side of the passive flatten order THIS phase placed and the venue acknowledged.
        #: The cross below trades only these books: an order we cannot name is an order we cannot
        #: cancel, and crossing beside our own live passive order is the NOT-SAFE shape
        #: (cancel_all runs BEFORE flatten, sweep AFTER, so nothing else retires it).
        flatten_orders: dict[str, str] = {}
        for slug, qty in residual.items():
            if not venue_fresh:
                unsafe[slug] = f"{qty} (venue unreadable at teardown — not corroborated)"
                continue
            # ⛔ THE FLATTEN MUST AGREE WITH THE VENUE BEFORE IT TRADES. This is the one path that
            # can send an order in the direction that GROWS a position nothing is measuring, and
            # it is reachable from every halt — including the venue breach, whose own message says
            # "we do not know our position". Belief +12 against a venue −24 produced a post-only
            # SELL 12 at the offer: a "flatten" taking a 24-short to 36.
            # So: trade only where the venue CORROBORATES belief IN SIGN, and size to the smaller
            # of the two so we cannot overshoot either estimate. Everything else is REPORTED.
            if slug in self.venue_stale_rows:
                unsafe[slug] = f"{qty} (venue row unparseable — not corroborated)"
                continue
            venue_qty = self.venue_inventory.get(slug)
            if venue_qty is None or venue_qty == _ZERO:
                # "No row" and "row says zero" are the same instruction here — the venue does not
                # think we hold this — and both must REPORT rather than trade. Distinguished in
                # the text so an operator can tell a missing book from a flat one. The observed
                # market status rides the "no venue row" case: it is the discriminator between a
                # settled book and a real divergence.
                _st = settled_status_seen.get(slug)
                unsafe[slug] = (f"{qty} (venue says flat)" if venue_qty == _ZERO
                                else f"{qty} (no venue row"
                                     + (f", market status {_st}" if _st else "") + ")")
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
            _md = book.get("marketData") if isinstance(book, dict) else None
            bid, ask, _ = touch_from_md(_md)
            if bid is None or ask is None:
                continue
            # Reducing side only, joining the touch: a long is reduced by an ask at the offer.
            side = "ask" if qty > _ZERO else "bid"
            # ⛔ THE NEWER WITNESS STATES THE TOUCH. The
            # rule is unchanged — JOIN the touch, never cross — but the book above is a REST read,
            # and REST reaches ORIGIN, which FREEZES for minutes while our own WS book is current.
            # That is how a flatten of a short went out one tick behind a live bid, so it rested, the
            # sweep cancelled it, and the residual carried. So price off whichever of the two
            # books the VENUE stamped later. The comparison is the client's own rule, called not
            # copied. No witness, an unstamped REST body, or a witness failing this maker's WS
            # rails (`_ws_guard_witness`: the OR-rule via `_ws_book_servable`, a readable
            # content stamp, feed up) → REST, exactly as
            # before. The witness that priced it is the one handed to the crossing guard below.
            _now = time.time()
            _rest_age = transact_age_s((_md or {}).get("transactTime"), _now)
            _ws = self._ws_guard_witness(slug)
            _src, _age = "rest", _rest_age
            if _ws is not None and PolyUSClient._newer_witness_clears(
                    "buy" if side == "bid" else "sell",
                    _ws[0] if side == "bid" else _ws[1],
                    None if _rest_age is None else _now - _rest_age, _ws, True) is not None:
                bid, ask = _ws[0], _ws[1]
                _src, _age = "ws", max(0.0, _now - _ws[2])
            price = ask if qty > _ZERO else bid
            # Both witnesses on the line: `src=rest` alone cannot tell "no WS witness" from
            # "WS stale" from "WS < margin newer" — the three regimes that decide whether
            # pricing off the newer witness worked.
            log.info(f"flatten: {slug} {side} @ {price} src={_src} "
                     f"age={'?' if _age is None else format(_age, '.0f')}s "
                     f"rest_age={'?' if _rest_age is None else format(_rest_age, '.0f')}s "
                     f"ws={'none' if _ws is None else f'{_ws[0]}/{_ws[1]} age={max(0.0, _now - _ws[2]):.0f}s'}")
            # ⛔ Sized to the RESIDUAL, never to `--size`, and never rounded UP — that would turn
            # a flatten into a fresh position in the opposite direction. Sub-contract residue is
            # closed FRACTIONALLY on both signs, never skipped: dust no teardown can place makes
            # the next launch's recovery gate refuse (`own_position_off_slate`).
            # ⛔ NOT `side="sell"` for a long — that is `ORDER_INTENT_BUY_SHORT`, a resting ask
            # that OPENS a short beside the long we are clearing.
            # ⛔ The minimum is THIS BOOK'S OWN `minimumTradeQty`, never a global constant.
            amount: Decimal | int = int(qty.copy_abs())
            if amount <= 0:
                dust = qty.copy_abs()
                # FETCH-ON-MISS, not a guess: the teardown is once per run and this is the
                # highest-stakes reader of the number.
                min_qty = await self._ensure_min_trade_qty(slug)
                if not self.fractional_close and qty > _ZERO:
                    log.warning(f"flatten: {slug} residual {qty} is a sub-contract LONG and "
                                f"lane.fractional_close is "
                                f"{self.fractional_close_mode!r} — reported instead.")
                    continue
                if dust < min_qty:
                    src = ("venue metadata" if slug in self.min_qty
                           else f"FALLBACK {MIN_TRADE_QTY}; the venue field was unreadable")
                    log.warning(f"flatten: {slug} residual {qty} is under THIS BOOK's minimum "
                                f"trade qty {min_qty} ({src}) — unplaceable; reported instead.")
                    continue
                amount = dust
                log.warning(f"flatten: {slug} residual {qty} is under one contract — placing a "
                            f"FRACTIONAL passive {side} close for {amount} "
                            f"(intent {'SELL_LONG' if qty > _ZERO else 'BUY_LONG'}).")
            # ⛔ THE FLATTEN'S OWN DEADLINE OUTLIVES THE WAIT IT IS GIVEN — see `_flatten_ttl_s`.
            # A flatten that expired inside its own wait would strand the position it was clearing
            # behind an order that quietly stopped existing. It carries a deadline rather than
            # plain GTC EVEN ON A LANE WITH NO QUOTE TTL: the cases this exists for are the process
            # dying mid-teardown and a venue ban that refuses our own cancels.
            # ⛔ A FAILED DUST CLOSE MUST NOT HANG THE TEARDOWN — log loudly, leave the residue to
            # be REPORTED below, and keep tearing down. The stray sweep still runs LAST.
            try:
                placed = await self._place(slug, side, price, improved=False, size=amount,
                                           # Tape-only; the REST body did not price a WS order.
                                           queue_ahead=(None if _src == "ws" else qty_at_price(
                                               book.get("marketData"), side, price)),
                                           ttl_s=self._flatten_ttl_s(),
                                           # ⛔ ONLY the sub-contract long close switches intent.
                                           # A WHOLE-contract flatten ask keeps `BUY_SHORT`.
                                           close_long=(qty > _ZERO
                                                       and isinstance(amount, Decimal)),
                                           # ⛔ ONLY when the WS book PRICED this order: the
                                           # guard's REST read is the same frozen origin, so
                                           # without the witness it refuses our own WS touch as a
                                           # cross. Priced off REST → no witness, as before.
                                           ws_witness=(_src == "ws"))
            except Exception as exc:
                log.error(f"⚠️ flatten: {slug} close for {amount} RAISED — {safe_exc(exc)}. "
                          f"Residual {qty} stays; continuing teardown.")
                continue
            if placed is not None:
                flatten_orders[slug] = side
            if placed is None and isinstance(amount, Decimal):
                log.error(f"⚠️ flatten: {slug} FRACTIONAL close for {amount} was not placed — the "
                          f"venue refused or the response was unusable. Dust residual {qty} "
                          f"remains; continuing teardown. Clear it by hand — see the operator runbook.")
        if self.flatten_wait_s > 0:
            await asyncio.sleep(self.flatten_wait_s)
            await self.poll_fills(force_full=True)
        crossed = await self._flatten_cross_residual(flatten_orders)
        # `left` excludes the unsafe slugs — they were `continue`d, so they are still in
        # `self.inventory` and were being re-listed under "Other residual", which an operator
        # could read as twice the position.
        left = {s: str(q) for s, q in self.inventory.items()
                if q != _ZERO and s not in unsafe}
        # ONE line per book: passive fill qty, cross qty, price, residual. Empty off the probe
        # lane and whenever nothing was placed — the string an operator reads is unchanged there.
        cross_line = ("" if not crossed
                      else " Teardown cross: " + " | ".join(crossed) + ".")
        if unsafe:
            self.flatten_outcome = "refused"
            return (f"⛔ NOT FLATTENED — the venue does not corroborate belief on {unsafe}. "
                    f"Trading on a position we cannot confirm is how a flatten GROWS it. "
                    f"Reconcile by hand (scripts.poly_cancel for orders; close positions "
                    f"by hand — see the operator runbook)."
                    + (f" Also holding: {left}." if left else "") + cross_line)
        if not left:
            self.flatten_outcome = "filled"
            return ("flat (LOCAL BELIEF — the verify line below is the certification)"
                    + cross_line)
        self.flatten_outcome = "resting_swept"
        return (f"⚠️ RESIDUAL INVENTORY, reported not force-sold: {left}. Flatten by hand with "
                f"the operator runbook; cancel strays with `scripts.poly_cancel` (the Kalshi recovery tool "
                f"is Kalshi-only and reads the wrong venue)." + cross_line)

    async def _flatten_cross_residual(self, flatten_orders: dict[str, str]) -> list[str]:
        """PROBE LANE ONLY. After the passive wait: retire THIS PHASE'S OWN flatten orders, then
        cross what the venue still corroborates. Returns one operator line per book.

        ⛔ THE ORDER OF OPERATIONS IS THE SAFETY PROPERTY: cancel → confirm the order's FINAL
        `cumQuantity` (the authority) → FRESH positions re-read → only then size. Sizing off
        belief while this phase's own flatten order still rests sells a lot that already filled.

        ⛔ EVERY UNCERTAINTY IS A NO-CROSS — report the residual and let the recorded-carry path
        carry it; crossing on a number we cannot corroborate opens a position the other way.

        ⛔ SIZE = min(|belief|, |venue|), SIGNS AGREEING, `int()` truncating DOWN (a rounded-up
        11.60 would go out as 12 and OPEN 0.40 the other way). ONE attempt, never a retry on an
        exception: `sell_back` RAISES to say "we do not know whether it filled".

        ⛔ NOT IN DRY/shadow: `client.sell_back` returns a `(0.50, size)` STUB under `_dry_run`.
        """
        # ⛔ `not self.real` is REDUNDANT-BY-DESIGN, kept deliberately [review r2 CONCERN 3]:
        # `_teardown_flatten`'s own venue read is `self.real`-gated, so a DRY run places no
        # passive order and `flatten_orders` reaches here empty. Removing this term is
        # therefore GREEN against the DRY pin — it is the second lock, not the first.
        #
        # ⛔ OPT-IN, DEFAULT OFF [operator decision]. The cross fired on a live run at cycle 1 and paid
        # the spread to close a residual the next cycle would have carried at the replayed basis. With
        # `teardown_cross` false this returns [] before any cancel or venue read, so the
        # teardown is byte-identical to the pre-cef8fd39 passive-only flatten: residual
        # reported, no `Teardown cross` clause, carried by the next cycle.
        if not self.teardown_cross:
            return []
        if self.lane != probe_notify.PROBE_LANE or not self.real or self.shadow:
            return []
        notes: list[str] = []
        eligible: dict[str, str] = {}     # slug → what the PASSIVE order filled, as printed
        for slug, side in flatten_orders.items():
            order = self.resting.get((slug, side))
            confirmed = await self._cancel(slug, side)
            # The FINAL read: the order is terminal now, so its `cumQuantity` is the last word on
            # what the passive leg sold. `_book_fill` dedupes on `order.filled_qty`, so this
            # re-read cannot double-count the increment `_cancel` already booked.
            #
            # ⛔ `order is None` IS NOT "UNREADABLE" [review r2 NIT 5]. Every path that pops a
            # resting order — the terminal-and-missing poll pop, the not-found retirement, a
            # confirmed cancel — either BOOKS its final state or PARKS a delayed verify for it,
            # and none of them leaves it live on the venue. Refusing there refused exactly the
            # fully-filled passive order, i.e. the case that needs no cross at all. What it
            # does NOT give us is a measured fill quantity, so the line says so rather than
            # asserting one; belief is bounded either way by min(|belief|, |venue|) below.
            final_read = bool(confirmed and (order is None
                                             or await self._reconcile_order(order)))
            # The ORDER's own cumulative fill, not an inventory delta: `poll_fills` may have
            # booked part of it during the wait, and a delta measured here would report only
            # what the final read added.
            filled = (f"{order.filled_qty}" if order is not None
                      else "unmeasured (order retired by the poll path)")
            if not confirmed:
                notes.append(f"{slug}: passive flatten order NOT confirmed cancelled — no cross")
                continue
            if not final_read:
                notes.append(f"{slug}: passive flatten order's final cumQuantity UNREADABLE — "
                             f"no cross")
                continue
            # ⛔ A CARRIED BASIS IS NOT OURS TO REALIZE AGAINST [review r2 CONCERN 4]. On a book
            # seeded at launch, `avg_entry` prices a position this run never opened, so booking
            # a cross against it writes a fabricated realized P&L into the durable cap. Skip:
            # the residual is reported and stays carryable, which is the fail-closed direction
            # and the same answer `poly_settle_book --auto` gives a `reset` row.
            if slug in self.basis_carried:
                notes.append(f"{slug}: passive fill {filled}, residual "
                             f"{self.inventory.get(slug, _ZERO)} — no cross (basis CARRIED, not "
                             f"this run's; realizing against it would fabricate P&L)")
                continue
            eligible[slug] = filled
        if not eligible:
            return notes
        # ⛔ FRESH, HERE, AFTER THE CANCELS — never the pre-wait read. The wait is the whole
        # window in which the passive order filled.
        if not await self._refresh_venue_inventory():
            for slug, filled in eligible.items():
                notes.append(f"{slug}: passive fill {filled}, residual "
                             f"{self.inventory.get(slug, _ZERO)} — no cross (venue positions "
                             f"re-read FAILED after the wait)")
            return notes
        for slug, filled in eligible.items():
            belief = self.inventory.get(slug, _ZERO)
            head = f"{slug}: passive fill {filled}"
            if belief == _ZERO:
                notes.append(f"{head}, residual 0 — no cross needed")
                continue
            venue_qty = self.venue_inventory.get(slug)
            if slug in self.venue_stale_rows:
                notes.append(f"{head}, residual {belief} — no cross (venue row unparseable)")
                continue
            if venue_qty is None or venue_qty == _ZERO:
                notes.append(f"{head}, residual {belief} — no cross (venue "
                             f"{'says flat' if venue_qty == _ZERO else 'has no row'})")
                continue
            if (belief > _ZERO) != (venue_qty > _ZERO):
                notes.append(f"{head}, residual {belief} — no cross (fresh venue {venue_qty} "
                             f"disagrees in sign)")
                continue
            want = int(min(belief.copy_abs(), venue_qty.copy_abs()))
            if want < 1:
                notes.append(f"{head}, residual {belief} (venue {venue_qty}) — no cross, under "
                             f"one contract: sell_back has no sub-contract path")
                continue
            # A long is disposed of as the bare slug (SELL_LONG at the bid); a short as
            # `slug::short` (SELL_SHORT buying the yes side back at the yes ASK). The translation
            # is `poly_close.py --cross`'s, not composed by hand — a mistranslated short leg
            # cannot fill AT ALL (the price-space incident).
            token = slug if belief > _ZERO else f"{slug}::short"
            try:
                vwap, sold = await self.client.sell_back(
                    token, want, label=f"{self.lane} teardown flatten cross")
            except Exception as exc:
                log.error(f"⚠️ flatten cross: {slug} {want} OUTCOME UNKNOWN — {safe_exc(exc)}; "
                          f"read the venue, do NOT re-run")
                notes.append(f"{head}, residual {belief} — cross OUTCOME UNKNOWN "
                             f"({safe_exc(exc)}): it MAY have filled, not re-run")
                continue
            if vwap is None or sold <= 0:
                notes.append(f"{head}, residual {belief} — cross sold nothing of {want}")
                log.error(f"⚠️ flatten cross: {slug} sold nothing of {want}")
                continue
            sold_d = Decimal(str(sold))
            # ⛔ THE CROSS MUST BE BOOKED: `recorded_carries` needs record == venue by EXACT
            # Decimal equality, and this IOC's id is in no pending map, so nothing else books it.
            # `vwap` is the qty-weighted mean of the LIMITS transacted, so the realized is
            # conservative in both directions.
            px = Decimal(str(vwap))
            # ⛔ NO SYNTHETIC ORDER ID. `client.sell_back` returns only (vwap, sold), and a made-up
            # id would be read by `scripts/poly_order_diff.py` as a PHANTOM booking on every cross
            # (`tape_booked` keys on `order_id`, SKIPS blank ones, and `phantom_bookings` accuses
            # any id the venue does not know). Blank is the honest value.
            crossed_order = RestingOrder(
                slug=slug, side=("ask" if belief > _ZERO else "bid"), price=px, size=want,
                order_id=None,
                intent_id="", placed_ts=time.time(), queue_ahead=None, improved=False)
            self._book_fill(
                crossed_order,
                {"cumQuantity": str(sold_d), "state": "ORDER_STATE_FILLED", "avgPx": str(px)},
                booked_via="cross")
            # ⛔ NO `_write_fill` HERE. `_book_fill` TAPES THE ROW ITSELF (one call, at the end of
            # its booking) — an extra write put every cross on the tape TWICE. Inventory,
            # `avg_entry` and the durable `add_realized` were each booked ONCE, but a doubled row
            # double-counts every per-fill number a reader builds off the tape.
            if self.state is not None and self.real:
                self.state.set_inventory(self.inventory)
            # The residual is BELIEF AFTER THE BOOKING — the true remainder, and the number the
            # next cycle's recorded carry has to match.
            notes.append(f"{head}, crossed {sold_d} of {want} @ vwap {vwap}, residual "
                         f"{self.inventory.get(slug, _ZERO)}")
            log.warning(f"flatten cross: {slug} crossed {sold_d} of {want} @ vwap {vwap}")
        return notes

    async def _teardown_sweep(self) -> str:
        """LAST. Cancel anything still resting ON THE MARKETS THIS RUN QUOTES, including orders we
        never recorded.

        ⛔ SCOPED TO OUR OWN SLUGS — a safety property, not tidiness: `get_open_orders` is
        ACCOUNT-WIDE, so an unscoped sweep would cancel another process's resting orders at exit.
        An order with no readable slug is REPORTED, never cancelled: we cannot establish it is ours.
        """
        if self.shadow:
            return "shadow: nothing to sweep"
        self._swept_clean = False
        self._sweep_refused = None      # cannot-verify until the listing actually answers
        mine = set(self.ticks) | set(self.slugs)
        # ⛔ DELIBERATELY ACCOUNT-WIDE — the poll is scoped, this is NOT, and the asymmetry is the
        # point: `swept_clean` certifies on listing-ABSENCE, and this venue is already documented
        # to accept-and-silently-ignore a filter param. A mishandled `slugs` filter returning []
        # here would close the durable crash record over live resting orders — a certified false
        # clean. The client-side `mine` filter below scopes what we CANCEL.
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
        # The venue's own evidence that nothing of ours can still be resting ON THIS RUN'S MARKETS:
        # the listing succeeded (a raise fails this phase), every order on our markets cancelled
        # cleanly, and nothing was unattributable — an order we cannot attribute might be ours, so
        # the record may not close over it. Foreign orders do not block. Held in a LOCAL until the
        # phase completes, so a raise anywhere in this method leaves `self._swept_clean` False.
        swept_clean = (refused == 0 and unattributable == 0)
        # The refused count outlives the phase: the teardown's remediation text branches on it —
        # the terminal-but-listed ("ghost") diagnosis is only honest when the venue actually
        # refused a cancel. It stays None (cannot-verify) if the listing raised.
        self._sweep_refused = refused
        # The residual reported by `flatten` was computed BEFORE this sweep cancelled the flatten
        # order, so it can be stale high. Re-read here, last, and report the reconciled number —
        # an operator hand-flattening a phantom residual opens a position in the other direction.
        await self.poll_fills(force_full=True)
        left = {s: str(q) for s, q in self.inventory.items() if q != _ZERO}
        note = f"swept {swept} resting order(s) on this run's {len(mine)} market(s)"
        # "LOCAL BELIEF" because it is one — the venue-certified statement is the shim's verify
        # line, and printing an unlabeled "FLAT" three lines above it invited that misreading.
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
            # The tag is PER TAPE and names that tape's LAST schema change (a mismatched old file
            # freezes to rotated/<stem>.pre-<tag>.csv) — a shared tag would stamp a future
            # unrelated drift with a misleading name.
            self._writers[path] = _open_writer(path, header, rotate_tag=rotate_tag)
        return self._writers[path]

    def _capture_guard_book(self, slug: str) -> None:
        """Carry the client crossing guard's own book onto this cycle's quote row.

        ⛔ INSTRUMENTATION ONLY. The guard's decision, its book read and its refusal are untouched;
        this copies what it already saw so a reader can measure a refusal episode off the tape
        instead of parsing WARNING strings. `getattr` because a stub client has no such attribute.

        ⛔ AND IT NEVER RAISES. One call site sits inside `_place`'s `try:` — a raise here would be
        caught as a CREATE FAILURE, arming the orphan rail and returning None over an order that
        really rested — and the other sits inside that same block's `except`, where a raise would
        replace the venue error the caller has to see.
        and the row's guard cells stay blank."""
        try:
            seen = getattr(self.client, "last_guard_book", {}).get(slug)
            if seen is not None:
                self._guard_seen[slug] = dict(seen)
        except Exception as exc:
            log.debug(f"_capture_guard_book({slug}) skipped: {safe_exc(exc)}")

    def _tape_map(self, name: str) -> dict:
        """One of the four I1/I2 tape-only maps, created on demand.

        ⛔ IT EXISTS BECAUSE A MAKER IS NOT ALWAYS BUILT BY `__init__`. `tests/test_park_seat.py`
        drives the real quote cycle on a `PolyMaker.__new__` + `__dict__.update` instance, so
        `_reach_permits` and `_cancel` — both money-path — would raise `AttributeError` on a
        map that only `__init__` seeds. An instrumentation write may never be able to raise into
        a rail. `__init__` still seeds all four; this is the floor, not the seeding."""
        m = getattr(self, name, None)
        if m is None:
            m = {}
            setattr(self, name, m)
        return m

    def _write_quote(self, stats: CycleStats, slug: str, tick: Decimal,
                     touch: Optional[tuple[Decimal, Decimal]],
                     quotes: Optional[tuple[Decimal, Decimal]], *,
                     improved: bool = False, actions: Optional[dict] = None,
                     reasons: Optional[dict] = None,
                     queue: Optional[dict] = None, inventory: Decimal = _ZERO,
                     allow: tuple[bool, bool] = (False, False),
                     status: str = "ok", read_ms: Optional[float] = None,
                     gap_s: Optional[float] = None,
                     book_stats: Optional[dict] = None,
                     net: tuple[Optional[Decimal], Optional[Decimal]] = (None, None),
                     sizes: tuple[Optional[int], Optional[int]] = (None, None),
                     rebate_step: Optional[str] = None,
                     hold_cause: str = "",
                     ) -> None:
        """EVERY market gets a row every cycle, quotable or not.

        ⛔ WRITER BOUND the readers lean on: rows are APPENDED in cycle order, so `ts` is
        non-decreasing up to one cycle's jitter. `bot.core.presence.iter_quote_rows` byte-seeks
        on `ts` and stops `QUOTE_TS_SLACK_S` (600 s) past a window's end; a writer that disorders
        rows by more than that would silently drop them from every windowed read.

        A silent GAP is unrecoverable once the tape is written, and the dominant gap cause — a
        failed book read — correlates with venue stress, i.e. with the busiest moments.

        `gap_s` is this market's REAL sampling interval — the wait since its last successful read,
        blank on the first cycle. The cycle tape says the loop kept up on average; this says
        whether it kept up on THIS market.

        `sizes` is the (bid, ask) size this cycle INTENDS to rest, `None` on a side not being
        quoted. The believed-resting size is read here from `self.resting` rather than passed in,
        so the SKIP path records it too — a market that could not be quoted still has orders on
        the venue, and that is exactly when knowing their size matters.
        """
        actions = actions or {}
        # A row with no `reasons` is a SKIP row (`_skip_market`), where nothing was acted on —
        # `hold` on both sides, never blank.
        reasons = reasons or {}
        queue = queue or {}
        # ⚠️ `rotate_tag` names this tape's LAST schema change; an old-header file freezes to
        # rotated/poly_live_mm_quotes.pre-<tag>.csv on the next start. A stale tag would file two
        # different schema boundaries under one archive name.
        _handle, writer = self._writer(self._quote_path, _QUOTE_HDR, rotate_tag="epoch")
        _resting = tuple(self.resting.get((slug, side)) for side in ("bid", "ask"))
        # ⛔ POPPED ONCE, read twice: the `guard_*` and `guard_ws_*` cells are two halves of ONE
        # guard read, separated in the header only by the schema's append-only rule.
        _seen_guard = self._guard_seen.pop(slug, None)
        # Same popped-once rule, for the two numeric I2 cells (`None` is not "" and not 0).
        _ack_lag = self._tape_map("_ack_lag_ms").pop(slug, None)
        _unres = self._tape_map("_unresolved_seen").pop(slug, None)
        writer.writerow([
            f"{time.time():.3f}", stats.cycle, slug, str(tick),
            # Per side, for the same reason as `quotes` below: a park book's touch is
            # legitimately one-sided, and blank means "not readable", never the string "None".
            str(touch[0]) if touch and touch[0] is not None else "",
            str(touch[1]) if touch and touch[1] is not None else "",
            # ⛔ PER SIDE, not per tuple [park lane]: a park book quotes ONE side, so the other
            # element is legitimately None and `str(None)` would tape the literal "None".
            str(quotes[0]) if quotes and quotes[0] is not None else "",
            str(quotes[1]) if quotes and quotes[1] is not None else "",
            "Y" if improved else "N",
            actions.get("bid", ""), actions.get("ask", ""),
            "" if queue.get("bid") is None else str(queue["bid"]),
            "" if queue.get("ask") is None else str(queue["ask"]),
            str(inventory), "Y" if allow[0] else "N", "Y" if allow[1] else "N",
            status, f"{read_ms:.1f}" if read_ms is not None else "",
            f"{gap_s:.3f}" if gap_s is not None else "",
            # shares_traded is :.0f — ":g" caps at 6 significant digits, so past 1e6 the
            # counter would quantize and Δ-exactness silently dies [r7-C2]. The rest are :g.
            ("" if (book_stats or {}).get("shares_traded") is None
             else f"{(book_stats or {})['shares_traded']:.0f}"),
            *(("" if (book_stats or {}).get(k) is None else f"{(book_stats or {})[k]:g}")
              for k in ("last_trade_px", "last_trade_qty", "last_trade_age_s")),
            "" if net[0] is None else str(net[0]),
            "" if net[1] is None else str(net[1]),
            self.last_book_src.get(slug, self.book_source),   # book_src [WS-maker book feed]
            # Intended: blank never zero [G4].
            *("" if s is None else str(s) for s in sizes),
            # Believed-resting: the belief REMAINDER `size − filled_qty` — NOT `RestingOrder.size`,
            # which is the size at PLACEMENT and overstates our presence on exactly the cycles
            # where an order has just partially filled. Here 0 IS a value (a tracked order fully
            # filled and not yet reaped); blank means no order is tracked at all.
            *("" if r is None else str(Decimal(r.size) - r.filled_qty) for r in _resting),
            reasons.get("bid", "hold"), reasons.get("ask", "hold"),
            # ⛔ READ OFF THE LATCH MAP, NOT OFF `status`. This is the only column on either tape
            # that reports latched state independently of the precedence chain, and it is written
            # on the SKIP path too, which is precisely where the chain is never evaluated.
            "Y" if slug in self.adverse_latched else "N",
            # ⛔ POPPED, not read: the guard's book belongs to the cycle whose placement produced
            # it. Leaving it in the map would re-tape a stale read on every later hold row —
            # precisely the frozen-value artefact this column exists to detect.
            *_guard_cols(_seen_guard),
            # Blank when the rule is off or the row judged no side — never "ok", which is a
            # POSITIVE statement that the rule ran and found every adding side already paid.
            rebate_step or "",
            *_guard_ws_cols(_seen_guard),
            # open_interest is :.0f for the same reason shares_traded is — ":g" caps at 6
            # significant digits, so a large level would quantize. Blank never 0 [absent ≠ 0].
            ("" if (book_stats or {}).get("open_interest") is None
             else f"{(book_stats or {})['open_interest']:.0f}"),
            ("" if (book_stats or {}).get("oi_age_s") is None
             else f"{(book_stats or {})['oi_age_s']:g}"),
            # A RUN constant, so it is written on the SKIP path too — a reader forming the
            # A/B's clusters must not have to guess the mode of a cycle that did not quote.
            self.quote_mode,
            # Blank on every row that is not an `ok` cycle with BOTH sides refused — including
            # the SKIP path, which never evaluates the gates at all.
            hold_cause,
            # ── I1/I2. Read/POPPED here, decided nowhere [profit sweep § 7].
            # ⛔ `self.inventory`, NOT the `inventory` argument: this is the belief
            # `_reach_permits` reads, at the write instant. Always a value — the belief exists
            # on every book — so this column is never blank.
            str(self.inventory.get(slug, _ZERO)),
            self._tape_map("_inv_src").get(slug, "carry"),
            # Popped, exactly like `_guard_seen`: an event belongs to the row that follows it,
            # once. Blank = it did not happen since the last row, never 0.
            "" if _ack_lag is None else f"{_ack_lag:.1f}",
            self._tape_map("_cancel_readback").pop(slug, ""),
            "" if _unres is None else str(_unres),
            self.slate_epoch,
            self.run_id,
        ])
        _handle.flush()

    def _write_cycle(self, stats: CycleStats) -> None:
        # ⚠️ `rotate_tag` names this tape's LAST schema change; an old-header file freezes to
        # rotated/poly_live_mm_cycles.pre-<tag>.csv on the next start.
        if self._ws_down_since is not None:
            stats.ws_down_s = max(0.0, time.time() - max(self._ws_down_since, stats.started_ts))
        _handle, writer = self._writer(self._cycle_path, _CYCLE_HDR, rotate_tag="epoch")
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
            stats.ws_reverifies, stats.ws_reverify_stale_cache,
            f"{stats.ws_down_s:.1f}" if stats.ws_down_s else "0",
            stats.halt_reason or "",
            # Derived here rather than carried on CycleStats, so the halt/teardown paths that
            # write a cycle row without running a full quote pass report it too.
            sum(1 for _s in self.slugs if _s in self.adverse_latched),
            self.orphans_seen, self.orphans_cancelled, self.orphans_reported,
            self.slate_epoch, f"{self.requote_s:g}",
            self.run_id,
        ])
        _handle.flush()

    def _write_fill(self, fill: Fill) -> None:
        # ⚠️ `rotate_tag` must name the tape's LAST schema change: a stale tag files two schema
        # boundaries under one archive name (rotation history: data_dictionary.md).
        _handle, writer = self._writer(self._fill_path, _FILL_HDR, rotate_tag="epoch")
        writer.writerow([
            f"{fill.ts:.3f}", fill.slug, fill.side, fill.order_id or "", str(fill.price),
            fill.size, str(fill.filled_qty), str(fill.cum_filled_qty),
            "" if fill.commission_order_total is None else str(fill.commission_order_total),
            fill.verdict,
            # Predicted on the CUMULATIVE quantity, so it is comparable with the order-total
            # commission beside it. ⛔ And predicted at the VENUE's fill price when readable, not
            # at our limit — the venue computes the rebate at ITS price, and on a BUY_SHORT the
            # two are documented to diverge. Falling back to our limit is marked by the avg_px
            # column being empty on the same row.
            # ⛔ A TAKER FILL EARNS NO REBATE. The teardown cross is an IOC that crosses the
            # spread: it PAYS commission and the maker credit does not apply, so the prediction is
            # a hard 0 rather than the passive formula.
            # ⚠️ GAP: `client.sell_back` returns only (vwap, sold), so the cross's actual taker
            # commission is not available here and `commission_order_total` stays BLANK.
            "0" if fill.booked_via == "cross" else
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
            "" if fill.net_bid_place is None else str(fill.net_bid_place),
            "" if fill.net_ask_place is None else str(fill.net_ask_place),
            "" if fill.booking_bid is None else str(fill.booking_bid),
            "" if fill.booking_ask is None else str(fill.booking_ask),
            "" if fill.booking_touch_age_s is None else f"{fill.booking_touch_age_s:.3f}",
            "" if fill.fill_stamp_lag_s is None else f"{fill.fill_stamp_lag_s:.3f}",
            # Full sub-ms precision deliberately — the 3-dp lag column puts a ~1 ms floor
            # under any reconstructed venue instant; this column IS the venue instant.
            "" if fill.venue_ts is None else f"{fill.venue_ts:.6f}",
            "" if fill.booking_net_bid is None else str(fill.booking_net_bid),
            "" if fill.booking_net_ask is None else str(fill.booking_net_ask),
            "" if fill.latch_hs is None else str(fill.latch_hs),
            fill.latch_hs_src,
            "" if fill.latch_ratio is None else str(fill.latch_ratio),
            "" if fill.latch_ban_s is None else f"{fill.latch_ban_s:.0f}",
            "" if fill.latch_fallback is None else ("Y" if fill.latch_fallback else "N"),
            fill.latch_rule,
            "1" if fill.feed_subscribed else "0",
            # 3 dp, matching `time_to_fill_s` beside them — same unit, same precision, so the
            # three clocks can be differenced without a rounding artefact. Blank is CANNOT-DATE.
            "" if fill.order_rest_s is None else f"{fill.order_rest_s:.3f}",
            "" if fill.price_rest_s is None else f"{fill.price_rest_s:.3f}",
            self.slate_epoch,
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

    def _beat(self, stats: CycleStats, *, slate_applied: bool = False) -> None:
        """The operator's three questions, every cycle. Never raises — the deadman is
        instrumentation, and instrumentation must not be able to stop trading."""
        if self.heartbeat is None:
            return
        try:
            # A slate receipt is not a completed quote cycle. Preserve the last actual cycle
            # counters when available; inventory, sizes and guards below are the current state.
            previous = getattr(self.heartbeat, "_last_fields", {}) if slate_applied else {}
            self.heartbeat.beat(
                # ⛔ WHICH RUN IS BEATING [I-OPS-1]. opswatch suppresses the resting-order alarm
                # for a run that is still alive, and without this field it could only identify a
                # run by PID against a heartbeat FILE that every lane of a mode shares — so a live
                # lane's own quotes paged as strays.
                run_id=self.run_id,
                markets_quoted=previous.get("markets_quoted", stats.markets_quoted),
                markets_total=previous.get("markets_total", stats.markets_total),
                inventory={k: str(v) for k, v in self.inventory.items() if v != _ZERO},
                gross_exposure=str(self.gross_exposure),
                resting=len(self.resting),
                cycle=previous.get("cycle", stats.cycle),
                requests=previous.get("requests", stats.requests),
                missed_cycles=previous.get("missed_cycles", stats.missed),
                paused=previous.get("paused", stats.paused),
                cycle_phase="slate_applied" if slate_applied else "complete",
                replace_blocked=self.replace_blocked,
                # ⛔ THE SEAT, AS IT IS RIGHT NOW. A hot raise changes the sizes AND the
                # reach the external guards cover, and opswatch's only window into a run is this
                # beat: without these two it cannot tell a seat that grew from one that did not.
                sizes=dict(self.sizes),
                guard_thresholds=dict(self._guard_thresholds),
                # cum_regressions rides too [r3]: "a fill read lied" is the more alarming of
                # the two counters and must not die with a SIGKILLed process.
                cum_regressions=self.cum_regressions,
                # Through-zero re-basings on RECOVERED fills: the loss cap's known interim
                # window — a fictional trip writes the DURABLE ratchet.
                recovered_through_zero=self.recovered_through_zero,
                mode="shadow" if self.shadow else ("real" if self.real else "dry"),
                # ⛔ THE hot_settings ACK — a writer's own change tape cannot answer
                # applied-vs-refused (a voided file and an already-current file both write no
                # receipt row), so without this the only source is the maker's LOG.
                **self._hot_ack,
                # ⛔ THE hot_slate ACK [continuous maker P1]. A reseater WAITS on `slate_hash`
                # before it journals the reseat: without it a voided file and an applied one look
                # identical from outside, and the next diff would be computed against a slate the
                # maker never adopted. `slate_epoch` is what the tapes are keyed on.
                **self._slate_ack,
            )
        except Exception as exc:                      # defensive
            log.error(f"heartbeat failed ({exc!r}) — this process is INVISIBLE to the deadman")
