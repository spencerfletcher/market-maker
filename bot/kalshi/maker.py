"""
bot/kalshi/maker.py — first-generation Kalshi maker: post_only quotes, REAL orders on prod.
⛔ DORMANT since 2026-07-26, superseded by bot/poly_us/maker.py. Do not run it.
It placed real orders: post_only never crosses, config.DRY_RUN alone gates placement, the
loss cap dies with the process, and a positions read failure is CANNOT-VERIFY (halt), not flat.
Story, run history and fill counts: the private design notes §1, the private design notes.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import datetime as _dt
import json
import math
import os
import signal
import sys
import time
from collections import defaultdict
from decimal import Decimal, InvalidOperation

# Real-money enablement happens in the CLI shim: config snapshots the environment at import time.
# Rate-limit budget: /portfolio/* list reads cost 10 units each, cancel 2, create 10.
_BOOK_WARMUP_S = 3.0      # per-attempt wait for the WS book to populate before we refuse to start
_BOOK_WARMUP_TRIES = 5    # → 15s total; a live snapshot lands in ~1s, so this is generous
_POS_RETRIES = 4          # transient-429 attempts before the fail-closed halt
_POS_BACKOFF_S = 1.5      # exponential: 1.5s, 3s, 6s
_FILLS_EVERY = 1          # poll get_fills EVERY cycle.
# ⚠️ Must stay 1. Markout horizons are anchored to the venue fill ts (`_fill_ts`), but discovery
# lag still bounds how early a horizon can be valued, so thinning this poll destroys the 5s number.

# ⚠️ The `--i-understand-real-money` sys.argv sniff lives in scripts/kalshi_live_mm.py, not here:
# it must run before `bot.core.config` is imported, and a library must never read argv.
# `config.DRY_RUN` alone decides whether money moves; a flag/DRY_RUN mismatch is a refusal in main().
# NB: KALSHI_ENV is deliberately NOT set here — it comes from the deployed .env (prod).

from bot.core import config, heartbeat, maker_state, memguard
from bot.core.money import ONE as _ONE, ZERO as _ZERO, ceil_to, floor_to, from_float, parse_wire
from bot.core.safety import is_paused
from bot.kalshi.client import KalshiClient
from bot.kalshi.feed import KalshiOrderBookCache
from bot.kalshi.queue_tracker import QueueTracker
from bot.kalshi.trade_feed import KalshiTradeFeed
from bot.kalshi.scanner import KalshiScanner


def _live_guard(client: KalshiClient) -> None:
    if config.KALSHI_ENV != "prod":
        raise SystemExit(f"REFUSING: KALSHI_ENV={config.KALSHI_ENV!r}, expected 'prod' for the live MM.")
    if "demo" in client._base_url:
        raise SystemExit(f"REFUSING: base_url {client._base_url!r} is the demo host, not prod.")


def fill_counts(seen_fills: set, flatten_fill_ids: set) -> tuple[int, int]:
    """(maker fills, flatten fills), counted in FILL RECORDS — not orders.

    The flatten's fills are excluded whenever we captured its `order_id` (`flatten_oids`, recorded
    at placement); a flatten order whose create-response was LOST is booked as maker flow. The
    venue partial-fills below one contract, so one filled order can produce several fill records.
    """
    flatten = len(flatten_fill_ids)
    return len(seen_fills) - flatten, flatten


def summary_note(stats: dict, seen_fills: set, flatten_fill_ids: set) -> str:
    """The `end` row's note PREFIX — the quotes/rejects/fills segment as written to the ledger.

    ⚠️ NOT the exact string: `main()` may append UNMARKABLE=/UNBOOKED=/VANISHED=/VENUE_READ=FAILED
    diagnostics before the row is written, so assert containment, not equality."""
    maker, flat = fill_counts(seen_fills, flatten_fill_ids)
    return (f"quotes={stats['placed']} rejects={stats['rejected']} fills={maker}"
            + (f" flatten_fills={flat}" if flat else ""))


def summary_line(stats: dict, seen_fills: set, flatten_fill_ids: set) -> str:
    """The operator-facing closing line — pinned separately from `summary_note` so a mutant cannot
    print an inflated count on one of the two sites while the other stays correct."""
    maker, flat = fill_counts(seen_fills, flatten_fill_ids)
    return (f"  quotes placed={stats['placed']}  post_only-cross rejects={stats['rejected']}  "
            f"real fills={maker}"
            + (f"  (+{flat} flatten fills, NOT maker edge)" if flat else ""))


def _should_flatten(real: bool, flatten_on_exit: bool, halted: bool) -> bool:
    """Whether the teardown posts its passive reducing order. **`halted` is deliberately ignored.**

    The process is EXITING and the loss cap dies with it, so declining to flatten on a halt leaves
    an adverse position unattended with no cap. The flatten is passive, post-only, reduces OUR net
    (`venue − base`) so it cannot cross, and self-refuses on an unreadable venue read."""
    return real and flatten_on_exit


def _install_sigterm_handler() -> None:
    """Make SIGTERM **and SIGHUP** raise KeyboardInterrupt so a plain `kill`, or a dropped terminal,
    unwinds the teardown `finally` exactly as Ctrl-C (SIGINT) does (the private design notes M17). Without it the
    default disposition terminates WITHOUT unwinding, so the cancel-all + venue stray-sweep never runs
    and real orders are left resting with the loss cap dead. ⚠️ THIS OVERRIDES `nohup`, so a
    bare-`nohup` maker now tears down on a dropped terminal; `setsid nohup` is unaffected. Installed at
    the process entry point (the shim), NOT in main(), and on the main thread. SIGKILL (`kill -9`) is
    uncatchable and always strands: the one forbidden signal."""
    def _raise_keyboard_interrupt(signum: int, frame: object) -> None:
        # ⛔ DISARM BEFORE RAISING — a SECOND signal during teardown kills the venue sweep. The teardown
        # AWAITS (`flatten_on_exit` waits up to --flatten-wait-s), and a second signal in that window
        # tears the loop down so the sweep dies on "no running event loop", leaving the flatten's own
        # resting post-only order live with the loss cap dead. ⚠️ The teardown is then uninterruptible
        # by these three signals and bounded only at ~5 min per stuck REST call (aiohttp's 300s total).
        # Wait it out; never `kill -9`.
        for _s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            signal.signal(_s, signal.SIG_IGN)
        raise KeyboardInterrupt(signal.Signals(signum).name)
    # ⛔ SIGINT IS INSTALLED TOO, not just disarmed. `Runner` raises KeyboardInterrupt out of
    # `run_until_complete`, skipping everything AFTER the sweep — the fills catch-up, `client.close()`,
    # and the OPEN-POSITION warning — so a second Ctrl-C exited silently holding inventory.
    # Measured over all 9 first×second signal pairs: 6/9 teardowns completed without this, 9/9 with it.
    for _sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(_sig, _raise_keyboard_interrupt)


# Kalshi's grid in the tradeable band: every candidate series measures 0.0100 at p=0.50, and the
# `tapered_deci_cent` series are 0.001 only outside [0.10, 0.90], which the contested gate excludes.
# ⚠️ EXACT Decimal: as a float, `0.47 - 0.01` is 0.45999999999999996 and FLOORS to 0.45, giving away
# a tick on the accumulating side. CLAUDE.md § Code style.
_TICK = Decimal("0.01")
# The venue's tradeable band. Decimal so the clamp cannot re-introduce a float into a quote price.
_MIN_PX = Decimal("0.01")
_MAX_PX = Decimal("0.99")
# Absurdity ceiling on a parsed venue POSITION (see `_positions`) — a sanity bound that keeps a
# garbage magnitude on the fail-CLOSED path instead of letting it into cash → pnl → the loss cap.
# Not a business limit; that is `--inv-cap`.
_MAX_ABS_QTY = Decimal("1e12")


def _tick(px: Decimal, side: str) -> str:
    """Snap a quote onto the venue's cent grid, CONSERVATIVELY for the side being quoted: bids FLOOR
    and asks CEIL, so discretization can only ever quote a **better** price than asked for.
    ⚠️ On an on-grid base that turns a sub-tick lean into a FULL-tick move on the accumulating side, so
    the skew is discretised to whole ticks UPSTREAM by `_skew_ticks` and `_tick` only ever sees an
    on-grid price. Bare `round()` here is a money bug: it makes the shipped 0.3–<n> skew a step
    function conceding a full tick at |inv| ≥ 2, against a measured +0.25–<n>/fill edge.
    ⚠️ TAKES A `Decimal`; `floor_to`/`ceil_to` are exact and an IN-BAND float RAISES. Only in-band: the
    `[_MIN_PX, _MAX_PX]` clamp runs FIRST, so an out-of-band float returns the clamp.
    """
    px = min(_MAX_PX, max(_MIN_PX, px))
    snapped = floor_to(px, _TICK) if side == "buy" else ceil_to(px, _TICK)
    return f"{min(_MAX_PX, max(_MIN_PX, snapped)):.4f}"


def _real_money_refusal(dry_run: bool, flagged: bool) -> str | None:
    """Reason to refuse this invocation outright, or None if the mode is coherent.

    ⚠️ `real` DOES NOT GATE ORDER PLACEMENT: the only DRY short-circuit lives inside
    `KalshiClient.create_order`, so **`config.DRY_RUN` alone decides whether money moves** and `real`
    decides only whether the run is INSTRUMENTED. So `DRY_RUN=false` with no `--i-understand-real-money`
    would place REAL quotes under a DRY banner, uninstrumented and unswept — refused, and the arb bot's
    go-live flip is a HARD STOP, never an arming (CLAUDE.md § Posture)."""
    if not dry_run and not flagged:
        return ("config.DRY_RUN is False (from .env or the shell environment) but "
                "--i-understand-real-money was NOT passed. "
                "Orders are gated ONLY by DRY_RUN, so this would place REAL quotes while reporting "
                "a DRY preview, stamping rows `dry-`, logging no fills and skipping the stray "
                "sweep. Pass the flag to trade for real, or set DRY_RUN=true to preview.")
    return None


def _skew_ticks(obi: Decimal, inv: Decimal, obi_coef: float, inv_coef: float) -> int:
    """The inventory/OBI lean as a WHOLE number of ticks, rounded once from the combined intent.

    `obi_coef·obi − inv_coef·inv` (long inventory leans NEGATIVE: both quotes move DOWN), rounded to
    the nearest tick and applied to BOTH quotes so the reservation price recenters symmetrically
    (Avellaneda-Stoikov). Discretising HERE, before `_tick`, stops a sub-tick shift becoming a
    one-sided full-tick step; `round`, not `floor`, so it is symmetric in sign. ⚠️ `obi`/`inv` are
    Decimal; the two COEFFICIENTS are argparse floats and cross the named `from_float` boundary here
    and nowhere else. See the private design notes §4.2."""
    intent = from_float(obi_coef) * obi - from_float(inv_coef) * inv
    return int(round(intent / _TICK))


def _skew_guard(inv_coef: float, inv_cap: float) -> str | None:
    """Reason to refuse an inventory-skew coef that can NEVER move a quote, or None if it can.

    The skew is rounded to whole ticks (`_skew_ticks`), so a coef is inert only when its maximum
    effect — at the inventory cap — rounds to ZERO ticks. A coef that small tells an operator
    inventory is managed while no quote ever moves."""
    if inv_coef <= 0:
        return None                                  # deliberately off
    # Mirrors `_skew_ticks`' rounding at the cap: the guard must not answer differently from the
    # arithmetic it guards. Both coefs are argparse floats and cross the boundary the same way.
    max_ticks = int(round(from_float(inv_coef) * from_float(inv_cap) / _TICK))
    if max_ticks == 0:
        return (f"--inv-coef {inv_coef:g} at --inv-cap {inv_cap:g} skews at most "
                f"{inv_coef * inv_cap * 100:.2f}¢ at the cap, which rounds to 0 ticks — the "
                f"inventory arm never moves a quote. Use 0 to disable it explicitly, or at least "
                f"{float(_TICK) / inv_cap:.4g} to reach one tick at the cap.")
    return None


_RATE_LIMIT_BUDGET_U_PER_S = 8.0


def _rate_limit_units_per_s(markets: int, requote_s: float) -> float:
    """Kalshi rate-limit units per second at this configuration.

    Cost model (venue-reference skill): `/portfolio/*` list reads 10 units, cancel 2, create 10, so a
    cycle is 10 (positions) + 10 (fills) + markets · (2 creates + 2 cancels). The run that 429'd within
    minutes was markets=3 @ 6s ≈ 15.3 u/s. ⚠️ A MEAN-RATE model; Kalshi bills from a token BUCKET."""
    return (20.0 + 24.0 * markets) / max(requote_s, 1e-9)


def _fill_ts(fill: dict) -> tuple[float, str]:
    """(epoch seconds, source) for a fill — the VENUE'S timestamp where possible.

    The source is returned alongside the value so the `now` fallback is visible in the data: a
    markout horizon measured from when we NOTICED the fill is the defect this exists to fix, and an
    analysis must be able to drop `ts_src=detected` rows. Kalshi sends `ts` as epoch seconds here,
    but ISO strings appear elsewhere in the same API, so both are accepted."""
    raw = fill.get("ts") or fill.get("created_time")
    if raw is not None:
        try:
            v = float(raw)
            # Guard against a millisecond epoch, which would put the fill ~55,000 years out and make
            # every markout horizon instantly "due".
            if v > 1e11:
                v /= 1000.0
            if 1e9 < v < 4e9:
                return v, "venue"
        except (TypeError, ValueError):
            pass
        if isinstance(raw, str):
            try:
                import datetime as _d
                return _d.datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp(), "venue"
            except (ValueError, TypeError):
                pass
    return time.time(), "detected"


def _fmt_ahead(v) -> str:
    """`unknown` is not 0. A worse-than-touch price has depth we never observed, and printing it as
    0 would read as 'we were first in an empty queue' — the opposite of the truth.

    Delegates to `_fmt_qty` so one queue number has ONE spelling across log families."""
    return _fmt_qty(v)


def _fmt_qty(v) -> str:
    """Render a `Decimal` contract count for a log row, or `unknown` for None.

    `f"{Decimal('120.0'):g}"` keeps the trailing zero where the float spelling printed `120`, and two
    spellings of one number is how a grep-based analysis quietly splits a population in half.
    Non-integral values — the venue really does partial-fill below one contract — keep their scale."""
    if v is None:
        return "unknown"
    d = Decimal(v)
    return str(d.to_integral_value()) if d == d.to_integral_value() else str(d)


def _queue_ahead(action: str, px: Decimal, touch_b: Decimal, touch_a: Decimal,
                 depth_b: Decimal, depth_a: Decimal) -> tuple[Decimal | None, bool]:
    """(contracts resting AHEAD of us at our price, did-we-improve).

    INSIDE the touch nobody else holds our price so it is 0; AT the touch the whole visible level is
    ahead of us, FIFO; OUTSIDE it we only observe L1, so depth is UNKNOWN — returned as None, never 0,
    since claiming an empty queue would invent priority we do not have (half-tick tolerance, not `==`)."""
    tol = _TICK / 2
    if action == "buy":
        if px > touch_b + tol:
            return _ZERO, True
        if abs(px - touch_b) <= tol:
            return depth_b, False
        return None, False
    if px < touch_a - tol:
        return _ZERO, True
    if abs(px - touch_a) <= tol:
        return depth_a, False
    return None, False


def _own_at(own_resting: dict, action: str, px: Decimal) -> Decimal:
    """Our own size resting at `px` on `action`, matched with `_queue_ahead`'s half-tick tolerance.

    ONE spelling of the price comparison: a miss here fails SILENTLY in the direction of not
    subtracting. The `_ZERO` start keeps the no-match answer an exact Decimal (a bare `sum([])`
    hands back a plain int) since it is subtracted from an exact depth."""
    tol = _TICK / 2
    return sum((v for (a, p), v in own_resting.items() if a == action and abs(p - px) <= tol), _ZERO)


def _mid_from(bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    """Mid, or None when the book cannot price one — and a ONE-SIDED book cannot.

    ⚠️ `bid > 0 and ask > 0` is NOT a sufficient test: `feed._derive` FABRICATES the missing side
    (empty NO ladder → `yes_ask = 1.0`, empty YES ladder → `yes_bid = 0.0` [feed.py:330-331]), both
    finite and positive. This value marks inventory for the LOSS CAP: an empty NO ladder means NOBODY
    IS BIDDING NO, never that YES is worth 1.00 (long 3 @0.23 marked as +<n> that does not exist)."""
    if not bid or not ask:
        return None
    if not (_ZERO < bid <= ask < _ONE):   # crossed, one-sided, or a derived 0.0/1.0 placeholder
        return None
    # Exact: on the cent grid `(0.44 + 0.45) / 2` is 0.445, so the number that marks inventory for
    # the LOSS CAP is the venue's, not a representation of it.
    return (bid + ask) / 2


def _improve(bid: Decimal, ask: Decimal, ticks: int) -> tuple[Decimal, Decimal]:
    """Step `ticks` inside the touch on BOTH sides, or neither if the spread cannot hold it.

    Quoting AT the touch joins the BACK of the resting queue, and the quote loop resets its own time
    priority every cycle. ⚠️ P(fill) at the touch is low, NOT zero: a SWEEP reaches us regardless of
    queue position (~16.8% of trades in the wide lane). ⚠️ `--improve-ticks ≥ 1` sets `queue_ahead` to
    0 BY CONSTRUCTION, so the M23 attribution collects nothing (the private design notes § Queue
    attribution). Both sides or neither (spread ≥ 2·ticks + 1) or the quotes lock."""
    if ticks <= 0:
        return bid, ask
    imp = ticks * _TICK
    if (ask - bid) >= (2 * imp + _TICK):
        return bid + imp, ask - imp
    return bid, ask


async def _positions(client: KalshiClient, targets=None,
                     raw_out: list | None = None) -> dict[str, Decimal] | None:
    """Positions per ticker, or None on ANY read failure. The caller MUST fail-CLOSED on None
    (halt + cancel-all), so a REST outage maps to CANNOT-VERIFY, never to "flat" (which would blind
    the loss cap + inventory cap). An empty dict {} = a confirmed-flat account, DISTINCT from None.
    ⚠️ `targets` SCOPES the fail-closed (the private design notes M15): a single unreadable record halts only when
    its ticker is a TARGET or is unidentifiable, else it is logged and SKIPPED. ⚠️ CALLERS MUST READ
    PER-TARGET (`.get(t)`), never aggregate the raw dict. ⚠️ `raw_out` is a PURE DIAGNOSTIC
    side-channel on the same response, filled AFTER the fail-closed checks."""
    # A 429 is TRANSIENT, not "cannot verify": /portfolio/* list endpoints cost 10 units each, so a
    # busy cycle can trip the bucket. Retry a bounded number of times with backoff, then fail closed.
    pos = None
    for attempt in range(_POS_RETRIES):
        try:
            pos = await client.get_positions()
            break
        except Exception as e:
            transient = "429" in str(e) or "Too Many Requests" in str(e)
            if transient and attempt < _POS_RETRIES - 1:
                wait = _POS_BACKOFF_S * (2 ** attempt)
                print(f"  ⏳ get_positions rate-limited (429) — retry {attempt+1}/{_POS_RETRIES-1} "
                      f"in {wait:.1f}s")
                await asyncio.sleep(wait)
                continue
            print(f"  ⚠️ get_positions FAILED — CANNOT VERIFY: {e!r}")
            return None
    if pos is None:
        return None
    out = {}
    for p in pos:
        t = p.get("ticker")
        # ⚠️ AN UNREADABLE RECORD IS CANNOT-VERIFY, NOT FLAT. `float(p.get("position_fp") or 0.0)`
        # inside `except (TypeError, ValueError): continue` had TWO fail-OPEN paths — a missing/null
        # `position_fp` read as flat, an unparseable one was dropped (also flat, since every consumer
        # treats absence as 0). `_mark_step` then books the disappearance as a TRADE: a held long of 3
        # produced a phantom SELL at the ask, +<n> of cash the account never received.
        if not t:
            print("  ⚠️ position record with no ticker — CANNOT VERIFY, failing closed.")
            return None
        # ⚠️ M15: SCOPE THE FAIL-CLOSED TO TARGETS. A bad record for a non-target market is invisible
        # to the caps, so halting (and, via the baseline read, REFUSING TO START) on it is a false
        # positive: log loudly and skip. The scope rests on EXACT string identity between the venue
        # `ticker` and `targets`, the same match already load-bearing for marking — keep the forms
        # identical end to end.
        target = targets is None or t in targets
        raw_qty = p.get("position_fp")
        # ⚠️ BEHAVIOURALLY REDUNDANT, KEPT FOR THE MESSAGE — an EQUIVALENT mutant, not a coverage
        # gap. With the `or 0.0` gone, `float(None)`/`float("")` raise and the guard below halts on
        # both; this buys an operator "has no position_fp" instead of "unparseable … None".
        if raw_qty is None or raw_qty == "":
            if target:
                print(f"  ⚠️ position record for {t} has no position_fp — CANNOT VERIFY, failing closed.")
                return None
            print(f"  ⚠️ non-target {t}: no position_fp — logged and SKIPPED (not read by the caps).")
            continue
        try:
            # `position_fp` is the venue's fixed-point decimal STRING ("-4.65"); long +, short −.
            # parse_wire, not float(): every money number downstream is built from this quantity and
            # the venue partial-fills below one contract (CLAUDE.md § Code style).
            qty = parse_wire(raw_qty)
        except (TypeError, ValueError, InvalidOperation):
            if target:
                print(f"  ⚠️ unparseable position_fp for {t} ({raw_qty!r}) — CANNOT VERIFY, failing closed.")
                return None
            print(f"  ⚠️ non-target {t}: unparseable position_fp ({raw_qty!r}) — logged and SKIPPED.")
            continue
        # ⚠️ A NON-FINITE position is CANNOT-VERIFY, not a position. `Decimal("NaN")` parses, then
        # poisons cash → pnl: under float `nan < -loss_cap` was FALSE, silently disabling the loss
        # kill-switch; under Decimal the same comparison RAISES inside the quote loop. Infinity trips
        # the cap forever. Route garbage down the same fail-closed path as a failed read.
        if not qty.is_finite():
            if target:
                print(f"  ⚠️ non-finite position for {t} ({qty!r}) — CANNOT VERIFY, failing closed.")
                return None
            print(f"  ⚠️ non-target {t}: non-finite position ({qty!r}) — logged and SKIPPED.")
            continue
        # ⚠️ AND A MAGNITUDE BOUND BESIDE IT: `Decimal("1E+400")` is FINITE where `float("1E+400")`
        # was `inf`, so the guard above waves it through into cash → pnl. Same fail-CLOSED direction —
        # a magnitude nothing can hold is exactly as unverifiable as a failed read.
        if abs(qty) >= _MAX_ABS_QTY:
            if target:
                print(f"  ⚠️ absurd position magnitude for {t} ({qty!r}) — CANNOT VERIFY, failing closed.")
                return None
            print(f"  ⚠️ non-target {t}: absurd position magnitude ({qty!r}) — logged and SKIPPED.")
            continue
        out[t] = qty
    if raw_out is not None:
        raw_out.extend(pos)          # diagnostic only; never read by the caps
    return out


def _quote_price(qty: Decimal, quote, *, as_fill: bool = False) -> Decimal | None:
    """The price at which a MAKER transacts a position or delta of this sign — bid for long/buy,
    ask for short/sell. `quote` is a `(bid, ask)` pair; a bare scalar is NOT accepted. Returns None
    when the needed side is absent, when the book is CROSSED, and — under `as_fill` — when the price
    is a fabricated placeholder outside (0,1): a MARK may use the settlement bound an empty ladder
    fabricates, a FILL may not, since booking a buy as free or a sell at <n> is permanent.
    One rule otherwise: a maker BUYS at its own bid and SELLS at its own ask, and a position
    liquidates the same way round, so P&L is exactly 0 at the fill and mixing sides is phantom."""
    if quote is None:
        return None
    # ⚠️ NO SCALAR FALLBACK: every caller passes `(bid, ask)`. A `Decimal` was never accepted by the
    # old scalar branch either; the one real behavioural difference is that the branch returned BEFORE
    # the `as_fill` placeholder guard, so a scalar 0.0/1.0 was bookable as a fill price.
    bid, ask = quote
    # ⚠️ A CROSSED BOOK IS NOT A PRICE. On the real captured crossed shape (yes_bid 0.65 / yes_ask
    # 0.61) a long marks off the inflated bid for a phantom +<n> — the direction that disarms the
    # cap. `bid == ask` is a legitimate zero-spread book and stays allowed.
    if bid is not None and ask is not None and bid > ask:
        return None
    px = bid if qty > 0 else ask
    # ⚠️ THE FABRICATED PLACEHOLDER IS A VALID MARK AND AN INVALID FILL PRICE. `feed._derive` writes
    # `yes_bid = 0.0` for an empty YES ladder and `yes_ask = 1.0` for an empty NO ladder: marking held
    # inventory there is the settlement bound, but booking a FILL there buys FREE or sells at <n>
    # and, since booking advances `last`, is permanent (+<n>/+<n> phantom per contract measured).
    # Real prices live in [0.01, 0.99], so these bounds are unambiguous sentinels.
    if as_fill and px is not None and not (0.0 < px < 1.0):
        return None
    return px


def _mark_step(targets, base, last, cur, quotes, cash):
    """One pure P&L/inventory step (unit-tested in tests/test_live_mm_pnl.py). `cur` is a fresh
    positions dict (the caller fail-closes on None BEFORE calling this); `quotes[t]` is a `(bid, ask)`
    pair; `base` is the starting positions, so only OUR fills count. ⚠️ EXIT-SIDE, NOT MID: each part
    is priced on the side that transacts it, so P&L is 0 at the fill and accrues only on an adverse
    move (mid-booking netted a maker round trip to exactly ZERO). A NEW FILL whose transacting side is
    unpriceable is SKIPPED with `last` NOT advanced; ⚠️ HELD inventory whose exit side is unpriceable is
    **marked at its WORST CASE** (long → 0, short → 1), or an unmarkable SHORT reads as a profit (M1).
    Returns (cash, inv, pnl, new_last, fills)."""
    new_last = dict(last)
    inv = {}
    fills = []
    # ⚠️ `0`, not `0.0`. This function is arithmetic-only and type-neutral: production feeds it exact
    # Decimals and `int + Decimal` stays Decimal, while a float literal would raise on contact.
    mark = 0
    for t in targets:
        q = quotes.get(t)
        prev = last.get(t, base.get(t, 0))     # position as of the last cycle we could VALUE
        # ⚠️ M13: ABSENT ≠ FLAT. A flat-but-LIVE ticker is returned at position_fp 0.00; a SETTLED one
        # is DROPPED from the read. `cur.get(t, 0.0)` conflated them and booked `d = 0 − prev`, a
        # phantom SELL of the whole position at the ask (a held long of 3 → +<n>). A disappearance is
        # NEVER a trade: carry the last-known position (`c = prev` ⇒ `d = 0`) and worst-case it below.
        # `last` is not advanced, so a later read that lists the ticker can still correct it.
        vanished = t not in cur
        c = prev if vanished else cur[t]
        d = c - prev
        inv[t] = c - base.get(t, 0)      # the POSITION is always known (even when the book isn't) —
        # ⚠️ EXIT-SIDE, NOT MID. A ONE-SIDED book still prices the direction it can: an empty NO ladder
        # leaves a real bid, so a LONG still marks. And when the needed side is genuinely absent the
        # feed already fabricates the worst case (`yes_bid=0.0` / `yes_ask=1.0` [feed.py:_derive]).
        fill_px = _quote_price(d, q, as_fill=True) if d else None
        mark_px = None if vanished else (_quote_price(inv[t], q) if inv[t] else None)
        if (d and fill_px is None) or (inv[t] and mark_px is None):
            # ⚠️ SPLIT THE UNVALUABLE PARTS, don't skip the whole ticker. Keying the skip on `d != 0`
            # also suppressed the mark for ALREADY-BOOKED inventory, and since `last` is never advanced
            # the cap stayed disarmed for the whole one-sided window. BOOKED (`prev − base`) has its
            # cash leg in `cash` and MUST be marked; UNBOOKED (`d = cur − prev`) is in neither and
            # cannot be valued, so leave `last` unadvanced and it books when the book returns.
            # WORST CASE is the settlement bound: long → 0, short YES (= long NO, fully collateralized)
            # → 1 against us, i.e. pnl = −|q|·(1−p) for a short of q at entry p. ⚠️ It BRACKETS
            # `market_exposure_dollars` OPTIMISTICALLY: `cash` accrues at the TOUCH one cycle after the
            # fill (+<n>/fill measured, LIVE sports lane only). Marking rather than HALTING on a
            # one-sided book is deliberate [operator decision 2026-07-20]. See the private design notes M8(b), M9.
            booked = prev - base.get(t, 0)
            if booked:
                mark += booked * (0 if booked > 0 else 1)
            continue
        if d:
            cash -= d * fill_px
            fills.append((t, d, fill_px))
        if inv[t]:
            mark += inv[t] * mark_px
        new_last[t] = c
    return cash, inv, cash + mark, new_last, fills


def _book_mid(okb: dict) -> Decimal | None:
    """Yes-space mid from a Kalshi orderbook dict, or None if not a sane two-sided book. `yes`/`no`
    are bid ladders of [price, qty]; best YES bid = max(yes price), best YES ask = 1 − max(no price)
    (best_bid = max(price) per feed._derive). EXACT: the complement is the most repeated money
    operation here (`1 − 0.58` is 0.42000000000000004 in float), and the `[min_price, max_price]` gate
    downstream is an inclusive `<=`, so exactness decides a market sitting ON the band boundary."""
    yes = okb.get("yes_dollars") or okb.get("yes") or []
    no = okb.get("no_dollars") or okb.get("no") or []
    try:
        yb = max(parse_wire(lvl[0]) for lvl in yes)      # best YES bid
        ya = _ONE - max(parse_wire(lvl[0]) for lvl in no)  # best YES ask = 1 − best NO bid
    except (ValueError, TypeError, IndexError, InvalidOperation):
        return None
    # Finiteness BEFORE the ordering compare: on a SINGLE-level ladder a NaN price reaches here
    # unraised, and `Decimal("NaN") < x` raises InvalidOperation OUTSIDE the try — one garbage level
    # would abort target selection where the float path skipped the book.
    if not (yb.is_finite() and ya.is_finite()):
        return None
    if not (_ZERO < yb <= ya < _ONE):                    # crossed / one-sided / empty → not quotable
        return None
    return (yb + ya) / 2


# ⛔ SERIES THE MAKER MUST NEVER QUOTE — defined HERE, on the money path, and imported by the scanner
# so there is exactly one copy. Hourly crypto strikes priced off a public sub-second feed pick off a
# non-colocated maker BY CONSTRUCTION, and they are top of every raw-volume screen (KXBTCD was #2 on
# the venue by contested 24h flow, 2026-07-24).
TRAP_SERIES = frozenset({"KXBTCD", "KXBTC", "KXETHD", "KXXRPD"})
# Resolution floor for RAW REST orderbook levels. The WS path floors at feed._QTY_STEP; the REST
# snapshot does not, and the venue has been observed sending 5.68e-14 qty levels.
_QTY_STEP_REST = Decimal("0.0001")


def _book_touch_depth(okb: dict) -> Decimal | None:
    """Contracts resting AT the touch, worse side of the two, or None if unreadable.

    This is the queue a size-1 maker must get through before a trade reaches it, so it — not raw
    volume — is what decides whether we ever fill (measured 2026-07-24: at matched flow, series differ
    by three orders of magnitude here). Takes the WORSE (deeper) side because we quote BOTH.
    """
    yes = okb.get("yes_dollars") or okb.get("yes") or []
    no = okb.get("no_dollars") or okb.get("no") or []

    def _touch_qty(ladder) -> Decimal | None:
        """Qty at the best REAL price. Sub-resolution levels are dropped BEFORE taking the max.

        ⚠️ Without that filter this fails OPEN, in the flattering direction: a dust level (the venue
        has sent 5.68e-14) at a better price wins `max(price)`, the qty sum returns ~0, and a
        16,000-deep book reports an EMPTY queue. An unknown queue is not a safe queue.
        """
        real = []
        for lvl in ladder:
            # parse_wire, not float(): exact prices make the `p == best` level match an arithmetic
            # identity, and exact quantities compare the dust filter against the venue's own grid.
            p, q = parse_wire(lvl[0]), parse_wire(lvl[1])
            if q >= _QTY_STEP_REST:
                real.append((p, q))
        if not real:
            return None
        best = max(p for p, _ in real)
        return sum((q for p, q in real if p == best), _ZERO)

    try:
        yq, nq = _touch_qty(yes), _touch_qty(no)
    except (ValueError, TypeError, IndexError, InvalidOperation):
        return None
    if yq is None or nq is None:
        return None            # one-sided/dust-only → unreadable → caller skips (fail-CLOSED)
    return max(yq, nq)


def _event_of(ticker: str) -> str:
    """Event key = ticker minus the final outcome segment (…CHUYOM-CHU and -YOM share one event)."""
    return ticker.rsplit("-", 1)[0]


# Kalshi MAKER-fee status per series. ⚠️ READ FROM THE VENUE (`/series`.fee_type), never inferred
# from the fee-schedule PDF: the PDF lists 76 maker-charged series against the API's 130, and that
# inference mislabelled KXNBAGAME/KXNHLGAME as free. The discriminator is `fee_type`, NOT
# `fee_multiplier` (1 on both kinds). Populated by _load_maker_fees() before selection.
_MAKER_CHARGED: dict[str, bool] = {}


# A game's clock: `expected_expiration_time` is the expected END, so a market inside the last
# _GAME_LEN_S before it is a game IN PROGRESS. Beyond _LONG_DATED_S there is no game clock at all.
_GAME_LEN_S = 3 * 3600
_LONG_DATED_S = 24 * 3600


def _expiry_ts(m) -> float | None:
    exp = getattr(m, "expected_expiration_time", None)
    try:
        return _dt.datetime.fromisoformat(exp.replace("Z", "+00:00")).timestamp() if exp else None
    except (ValueError, AttributeError):
        return None


def _phase(m) -> str:
    """Where this market sits relative to its own expiry: live / pregame / long-dated / unknown.

    Naming the long-dated case explicitly is the point: the boolean `_live()` this replaces applied a
    SPORTS concept to every market, so a weather or macro market 93 days out silently answered False
    and fell into a fallback tier."""
    e = _expiry_ts(m)
    if e is None:
        return "unknown"
    dt = e - time.time()
    if dt <= 0:
        return "closing"
    if dt > _LONG_DATED_S:
        return "long-dated"
    return "live" if dt <= _GAME_LEN_S else "pregame"


def feed_prefixes(targets) -> list[str]:
    """Series prefixes the WS feed must accept in order to deliver books for `targets`.

    `feed._matches_prefix` is a HARD FILTER on inbound messages and defaults to config.KALSHI_SERIES,
    the 12 cross-arb SPORTS series. The MM universe is ~97% of Kalshi, so that default drops every
    book message for a non-arb target and the run places ZERO orders while logging a healthy connect."""
    return sorted({_series_of(t) for t in targets})


async def wait_for_book(book, targets, *, tries: int = 5, delay: float = 3.0) -> bool:
    """True once ANY target has a two-sided book. Refusing to start blind on the BOOK mirrors the
    existing refusal to start blind on POSITIONS.

    ANY, not ALL: a quiet one-sided market is quotable-in-principle, whereas the failure this guards
    (a filter dropping every message) takes out every target at once."""
    for _ in range(tries):
        await asyncio.sleep(delay)
        # `_d`, like every other read in this module: ONE spelling of the getters, so a book that only
        # serves floats fails loudly here rather than silently in the quote loop.
        if any(book.get_best_bid_d(t) and book.get_best_ask_d(t) for t in targets):
            return True
    return False


def eligible_tickers(markets, *, pregame_only: bool = False) -> list[str]:
    """Markets we are willing to quote, by phase.

    Excluded: `closing` (already PAST its expected expiry — these stay `open` for hours and can settle
    out from under any inventory we hold); `unknown` (timing UNREADABLE — fail-closed, since a shape
    change would classify every market `unknown` and make this gate silently inert, and cannot-verify
    is not permission); and `live` only under --pregame-only. ⚠️ `live` means "within _GAME_LEN_S of
    expiry", exact for sports (expected_expiration = start + 3h) and just "about to resolve" elsewhere."""
    return [m.ticker for m in markets
            if _phase(m) in (("pregame", "long-dated") if pregame_only
                             else ("pregame", "long-dated", "live"))]


def _series_of(ticker: str) -> str:
    """Series ticker = the segment before the first '-' (KXNPBGAME-26JUL…-CHU → KXNPBGAME)."""
    return ticker.split("-", 1)[0]


def _maker_mult(ticker: str) -> int:
    """1 = maker fee charged, 0 = maker-free — from the venue, with a FAIL-SAFE default.

    An unknown series returns 1 (CHARGED): `--maker-free-only` must never select a market whose fee
    status we could not verify."""
    known = _MAKER_CHARGED.get(_series_of(ticker))
    if known is None:
        return 1                      # unverified ⇒ assume charged ⇒ excluded from the free lane
    return 1 if known else 0


async def _load_maker_fees(client: KalshiClient, tickers) -> None:
    """Populate _MAKER_CHARGED from `/series/{s}.fee_type` for every series in play (once, before
    selection). A series that cannot be read stays absent, so _maker_mult treats it as charged."""
    for ser in sorted({_series_of(t) for t in tickers}):
        charged = await client.series_charges_maker_fee(ser)
        if charged is None:
            print(f"  ⚠️ {ser}: maker-fee status UNREADABLE — treating as CHARGED (excluded from "
                  f"--maker-free-only)")
            continue
        _MAKER_CHARGED[ser] = charged


async def _two_sided(client: KalshiClient, tickers: list[str], want: int, *,
                     min_price: float = 0.15, max_price: float = 0.85,
                     maker_free_only: bool = False,
                     vol_of: dict[str, float] | None = None,
                     max_touch: float | None = None) -> list[str]:
    """Pick up to `want` CONTESTED two-sided markets, at most ONE per event, PREFERRING maker-free series.

      • contested band — mid in [min_price, max_price]; the tails of a near-resolved game are the
        worst place to measure MM economics. Both edges are INCLUSIVE and SYMMETRICALLY so (the
        argparse floats cross `from_float` at the top of this function).
      • one-per-event — never quote both complementary sides of the same game (same risk twice).
      • maker-fee preference — M=0 (maker-FREE) series FIRST, since the ~<n>/fill fee on an M=1
        series ~cancels the favorable markout; `maker_free_only` hard-excludes M=1 entirely.
      • FLOW ranking — within a fee class, the most-traded market first. ⚠️ `vol_of` is PER-MARKET,
        not the per-SERIES sums in the private design notes §4; the series is --series."""
    vol_of = vol_of or {}
    # `--min-price` / `--max-price` are argparse FLOATS; the mid they gate is exact Decimal. Cross the
    # boundary ONCE, here. Not cosmetic: the two band edges round opposite ways as doubles, so an exact
    # mid of 0.15 was admitted while an exact mid of 0.85 was REJECTED from an inclusive `<=`
    # (the private design notes §4.3).
    lo, hi = from_float(min_price), from_float(max_price)
    # `--max-touch` is the THIRD argparse float this function gates Decimals with: `depth > max_touch`
    # mixed-compares at the 2-dp boundary. Same boundary, once.
    max_touch_d = None if max_touch is None else from_float(max_touch)
    tickers = sorted(tickers, key=lambda t: (_maker_mult(t), -vol_of.get(t, 0.0)))
    out: list[str] = []
    seen_events: set[str] = set()
    for t in tickers:
        # UNCONDITIONAL — not gated on maker_free_only. These are maker-FREE, so the fee filter waves
        # them straight through; the fee is not what makes them wrong.
        if _series_of(t) in TRAP_SERIES:
            continue
        if maker_free_only and _maker_mult(t):
            continue
        ev = _event_of(t)
        if ev in seen_events:
            continue
        try:
            ob = await client.get_orderbook(t)
        except Exception:
            continue
        # PROD shape is orderbook_fp / *_dollars (kalshi_arb.py:_best_ask_from_book); demo returned the
        # older orderbook / yes|no — _book_mid accepts both.
        okb = ob.get("orderbook_fp") or ob.get("orderbook") or {}
        mid = _book_mid(okb)
        if mid is None or not (lo <= mid <= hi):
            continue
        # QUEUE GATE. Raw volume ranks a market by how much trades, not by whether any of it reaches
        # US: behind a 16,000-deep touch a size-1 back-of-queue maker effectively never fills.
        # NARROWING ONLY. `None` (unreadable depth) is treated as FAIL — an unknown queue is not safe.
        if max_touch_d is not None:
            depth = _book_touch_depth(okb)
            if depth is None or depth > max_touch_d:
                continue
        out.append(t)
        seen_events.add(ev)
        if len(out) >= want:
            break
    return out


class MakerSession:
    """The maker run's order lifecycle and its state, in one testable object.

    Extracted from closures inside a 720-line `main()` that no test could reach except by
    string-matching their own source; every method body is AST-identical to its closure — ⚠️ which is
    necessary, NOT sufficient: re-binding from `self` preserves MUTATION, not REBINDING. `inv` is
    rebound (`_mark_step` returns a fresh dict), so `self.inv` froze at zeros and both `--inv-cap` and
    `--inv-coef` went inert until `main()` began writing it back via `sync_inventory`.
    """

    # Markout horizons, measured from the VENUE'S FILL TIMESTAMP — see `_fill_ts`. A class constant
    # rather than a loop-local so a test can shorten it without reaching into `main()`.
    MK_HORIZONS = (5.0, 30.0, 300.0)

    def __init__(self, client, book, w, args, targets, run_start):
        self.client = client
        self.book = book
        self.w = w
        self.args = args
        self.targets = list(targets)
        self.run_start = run_start
        # order lifecycle
        self.resting: dict[str, list[str]] = {t: [] for t in targets}
        self.order_meta: dict[str, dict] = {}
        self.filled_oids: set[str] = set()
        # measurement accumulators
        self.inv: dict[str, Decimal] = {t: _ZERO for t in targets}
        self.stats = {"placed": 0, "rejected": 0}
        self.seen_fills: set[str] = set()
        # ⛔ THE FLATTEN'S OWN ORDERS, KEYED ON `order_id` AT PLACEMENT — NOT on a fill read, which is a
        # 10-unit call in a teardown and fails on exactly the 429-driven halts this matters for. A
        # re-booked flatten fill is not a neutral duplicate: the flatten sells at the ASK and buys at
        # the BID, so its `spread_capture` is POSITIVE BY CONSTRUCTION, and its `fill_ts` is stale so
        # all three markout horizons resolve off one mid. The id is known before any read happens.
        self.flatten_oids: set[str] = set()
        self.flatten_fill_ids: set[str] = set()
        self.markout_pending: list[dict] = []
        # WHY DID THE QUEUE AHEAD OF US CLEAR — did it trade away, or cancel away? (the private design notes M23.)
        # `qtrack` accumulates the tape and the L2 deltas at our own price levels; `tape` is None until
        # main() wires it (never in DRY). `tape_ok` keeps a dead socket from reading as "the queue never
        # traded", i.e. NOT_TRADE_THROUGH on every fill.
        self.qtrack = QueueTracker()
        self.tape = None
        self.verdicts: dict[str, int] = defaultdict(int)
        # ── crash-durable order record (bot/core/maker_state.py) ────────────────────────────────
        # None until main() attaches one, and ONLY on a real run: a DRY preview places nothing, so
        # letting it write would reset a crashed real run's order list. When attached, `quote()`
        # records each order's INTENT before sending it, so a SIGKILL between the send and the
        # response still leaves a trace of what may be resting.
        self.state: maker_state.MakerStateStore | None = None

    def sync_inventory(self, inv: dict[str, Decimal]) -> None:
        """Adopt the inventory `_mark_step` just computed.

        `_mark_step` RETURNS a fresh dict rather than mutating, so rebinding main's local `inv` left
        the copy `quote()` reads frozen at zeros — silently disabling `--inv-cap` and `--inv-coef`.
        Mutates in place rather than reassigning, so no other holder of the dict goes stale."""
        self.inv.clear()
        self.inv.update(inv)

    def mid(self, t):
        book = self.book

        # ⚠️ `_d`, the EXACT getters. The float spelling still exists on the cache for the stopped arb
        # bot; reading it here would put the number that marks inventory for the LOSS CAP back on
        # binary floats. Both spellings derive from one value — see feed.py.
        return _mid_from(book.get_best_bid_d(t), book.get_best_ask_d(t))

    def quote_of(self, t):
        """`(bid, ask)` for P&L, EXACT. ⚠️ Deliberately NOT `mid()`: a one-sided book still prices
        the side that can transact, and the feed's fabricated `0.0`/`1.0` on an empty ladder IS the
        worst case — so this degrades to the conservative bound without a special branch."""
        book = self.book
        return (book.get_best_bid_d(t), book.get_best_ask_d(t))

    @property
    def tape_ok(self) -> bool:
        """Is the trade tape CONFIRMED live — venue-acked, not merely connected?

        Read straight off `KalshiTradeFeed.subscribed`, which stays False until the venue acks.
        Load-bearing: `traded = 0` from a rejected subscription is arithmetically identical to a
        queue that pulled, and would report NOT_TRADE_THROUGH on 100% of fills."""
        return bool(self.tape is not None and self.tape.subscribed)

    def log_queue_dynamics(self, oid: str, *, outcome: str,
                           filled_qty=None, fill_ts: float | None = None) -> None:
        """Close out one order's queue attribution and write its `queue_dynamics` row.

        Called on BOTH fill and cancel: the cancelled population is the survivorship half that a
        fills-only log cannot express. One row per order by construction — `close()` removes the
        tracker, so a failed-then-retried cancel writes a second `order_outcome` row but not one here."""
        q = self.qtrack.close(oid)
        if q is None:
            return                      # never tracked (DRY, a lost create response, a stray)
        row = self.qtrack.summarize(q, outcome=outcome, tape_ok=self.tape_ok,
                                    filled_qty=filled_qty, fill_ts=fill_ts, now=time.time())
        lag = row["detect_lag_s"]
        detail = (
            f"{row['ticker']} {row['action']} px={row['px']} oid={oid[:8]} "
            f"outcome={row['outcome']} verdict={row['verdict'] or 'NA'} "
            f"rest_s={row['rest_s']:.1f} track_lag_s={row['track_lag_s']:.3f} "
            f"ahead={_fmt_qty(row['ahead'])} "
            f"traded_ahead={_fmt_qty(row['traded_ahead'])} "
            f"traded_ahead_by_ts={_fmt_qty(row['traded_ahead_by_ts'])} "
            f"boundary_traded={_fmt_qty(row['boundary_traded'])} "
            f"traded_total={_fmt_qty(row['traded_total'])} "
            f"cancelled_ahead_implied={_fmt_qty(row['cancelled_ahead_implied'])} "
            f"cancelled_at_level={_fmt_qty(row['cancelled_at_level'])} "
            f"removed={_fmt_qty(row['removed'])} added={_fmt_qty(row['added'])} "
            f"prints={row['prints']} "
            f"detect_lag_s={'unknown' if lag is None else f'{lag:.1f}'} "
            f"book_gap={'Y' if row['book_gap'] else 'N'} "
            f"tape_gap={'Y' if row['tape_gap'] else 'N'} "
            f"tape={'ok' if row['tape_ok'] else 'DOWN'}"
        )
        self.w.writerow([f"{time.time():.0f}", "queue_dynamics", detail, "", "", ""])
        # Running tally, printed each cycle by the caller: a run whose tape is dying returns ~100%
        # UNKNOWN, which is safe-direction but abortable at minute 5 rather than at teardown.
        # ⚠️ FILLS ONLY. The tape guards fire BEFORE the `outcome != "fill"` check, so tallying cancels
        # too mixes populations and reads as "96% unreadable" on a run whose fills are 1/1 readable.
        if row["verdict"] and outcome == "fill":
            self.verdicts[row["verdict"]] += 1

    def log_unfilled(self, oid: str, cancel_status: str = "cancelled") -> None:
        """Record the fate of an order that is being cancelled without having filled.

        The survivorship point: a run that logs only its FILLS is silent about the population that says
        whether the policy reaches the front at all. Best-effort — a cancel whose response was lost
        still lands here, and the venue sweep reconciles."""
        filled_oids = self.filled_oids
        order_meta = self.order_meta
        w = self.w

        meta = order_meta.get(oid)
        if not meta or oid in filled_oids:
            return
        # `already_gone` means the venue had nothing to cancel — the order filled (or was already
        # cancelled) between our last fill poll and now. Recording that as `unfilled` would be the same
        # lie by a slower route, so it gets its own outcome and an analysis can exclude it.
        outcome = "gone_at_cancel" if cancel_status == "already_gone" else (
            "cancel_failed_may_still_rest" if cancel_status == "cancel_failed" else
            "cancelled_unfilled")
        w.writerow([f"{time.time():.0f}", "order_outcome",
                    f"{meta['tk']} {meta['action']} px={meta['px']:.4f} outcome={outcome} "
                    f"age={max(0.0, time.time() - meta['t_placed']):.1f}s "
                    f"queue_ahead={_fmt_ahead(meta['ahead'])} "
                    f"improved={'Y' if meta['improved'] else 'N'} "
                    f"spread_ticks={meta['spread_ticks']}", "", "", ""])
        # WHAT the queue did while we waited, alongside WHETHER we got through it. On this path the
        # answer is "we didn't", and the traded/cancelled split is still the interesting part.
        self.log_queue_dynamics(oid, outcome=outcome)

    async def cancel_all(self, t) -> bool:
        """Cancel every tracked resting order for ticker `t`. Returns True iff ALL cancels succeeded
        (safe to place fresh quotes on `t`); False iff any FAILED — an order may still be live on the
        venue, and the caller MUST NOT place on top of it (the private design notes M1). On ANY failure, RECONCILE
        `resting[t]` from the venue so still-live orders stay tracked; if that reconcile ALSO fails,
        KEEP the tracked oids. Only the all-success path clears to []."""
        _log_unfilled = self.log_unfilled
        client = self.client
        resting = self.resting

        failed = False
        for oid in list(resting[t]):
            status = "cancelled"
            try:
                r = await client.cancel_order(oid)
                # READ THE VENUE, don't infer. `cancel_order` maps a 404 to {"status":
                # "already_gone"} — the venue telling us it filled. Inferring "unfilled" from our own
                # not-yet-updated fill set is how ~95% of fills got labelled as non-fills.
                if isinstance(r, dict) and r.get("status"):
                    status = str(r.get("status"))
            except Exception as e:
                # The order may still be RESTING — a failed cancel is not a cancelled order. `failed`
                # makes the whole method report that, so the caller skips placement.
                status = "cancel_failed"
                failed = True
                print(f"  cancel {oid[:8]} failed: {e!r}")
            else:
                # Cancelled (or already gone) — it can no longer be resting, so drop it from the crash
                # record. Deliberately in the `else`: a FAILED cancel means the order may still be live,
                # and forgetting it here would hide it from recovery.
                if self.state is not None:
                    with contextlib.suppress(Exception):
                        self.state.clear_order(oid)
            # Logging must never break cancellation. Uncaught, this propagates out of _cancel_all →
            # _quote → the outer finally → re-enters _cancel_all → raises again and escapes the finally
            # itself, skipping the venue stray sweep and leaving live orders resting.
            try:
                _log_unfilled(oid, status)
            except Exception as e:                        # logging is never fatal
                print(f"  (order_outcome logging error, ignored: {e!r})")
        if failed:
            try:
                live = await client.get_resting_orders([t])
                resting[t] = [o.get("order_id") for o in live if o.get("order_id")]
                print(f"  ⚠️ cancel_failed on {t}: reconciled {len(resting[t])} still-resting order(s) "
                      f"from the venue — skipping placement this cycle, retrying cancel next cycle.")
            except Exception as e:                        # keep tracked oids for retry
                # ⚠️ Keeping ALL original oids means an already-cancelled one is re-cancelled and
                # re-logged next cycle (two order_outcome rows for one order). Self-heals once any
                # cancel succeeds; instrumentation-only, never the caps.
                print(f"  ⚠️ cancel_failed on {t} AND the resting-order reconcile failed ({e!r}) — "
                      f"keeping {len(resting[t])} tracked oid(s) for retry; CHECK THE KALSHI UI if it "
                      f"persists.")
            return False
        resting[t] = []
        return True

    async def cancel_everything(self):
        _cancel_all = self.cancel_all
        targets = self.targets

        for t in targets:
            await _cancel_all(t)

    async def sweep_venue_strays(self) -> bool:
        """Belt-and-suspenders after cancelling TRACKED orders: ask the venue what is STILL resting in
        our target markets and cancel it, reaching the one residual `_cancel_everything` cannot — an
        order whose create-response was LOST has no captured order_id. Fail-LOUD: an unreadable listing
        can never be reported as 'no strays'. ⚠️ Cancels ALL resting orders in the target markets, not
        just this run's. RETURNS True only on the VENUE'S evidence, which is what lets the teardown
        record a CLEAN EXIT in the crash state."""
        client = self.client
        targets = self.targets

        try:
            strays = await client.get_resting_orders(targets)
        except Exception as e:
            print(f"  ⚠️ venue sweep: could NOT list resting orders — cannot confirm no strays "
                  f"remain, CHECK THE KALSHI UI: {e!r}")
            return False
        if not strays:
            print("  venue sweep: no resting orders remain in target markets ✓")
            return True
        print(f"  venue sweep: {len(strays)} resting order(s) still on venue — cancelling stray(s)...")
        all_cancelled = True
        for o in strays:
            oid = o.get("order_id")
            if not oid:
                all_cancelled = False        # a stray we cannot even name is a stray we cannot clear
                continue
            try:
                await client.cancel_order(oid)
                print(f"    swept stray {oid[:8]} ({o.get('ticker')})")
            except Exception as e:
                all_cancelled = False
                print(f"    stray {oid[:8]} cancel FAILED — check UI: {e!r}")
        return all_cancelled

    async def flatten_on_exit(self, base, wait_s: float) -> None:
        """ — PASSIVE flatten of OUR inventory, on EVERY exit; the caller gates on
        `real and --flatten-on-exit`. `halted` is deliberately not a gate: the process is exiting and
        the loss cap dies with it. It reads the VENUE position (a FAILED read does NOT flatten), then
        for each of OUR net positions (`venue − base`, so INHERITED `base` is left alone) posts ONE
        post-only reducing order AT THE TOUCH (long → sell at the ask, short → buy at the bid) — earns
        the half-spread, never crosses, so the post_only rail is PRESERVED — waits `wait_s`, then reads
        OUR OWN fills by `order_id` into `seen_fills` so the teardown catch-up cannot re-book them as
        maker edge. Residual is left for the venue sweep and the OPEN-POSITION warning still fires.
        ⚠️ v1: a SECOND Ctrl-C during the wait truncates teardown; a LOST create-response is not deduped."""
        client, book, w = self.client, self.book, self.w
        seen_fills, targets, run_start = self.seen_fills, self.targets, self.run_start

        venue = await _positions(client, targets=targets)
        if venue is None:
            print("  ⚠️ flatten-on-exit: position read FAILED — NOT flattening (won't size off stale "
                  "belief); leaving inventory for the sweep + OPEN-POSITION warning. Check the UI.")
            w.writerow([f"{time.time():.0f}", "flatten_summary", "SKIPPED read_failed", "", "", ""])
            return
        placed: dict[str, str] = {}          # order_id -> ticker, for exact fill attribution
        for t in targets:
            if t not in venue:
                # ⚠️ M13: ABSENT ≠ FLAT. A settled or transiently-dropped ticker is absent from the
                # read; `venue.get(t, 0.0)` would read it as flat and — with inherited `base` — FLIP
                # net's sign, placing a real reducing order in the WRONG direction. Never flatten a
                # ticker we cannot read.
                continue
            net = venue[t] - base.get(t, _ZERO)          # OUR fills only; sized off the READ (t present)
            qty = math.floor(abs(net))                   # FLOOR: never OVER-sell into a flip if the
            if qty < 1:                                  # venue rounds a fractional count up. A flat or
                continue                                 # sub-contract residual is left for the sweep +
            #                                              the OPEN-POSITION warning — no dust order.
            bid, ask = book.get_best_bid_d(t), book.get_best_ask_d(t)
            if _mid_from(bid, ask) is None:              # one-sided/crossed → cannot post a passive
                print(f"  flatten-on-exit: {t[:30]} book unpriceable — leaving {net:+.2f} for the sweep.")
                continue
            action, px = ("sell", _tick(ask, "sell")) if net > 0 else ("buy", _tick(bid, "buy"))
            try:
                r = await client.create_order(t, "yes", action, qty, px,
                                              time_in_force="good_till_canceled", post_only=True)
                oid = r.get("order_id")
                if oid:
                    placed[oid] = t
                    # ⛔ RECORD IT NOW, BEFORE ANY READ CAN FAIL. The catch-up excludes fills by
                    # `order_id`, so a failed `get_fills` below can lose the flatten's ACCOUNTING but
                    # can no longer let its fills be re-booked as maker flow. See `self.flatten_oids`.
                    self.flatten_oids.add(oid)
                    print(f"  flatten-on-exit: PASSIVE {action} {qty}@{px} on {t[:30]} "
                          f"(net {net:+.2f}, flattening {qty})")
            except Exception as e:
                print(f"  ⚠️ flatten-on-exit: place {action} on {t} failed: {e!r}")
        if not placed:
            w.writerow([f"{time.time():.0f}", "flatten_summary",
                        "nothing placed (flat or unpriceable)", "", "", ""])
            return
        await asyncio.sleep(wait_s)
        try:
            fills = await client.get_fills(min_ts=run_start, tickers=targets)
        except Exception as e:
            print(f"  ⚠️ flatten-on-exit: fills read failed — exit fills not recorded: {e!r}")
            fills = []
        total_n = total_cost = _ZERO
        per_ticker: dict[str, Decimal] = defaultdict(lambda: _ZERO)
        for f in fills:
            oid, fid = f.get("order_id"), f.get("fill_id")
            if oid not in placed or not fid or fid in seen_fills:
                continue
            seen_fills.add(fid)              # so the teardown catch-up does not re-book this fill
            self.flatten_fill_ids.add(fid)   # ...and so it is EXCLUDED from the run's fill COUNT
            try:
                # parse_wire, not float(): price, count and fee arrive as decimal STRINGS and are
                # money/quantities (CLAUDE.md § Code style). `total_cost` below is a dollar figure.
                raw_px = f.get("yes_price_dollars")
                px = parse_wire(raw_px) if raw_px not in (None, "") else None
                n = parse_wire(f.get("count_fp") or "0")
                fee = parse_wire(f.get("fee_cost") or "0")
            except (TypeError, ValueError, InvalidOperation):
                continue
            # Reject a missing/out-of-range price rather than `or 0.0`-booking a phantom flatten fill
            # (would understate the flatten cost; inventory itself is tracked via the venue read) — A2.
            if px is None or not px.is_finite() or not (0 < px < 1):
                print(f"  ⚠️ flatten: skipping fill with unusable yes_price_dollars="
                      f"{f.get('yes_price_dollars')!r}")
                continue
            per_ticker[placed[oid]] += n
            total_n += n
            total_cost += px * n + fee
            w.writerow([f"{f.get('ts') or int(time.time())}", "flatten_fill",
                        f"{placed[oid]} {f.get('action')} n={_fmt_qty(n)} px={px:.4f} fee={fee:.4f}",
                        "", "", ""])
        w.writerow([f"{time.time():.0f}", "flatten_summary",
                    f"filled {_fmt_qty(total_n)} across {len(per_ticker)} ticker(s) "
                    f"Σpx·n+fee=${total_cost:.4f}; residual (if any) left for the sweep", "", "", ""])

    async def quote(self, t):
        _cancel_all = self.cancel_all
        args = self.args
        book = self.book
        client = self.client
        inv = self.inv
        order_meta = self.order_meta
        resting = self.resting
        stats = self.stats
        w = self.w

        # EXACT prices — every quote below is derived from these by whole-tick arithmetic, so the sent
        # price lands ON the cent grid rather than a representation of it.
        bid, ask = book.get_best_bid_d(t), book.get_best_ask_d(t)
        # ⚠️ M1: QUOTE ONLY WHERE WE CAN PRICE BOTH SIDES. `bid>0 and ask>0` waved through the feed's
        # FABRICATED one-sided placeholders, so the maker placed a real order against a fabricated
        # side (a one-directional lean, since only the real side can fill) whose inventory could not
        # then be two-sided-marked. Gate on the SAME priceability contract the mark/selection path
        # uses (`0.0 < bid <= ask < 1.0`), so quotable == markable.
        if _mid_from(bid, ask) is None:
            return
        # ⚠️ get_depth(side) = "size available to BUY side", i.e. the OPPOSITE ladder: _depth["yes"]
        # comes from the NO bids (= yes ASK depth), _depth["no"] from the YES bids (= yes BID depth)
        # [VERIFIED feed.py:314]. Mapping bid=get_depth("yes") INVERTS OBI, which leaned the skew INTO
        # adverse flow instead of away from it.
        depth_b = book.get_depth_d(t, "no") or _ZERO    # yes BID depth
        depth_a = book.get_depth_d(t, "yes") or _ZERO   # yes ASK depth
        t_depth = time.time()                       # the reference time for `ahead` — see track_lag_s
        # ⚠️ THIS DEPTH INCLUDES OUR OWN LAST-CYCLE ORDERS — read BEFORE `_cancel_all` so the
        # `quote_ctx` replay row sees the untouched pre-policy book. The subtraction below is exact,
        # from `order_meta`, not by re-reading the book and racing our own WS delta. ⚠️ SKIP ORDERS
        # THAT ALREADY FILLED: fills are polled BEFORE quoting, so `resting[t]` still names orders gone
        # from the book, and subtracting them understates `ahead` toward TRADE_THROUGH — under-detecting
        # adverse selection. [mm-review 2026-07-24 B-1]
        own_resting: dict[tuple[str, Decimal], Decimal] = defaultdict(lambda: _ZERO)
        for _oid in resting[t]:
            _m = order_meta.get(_oid)
            if _m and _oid not in self.filled_oids:
                own_resting[(_m["action"], _m["px"])] += Decimal(int(args.size))
        obi = (depth_b - depth_a) / (depth_b + depth_a) if (depth_b + depth_a) > 0 else _ZERO
        # Whole-tick lean, rounded ONCE from the combined OBI+inventory intent (see _skew_ticks) — both
        # quotes move by the same tick count, so there is no sub-tick shift for _tick's floor/ceil to
        # turn into a one-sided step function.
        shift = _skew_ticks(obi, inv[t], args.obi_coef, args.inv_coef) * _TICK
        touch_b, touch_a = bid, ask                 # the untouched BBO, before any policy applies
        bid, ask = _improve(bid, ask, args.improve_ticks)
        if not await _cancel_all(t):
            # M1: a cancel FAILED, so an order may still be resting on the venue. Placing now would
            # stack a new order on top of it (unbounded on a 429 storm, and invisible to the inv cap).
            # Skip placement this cycle; next cycle re-attempts the cancel before quoting.
            w.writerow([f"{time.time():.0f}", "quote_ctx",
                        f"{t} SKIP=cancel_failed resting={len(resting[t])} — retry next cycle",
                        "", "", ""])
            return
        pairs = []
        # `--inv-cap` is an argparse float; the inventory it is compared against is exact. Cross the
        # boundary once, here, rather than floating the position (which is money) to meet it.
        inv_cap = from_float(args.inv_cap)
        if inv[t] < inv_cap:
            pairs.append(("buy", _tick(bid + shift, "buy")))
        if inv[t] > -inv_cap:
            pairs.append(("sell", _tick(ask + shift, "sell")))
        # THE REPLAY ROW. One account cannot run two arms at once, so a live run can only A/B its policy
        # by recording the world: the untouched touch, both queue depths, our inventory, the prices we
        # sent. Pure observation; nothing downstream reads it. ⚠️ NO `improved` FIELD, deliberately —
        # `shift` moves both quotes the SAME way, so the honest per-SIDE value is on the order rows.
        w.writerow([f"{time.time():.0f}", "quote_ctx",
                    f"{t} touch={touch_b:.4f}/{touch_a:.4f} spread_ticks="
                    f"{round((touch_a - touch_b) / _TICK)} depth_bid={_fmt_qty(depth_b)} "
                    # `_fmt_qty`, not `:g`: `format(Decimal('146.00'), 'g')` keeps the trailing zeros
                    # where the float spelling printed `146`, and two spellings of one number splits a
                    # grep-based analysis in half.
                    f"depth_ask={_fmt_qty(depth_a)} obi={obi:+.4f} inv={inv[t]:+.2f} "
                    f"shift={shift:+.4f} "
                    f"sent={'/'.join(f'{a}@{p}' for a, p in pairs) or 'NONE(cap)'}",
                    "", "", ""])
        for side_action, px in pairs:
            # ── DURABLE INTENT, WRITTEN BEFORE THE SEND ────────────────────────────────────────
            # The window nothing else covers is "the create reached the venue and we died before the
            # response arrived": the order rests and no in-process record exists (SIGKILL removes even
            # the teardown sweep). ⛔ FAIL-CLOSED, SKIP-NOT-FIRE: if the intent cannot be written we do
            # NOT place the order — an order nothing can ever cancel is worse than a missed quote.
            intent_id = ""
            if self.state is not None:
                intent_id = f"{t}:{side_action}:{px}:{time.time():.3f}"
                try:
                    self.state.record_intent(intent_id, ticker=t, side=side_action,
                                             price=px, count=args.size)
                except Exception as e:
                    stats["rejected"] += 1
                    print(f"  {t[:32]:32} {side_action:4} @{px} SKIPPED — could not durably "
                          f"record the order intent ({e!r}); refusing to place an order that "
                          f"nothing could cancel after a crash.")
                    continue
            try:
                r = await client.create_order(t, "yes", side_action, args.size, px,
                                              time_in_force="good_till_canceled", post_only=True)
                oid = r.get("order_id")
                if self.state is not None:
                    # Attach the venue id (or leave it None on a 2xx-with-no-order_id, which stays
                    # counted as maybe-live — that case is exactly what recovery must still reach).
                    with contextlib.suppress(Exception):
                        self.state.record_placed(intent_id, oid)
                if oid:
                    resting[t].append(oid)
                    stats["placed"] += 1
                    # ⚠️ `ahead` IS DEFINITIONAL, NOT A MEASUREMENT — do not regress fill-rate on it.
                    # It is a deterministic function of `improved` (0 by construction inside the touch,
                    # the visible level at it), so regressing on it is regressing on the arm label. Same
                    # for the queue attribution: an improved quote's `ahead == 0` made every fill
                    # TRADE_THROUGH, so `queue_tracker._verdict` returns ALONE_AT_PRICE there — do not
                    # "simplify" that branch away. A post-ack `touch_depth` was tried and REMOVED (it
                    # returns our own size once we are the touch); read `quote_ctx` instead.
                    px_f = Decimal(px)
                    # Net out our OWN last-cycle size at each touch price before asking how many
                    # contracts were in front of us — see the `own_resting` note above. Matched with
                    # the SAME half-tick tolerance `_queue_ahead` uses, not by dict equality on a
                    # float; a miss here silently skips the subtraction.
                    ahead, improved = _queue_ahead(
                        side_action, px_f, touch_b, touch_a,
                        max(_ZERO, depth_b - _own_at(own_resting, "buy", touch_b)),
                        max(_ZERO, depth_a - _own_at(own_resting, "sell", touch_a)))
                    t_placed = time.time()
                    order_meta[oid] = {"tk": t, "action": side_action, "px": px_f,
                                       "t_placed": t_placed, "ahead": ahead,
                                       "improved": improved,
                                       "spread_ticks": round((touch_a - touch_b) / _TICK)}
                    # Start watching the queue in front of this order (the private design notes M23). `px` is passed
                    # as the SENT STRING: the tracker keys levels by exact Decimal. `ahead` is forwarded
                    # as-is INCLUDING None — "depth unobserved" must not become "the queue was empty",
                    # which would flatter TRADE_THROUGH. `t_sent` is the DEPTH read, not the create call:
                    # `ahead`'s reference time is when we measured the book (mm-review C3).
                    self.qtrack.track(oid, t, side_action, px, args.size, ahead, t_placed,
                                      t_sent=t_depth, tape_ok=self.tape_ok)
                print(f"  {t[:32]:32} {side_action:4} {args.size}@{px} → {'rest '+oid[:8] if oid else 'dry/no-oid'}")
            except Exception as e:
                # post_only would-cross is the modal reject — it means the spread was takeable, i.e. NO
                # room to make. Counting rejects vs placed is real signal, not noise.
                stats["rejected"] += 1
                # Drop the durable intent. ⚠️ We do NOT know whether a raised create booked, so this is
                # not a claim that it didn't; it is bounded-file housekeeping (post_only rejects are the
                # MODAL outcome and a 24/7 maker would grow this file without limit). What protects a
                # lost create is the VENUE LISTING: the next cycle's `cancel_all` reconcile, the
                # teardown stray sweep, and `scripts/maker_recover.py`. The crash flag
                # (`clean_exit=False`) forces that last read to happen, and is untouched here.
                if self.state is not None:
                    with contextlib.suppress(Exception):
                        self.state.clear_order(intent_id)
                print(f"  {t[:32]:32} {side_action:4} @{px} REJECTED: {e!r}")

    def resolve_markouts(self):
        """Value any markout horizon that has come due. Costs ZERO rate-limit units — `_mid` is a read
        of the local WS book cache — so it can run far more often than the quote loop.

        ⚠️ THIS MUST NOT BE TIED TO THE REQUOTE INTERVAL. Resolving inside the cycle loop meant that at
        --requote-s 30 a 5s horizon could not be valued until age>=30 and fired on the same pass as the
        30s one, writing two rows with IDENTICAL mk under two labels."""
        MK_HORIZONS = self.MK_HORIZONS
        _mid = self.mid
        markout_pending = self.markout_pending
        w = self.w

        now = time.time()
        keep = []
        for ob in markout_pending:
            age, m = now - ob["t"], _mid(ob["tk"])
            for h in MK_HORIZONS:
                # mark a horizon done ONLY when we could actually value it — if the mid is unavailable
                # this tick, leave it pending and retry (don't lose the observation).
                if age >= h and h not in ob["done"] and m is not None:
                    ob["done"].add(h)
                    mk = (m - ob["px"]) if ob["action"] == "buy" else (ob["px"] - m)
                    # `h` is the horizon this row is FOR; `age` is when we actually valued it. Logging
                    # both makes a late valuation detectable — an analysis should discard rows where
                    # they diverge materially.
                    w.writerow([f"{now:.0f}", "markout",
                                f"{ob['tk']} {ob['action']} h={h:.0f}s age={age:.1f}s "
                                f"px={ob['px']:.4f} mid={m:.4f} mk={mk:+.4f} "
                                f"ts_src={ob['ts_src']}", "", "", f"{mk:+.4f}"])
            # keep until the LONGEST horizon is logged, but bound the lifecycle: if the mid never
            # returned, drop it anyway so the list can't grow unbounded on a persistently-dead book.
            if MK_HORIZONS[-1] not in ob["done"] and age < MK_HORIZONS[-1] * 2:
                keep.append(ob)
        markout_pending[:] = keep

    async def markout_ticker(self):
        """Resolve markouts every second, independently of the requote loop."""
        _resolve_markouts = self.resolve_markouts

        while True:
            await asyncio.sleep(1.0)
            try:
                _resolve_markouts()
            except Exception as e:                    # logging is never fatal
                print(f"  (markout resolve error, ignored: {e!r})")
            # Piggy-backed on the 1 Hz ticker rather than the requote loop, for the same reason the
            # markouts are: a tape outage has to be noticed while the affected orders are still LIVE.
            try:
                if self.tape is not None:
                    # `n_bad_trades`, NOT `n_dropped`: the wide counter includes every unrecognised
                    # control frame and the ack types are a guess.
                    # ⚠️ BOTH counters here are LOST-PRINT counters, which is what `note_tape_state`
                    # requires. `n_callback_errors` is deliberately NOT `errors`, which also accrues
                    # reconnect exceptions and venue `error` frames — an error storm fed in here would
                    # return a 100%-unreadable run.
                    self.qtrack.note_tape_state(
                        self.tape.n_reconnects,
                        self.tape.n_bad_trades + self.tape.n_callback_errors)
                self.qtrack.note_observer_errors(sum(self.book.level_observer_errors.values()))
            except Exception as e:                    # logging is never fatal
                print(f"  (tape-gap poll error, ignored: {e!r})")

    async def record_fills_and_markout(self):
        """Log REAL fills (actual price + fee + is_taker — the economics truth the mid-approximation
        can't give) and advance forward markout. Does NOT feed the loss cap; a read failure here only
        degrades logging. Markout(buy)=mid_now−fill_px, markout(sell)=fill_px−mid_now — NEGATIVE means
        we got picked off (adverse selection), the number that decides maker viability."""
        _mid = self.mid
        _resolve_markouts = self.resolve_markouts
        client = self.client
        filled_oids = self.filled_oids
        markout_pending = self.markout_pending
        order_meta = self.order_meta
        run_start = self.run_start
        seen_fills = self.seen_fills
        targets = self.targets
        w = self.w

        try:
            recent = await client.get_fills(min_ts=run_start, tickers=targets)
        except Exception as e:
            # ⚠️ MUST LEAVE A ROW. `recent = []` means `filled_oids` stays empty, so every order
            # cancelled this cycle is written `cancelled_unfilled` — a failed read indistinguishable
            # from a market that did not trade. The likeliest cause is a 429 on the fail-closed halt,
            # i.e. the most interesting cycle of the run, and a stdout print never reaches the CSV.
            print(f"  (fills read failed — outcomes this cycle are UNRELIABLE: {e!r})")
            w.writerow([f"{time.time():.0f}", "fills_read_failed", str(e)[:200], "", "", ""])
            recent = []
        # Our OWN filled quantity per order across this batch, summed BEFORE the loop marks ids seen.
        # The queue attribution subtracts it from the tape's print total to recover what traded AHEAD
        # of us; the venue partial-fills below one contract, so one order can appear as several records.
        # ⚠️ DECIMAL, not float: `float("0.33") + float("0.80")` is 1.1300000000000001 against the
        # tape's exact 1.13, which trips the `traded < filled` guard and reports UNKNOWN_TAPE_INCOMPLETE
        # — a label whose docstring calls it ARITHMETIC PROOF of a dropped print — on a healthy row.
        batch_filled: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
        for f in recent:
            _fid, _oid = f.get("fill_id"), f.get("order_id")
            if _fid and _oid and _fid not in seen_fills:
                try:
                    batch_filled[_oid] += Decimal(str(f.get("count_fp") or "0"))
                except (TypeError, ValueError, InvalidOperation):
                    pass            # unparseable count → the per-fill loop rejects it too
        for f in recent:
            fid = f.get("fill_id")
            if not fid or fid in seen_fills:
                continue
            # ⛔ FAIL-CLOSED EXCLUSION OF OUR OWN FORCED EXIT, keyed on the order we PLACED rather than
            # on a fill read that can 429. A flatten fill booked here becomes `real_fill` + a
            # `spread_capture` POSITIVE BY CONSTRUCTION + three markout horizons off one stale mid:
            # our own exit recorded as favourable market flow, on precisely the halts where it was
            # forced. `seen_fills` alone cannot carry this. [mm-review 2026-07-25]
            if f.get("order_id") in self.flatten_oids:
                seen_fills.add(fid)
                self.flatten_fill_ids.add(fid)
                # ⛔ WRITE IT, DO NOT JUST SKIP IT. A bare `continue` made the fill vanish from the
                # ledger entirely on the path this exclusion exists for: when `flatten_on_exit`'s own
                # `get_fills` 429s, the run asserted NOTHING FILLED while a real position move had
                # happened, and `reconcile_maker_pnl` blamed the VENUE with ❌ NO CONVENTION MATCHES.
                # Marked UNCONFIRMED because the flatten's accounting pass never saw it.
                # ⚠️ `_fill_ts` returns (epoch, source) — take [0]; the tuple repr would be dropped by
                # `reconcile_maker_pnl`'s `row[0].isdigit()` filter, reproducing the vanishing row.
                _fts, _ = _fill_ts(f)
                _fpx, _fn = f.get("yes_price_dollars"), f.get("count_fp")
                w.writerow([f"{int(_fts)}", "flatten_fill_UNCONFIRMED",
                            f"{f.get('ticker') or f.get('market_ticker')} {f.get('action')} "
                            f"n={_fn} px={_fpx} fee={f.get('fee_cost')} "
                            f"(booked by the teardown catch-up; flatten's own fills read failed)",
                            "", "", ""])
                continue
            seen_fills.add(fid)
            side, action = str(f.get("side") or ""), str(f.get("action") or "")
            tk = f.get("ticker") or f.get("market_ticker")
            try:
                # ALWAYS yes-space. A Kalshi fill record carries BOTH yes_price_dollars and
                # no_price_dollars, and a sell-YES books as side="no" (short YES == long NO), so keying
                # the price off `side` yields a NO-space price compared against a YES-space mid.
                # parse_wire, not float(): `px` feeds `spread_capture`, the column that IS the measured
                # maker edge (CLAUDE.md § Code style).
                raw_px = f.get("yes_price_dollars")
                px = parse_wire(raw_px) if raw_px not in (None, "") else None
                n = parse_wire(f.get("count_fp") or "0")
                fee = parse_wire(f.get("fee_cost") or "0")
            except (TypeError, ValueError, InvalidOperation):
                continue
            # A missing/out-of-range price would `or 0.0` into a phantom fill at px≈0.0 → spread_capture
            # ≈ ±0.5 (a ~100× outlier) → the maker-viability metric silently poisoned. Reject it, as
            # _quote_price(as_fill=True) does. `is_finite` first: `Decimal("NaN")` parses, and every
            # comparison against it RAISES.
            if px is None or not px.is_finite() or not (0 < px < 1):
                print(f"  ⚠️ skipping fill with unusable yes_price_dollars="
                      f"{f.get('yes_price_dollars')!r} (would poison spread_capture)")
                continue
            taker = bool(f.get("is_taker"))
            print(f"  ✎ real-fill {str(tk)[:26]:26} {side}/{action} {_fmt_qty(n)}@{px:.3f} "
                  f"fee={fee:.4f}{'  ⚠️TAKER(post_only leaked!)' if taker else ''}")
            w.writerow([f"{f.get('ts') or int(time.time())}", "real_fill",
                        f"{tk} {side} {action} n={_fmt_qty(n)} px={px:.4f} fee={fee:.4f} taker={taker}",
                        "", "", ""])
            # Direction comes from `action`, NOT `side`: we only ever send yes-space orders
            # (create_order(t, "yes", ...)), so action buy/sell IS our yes-space direction. Gating on
            # side == "yes" recorded NOTHING, because every sell-YES fill reports side="no".
            if action in ("buy", "sell"):
                # Anchor on the VENUE'S fill time, falling back to now only if unusable. The fallback
                # must be VISIBLE: `ts_src` records which anchor was used, so an analysis can drop
                # detection-anchored rows instead of averaging them in.
                fill_ts, ts_src = _fill_ts(f)
                mid_at_fill = _mid(tk)
                # Join the fill back to the order that produced it. `order_id` is on the fill record,
                # so this is an exact join rather than a price/time guess.
                oid = f.get("order_id")
                meta = order_meta.get(oid or "")
                if meta:
                    # BEFORE the add, so the queue attribution fires once per ORDER rather than once per
                    # partial fill. `fill_attrib` deliberately still writes per fill.
                    # ⚠️ WRAPPED, unlike `fill_attrib` below: `seen_fills.add(fid)` has already stamped
                    # this fill, so a raise here would permanently lose this fill's markout and
                    # spread_capture rows AND every later fill in the batch. The cap reads `_positions`,
                    # never this path, so nothing here can touch money.
                    if oid not in filled_oids:
                        try:
                            # `batch_filled` not `n`: the tape carries every partial's print, so
                            # subtracting only the FIRST partial leaves the rest counted as queue that
                            # traded ahead of us (mm-review C4).
                            self.log_queue_dynamics(
                                oid, outcome="fill", filled_qty=batch_filled.get(oid, n),
                                fill_ts=fill_ts if ts_src == "venue" else None)
                        except Exception as e:      # logging is never fatal
                            print(f"  (queue attribution failed for {oid[:8]} — non-fatal: {e!r})")
                    filled_oids.add(oid)
                    w.writerow([f"{fill_ts:.0f}", "fill_attrib",
                                f"{tk} {action} px={px:.4f} "
                                f"time_to_fill={max(0.0, fill_ts - meta['t_placed']):.1f}s "
                                f"queue_ahead={_fmt_ahead(meta['ahead'])} "
                                f"improved={'Y' if meta['improved'] else 'N'} "
                                f"spread_ticks={meta['spread_ticks']} ts_src={ts_src}", "", "", ""])
                elif oid:
                    # A fill we cannot attribute — a stray from a previous run, or a create whose
                    # response was lost. Say so rather than dropping it; an unattributed fill silently
                    # shrinks the denominator of every rate below.
                    w.writerow([f"{fill_ts:.0f}", "fill_attrib",
                                f"{tk} {action} px={px:.4f} UNATTRIBUTED order_id={oid[:8]}",
                                "", "", ""])
                markout_pending.append({"tk": tk, "action": action, "px": px, "t": fill_ts,
                                        "ts_src": ts_src, "done": set()})
                # REALIZED SPREAD CAPTURE — the maker edge itself, measured against the VENUE fill price
                # rather than a book-derived approximation. Signed so POSITIVE = we captured spread:
                # bought below the mid, or sold above it. Marked P&L books exit-side, which sees capture
                # but off the TOUCH, not off our sent limit (TODO M9); this column is right either way.
                if mid_at_fill is not None:
                    cap = (mid_at_fill - px) if action == "buy" else (px - mid_at_fill)
                    w.writerow([f"{fill_ts:.0f}", "spread_capture",
                                f"{tk} {action} px={px:.4f} mid={mid_at_fill:.4f} "
                                f"cap={cap:+.4f} ts_src={ts_src}", "", "", f"{cap:+.4f}"])
                else:
                    w.writerow([f"{fill_ts:.0f}", "spread_capture",
                                f"{tk} {action} px={px:.4f} mid=UNPRICEABLE cap=NA "
                                f"ts_src={ts_src}", "", "", ""])
        _resolve_markouts()


async def main() -> None:
    # ⚠️ LINE-BUFFER STDOUT. `print()` to a TERMINAL is line-buffered, but to a REDIRECTED FILE it is
    # block-buffered (~8 KB) — and every long run is redirected, which is exactly when you need to
    # watch it. Progress you cannot see is indistinguishable from a hang, and this tool has two real
    # runs that placed nothing and looked identical to a quiet market.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:                       # never fatal — see below for what can actually raise
        # The genuine raisers are a CLOSED stream (ValueError) and a replacement object without the
        # method (AttributeError) — both caught, while KeyboardInterrupt/SystemExit correctly are not.
        # `logging` is unaffected: reconfigure mutates the wrapper in place rather than rebinding
        # `sys.stdout`, so handlers already holding it keep a valid stream.
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=1200.0)
    ap.add_argument("--markets", type=int, default=3)
    ap.add_argument("--size", type=int, default=1)
    ap.add_argument("--inv-cap", type=float, default=3.0)
    # ⚠️ A RUNAWAY BACKSTOP, NOT A LOSS BOUND (B3). It compares an OPTIMISTIC exit-side touch mark, so
    # it is wrong in both directions: it can trip on nothing, and it cannot see the loss it is named
    # for — a real 6-minute run showed the cap's input at +<n> while the run realized −<n>. The
    # venue's `market_exposure_dollars`, written every cycle on `positions_raw_cycle`, is the actual
    # cost basis and the actual max loss. Do not size this as if it caps a loss.
    ap.add_argument("--loss-cap", type=float, default=5.0,
                    help="RUNAWAY BACKSTOP: halt + cancel-all if the OPTIMISTIC marked P&L < -this "
                         "($). Not a loss bound — see market_exposure_dollars for the real one")
    # DEFAULT 0 (no OBI skew) — deliberately. The old 0.006 was tuned while the get_depth mapping was
    # INVERTED, and post-fix measurement shows NO usable OBI signal on Kalshi (corr≈0, flat tail; the
    # signal is real on POLY, not here). Opt in explicitly once Kalshi data supports a sign.
    ap.add_argument("--obi-coef", type=float, default=0.0)
    ap.add_argument("--inv-coef", type=float, default=0.003)
    ap.add_argument("--improve-ticks", type=int, default=0,
                    help="step this many ticks INSIDE the touch on BOTH sides (needs enough spread, "
                         "else quotes at the touch). Buys queue priority, which is the binding "
                         "constraint for a size-1 maker; costs that much spread. 0 = quote at touch.")
    ap.add_argument("--requote-s", type=float, default=6.0)
    ap.add_argument("--flatten-on-exit", action="store_true",
                    help="On EVERY exit, INCLUDING a halt, post PASSIVE (post-only) reducing "
                         "orders to wind down OUR inventory before the sweep; any residual is left "
                         "for the sweep and still triggers the OPEN-POSITION warning. Never takes — "
                         "the post_only rail is preserved. (Was clean-exit-only until 2026-07-25; a "
                         "halt is when inventory most needs clearing, since the cap dies with the "
                         "process.)")
    ap.add_argument("--flatten-wait-s", type=float, default=60.0,
                    help="how long to let the --flatten-on-exit passive orders rest before recording "
                         "what filled and leaving the residual for the sweep (M16)")
    ap.add_argument("--min-price", type=float, default=0.15,
                    help="skip markets whose mid is below this (near-decided tail — bad for MM)")
    ap.add_argument("--max-price", type=float, default=0.85, help="skip markets whose mid is above this")
    ap.add_argument("--max-touch", type=float, default=None,
                    help="skip a market whose touch queue (worse side) exceeds this many contracts. "
                         "Raw volume ranks how much TRADES, not how much reaches US: behind a "
                         "16,000-deep touch a size-1 back-of-queue maker effectively never fills, so a "
                         "high-volume market can be strictly worse than a quiet one with a 47-deep "
                         "touch. Default None = off (unchanged behaviour); set it to make the queue a "
                         "hard gate rather than a hope. Unreadable depth counts as FAIL.")
    ap.add_argument("--maker-free-only", action="store_true",
                    help="hard-exclude maker-fee (M=1) series (MLB/WNBA/UCL/WC-game); only quote "
                         "maker-FREE series where the favorable markout survives")
    ap.add_argument("--pregame-only", action="store_true",
                    help="exclude games already in progress. The measured adverse markout is a LIVE "
                         "single-name-sports phenomenon (pregame 0%% tail vs live 5.8%%), so this is the "
                         "clean lane — at the cost of thinner flow")
    ap.add_argument("--series", type=str, default="", help="comma list to restrict to; default = all series")
    ap.add_argument("--ticker", type=str, default="",
                    help="comma list of EXACT market tickers to quote (overrides volume-ranked selection — "
                         "the supervisor's precise steering). Series is derived from the tickers. Named "
                         "tickers still pass the full eligibility/maker-free/contested/book validation; a "
                         "bad one is DROPPED, not force-quoted. No series fallback when set.")
    ap.add_argument("--confirm", action="store_true")
    ap.add_argument("--i-understand-real-money", action="store_true")
    args = ap.parse_args()

    client = KalshiClient()
    _live_guard(client)
    # ⚠️ `real` DOES NOT GATE ORDER PLACEMENT — the ONLY DRY short-circuit is `if config.DRY_RUN`
    # inside `KalshiClient.create_order`, so DRY_RUN alone decides whether money moves and `real` only
    # decides whether the run is INSTRUMENTED. `.env` DRY_RUN=false with no flag would place REAL
    # post-only quotes under a "DRY PREVIEW" banner, so the arb bot's go-live flip is a HARD STOP here.
    flagged = args.i_understand_real_money       # plain attribute: a renamed dest must raise here,
    #                                              not silently degrade to permanent DRY
    refusal = _real_money_refusal(config.DRY_RUN, flagged)
    if refusal:
        raise SystemExit(f"REFUSING: {refusal}")
    real = flagged and not config.DRY_RUN
    # One CSV accumulates every run, and it already held 11 DRY previews interleaved with 3 real runs
    # with nothing to tell them apart. Stamp each row's run and mode instead of hoping timestamps
    # disambiguate it.
    run_id = f"{'real' if real else 'dry'}-{int(time.time())}"
    # ⚠️ RATE-LIMIT GATE — this is a MONEY guard, not a tidiness one.
    # Kalshi bills /portfolio/* list reads at 10 units each; polling positions AND fills every cycle at
    # a 6s requote is ~90 units/cycle, which 429'd a real-money run within minutes and tripped its
    # fail-closed halt. The failure is not a lost log line: a 429 storm makes `cancel_order` fail, the
    # order is dropped from tracking while STILL RESTING, the stray sweep's listing also 429s, and the
    # process exits leaving live quotes with the loss cap dead. Refuse rather than warn.
    if real:
        # (1) The ordering fix REQUIRES per-cycle polling. `_cancel_all` runs every cycle but
        # `filled_oids` is only populated by the fill poll, so with _FILLS_EVERY=N>1, N−1 out of every
        # N cancels write their fills as `cancelled_unfilled`. Raising it is NOT a valid remedy for the
        # rate limit. Assert it instead of trusting a comment.
        if _FILLS_EVERY != 1:
            raise SystemExit(
                f"REFUSING: _FILLS_EVERY={_FILLS_EVERY} breaks the fill/cancel ordering invariant — "
                f"cancels run every cycle, so a fill between polls is logged as cancelled_unfilled. "
                f"Lengthen --requote-s to pay for the rate limit; never thin the poll.")
        # (2) Rate-limit budget, dominated by --markets, not by the requote interval:
        #     units/cycle = 20 (positions + fills) + markets · (2·10 create + 2·2 cancel)
        # The run that 429'd within minutes was markets=3 @ 6s ≈ 15.3 u/s; gating on requote-s ALONE let
        # `--markets 18 --requote-s 30` reproduce that rate while passing.
        # (3) The lean is rounded to whole ticks, so refuse only a coef whose max effect at the cap
        # rounds to ZERO ticks — a knob that never moves a quote must not look like it is working.
        skew_problem = _skew_guard(args.inv_coef, args.inv_cap)
        if skew_problem:
            raise SystemExit(f"REFUSING: {skew_problem}")
        units_per_s = _rate_limit_units_per_s(args.markets, args.requote_s)
        if units_per_s > _RATE_LIMIT_BUDGET_U_PER_S:
            raise SystemExit(
                f"REFUSING: ~{units_per_s:.1f} rate-limit units/s "
                f"(--markets {args.markets} @ --requote-s {args.requote_s:g}). The run that 429'd and "
                f"left orders resting with the loss cap dead was ~15.3 u/s; this budget caps "
                f"at {_RATE_LIMIT_BUDGET_U_PER_S:.1f}. "
                f"Raise --requote-s or lower --markets "
                f"(e.g. --markets {args.markets} needs --requote-s "
                f"{(20.0 + 24.0 * args.markets) / _RATE_LIMIT_BUDGET_U_PER_S:.0f}).")
    print(f"LIVE paper-MM | venue={client._base_url} | REAL ORDERS={real}\n"
          f"  size={args.size} inv_cap=±{args.inv_cap:.0f} loss_cap=${args.loss_cap:.2f} "
          f"markets={args.markets} for {args.seconds:.0f}s")
    if not real:
        print("  ⚠️ DRY PREVIEW — create_order short-circuits, NO real orders placed. "
              "Add --i-understand-real-money to place real orders.")

    # --ticker: quote EXACTLY the named markets (the supervisor's precise steering, since _two_sided
    # ranks by VOLUME not spread width). The series is derived from the tickers so --ticker works
    # standalone. The named set still runs the FULL validation pipeline below (eligibility + maker-free
    # + contested + book), so a closing / charged / one-sided / out-of-band ticker is DROPPED.
    want_tickers = {t.strip() for t in args.ticker.split(",") if t.strip()}
    if want_tickers:
        series = sorted({t.split("-", 1)[0] for t in want_tickers})
    else:
        series = [s.strip() for s in args.series.split(",") if s.strip()] or config.KALSHI_SERIES
    mkts = await KalshiScanner(client).fetch_markets(series)
    if want_tickers:
        mkts = [m for m in mkts if m.ticker in want_tickers]
        if not mkts:
            print(f"--ticker {sorted(want_tickers)}: not found in series {series} — "
                  f"check the ticker (it may have already settled and dropped off the board).")
            await client.close(); return
    await _load_maker_fees(client, [m.ticker for m in mkts])
    vol_of = {m.ticker: m.volume_24h for m in mkts}
    band = {"min_price": args.min_price, "max_price": args.max_price,
            "maker_free_only": args.maker_free_only, "vol_of": vol_of,
            "max_touch": args.max_touch}

    def _eligible(ms):
        return eligible_tickers(ms, pregame_only=args.pregame_only)

    fallback_mkts: list = []
    targets = await _two_sided(client, _eligible(mkts), args.markets, **band)
    if not targets and not want_tickers and series != config.KALSHI_SERIES:
        fallback_mkts = await KalshiScanner(client).fetch_markets(config.KALSHI_SERIES)
        band["vol_of"] = {m.ticker: m.volume_24h for m in fallback_mkts}
        targets = await _two_sided(client, _eligible(fallback_mkts), args.markets, **band)
    if not targets:
        extra = " AND maker-free (M=0)" if args.maker_free_only else ""
        hints = ([" Or drop --maker-free-only."] if args.maker_free_only else []) + \
                ([" Or drop --pregame-only (every game may already be in progress)."]
                 if args.pregame_only else [])
        print(f"no CONTESTED two-sided{extra} quotable market right now (need mid in "
              f"[{args.min_price:.2f}, {args.max_price:.2f}] — competitive, not near-decided — and a "
              f"market that has not already passed its expiry)." + "".join(hints))
        await client.close(); return

    # Annotate each target with its phase + flow so the operator can see WHY it was picked. LIVE
    # single-name sports is the measured-adverse lane (3/3 adverse, worst −<n>/6s) and pregame is the
    # clean one, so a LIVE pick is called out, not just labelled.
    m_of = {m.ticker: m for m in mkts + fallback_mkts}

    def _liveness(t: str) -> str:
        m = m_of.get(t)
        if m is None:
            return "timing unknown"
        ph = _phase(m)
        e = _expiry_ts(m)
        if ph == "live":
            return "LIVE (in progress) ⚠️ adverse lane"
        if e is None:
            return "timing unknown"
        # Say EXPIRES, never "starts": the old line printed "starts in ~Xh" off the same timestamp, so
        # the same slot silently changed basis by 3h. For a weather or macro market there is no start.
        return f"{ph} (expires in ~{(e - time.time()) / 3600.0:.1f}h)"

    print("  targets:")
    for t in targets:
        maker = "maker M=0 FREE ✅" if _maker_mult(t) == 0 else "maker M=1 fee per fill ⚠️"
        v = band["vol_of"].get(t, 0.0)
        print(f"    {t}  [{_liveness(t)}]  {maker}  vol24={v:,.0f}")
    n_m1 = sum(1 for t in targets if _maker_mult(t))
    if n_m1:
        print(f"  ⚠️ {n_m1}/{len(targets)} target(s) charge the maker fee (M=1) — that roughly "
              f"CANCELS the favorable markout on those. ~97% of Kalshi series are maker-FREE — including "
              f"the sports DERIVATIVES (KXMLBTOTAL/SPREAD/RFI) of charged moneylines; pass "
              f"--maker-free-only to force them. NOTE: NBA/NHL are CHARGED, not free.")
    if all(_phase(m_of[t]) in ("pregame", "long-dated") for t in targets if t in m_of):
        print("  ⚠️ NONE are in-progress. That is the CLEAN lane for markout, but flow is thinner — if "
              "this run is measuring THROUGHPUT, prefer a high-volume series (see vol24 above).")
    if not args.confirm:
        print("  preview only — add --confirm (and --i-understand-real-money for real orders)."); await client.close(); return

    # ⚠️ The prefix list is a HARD FILTER on inbound WS messages (`feed._matches_prefix`) and defaults
    # to config.KALSHI_SERIES — the 12 cross-arb SPORTS series. Any target outside that set had every
    # book message dropped, so the book never populated and the run placed ZERO orders for its full
    # duration while WS connect/subscribe logged normally. Two real-money runs ended quotes=0 fills=0
    # this way. The MM universe is ~97% of Kalshi, NOT the arb set, so --series must drive the feed too.
    book = KalshiOrderBookCache(client, series_prefixes=feed_prefixes(targets))
    book.set_book_tickers(targets)
    book_task = asyncio.create_task(book.run_forever())
    # ── Order provenance, keyed by the venue's order_id (which fills carry, so the join is exact).
    # SIDE TABLE ON PURPOSE: `resting` and the cancel path are untouched, so nothing about what gets
    # placed or cancelled changes. It answers the three questions a fills-only log cannot:
    #   · how much depth was AHEAD of us when we quoted   (did improvement actually buy priority?)
    #   · how long the order sat before filling            (time-to-fill)
    #   · what happened to the ones that did NOT fill      (cancelled unfilled, and how old)
    halted = False
    # WHY the run stopped, for the heartbeat's exit status. The CSV records each reason on its own
    # `halt` row; this carries it to the DEADMAN, where a halt must not read as a clean exit and a
    # clean exit must not page anyone. Set beside every `halted = True`.
    halt_reason = ""


    fh = open("logs/kalshi_live_mm.csv", "a", newline="")
    _raw_w = csv.writer(fh)
    if fh.tell() == 0:
        _raw_w.writerow(["ts", "event", "detail", "cash", "inv", "pnl", "run_id"])

    class _RunWriter:
        """csv.writer that stamps every row with this run's id and mode.

        Wrapped rather than threaded through ~20 call sites: a stamp that depends on remembering to
        pass it is a stamp that will be missing from the one row that matters. Rows predating this have
        6 columns and no id — treat a missing run_id as 'unknown run', not as part of this one."""

        @staticmethod
        def writerow(row):
            _raw_w.writerow(list(row) + [run_id])
            # ⚠️ FLUSH EVERY ROW. Without this the CSV is block-buffered (~8 KB), so a 90-minute
            # real-money run is INVISIBLE while it runs, and a hard kill (SIGKILL/OOM — a graceful exit
            # still flushes via `fh.close()`) silently discards the tail. One line here covers ~20 call
            # sites, which is why the writer is wrapped in the first place.
            fh.flush()

    w = _RunWriter()







    # ── preflight (real runs): snapshot balance (CASH, ⚠️ not P&L — snapshotted for the delta, not as
    #    truth) and clean the target markets of any leftover resting orders so a prior-run stray
    #    can't contaminate this run's fills. ──
    start_bal = None
    if real:
        try:
            allrest = await client.get_resting_orders()
        except Exception as e:
            print(f"  ⚠️ preflight: could not list resting orders: {e!r}")
            allrest = None
        if allrest:
            in_tgt = [o for o in allrest if o.get("ticker") in set(targets)]
            others = len(allrest) - len(in_tgt)
            print(f"  preflight: {len(allrest)} resting order(s) on account "
                  f"({len(in_tgt)} in target markets, {others} elsewhere).")
            for o in in_tgt:            # clean the baseline — a leftover stray would pollute fills/markout
                oid = o.get("order_id")
                if oid:
                    try:
                        await client.cancel_order(oid)
                        print(f"    preflight-cancelled {oid[:8]} ({o.get('ticker')})")
                    except Exception as e:
                        print(f"    {oid[:8]} cancel failed: {e!r}")
            if others:
                print(f"  ⚠️ {others} resting order(s) in OTHER markets left untouched — likely strays "
                      f"from a prior run; check the Kalshi UI.")

    # ⚠️ THE BASELINE IS READ **AFTER** THE PREFLIGHT STRAY CANCEL [M6]. Safety: this refusal is a bare
    # `return` ahead of the `try`, so it does NOT run the teardown's venue sweep, and read first a 429
    # here left a PRIOR run's orders unlisted. Correctness: the preflight cleans the baseline, which
    # only holds if it happens BEFORE the baseline is taken. ⚠️ RESIDUAL: a stray that FILLS during the
    # preflight is absorbed into `base`, and `--inv-cap` and the loss cap are both `cur − base`, so it
    # sits OUTSIDE them. ⚠️ THE BALANCE SNAPSHOT MUST STAY ADJACENT TO THE BASELINE, on the SAME side
    # of the cancel: cancelling a resting order releases its collateral back to cash.
    if real:
        try:
            start_bal = await client.get_balance()
            w.writerow([f"{time.time():.0f}", "balance_start", "", f"{start_bal:.4f}", "", ""])
            print(f"  preflight: starting balance ${start_bal:.2f}")
        except Exception as e:
            print(f"  ⚠️ preflight: balance read failed (CASH cross-check degraded — see the header: this is cash, not P&L): {e!r}")

    # ── SAY WHICH FILE THE KILL SWITCH IS WATCHING, resolved against THIS process's cwd ──
    # `KILL_SWITCH_FILE` is a RELATIVE default and this program is launched by hand (no systemd
    # `WorkingDirectory=`), so a maker started from ~ watches ~/pause.json while `scripts/show_config`,
    # run from the repo, prints the repo one — and an operator who touches that gets no error and no
    # effect. The empty-value case is louder still: `is_paused()` is then a constant False.
    _ks = config.KILL_SWITCH_FILE
    if _ks:
        _ks_abs = os.path.abspath(_ks)
        print(f"  kill switch: watching {_ks_abs} "
              f"({'⚠️ PRESENT — this run will halt on its first cycle' if os.path.exists(_ks_abs) else 'absent'})"
              f"  — `touch` it to halt this run")
        w.writerow([f"{time.time():.0f}", "kill_switch_path", _ks_abs, "", "", ""])
    else:
        print("  ⛔ kill switch DISABLED (KILL_SWITCH_FILE is empty) — there is NO way to halt this "
              "run short of SIGINT/SIGTERM.")
        w.writerow([f"{time.time():.0f}", "kill_switch_path", "DISABLED", "", "", ""])
    # ⚠️ `raw_out` here is the PRE-TRADE baseline and the only one there is. `E`/`R`/`F`
    # (market_exposure_dollars / realized_pnl_dollars / fees_paid_dollars) are LIFETIME per-market, so
    # M8(b)'s `pnl = (R − R_base) − (F − F_base) + Σ[mark − (E − E_base)]` is unanchored without it, and
    # the first in-loop `positions_raw_cycle` row is written AFTER the first quote round.
    base_raw: list = []
    base = await _positions(client, targets=targets, raw_out=base_raw)  # baseline: only OUR fills count
    if base is None:
        print("REFUSING: cannot read baseline positions (get_positions failed) — won't start blind.")
        await client.close(); return
    last = dict(base)
    # EXACT. `cash` accumulates across every cycle and is one of the two terms in the number the LOSS
    # CAP is compared against, so a float here drifts from the first fill onward (`3 × 0.47` is
    # 1.4100000000000001) and every later comparison inherits it.
    cash = _ZERO
    # `--loss-cap` / `--inv-cap` arrive from argparse as floats. Cross that boundary ONCE, here, so the
    # comparisons below are exact and there is a single named conversion rather than one per use.
    loss_cap = from_float(args.loss_cap)
    if real:
        # The pre-trade E/R/F baseline, written BEFORE any order is placed. Wrapped: logging must never
        # touch the money path. Not filtered to targets — the selected market is decided below.
        try:
            w.writerow([f"{time.time():.0f}", "positions_raw_base", json.dumps(base_raw), "", "", ""])
        except Exception as e:
            print(f"  (baseline positions_raw log failed — non-fatal: {e!r})")

    # ── economics-measurement state. ADDITIVE and fully separate from the positions-based loss cap:
    #    a read failure here degrades LOGGING only, never the kill-switch. ──
    run_start = int(time.time())
    _cycle = 0

    # ── The order lifecycle lives in MakerSession, which is testable; main() keeps orchestration.
    #    ⚠️ THE EXTRACTION WAS NOT BEHAVIOUR-PRESERVING: re-binding from `self` preserves MUTATION and
    #    not REBINDING. `inv` is rebound (`_mark_step` returns a fresh dict), so `sess.inv` froze at
    #    zeros and both `--inv-cap` and `--inv-coef` went inert. Check before adding any binding here.
    sess = MakerSession(client, book, w, args, targets, run_start)
    _mid = sess.mid
    _quote_of = sess.quote_of
    _log_unfilled = sess.log_unfilled
    _cancel_all = sess.cancel_all
    _cancel_everything = sess.cancel_everything
    _sweep_venue_strays = sess.sweep_venue_strays
    _quote = sess.quote
    _resolve_markouts = sess.resolve_markouts
    _markout_ticker = sess.markout_ticker
    _record_fills_and_markout = sess.record_fills_and_markout
    # Shared state. `resting`, `order_meta`, `filled_oids`, `stats`, `seen_fills` are the SAME objects
    # for the whole run (mutate-only), so main()'s reads track the session.
    # ⚠️ `inv` DOES NOT: `_mark_step` rebinds main's local to a fresh dict every cycle, so this alias
    # and `sess.inv` are kept equal only by the `sess.sync_inventory(inv)` call below.
    resting, order_meta, filled_oids = sess.resting, sess.order_meta, sess.filled_oids
    inv, stats, seen_fills = sess.inv, sess.stats, sess.seen_fills
    markout_pending = sess.markout_pending
    MK_HORIZONS = MakerSession.MK_HORIZONS
    # Markout horizons, measured from the VENUE'S FILL TIMESTAMP (`f["ts"]`).
    # ⚠️ Measured from fill DETECTION instead, the 2026-07-19 run wrote all 12 markout rows at
    # `age=151s`: `_FILLS_EVERY=5` at --requote-s 30 discovered a fill up to 150s late and BOTH horizons
    # fired on one pass — one 2.5-minute number recorded twice, labelled 5s and 30s. Polling every cycle
    # keeps discovery lag under one requote.




    # Runs only on real runs — DRY places nothing, so there is nothing to mark out.
    mk_task = asyncio.create_task(_markout_ticker()) if real else None

    # ── QUEUE ATTRIBUTION (the private design notes M23) ─────────────────────────────────────────────────────
    # Half the answer is the L2 delta stream (what LEFT our price level), half is the trade tape (what
    # TRADED there). Both are consumed on THIS process's clock, which is why this lives in the maker:
    # the offline scorer had to join fills against a separately-collected tape across whole-second
    # timestamps, and that join is what made it unsalvageable. Nothing here changes what gets placed,
    # sized or cancelled. Real runs only: DRY places no orders, so there is no queue to attribute.
    if real:
        book.set_level_observer(sess.qtrack.on_level)
        sess.tape = KalshiTradeFeed(client, targets, sess.qtrack.on_trade)
        tape_task = asyncio.create_task(sess.tape.run_forever())
    else:
        tape_task = None

    # ── OPERATIONAL RAILS (bot/core/{heartbeat,maker_state,memguard}.py) ────────────────────────
    # All three exist for ONE death: SIGKILL, which runs no `finally`, so the record has to be on disk
    # BEFORE it is needed and the observer has to be a different process. This box OOM-kills roughly
    # every other day.
    # ⚠️ THE DURABLE STATE IS REAL-RUNS ONLY: letting a DRY preview call `begin_run` would reset a
    # crashed REAL run's order list. The HEARTBEAT beats either way.
    run_id = f"{'real' if real else 'dry'}-{run_start}"
    hb = heartbeat.Heartbeat(
        "kalshi_maker", interval_s=float(args.requote_s), directory=config.HEARTBEAT_DIR,
        # The teardown legitimately blocks for up to --flatten-wait-s with no beats, so the deadman
        # budget has to cover it or every flatten looks like a death.
        stale_after_s=max(3.0 * args.requote_s, args.requote_s + 30.0)
        + float(getattr(args, "flatten_wait_s", 0.0) or 0.0) + 60.0)
    hb.beat(markets_quoted=0, inventory=0.0, marked_pnl=0.0, phase="starting", run_id=run_id)

    state: maker_state.MakerStateStore | None = None
    if real:
        try:
            # Venue-split path []: this store opens the KALSHI file and adopts a
            # kalshi-prefixed legacy record once; a Poly record can no longer be reset here.
            state = maker_state.store_for_venue("kalshi", base_path=config.MAKER_STATE_FILE)
            # inventory={} ALWAYS on the Kalshi arm [design v2 §B]: this maker's inventory is a DELTA
            # from the venue baseline read (`_mark_step`: inv = venue − base), so pre-existing positions
            # are baseline to ignore. There is no carry flow on this venue.
            state.begin_run(run_id, mode="real", loss_cap=loss_cap,
                            tickers=targets, inventory={})
            sess.state = state
        except Exception as e:
            # Covers BOTH `PriorRunUnresolved` (a previous run died holding orders — starting would
            # erase the only record of what is resting) and `StateCorrupt` (the SIGKILL-mid-write
            # artefact). A maker that trades on top of an unresolved crash turns a recoverable strand
            # into an invisible one.
            print(f"REFUSING TO START — the crash-durable maker state is not in a startable "
                  f"condition:\n{e}")
            w.writerow([f"{time.time():.0f}", "refused", "prior_run_unresolved", "", "", ""])
            hb.mark_exit("halted:prior_run_unresolved")
            await client.close()
            return

    t0 = time.time()
    try:
        # Refuse to start blind on the BOOK, exactly as we already refuse to start blind on POSITIONS.
        # Without this the quote path returns early every cycle and the run burns its whole duration
        # placing nothing, with no error anywhere (two real-money runs ended quotes=0 fills=0 this way).
        # Placed INSIDE the try, after the stray preflight, so a refusal still runs the full `finally`.
        if not await wait_for_book(book, targets, tries=_BOOK_WARMUP_TRIES, delay=_BOOK_WARMUP_S):
            print(f"REFUSING: no two-sided WS book for ANY target after "
                  f"{_BOOK_WARMUP_TRIES * _BOOK_WARMUP_S:.0f}s — the quote path would place nothing "
                  f"all run. Check that the feed's series prefixes cover these tickers.")
            w.writerow([f"{time.time():.0f}", "refused", "no_two_sided_book", "", "", ""])
            halted, halt_reason = True, "no_two_sided_book"
        # ── Is the tape actually live? (the private design notes M4 — the flag nothing read) ──
        # Deliberately a WARNING, not a refusal: the tape is pure instrumentation, and refusing to trade
        # because a logging feed is down would let an observability fault dictate a money decision.
        # Every `queue_dynamics` row records `tape=DOWN` and degrades to UNKNOWN_TAPE_DOWN rather than
        # reading the resulting `traded=0` as "the queue in front of us pulled".
        if sess.tape is not None and not halted:
            tape_note = ("ok" if sess.tape_ok else
                         f"NOT_ACKED dropped={sess.tape.n_dropped} "
                         f"last_error={sess.tape.last_error or 'none'}")
            if not sess.tape_ok:
                print(f"  ⚠️ TRADE TAPE NOT CONFIRMED ({tape_note}) — trading proceeds, but the "
                      f"traded-vs-cancelled split will read UNKNOWN_TAPE_DOWN for this run.")
            # ⚠️ The queue attribution needs ORDERBOOK mode and fails SILENTLY without it: outside it
            # `get_depth` returns None, `quote()` coerces that with `or 0.0`, and "depth unobserved"
            # becomes "empty queue" — every fill scores ALONE_AT_PRICE, which looks like data.
            if config.KALSHI_PRICE_SOURCE != "orderbook":
                tape_note += f" ⚠️NO_L2(KALSHI_PRICE_SOURCE={config.KALSHI_PRICE_SOURCE})"
                print(f"  ⚠️ KALSHI_PRICE_SOURCE={config.KALSHI_PRICE_SOURCE!r}, not 'orderbook' — "
                      f"there is NO level feed, so the queue attribution will collect nothing "
                      f"usable this run.")
            w.writerow([f"{time.time():.0f}", "tape_status", tape_note, "", "", ""])
        while time.time() - t0 < args.seconds and not halted:
            # ── THE OPERATOR'S KILL SWITCH, checked FIRST so no cycle can quote past it ──
            # `touch pause.json` (= config.KILL_SWITCH_FILE, a RELATIVE default, and the maker is
            # launched by hand) is what the private design notes documents as THE way to stop trading without a
            # restart. `break`, not a bare `return`: it hands off to the SAME `finally` the loss-cap halt
            # takes — cancel-all → optional passive flatten → venue stray sweep, sweep LAST.
            # ⚠️ SO A KILL-SWITCH HALT CAN PLACE AN ORDER AND THEN WAIT (up to --flatten-wait-s): it is
            # post-only and reduces OUR net, but with an inherited `base` that can INCREASE the account's
            # position and can be a BUY. Latency is ~70s+ at the supervisor's 10s/60s, not one requote.
            if is_paused():
                print("  🛑 KILL SWITCH: pause file present — halting. Cancelling all resting "
                      "orders, then (if --flatten-on-exit) placing ONE passive reducing order and "
                      "waiting --flatten-wait-s for it, then sweeping the venue. A new resting "
                      "order appearing now is that flatten, not a failed halt. "
                      "Remove the file before restarting.")
                w.writerow([f"{time.time():.0f}", "halt", "kill_switch", f"{cash:.4f}", "", ""])
                halted, halt_reason = True, "kill_switch"
                break
            # ── MEMORY HEADROOM, checked beside the kill switch because it is the same question: should
            # this process still be running? Six OOM kills on record, and the OOM killer sends SIGKILL —
            # no teardown, no cancel-all, live orders left resting. Halting here takes the SAME proven
            # teardown while there is still memory left to run it.
            # ⚠️ Fail-OPEN, unlike every position read in this file: an unreadable /proc carries no
            # information about memory pressure, so it must not become the outage.
            try:
                _mem = memguard.check(label="kalshi_maker")
            except Exception as e:
                print(f"  (memory check failed — non-fatal, run continues: {e!r})")
                _mem = None
            if _mem is not None and _mem.should_halt:
                print(f"  🛑 MEMORY GUARD: {_mem.detail} — halting cleanly before the OOM killer "
                      f"does it with SIGKILL (which would leave orders resting).")
                w.writerow([f"{time.time():.0f}", "halt", "memory", f"{cash:.4f}", "",
                            _mem.detail])
                halted, halt_reason = True, "memory"
                break
            # ⚠️ FILLS ARE POLLED BEFORE QUOTING, and the order matters. `_quote` starts by cancelling
            # last cycle's orders, and `_log_unfilled` decides "did this order fill?" from `filled_oids`,
            # which only `_record_fills_and_markout` populates. Polling AFTER the cancel wrote an order
            # that filled during its resting life as `cancelled_unfilled` AND, moments later, as a fill —
            # at --requote-s 30 that mislabels ~95% of fills as non-fills.
            if real and (_cycle % _FILLS_EVERY == 0):
                try:
                    await _record_fills_and_markout()
                except Exception as e:
                    print(f"  (fills/markout logging error — non-fatal, cap unaffected: {e!r})")
            for t in targets:
                await _quote(t)
            # ── fills + loss kill-switch (authoritative from get_positions) ──
            # `_cycle_raw` rides along on the SAME read (zero extra rate-limit units) so every cycle's
            # venue record — market_exposure_dollars / realized_pnl_dollars / fees_paid_dollars — is
            # logged next to that cycle's fills. Diagnostic only; nothing below reads it (M8(b)).
            _cycle_raw: list = []
            cur = await _positions(client, targets=targets, raw_out=_cycle_raw if real else None)
            if cur is None:                              # CANNOT-VERIFY → fail CLOSED
                print("  🛑 positions unreadable — fail-closed: halt + cancel-all.")
                w.writerow([f"{time.time():.0f}", "halt", "positions_cannot_verify", f"{cash:.4f}", "", ""])
                halted, halt_reason = True, "positions_cannot_verify"
                break
            if real:
                # One row per cycle carrying only the TARGET markets' raw records — the reconciliation
                # input for M8(b). Wrapped: a logging failure must never touch the cap.
                # ⚠️ GATED ON THE READ SUCCEEDING (`cur is not None`), NOT on the list being non-empty:
                # `if _cycle_raw:` suppressed the E/R/F BASELINE on a never-traded target.
                try:
                    _tgt = set(targets)
                    w.writerow([f"{time.time():.0f}", "positions_raw_cycle",
                                json.dumps([p for p in _cycle_raw if p.get("ticker") in _tgt]),
                                "", "", ""])
                except Exception as e:
                    print(f"  (per-cycle positions_raw log failed — non-fatal: {e!r})")
            mids = {t: _quote_of(t) for t in targets}   # (bid, ask) — see _quote_price
            cash, inv, pnl, last, fills = _mark_step(targets, base, last, cur, mids, cash)
            # ⚠️ WRITE IT BACK. `_mark_step` RETURNS A NEW DICT rather than mutating, so rebinding the
            # local `inv` leaves `sess.inv` — which `quote()` reads for the inventory cap and the skew —
            # frozen at all-zeros for the life of the run. Pre-refactor this worked by accident via the
            # closure cell. Mutate in place rather than reassigning, so every holder stays correct.
            sess.sync_inventory(inv)
            # ── HEARTBEAT + durable inventory, once per cycle ────────────────────────────────────
            # Placed HERE because this is the first point where all three operator-facing numbers exist
            # together, and BEFORE the loss-cap break so a halting cycle still beats.
            # ⚠️ `marked_pnl`, not `pnl`: this is `_mark_step`'s MARKED figure, and marking a fill at the
            # same touch that priced it makes P&L 0 at the fill by construction — a venue-confirmed
            # −<n> was fed to the cap as +<n> (health audit addendum 1.2, still BLOCKING). The field
            # name says "marked" so nobody downstream reads it as realized.
            try:
                # FLOAT BOUNDARY: the heartbeat is a JSON file for an out-of-process deadman, and
                # `json.dumps` cannot serialise a Decimal. Reporting only — nothing reads these back
                # into a money decision.
                hb.beat(markets_quoted=len(targets),
                        inventory=float(sum(inv.values(), _ZERO)),
                        inventory_by_ticker={t: float(round(q, 4)) for t, q in inv.items()},
                        marked_pnl=float(pnl), cash=float(cash), cycle=_cycle,
                        fills=len(sess.seen_fills), run_id=run_id, phase="quoting")
            except Exception as e:
                print(f"  (heartbeat write failed — non-fatal: {e!r})")
            if state is not None:
                try:
                    # `dict(inv)` — the values are already exact Decimals. This used to be
                    # `Decimal(repr(q))` because `q` was a float; that hop is gone with the float.
                    state.set_inventory(dict(inv))
                except Exception as e:
                    print(f"  (durable inventory write failed — non-fatal: {e!r})")
            for t, d, m in fills:
                # 2dp not %.0f: position_fp comes back FRACTIONAL and %.0f masked it as clean ±1 / "-0".
                # ⚠️ NOT an anomaly: the venue partial-fills below one contract (a resting 1-lot sell
                # returned initial_count_fp 1.00 → fill_count_fp 0.11), and every snapshotted position
                # reconciles to the signed sum of count_fp. Never round the display below its precision.
                print(f"  FILL {t[:30]} Δ={d:+.2f} @~{m:.3f}")
                w.writerow([f"{time.time():.0f}", "fill", f"{t} {d:+.2f}", f"{cash:.4f}",
                            f"{inv.get(t, _ZERO):.4f}", ""])
            _cycle += 1
            # ⚠️ SHOW THE VERDICT MIX WHILE THE RUN IS STILL ABORTABLE. A run whose tape is flaky returns
            # ~100% UNKNOWN_* — safe-direction, but discovering that at teardown wastes the session.
            # Printed, not logged: the CSV already has every row, this is for the operator watching.
            if sess.verdicts:
                print("  verdicts: " + " ".join(f"{k}={v}" for k, v in
                                                sorted(sess.verdicts.items())))
            # Defence in depth: the cap is a COMPARISON, and a non-finite pnl makes it silently False
            # (nan < -5.0 is False) — the cap would look armed while protecting nothing. A position can
            # no longer be non-finite (see _positions), but a mid still can.
            # ⚠️ `is_finite()`, not `math.isfinite()`: under Decimal every comparison against NaN RAISES,
            # so an unguarded compare would take out the quote loop instead. Check first, as before.
            if not pnl.is_finite():
                print(f"  🛑 marked P&L is not finite ({pnl!r}) — cannot evaluate the loss cap; "
                      f"failing closed: halt + cancel-all.")
                w.writerow([f"{time.time():.0f}", "halt", "pnl_not_finite", f"{cash:.4f}", "", ""])
                halted, halt_reason = True, "pnl_not_finite"
                break
            # ⚠️ EXACT COMPARE. `pnl` is the venue's own arithmetic, so `pnl < -loss_cap` fires exactly
            # where it reads: strictly BELOW the cap. On the float path a drawdown of exactly the cap
            # computed as −0.30000000000000027 and halted a representation error early.
            if pnl < -loss_cap:
                print(f"  🛑 LOSS KILL-SWITCH: marked P&L ${pnl:+.2f} < −${loss_cap:.2f} — halting.")
                w.writerow([f"{time.time():.0f}", "halt", "loss_cap", f"{cash:.4f}", "", f"{pnl:.4f}"])
                halted, halt_reason = True, "loss_cap"
                break
            await asyncio.sleep(args.requote_s)
    finally:
        # ⚠️ POLL FILLS BEFORE THE FINAL CANCEL, for the same reason the loop does: cancel-first wrote
        # the FINAL cycle's orders as `cancelled_unfilled` even when they had filled, and on a run that
        # exits early via the loss-cap break that is the most interesting cycle of the lot. It also has
        # to happen before `book_task.cancel()` — spread capture IS the maker edge, and marking it off a
        # frozen mid is worse than not recording it.
        if real:
            try:
                await _record_fills_and_markout()
            except Exception as e:
                print(f"  (final fills/markout read failed — outcomes may be mislabelled: {e!r})")
        if mk_task is not None:
            mk_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await mk_task
        print("\n  cancelling all resting orders...")
        await _cancel_everything()
        # : optional passive flatten of OUR inventory, BEFORE the sweep so its residual passive
        # orders are cancelled by the sweep (the sweep is in a `finally`, so a second signal cannot skip
        # it). ⛔ THE `not halted` GATE IS GONE (B3): the process is EXITING and the loss cap dies with
        # it. Safe because the flatten is PASSIVE, post-only, reduces OUR NET so it cannot cross, and
        # self-refuses on an unreadable venue read. ⚠️ "REDUCING" MEANS *OUR NET*: with inherited
        # `base` (+5, run sold 4), the flatten posts a passive BUY 4 and the account is back to +5 long.
        try:
            if _should_flatten(real, args.flatten_on_exit, halted):
                await sess.flatten_on_exit(base, args.flatten_wait_s)
        except Exception as e:
            print(f"  ⚠️ flatten-on-exit raised (non-fatal — sweep still runs): {e!r}")
        finally:
            swept_clean = False
            if real:
                # reach lost-create strays + the flatten's residual
                swept_clean = bool(await _sweep_venue_strays())
            # ── CLOSE THE CRASH RECORD — but only on the VENUE'S evidence ───────────────────────
            # `swept_clean` is True only when the listing succeeded and nothing is left resting. If the
            # venue could not be read, or a stray would not cancel, the record stays OPEN
            # (`clean_exit=False`) so the next start REFUSES and `scripts/maker_recover.py` runs — this
            # flag is what tells recovery there is nothing to do.
            if state is not None:
                try:
                    if swept_clean:
                        for _iid in list(state.snapshot().orders):
                            state.clear_order(_iid)
                        state.end_run(f"halted:{halt_reason}" if halted else "clean")
                    else:
                        print("  ⚠️ the venue could not confirm that nothing is resting — leaving "
                              "the crash record OPEN. The next maker start will refuse until "
                              "the Kalshi recovery tool (see the operator runbook) resolves it.")
                except Exception as e:
                    print(f"  (durable state teardown failed — non-fatal: {e!r})")
        # Catch-up poll for fills that landed DURING teardown. It must run before `book_task.cancel()` —
        # it writes spread_capture and markout rows, and a mid read from a dead book is not a mark. It
        # cannot fix outcome labels, so its job is purely to complete the fill record.
        if real:
            try:
                await _record_fills_and_markout()
            except Exception as e:
                print(f"  (teardown fills catch-up failed — non-fatal: {e!r})")
        book_task.cancel()
        try:
            await book_task
        except asyncio.CancelledError:
            pass
        # ── Tape teardown + the run's queue-attribution health ───────────────────────────────────
        # ⚠️ The observer is detached BEFORE the tracker is emptied, so a delta in flight cannot
        # resurrect a tracker after we have reported on it.
        book.set_level_observer(None)
        if tape_task is not None:
            tape_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await tape_task
        if sess.tape is not None:
            n_tr, n_lv = sess.qtrack.matched
            # `matched` is the "is this actually wired?" pair. Zero on both across a run that took fills
            # means the plumbing is broken, NOT that the market was quiet, and the two are
            # indistinguishable without the counter. `lvl_obs_errors` is the feed-side half.
            health = (f"acked={sess.tape.subscribed} prints={sess.tape.n_trades} "
                      f"dropped={sess.tape.n_dropped} bad_trades={sess.tape.n_bad_trades} "
                      f"reconnects={sess.tape.n_reconnects} "
                      f"matched_prints={n_tr} matched_levels={n_lv} "
                      f"lvl_obs_errors={sum(book.level_observer_errors.values())} "
                      f"last_error={sess.tape.last_error or 'none'} "
                      f"errors={sess.tape.errors or '{}'}")
            print(f"  tape: {health}")
            w.writerow([f"{time.time():.0f}", "tape_health", health, "", "", ""])
            # The captured control frames settle M4's "the ack types are guessed, not captured".
            # Written only when we have them, so a clean run does not carry noise.
            if sess.tape.unknown_frames:
                w.writerow([f"{time.time():.0f}", "tape_unknown_frames",
                            json.dumps(sess.tape.unknown_frames), "", "", ""])
        # Final reconciliation: read venue positions (catches any FILLED unexpected/leaked fill). ⚠️ An
        # UNFILLED resting order whose create-response was LOST has no captured order_id, so
        # _cancel_everything can't reach it. At size=1 its exposure is ≤ ~<n> — verify the Kalshi UI.
        venue = await _positions(client, targets=targets)
        # Capture the RAW target-ticker position items so quantities are measurable next run instead of
        # rounded away — read-only, real runs only. Diagnostic, never gates anything.
        # ⚠️ There is NO integer `position` key: the key set is exactly {ticker, position_fp,
        # market_exposure_dollars, realized_pnl_dollars, fees_paid_dollars, total_traded_dollars,
        # last_updated_ts}. `position_fp` is the ONLY quantity, and it is genuinely fractional — the
        # venue partial-fills below one contract [VERIFIED 2026-07-20 against prod].
        if real:
            try:
                raw = [p for p in await client.get_positions() if p.get("ticker") in set(targets)]
                w.writerow([f"{time.time():.0f}", "positions_raw", json.dumps(raw), "", "", ""])
            except Exception as e:
                print(f"  (raw positions capture failed — non-fatal: {e!r})")
        # trailing economics catch-up + the CASH delta (⚠️ NOT P&L — see the header; money spent on
    # open inventory sits in the position, not the balance)
        end_bal = delta = None
        if real:
            try:
                end_bal = await client.get_balance()
                delta = (end_bal - start_bal) if start_bal is not None else None
                w.writerow([f"{time.time():.0f}", "balance_end",
                            f"delta={delta:+.4f}" if delta is not None else "delta=NA",
                            f"{end_bal:.4f}", "", ""])
            except Exception as e:
                print(f"  (end balance read failed: {e!r})")
        # ⚠️ THE HEADLINE IS COMPUTED BY `_mark_step`, NOT RE-DERIVED HERE — one function, one
        # convention. Every inline re-derivation has been wrong in a different direction: `_mid(t) or
        # 0.0` marked an unmarkable position at ZERO; EXCLUDING unmarkable tickers left a short's
        # proceeds unopposed and printed a WIN; and marking the FULL `inv` while `_mark_step` marks only
        # the BOOKED part let the two disagree in SIGN inside one run, with no warning printed.
        # Routing through `_mark_step` also re-books a pending fill for free.
        final_mids = {t: _quote_of(t) for t in targets}   # (bid, ask) — see _quote_price
        cur_pos = venue if venue is not None else last          # venue unreadable → last known
        cash, inv, final_pnl, _fl, _ff = _mark_step(targets, base, last, cur_pos, final_mids, cash)
        # ⚠️ TWO DIFFERENT INCOMPLETENESS SIGNALS:
        #   · UNMARKABLE — held inventory whose EXIT side is absent. Marked at the worst case, so the
        #     total is a bound, not a hole. Rare: `_derive` almost always writes both sides.
        #   · UNBOOKED — a fill we could not price, so it is in NEITHER `cash` NOR `mark`. Detected by
        #     `_mark_step` having declined to advance `last` to the current position.
        # The warning text describes the SECOND; testing only the first printed a confident
        # `marked P&L=$0.00` on a run holding a real short.
        unmarkable = [t for t in targets if inv.get(t, _ZERO)
                      and _quote_price(inv[t], final_mids.get(t)) is None]
        # ⚠️ KNOWN GAP (partly closed, M14): when the venue read FAILED, `cur_pos` IS `last` and `_fl`
        # derives from `last`, so this UNBOOKED predicate is identically empty. What it CAN do is STAMP
        # the `end` row `VENUE_READ=FAILED` and warn, so an analysis can tell a venue-backed headline
        # from one computed against stale local belief.
        unbooked = [t for t in targets
                    if _fl.get(t, base.get(t, _ZERO)) != cur_pos.get(t, _ZERO)]
        # M13: a target the (successful) venue read DROPPED is settled-and-gone (or a transient drop).
        # `_mark_step` worst-cased it, but `unmarkable` above re-derives markability from the BOOK, so a
        # vanished ticker whose book still prices is worst-cased WITHOUT being flagged. Flag it, so the
        # bound is not misread as loss.
        vanished = [t for t in targets
                    if venue is not None and t not in venue and inv.get(t, _ZERO)]
        # ⛔ MAKER FILLS ONLY — the flatten's own fills are excluded. `seen_fills` doubles as the dedup
        # set and the advertised count, so once the flatten started adding to it the count rose on orders
        # the maker never QUOTED: `stats['placed']` increments only inside `quote()`, so the flatten
        # inflated the numerator and not the denominator, and the implied fill rate with it. Reported
        # separately rather than hidden: the flatten count is real, it is just not maker edge.
        note = summary_note(stats, seen_fills, sess.flatten_fill_ids)
        if unmarkable:
            note += f" UNMARKABLE={','.join(t[:24] for t in unmarkable)}"
            print(f"  ⚠️ {len(unmarkable)} position(s) whose exit side is unpriceable — marked at "
                  f"the WORST CASE (long→0, short→1), so the total is a conservative bound for "
                  f"these: {unmarkable}")
        if unbooked:
            note += f" UNBOOKED={','.join(t[:24] for t in unbooked)}"
            print(f"  ⚠️ {len(unbooked)} market(s) hold a fill taken while the book was unpriceable. "
                  f"It is in NEITHER cash nor mark, so this total is INCOMPLETE — not a bound in "
                  f"either direction. CHECK THE VENUE for the true position: {unbooked}")
        if vanished:
            note += f" VANISHED={','.join(t[:24] for t in vanished)}"
            print(f"  ⚠️ {len(vanished)} position(s) VANISHED from the venue read — settled (dropped "
                  f"from /portfolio/positions) or a transient drop. Marked WORST-CASE (a settled WIN "
                  f"shows here as a full loss), so this total is a conservative BOUND pending offline "
                  f"settlement reconciliation, NOT realized P&L: {vanished}")
        if venue is None:
            # M14: the teardown position read failed, so the headline was computed against `last` (stale
            # local belief), not the venue. Stamp it so an analysis can never mistake this for a
            # venue-backed number, and warn.
            note += " VENUE_READ=FAILED(headline-vs-stale-belief)"
            print(f"  ⚠️ M14: the FINAL venue position read FAILED — this marked-P&L headline is "
                  f"computed against STALE LOCAL BELIEF (last known), NOT a venue read, and the UNBOOKED "
                  f"check could not run. The `end` row is stamped VENUE_READ=FAILED. CHECK THE KALSHI UI.")
        # ⚠️ WRAPPED, because everything below it is the operator's only warning. A raise here (ENOSPC
        # on a long run) skipped `fh.close()`, `client.close()`, the DONE / cash-Δ lines and — the line
        # that matters — the "⚠️ OPEN POSITION — NOT auto-closed and NOT covered by the loss cap" block.
        # The money would be fine while the operator is told the run ended clean. Never swallow this
        # silently — a truncated ledger must be loud — but never let it cost the warning either.
        try:
            w.writerow([f"{time.time():.0f}", "end", note, f"{cash:.4f}", "", f"{final_pnl:.4f}"])
        except Exception as e:
            print(f"  ⚠️ FINAL LEDGER ROW FAILED TO WRITE ({e!r}) — the CSV is INCOMPLETE for this "
                  f"run. The summary below is still accurate; the file is not.")
        try:
            fh.close()
        except Exception as e:
            print(f"  ⚠️ ledger close failed ({e!r}) — trailing rows may be missing.")
        await client.close()
        held = ("UNREADABLE" if venue is None
                else {t: venue.get(t, _ZERO) - base.get(t, _ZERO)
                      for t in targets if venue.get(t, _ZERO) - base.get(t, _ZERO)})
        # ⚠️ get_balance() is AVAILABLE CASH, not portfolio value: with inventory open, cash is down by
        # what the position cost while the position still has value, so the cash delta is NOT the P&L
        # (cash −<n> against a true mark-to-market of −<n> on 2026-07-19).
        # M21: `final_pnl` is an EXIT-SIDE (touch) mark, optimistically biased BY CONSTRUCTION with no
        # forward-adverse term (+<n> against a −<n> realized loss). On a FLAT exit `cash Δ` IS
        # realized P&L (matched /portfolio/settlements to the cent, n=2 NPB). ⚠️ The mid-run LOSS CAP
        # reads that SAME optimistic mark (M22).
        flat = isinstance(held, dict) and not held          # venue read OK AND net inventory closed
        if flat and delta is not None:
            print(f"\nDONE (flat exit). cash Δ = ${delta:+.2f} = REALIZED P&L "
                  f"(start ${start_bal:.2f} → end ${end_bal:.2f}) — for a net-zero yes+no pair still "
                  f"unsettled, confirm vs /portfolio/settlements.")
            print(f"  marked P&L=${final_pnl:+.2f} is an EXIT-SIDE (touch) mark, optimistically biased "
                  f"by construction — do NOT cite it as run P&L.")
        else:
            bal_line = (f"  cash Δ=${delta:+.2f} (start ${start_bal:.2f} → end ${end_bal:.2f}) "
                        f"— CASH MOVEMENT, not P&L: money spent on open inventory sits in the position, "
                        f"not the balance."
                        if delta is not None else "  cash Δ=unavailable")
            print(f"\nDONE. marked P&L (EXIT-SIDE touch mark, optimistic)=${final_pnl:+.2f}  "
                  f"net inventory (venue)={held}")
            print(bal_line)
        # An open position is NOT closed by this tool and rides to SETTLEMENT — and once this process
        # exits, nothing is watching it: the loss cap only runs inside the loop. Say so explicitly.
        if isinstance(held, dict) and held:
            print(f"  ⚠️ OPEN POSITION — NOT auto-closed and NOT covered by the loss cap once this "
                  f"process exits. It settles with the game:")
            for _t, _q in held.items():
                _m = _mid(_t)
                _side = "SHORT yes / long no" if _q < 0 else "LONG yes"
                _mk = f", marked ~${abs(_q) * (_ONE - _m if _q < 0 else _m):.2f}" if _m else ""
                print(f"       {_t}  {_q:+.2f} ({_side}{_mk})")
            print(f"     Flatten manually in the Kalshi UI if you don't want the exposure "
                  f"(exiting crosses the spread, so it costs the taker fee + spread).")
        print(summary_line(stats, seen_fills, sess.flatten_fill_ids))
        print("  post_only make-only; loss-capped; tracked orders cancelled + venue swept for strays. "
              "If the sweep reported it could NOT list, verify the Kalshi UI manually.")
        # ── STAMP THE EXIT ON THE HEARTBEAT — the last thing main() does ────────────────────────
        # Its ABSENCE is the SIGKILL signature: the deadman reads a heartbeat with no exit stamp and
        # beats that simply stopped as an unclean death. A HALT is stamped with its reason so it does
        # not read as `clean`. Both properties fail if this line is moved anywhere reachable by SIGKILL.
        try:
            hb.mark_exit(f"halted:{halt_reason}" if halted else "clean",
                         markets_quoted=len(targets), fills=len(seen_fills), run_id=run_id)
        except Exception as e:
            print(f"  (heartbeat exit stamp failed — non-fatal: {e!r})")

