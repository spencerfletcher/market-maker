"""
bot/kalshi/maker.py
─────────────────────────
LIVE paper-MARKET-MAKER — places REAL, tiny, bounded post_only quotes on the PROD Kalshi venue to
measure the one thing simulation/demo cannot: the real tick-discretized FILL-RATE of the
price-improving skewed policy, plus realized inventory / markout / P&L on real flow.

⚠️ THIS PLACES REAL ORDERS WITH REAL CAPITAL. It is the deliberate go-live step. It does NOT touch the
prod arb bot: this is a SEPARATE process; the bot stays DRY_RUN. ⚠️ A "max realistic loss" figure
computed as markets × inv-cap × $1/contract is a DERIVATION, not a bound — it inherits every
inventory-cap caveat below. The venue's own `market_exposure_dollars` is the cost basis, and it is
the number that actually bounds the loss.

SAFETY RAILS — ⚠️ READ THE QUALIFIERS, THEY ARE NOT ALL HARD. This heading said "(all hard)" while
two of the bullets below it said the opposite; that combination is what an operator sizes a run
from. Genuinely hard: post_only (cannot take), the PROD guard, the DRY_RUN gate, the fail-closed
positions READ, and cancel-all-plus-venue-sweep on exit. NOT hard: the inventory cap (does not bound
venue exposure); the loss cap still dies with the process; and the loss cap is BLIND to a fill taken
while the book was unpriceable — it no longer reads a *booked* short as a win, which is a NARROWER
claim than fail-closed. Details per bullet:
  • post_only make-only — never crosses / never takes; the only risk is inventory accumulated as a maker.
    (⚠️ `--flatten-on-exit` adds a passive, post-only exit flatten — it still NEVER takes, so
    this rail holds; a taker `--flatten-cross` last resort is deliberately NOT built yet.)
  • ORDERS ARE GATED ONLY BY config.DRY_RUN — the sole short-circuit is inside
    KalshiClient.create_order. ⚠️ `real` also gates THREE refusals, not just logging — the RATE-LIMIT
    budget, `_skew_guard`, and the `_FILLS_EVERY != 1` tripwire — which is safe only because the DRY_RUN/flag refusal forces
    `real ≡ not config.DRY_RUN`, so the two can never disagree. That equivalence is load-bearing; if
    the refusal is ever removed, both guards silently stop protecting a live run. Otherwise
    `main()`'s `real` flag gates INSTRUMENTATION (fills, markout,
    preflight, teardown stray sweep, the run_id mode stamp), NOT money. The CLI shim
    (scripts/kalshi_live_mm.py) sets DRY_RUN=false from --i-understand-real-money before config is
    imported; if DRY_RUN is False and that flag is absent, main() REFUSES TO START, because that
    combination would otherwise place real quotes while reporting a dry preview. --confirm on top.
  • PROD guard: refuses unless config.KALSHI_ENV == "prod" and the base_url is NOT the demo host.
  • Inventory cap (--inv-cap): stops quoting the side already at ±cap. ⚠️ NOT a hard bound on VENUE
    exposure, and not "would breach": it measures `cur − base`, so a position inherited from a prior
    un-flattened run sits OUTSIDE it (this tool does not auto-flatten), and it tests PRE-trade
    inventory so it overshoots by up to --size. Both are open defects, and they block a real run.
  • LOSS KILL-SWITCH (--loss-cap $): each cycle diffs get_positions (authoritative) valued EXIT-SIDE
    (a long at the bid, a short at the ask), NOT "conservative, halts EARLY" as this line said under
    the old mid convention. `pnl_exit − pnl_mid = (s/2)·(Σ|dᵢ| − |inv_net|) ≥ 0` — so at a constant
    spread it halts LATER or equal, by about one spread per completed round trip.
    ⚠️ **THAT IDENTITY IS EXACT, AND AN EARLIER EDIT WRONGLY RETRACTED IT** on the theory that
    `--improve-ticks`/`--inv-coef` break it. They cannot: `_mark_step` books AND marks off
    `quotes[t]` = `(get_best_bid, get_best_ask)`, the OBSERVED BBO — **our own `order_meta` price is
    never consulted** — a separate defect, but not a flaw in this relation. ⚠️ Not the same
    as "our price never enters": `get_best_bid` includes our OWN resting orders, so when we improve,
    the observed BBO IS our quote. That is why the identity survives improvement rather than being
    broken by it. Quoting inside the
    touch just makes our order the touch, i.e. narrows `s`; the identity still holds at the new `s`.
    The retraction confused "the two conventions differ by exactly (s/2)(Σ|d|−|inv|)" — true — with
    "the booked price equals what we transacted at" — false, and a separate defect.
    The caveats that ARE real: (a) a spread that MOVES between the fill cycle and the mark cycle
    (widening while inventory is held makes it halt EARLIER); (b) the worst-case fallback below,
    which is neither convention; (c) an offsetting leg inside one valuation interval
    never enters `Σ|d|` at all. It halts + cancels-all if marked
    P&L < −cap. Baselined to starting positions so only OUR fills count. FAIL-CLOSED on READS: any
    get_positions failure (REST outage) → CANNOT-VERIFY → halt + cancel-all (never falls through to
    "flat", which would blind both the loss cap and the inventory cap).
    ⚠️ NARROWER THAN "FAIL-CLOSED": a BOOKED short is never read as a win.
    That is not the same as fail-closed, and the difference matters for sizing — see (2) below: an
    unbooked fill is invisible to BOTH `cash` and `mark` while `inv[t]` still updates, so
    `--inv-cap` keeps authorising more of it. Cost basis can therefore accumulate, up to roughly
    markets × inv-cap × $1/contract, entirely OUTSIDE what the loss cap can see — which can exceed
    the cap itself. Blindness is not a closed gate.
    It used to add a mark only when the mid
    existed, while the fill's cash leg had been booked in an earlier cycle — so on a SHORT whose
    book later went one-sided, cash was positive, nothing offset it, and `pnl` read a WIN, disarming
    the switch exactly when the position became untradeable. BOOKED inventory with no mid is now
    marked at its settlement bound (long→0, short→1), in `_mark_step` AND in the teardown headline,
    which previously used opposite conventions and could print opposite SIGNS in one run.
    ⚠️ Consequences to know before sizing up: (1) the trip condition for unmarkable inventory is its
    COST BASIS, not a realised loss — a halt row can show a large negative without that much having
    actually been lost; (2) a genuinely NEW fill that cannot be valued is still excluded from both
    cash and mark, so a confident final number can still print while one fill was never booked.
  • Stop a live run with `kill`, `kill -INT`, or Ctrl-C — all three now unwind. The shim installs a
    SIGTERM handler (`_install_sigterm_handler`) that raises KeyboardInterrupt, so a plain `kill`
    runs the teardown below (cancel-all + venue stray-sweep) exactly as SIGINT does. ⚠️ `kill -9`
    (SIGKILL) is UNCATCHABLE and always strands orders — still the one forbidden signal. A direct
    `main()` call that bypasses the shim (i.e. tests) has no handler.
  • Cancels every TRACKED resting order on exit AND on any exception, THEN sweeps the venue: lists all
    still-resting orders in the target markets (get_resting_orders) and cancels them too — this reaches
    a stray whose create-response was LOST (booked on the venue, reply dropped) and has no captured
    order_id. The teardown sweep's residual is its OWN listing failing to read (it says so loudly and
    tells you to check the UI); it never pretends "no strays" on a read it couldn't complete.
    ⚠️ ONE PATH DOES NOT REACH THAT SWEEP: the baseline-position refusal returns before the `try`, so
    its stray protection is the PREFLIGHT listing+cancel instead. That listing has NO retries (unlike
    `_positions`, which retries a 429 four times), and it fails with a quieter message — so on a
    correlated 429 storm the protection is weaker there than here.

PREFLIGHT (real runs): snapshots starting balance, and lists+cancels any leftover resting orders in the
  target markets so a prior-run stray can't contaminate this run's fills (orders in OTHER markets are
  reported, not touched).

WHAT IT MEASURES → logs/kalshi_live_mm.csv (all ADDITIVE to, and separate from, the positions-based
  loss cap — a measurement read failure degrades logging only, never the kill-switch):
  • real_fill  — ACTUAL fill price + fee + is_taker from get_fills (not the mid-approximation; a
    taker=True would mean a post_only leaked into a take).
  • markout    — mid at +5s/+30s/+300s vs fill price; NEGATIVE = picked off (adverse selection), THE number
    that decides live-sports maker viability.
  • balance_start/balance_end — get_balance() CASH delta. ⚠️ NOT P&L and NOT "ground truth": Kalshi
    is fully collateralized, so cash is debited and the value sits in the open position. A run that
    merely OPENED a position reports a large cash loss and a near-flat true mark-to-market — the two
    are not even the same order of magnitude. Labelling this column ground-truth P&L is exactly how
    a flat run gets reported as a losing one; read it WITH the position, never alone.
  • end        — quotes placed vs post_only-cross rejects (spread-takeable = no room to make) vs fills.

Run (prod creds from the deployed .env; the arb bot stays DRY):
  .venv/bin/python -m scripts.kalshi_live_mm --seconds 1200 --markets 3 --size 1 --inv-cap 3 --loss-cap 5   # DRY preview
  .venv/bin/python -m scripts.kalshi_live_mm --seconds 1200 --markets 3 --size 1 --inv-cap 3 --loss-cap 5 --confirm --i-understand-real-money
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

# Real-money enablement happens in the CLI shim (see the note above `from bot.core import config`),
# because config snapshots the environment at import time.
# Rate-limit budget: /portfolio/* list reads cost 10 units each (Kalshi /account/endpoint_costs,
# default_cost=10) vs 2 for a cancel, so polling two of them every cycle is the dominant spend.
_BOOK_WARMUP_S = 3.0      # per-attempt wait for the WS book to populate before we refuse to start
_BOOK_WARMUP_TRIES = 5    # → 15s total; a live snapshot lands in ~1s, so this is generous
_POS_RETRIES = 4          # transient-429 attempts before the fail-closed halt
_POS_BACKOFF_S = 1.5      # exponential: 1.5s, 3s, 6s
_FILLS_EVERY = 1          # poll get_fills EVERY cycle.
# ⚠️ This was 5, on the reasoning that fills are LOGGING-ONLY and their pagination grows across a
# run, making them the cheapest thing to thin out. The cost side of that trade was invisible and
# large: at --requote-s 30 it meant a fill was discovered up to 150s after it happened, and since
# markout horizons were anchored to DETECTION, the 5s and 30s horizons both fired on the same pass.
# One real run recorded EVERY markout row at the same age — the 5s and 30s horizons were literally
# the same number written twice. Thinning the poll saved a handful of rate-limit units per cycle and
# destroyed the measurement the run existed to take. Horizons are now anchored to the venue's fill ts (`_fill_ts`), which fixes
# the labelling, but discovery lag still bounds how EARLY the first horizon can be valued — a 5s
# markout cannot be taken if we only learn about the fill at 150s. Both halves are needed.
# Rate-limit budget: at --requote-s 30 the per-cycle spend is small and this is affordable; at 6s
# requotes it is not, which is another reason to prefer slow requotes (they also buy queue time).

# ⚠️ THE sys.argv SNIFF THAT USED TO LIVE HERE NOW LIVES IN THE CLI (scripts/kalshi_live_mm.py).
# It has to run BEFORE `bot.core.config` is imported, because config snapshots the environment at
# import time — so it cannot move into main(). Keeping it here was safe while this was a script and
# is NOT safe now that it is library code under bot/: a module-level `"--i-understand-real-money" in
# sys.argv` fires on ANY process that imports this module with that string anywhere in its argv, and
# flips DRY_RUN off globally as a side effect of an import. A library must not read argv.
#
# ⚠️ Do NOT read this as a two-condition guarantee on ORDERS — there isn't one, and a previous
# version of this comment claimed there was. `config.DRY_RUN` alone decides whether money moves
# (the only short-circuit is inside `KalshiClient.create_order`). The flag's only job is to set that
# env var before config snapshots it. A disagreement between the flag and DRY_RUN is now a REFUSAL
# TO START in main(), because the mismatch silently produces live-but-uninstrumented runs.
# NB: KALSHI_ENV is deliberately NOT set here — it comes from the deployed .env (prod). The guard checks it.

from bot.core import config, heartbeat, maker_state, memguard
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

    The flatten's fills are excluded whenever we captured its `order_id` (`flatten_oids`, recorded at
    placement). ⚠️ The one hole: a flatten order whose create-response was LOST has no `order_id`, so
    the teardown catch-up books its fills as maker flow — the v1 limitation `_flatten_on_exit`
    documents. And because the venue partial-fills below one contract, a single filled order can
    produce several fill RECORDS, so this count is not "orders swept".

    ⛔ EXTRACTED SO THE REPORTED NUMBER IS TESTABLE. `seen_fills` doubles as the dedup set and the
    advertised count, so once the teardown flatten started adding to it the count rose on orders the
    maker never QUOTED — `stats['placed']` increments only inside `quote()`, so the flatten inflated
    the numerator and not the denominator, and the implied fill rate with it. That count is compared
    against a FIFO model's prediction when sizing the next run, so an inflated count argues the model
    under-predicts — which is exactly the reading that would justify a larger next rung.

    ⛔ **AND THE REASON FIRST GIVEN HERE FOR EXTRACTING IT WAS FALSE.** It said the two reporting
    sites are inside `main()`, which has "NO executable coverage" — a belief still stated verbatim at
    `tests/test_maker_session.py`'s `test_main_calls_sync_inventory` — so an AST assertion was the
    only available pin. `tests/test_maker_main_e2e.py` has driven `asyncio.run(main())` since it was
    written, in EVERY test in that file, and its own first line reads "the coverage that did not
    exist". (That draft was never committed, so this quotes it rather than citing it.) Believing that
    claim is what produced a string of source-shape assertions that mutants walked through — three
    successive pins for THIS number, each defeated. Extracting a pure function is still worth doing
    (it can be called directly and cheaply), but the pin that has teeth is the e2e one: drive
    `main()`, read the `end` row and the closing line, and assert on the numbers actually reported."""
    flatten = len(flatten_fill_ids)
    return len(seen_fills) - flatten, flatten


def summary_note(stats: dict, seen_fills: set, flatten_fill_ids: set) -> str:
    """The `end` row's note PREFIX — the quotes/rejects/fills segment, verbatim as written to the
    ledger. ⚠️ NOT the exact string: `main()` may append `UNMARKABLE=`/`UNBOOKED=`/`VANISHED=`/
    `VENUE_READ=FAILED(...)` diagnostics before the row is written, so assert containment, not
    equality, unless the run is known to have a priceable exit on both sides.
    The flatten count is REPORTED rather than hidden: it is real, it is just not maker edge."""
    maker, flat = fill_counts(seen_fills, flatten_fill_ids)
    return (f"quotes={stats['placed']} rejects={stats['rejected']} fills={maker}"
            + (f" flatten_fills={flat}" if flat else ""))


def summary_line(stats: dict, seen_fills: set, flatten_fill_ids: set) -> str:
    """The operator-facing closing line — extracted for the same reason as `summary_note`.

    ⚠️ BOTH SITES, because pinning only one leaves the other free: with the `end` row routed through
    `summary_note`, a mutant that printed the INFLATED `len(seen_fills)` here still passed the whole
    suite. The CSV row is the durable record and the stdout line is what an operator actually reads
    during an attended run — a disagreement between them is worse than either being wrong alone."""
    maker, flat = fill_counts(seen_fills, flatten_fill_ids)
    return (f"  quotes placed={stats['placed']}  post_only-cross rejects={stats['rejected']}  "
            f"real fills={maker}"
            + (f"  (+{flat} flatten fills, NOT maker edge)" if flat else ""))


def _should_flatten(real: bool, flatten_on_exit: bool, halted: bool) -> bool:
    """Whether the teardown posts its passive reducing order. **`halted` is deliberately ignored.**

    It takes `halted` as a parameter precisely so that "a halt does not change the answer" is a
    property a test can assert over the whole truth table, rather than a source line someone has to
    notice is absent. The teardown DID gate on `and not halted` once, and the first attempt to pin
    its removal asserted on source TEXT — which a one-line `if not halted:` nested inside the outer
    branch defeats while keeping the old string absent. Assert on behaviour, not on source.

    Why `halted` must not matter: the process is EXITING, so the loss cap dies with it. Declining to
    flatten on a halt does not avoid the adverse move; it walks away from an adverse position and
    leaves it unattended with no cap at all. The flatten is passive, post-only, at the touch and
    reduces OUR net (`venue − base`), so it cannot cross; and it re-reads the venue with its own
    fail-closed, so `positions_cannot_verify` self-refuses inside rather than needing a gate here."""
    return real and flatten_on_exit


def _install_sigterm_handler() -> None:
    """Make SIGTERM **and SIGHUP** raise KeyboardInterrupt so a plain `kill`, or a dropped terminal,
    unwinds the teardown finally exactly as Ctrl-C (SIGINT) does.

    ⛔ **SIGHUP WAS THE HOLE.** SIGTERM and SIGINT both unwound; SIGHUP did not, and SIGHUP is
    the signal an ATTENDED run actually meets — it is what the kernel sends every process in the
    foreground group when the controlling terminal goes away, i.e. an SSH drop or a closed laptop
    lid. Its default disposition is terminate-without-unwinding, so a dropped connection mid-run left
    live post-only quotes resting on the venue with the loss cap dead and nothing sweeping — the
    exact strand the SIGTERM handler exists to prevent, reached by the likeliest route. Operating
    procedure says run under tmux/screen, which is correct and remains the primary defence; this is
    the backstop for when nobody does.

    ⚠️ **THIS OVERRIDES `nohup`.** `nohup foo &` sets SIGHUP to `SIG_IGN` specifically so the run
    survives a hangup; installing here replaces that, so a bare-`nohup` maker now tears down and
    exits on a dropped terminal where it previously kept trading. That is the right call for an
    attended real-money maker — an unattended one with nobody watching the loss cap is the worse
    outcome — but it silently reverses the repo's `detach-long-running-jobs` convention, so it is
    stated rather than left to be discovered. `setsid nohup` is unaffected (new session, no
    controlling terminal, so no hangup SIGHUP is delivered), and the supervisor's children inherit
    its already-`setsid`-ed session, so that path is untouched.

    Without it, SIGTERM's default disposition terminates the process WITHOUT unwinding, so the
    cancel-all + venue stray-sweep in main()'s `finally` never runs and real orders are left resting
    with the loss cap dead. KeyboardInterrupt is the exact exception SIGINT already relies on to run
    that finally, so this makes `kill` and Ctrl-C take the identical teardown path.

    Installed at the process entry point (the shim), NOT in main(): a real CLI run is protected while
    a test that calls main() directly does not mutate the process-wide SIGTERM disposition. Must run
    on the main thread (`signal.signal`'s constraint) — the shim's `__main__` satisfies it. SIGKILL
    (`kill -9`) is uncatchable and always strands; it stays the one forbidden signal."""
    def _raise_keyboard_interrupt(signum: int, frame: object) -> None:
        # ⛔ DISARM BEFORE RAISING — a SECOND signal during teardown kills the venue sweep.
        #
        # The KeyboardInterrupt raised here starts a teardown that AWAITS: `flatten_on_exit` waits up
        # to `--flatten-wait-s` (default 60s) for its passive order. A second signal landing in that
        # window raises into the unwinding frame, `asyncio.run` tears the loop down, and the sweep in
        # the `finally` then dies on `RuntimeError: no running event loop` — swallowed by its own
        # `except Exception` into a printed warning, leaving the flatten's OWN resting post-only order
        # live on the venue with the loss cap dead. Measured on the deployed interpreter:
        #     SIGINT → anything      sweep completed, but the teardown TAIL was still skipped —
        #                            `Runner._on_sigint` cancels only the main task, so the loop
        #                            survives the sweep and then `KeyboardInterrupt` drops the
        #                            OPEN-POSITION warning and `client.close()`. Fixed by installing
        #                            on SIGINT as well (below), not by the disarm alone.
        #     SIGHUP/SIGTERM → any   sweep DIES        (a raw `raise` from a signal handler has no
        #                                               such protection)
        # That is the exact strand this handler exists to prevent, reached through this handler's own
        # path — and it got worse in the same change that added SIGHUP (the likeliest first signal)
        # and removed the flatten's `not halted` gate (making the 60s window reachable on EVERY exit).
        #
        # SIGINT is disarmed too: as a SECOND signal it also kills the sweep, because asyncio's
        # cancel lands on the teardown's awaits rather than the run loop's.
        # ⚠️ CONSEQUENCE, DELIBERATE — AND KNOW THE BOUND BEFORE REACHING FOR `kill -9`. Once the
        # first signal lands the teardown cannot be interrupted by any of the THREE signals disarmed
        # below. It is not uninterruptible in general: SIGQUIT (Ctrl-\) and SIGABRT are neither
        # ignored nor handled, and either still kills the teardown mid-flight. It IS bounded: no
        # unbounded path
        # exists on it (`_get` inherits aiohttp's 300s total; positions pagination caps at
        # `_POSITIONS_MAX_PAGES`; `_post`/`_delete` are 5s), but the bound is **up to ~5 minutes per
        # stuck REST call**, not seconds. An operator who does not know that will reach for `kill -9`
        # at the 60s mark and produce exactly the strand this exists to prevent. Wait it out; SIGKILL
        # is still the one forbidden signal.
        # ⚠️ ALSO: on the SIGHUP/SIGTERM path the WS book task is already cancelled by
        # `Runner._cancel_all_tasks` before the teardown body runs, so `flatten_on_exit` prices its
        # order off a book cache that stopped updating ~0.5s earlier (`get_best_bid/ask` have no
        # staleness guard). Bounded and small — `post_only` cannot cross, so the worst case is a
        # reducing order resting a few ticks off the touch at size 1–3 — but it means the teardown's
        # own "a mid read from a dead book is not a mark" note is already true of this path.
        for _s in (signal.SIGHUP, signal.SIGTERM, signal.SIGINT):
            signal.signal(_s, signal.SIG_IGN)
        raise KeyboardInterrupt(signal.Signals(signum).name)
    # ⛔ SIGINT IS INSTALLED TOO, not just disarmed. Leaving it to `asyncio.Runner`'s own handler
    # looked fine — the sweep survives a SIGINT-first teardown, because `Runner._on_sigint` cancels
    # only the MAIN task and the loop lives on. But `Runner` raises `KeyboardInterrupt` out of
    # `run_until_complete` once cancellation lands, and everything AFTER the sweep is then skipped:
    # the fills catch-up, `client.close()`, and the `⚠️ OPEN POSITION — NOT auto-closed and NOT
    # covered by the loss cap` warning. So a second Ctrl-C on a real run exited **silently holding
    # inventory** — orders correctly cancelled and swept, and the one line telling the operator to
    # check the UI dropped. Measured over all 9 first×second pairs: 6/9 completed the teardown with
    # SIGINT left alone, 9/9 with it installed here. Taking the same path SIGTERM already takes
    # measures strictly better, and `Runner` only installs its own handler when SIGINT is still
    # `default_int_handler` (runners.py), so this cleanly pre-empts it rather than fighting it.
    for _sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
        signal.signal(_sig, _raise_keyboard_interrupt)


# Kalshi's grid in the tradeable band: 0.01 at every price a maker would quote. The
# `tapered_deci_cent` series (elections/politics) step 0.001 ONLY below 0.10 and above 0.90, which the
# [min-price, max-price] contested gate already excludes.
_TICK = 0.01


def _tick(px: float, side: str) -> str:
    """Snap a quote onto the venue's cent grid, CONSERVATIVELY for the side being quoted.

    ⚠️ This used bare `round()`, i.e. round-to-nearest, and that is a money bug on a one-tick grid.
    The intended inventory skew is a FRACTION of a tick, so rounding turned it into a STEP FUNCTION
    at the rounding boundary: no effect at all until the boundary, then a FULL tick of concession
    past it. A full tick is several times the entire per-fill edge a maker is quoting for — so the
    run would measure ~zero spread capture for a reason that is arithmetic, not market.
    It is the same class of defect as `0.47/0.01 == 46.99…`, on the live path.

    Bids FLOOR and asks CEIL, so discretization can only ever quote us a **better** price than asked
    for, never a worse one — a safety net for any off-grid input. ⚠️ It does NOT make a sub-tick lean
    vanish: on an on-grid base, floor/ceil turns a 0.3¢ shift into a FULL-tick move on the
    accumulating side while leaving the other side put (floor and ceil round opposite ways, so the two
    sides can even move by different tick counts and distort the spread). So the skew is discretised to
    whole ticks UPSTREAM by `_skew_ticks` before it reaches here, and `_tick` only ever sees an on-grid
    price. ⚠️ Do NOT reason that a sub-tick skew "rounds to nothing": against an on-grid base,
    `_tick(0.45 − 0.003, "buy")` is `0.44` — a full-tick move from a third-of-a-tick intent.
    """
    px = min(0.99, max(0.01, px))
    steps = px / _TICK
    # round(...,9) first: 0.41/0.01 is 40.99999999999999 in binary float, so a bare floor would drop
    # an on-grid price a whole tick — the exact bug this repo already hit in shadow_mm_probe.
    steps = round(steps, 9)
    snapped = (math.floor(steps) if side == "buy" else math.ceil(steps)) * _TICK
    return f"{min(0.99, max(0.01, snapped)):.4f}"


def _real_money_refusal(dry_run: bool, flagged: bool) -> str | None:
    """Reason to refuse this invocation outright, or None if the mode is coherent.

    ⚠️ `real` DOES NOT GATE ORDER PLACEMENT. `MakerSession.quote` calls `create_order`
    unconditionally and the only DRY short-circuit lives inside `KalshiClient.create_order`, so
    **`config.DRY_RUN` alone decides whether money moves**. `real` decides only whether the run is
    INSTRUMENTED (fills, markout, preflight, teardown stray sweep, the `run_id` mode stamp).

    That split makes one combination silently catastrophic — `DRY_RUN=false` in the environment with
    no `--i-understand-real-money` on the command line places REAL post-only quotes while printing
    "DRY PREVIEW — NO real orders placed", stamping every row `dry-`, logging zero fills, and
    skipping the stray sweep. Live money, mislabelled, uninstrumented, unswept. Latent only because
    the deployed `.env` says True today, and the arb bot's go-live flip sets that same variable —
    which `_real_money_refusal` now converts into a HARD STOP rather than an arming. Do not read
    that flip as "the flip arms the maker" — it does the opposite.

    A pure function on purpose: the guard it replaces could only be tested by string-matching
    `main()`'s source, and setting DRY_RUN in a shell is a denied operation in this repo (correctly).

    The reverse mismatch — flag passed but config still DRY — is NOT refused: that is the ordinary
    "I forgot to configure it" case, it cannot move money, and `main()` already prints a DRY banner.
    """
    if not dry_run and not flagged:
        return ("config.DRY_RUN is False (from .env or the shell environment) but "
                "--i-understand-real-money was NOT passed. "
                "Orders are gated ONLY by DRY_RUN, so this would place REAL quotes while reporting "
                "a DRY preview, stamping rows `dry-`, logging no fills and skipping the stray "
                "sweep. Pass the flag to trade for real, or set DRY_RUN=true to preview.")
    return None


def _skew_ticks(obi: float, inv: float, obi_coef: float, inv_coef: float) -> int:
    """The inventory/OBI lean as a WHOLE number of ticks, rounded once from the combined intent.

    Both signals fold into one continuous intent — `obi_coef·obi − inv_coef·inv` (long inventory
    leans NEGATIVE, so both quotes move DOWN: discourage a buy, solicit a sell) — which is rounded
    to the nearest tick and applied to BOTH quotes together, so the reservation price recenters
    symmetrically (Avellaneda-Stoikov). Discretising HERE, before `_tick`, is the fix for a real
    defect: a sub-tick float shift fed into `_tick`'s floor/ceil moved the accumulating side a full
    tick while leaving the other side put — a one-sided step function that also distorted the spread
    (floor and ceil round opposite ways). `round`, not `floor`, so the lean is symmetric in sign and
    immune to the 0.47/0.01==46.99 binary-float trap that only bites `floor`. Stays float on purpose:
    the whole maker quote path is float, and an integer × _TICK is exact enough that `_tick`'s
    `round(steps, 9)` neutralises any residual dust — a Decimal shift here would only collide with the
    float `bid`/`ask`/`_TICK` it must be added to. Migrating the whole path is a separate job."""
    return round((obi_coef * obi - inv_coef * inv) / _TICK)


def _skew_guard(inv_coef: float, inv_cap: float) -> str | None:
    """Reason to refuse an inventory-skew coef that can NEVER move a quote, or None if it can.

    The skew is rounded to whole ticks (`_skew_ticks`), so a coef is inert only when its maximum
    effect — at the inventory cap — rounds to ZERO ticks (stays below half a tick all the way up).
    A coef that small tells an operator inventory is managed while no quote ever moves.
    ⚠️ An earlier form refused any max effect below a FULL tick, on a "rounds to nothing" premise
    that was false. Under `round`-based discretisation a max effect just under a tick IS a
    deliberate one-tick lean at the cap, so refusing it disabled a working knob."""
    if inv_coef <= 0:
        return None                                  # deliberately off
    max_ticks = round(inv_coef * inv_cap / _TICK)
    if max_ticks == 0:
        return (f"--inv-coef {inv_coef:g} at --inv-cap {inv_cap:g} skews at most "
                f"{inv_coef * inv_cap * 100:.2f}¢ at the cap, which rounds to 0 ticks — the "
                f"inventory arm never moves a quote. Use 0 to disable it explicitly, or at least "
                f"{_TICK / inv_cap:.4g} to reach one tick at the cap.")
    return None


_RATE_LIMIT_BUDGET_U_PER_S = 8.0


def _rate_limit_units_per_s(markets: int, requote_s: float) -> float:
    """Kalshi rate-limit units per second at this configuration.

    Module-level and pure so a test can bind to the REAL arithmetic. An earlier test re-implemented
    this formula locally and string-matched the call site, which meant it passed happily when the
    gate was mutated to hardcode `markets=3` — the test confirmed its own copy of the belief rather
    than the code. A test that re-implements the thing it is testing pins nothing.

    Cost model, from the venue's own endpoint-cost table: `/portfolio/*` list reads 10 units, cancel 2,
    create 10. Per cycle we spend 10 (positions) + 10 (fills) + markets · (2 creates + 2 cancels).
    The run that 429'd within minutes and tripped its fail-closed halt was markets=3 @ 6s ≈ 15.3.

    ⚠️ This is a MEAN-RATE model and Kalshi bills from a token BUCKET, which is sensitive to burst
    depth too. Lengthening the requote interval lowers the mean and leaves the burst unchanged, so
    the budget is necessary rather than sufficient — it is calibrated against one measured incident
    at markets=3, not derived from the venue's published bucket."""
    return (20.0 + 24.0 * markets) / max(requote_s, 1e-9)


def _fill_ts(fill: dict) -> tuple[float, str]:
    """(epoch seconds, source) for a fill — the VENUE'S timestamp where possible.

    Returns the source alongside the value on purpose. Falling back to `now` is sometimes necessary
    but it silently reintroduces the exact defect this exists to fix (a markout horizon measured
    from when we NOTICED the fill rather than when it happened), so the fallback has to be visible
    in the data. An analysis can then drop `ts_src=detected` rows instead of averaging them in.

    Kalshi sends `ts` as epoch seconds on the fills endpoint, but ISO strings appear elsewhere in the
    same API, so both are accepted."""
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


def _fmt_ahead(v: float | None) -> str:
    """`unknown` is not 0. A worse-than-touch price has depth we never observed, and printing it as
    0 would read as 'we were first in an empty queue' — the opposite of the truth."""
    return "unknown" if v is None else f"{v:g}"


def _fmt_qty(v) -> str:
    """Render a `Decimal` contract count for a log row, or `unknown` for None.

    Not `_fmt_ahead`: `f"{Decimal('120.0'):g}"` keeps the trailing zero, so the same queue would
    read `120` on a `fill_attrib` row (a float) and `120.0` on a `queue_dynamics` row (a Decimal
    carrying its scale). Two spellings of one number is how a grep-based analysis quietly splits a
    population in half. Non-integral values — the venue really does partial-fill below one contract
    — keep their scale."""
    if v is None:
        return "unknown"
    d = Decimal(v)
    return str(d.to_integral_value()) if d == d.to_integral_value() else str(d)


def _queue_ahead(action: str, px: float, touch_b: float, touch_a: float,
                 depth_b: float, depth_a: float) -> tuple[float | None, bool]:
    """(contracts resting AHEAD of us at our price, did-we-improve).

    Under price-time priority the queue ahead is what decides whether a size-1 maker ever trades, so
    this is the number that tests the improvement hypothesis directly:
      · INSIDE the touch  → nobody else holds this price, so the queue ahead is 0 and we are first.
      · AT the touch      → the whole visible level is ahead of us, FIFO.
      · OUTSIDE the touch → we only observe L1, so the depth at a worse price is UNKNOWN. Returned
        as None, never as 0 — claiming an empty queue there would invent priority we do not have and
        would flatter exactly the arm this is meant to test.

    Compared with a half-tick tolerance rather than `==`. The sent price is a formatted string
    (`_tick` → "0.4000") re-parsed to float, while the touch comes from the feed, so exact equality
    is a coin flip on representation — the same float-comparison trap that put 6 of 99 on-grid cent
    prices a full tick off the touch in the shadow probe."""
    tol = _TICK / 2.0
    if action == "buy":
        if px > touch_b + tol:
            return 0.0, True
        if abs(px - touch_b) <= tol:
            return depth_b, False
        return None, False
    if px < touch_a - tol:
        return 0.0, True
    if abs(px - touch_a) <= tol:
        return depth_a, False
    return None, False


def _own_at(own_resting: dict, action: str, px: float) -> float:
    """Our own size resting at `px` on `action`, matched with `_queue_ahead`'s half-tick tolerance.

    Exists so the price comparison has ONE spelling. The keys are floats built from a re-parsed
    order price and `px` comes from the feed; `Decimal("0.4000") == Decimal("0.40")` is True but
    `0.47/0.01 == 46.99…` is the standing counter-example, and an exact-equality miss here fails
    SILENTLY in the direction of not subtracting."""
    tol = _TICK / 2.0
    return sum(v for (a, p), v in own_resting.items() if a == action and abs(p - px) <= tol)


def _mid_from(bid: float | None, ask: float | None) -> float | None:
    """Mid, or None when the book cannot price one — and a ONE-SIDED book cannot.

    ⚠️ `bid > 0 and ask > 0` is NOT a sufficient test, because in orderbook mode `feed._derive`
    FABRICATES the missing side: an empty NO ladder yields `yes_ask = 1.0`, an empty YES ladder
    `yes_bid = 0.0` [feed.py:330-331]. Both are finite and positive, so the old check waved them
    through and invented a mid — on an empty NO ladder, `mid = (bid + 1)/2`.

    Not cosmetic: this value marks inventory for the LOSS CAP and for the headline P&L. A long held
    against an empty NO ladder marks at a large profit that does not exist; the mirror case
    fabricates a LOSS that trips the cap and halts the run, which would read as "this lane loses
    money". An empty NO ladder means NOBODY IS BIDDING NO — never that YES is worth 1.00. This is
    the error that has flipped the SIGN of a whole run's reported P&L.
    `_book_mid` already refused it, but only target SELECTION used that contract; the money path
    used the weaker test."""
    if not bid or not ask:
        return None
    if not (0.0 < bid <= ask < 1.0):   # crossed, one-sided, or a derived 0.0/1.0 placeholder
        return None
    return (bid + ask) / 2.0


def _improve(bid: float, ask: float, ticks: int) -> tuple[float, float]:
    """Step `ticks` inside the touch on BOTH sides, or neither if the spread cannot hold it.

    Quoting AT the touch joins the BACK of the resting queue under price-time priority — and since
    the quote loop cancels and re-posts every cycle, it also resets its own time priority before the
    queue ahead can clear.

    ⚠️ **THAT ARGUMENT IS ABOUT QUEUE ATTRITION ONLY, AND AN EARLIER VERSION OVERSTATED IT** as "at
    the touch a size-1 maker's P(fill) is ~0 by construction". Attrition really is ~0 under
    requoting. But it omits the channel that actually delivers at-touch fills: a **SWEEP** — one
    trade larger than the entire resting level — reaches us regardless of queue position, and
    requoting does not hurt it at all (we only need to be resting when it lands). Measured on a
    wide-spread tape, sweeps are a MINORITY of trades but arrive many times an hour per market —
    small, but not zero. (⚠️ The test is `trade_qty > depth` against a live-book depth read, so treat
    it as an order of magnitude, not a bound.) So P(fill) at the touch is low, not zero, and the
    overstatement mattered: it is the argument for `--improve-ticks ≥ 1`, which sets `queue_ahead`
    to 0 BY CONSTRUCTION and makes the queue attribution collect NOTHING (`ALONE_AT_PRICE` on every
    fill) while validating a policy the offline replay never modelled — the replay quotes at the
    touch and clamps its skew so it can only widen.

    Both sides or neither: the improved quotes need a tick of daylight between them (spread ≥
    2·ticks + 1), else they lock. Improving only the bid would buy with sole priority while selling
    from the back of the queue — a systematic long dressed up as a market-making result."""
    if ticks <= 0:
        return bid, ask
    imp = ticks * _TICK
    if (ask - bid) >= (2 * imp + _TICK) - 1e-9:
        return bid + imp, ask - imp
    return bid, ask


async def _positions(client: KalshiClient, targets=None, raw_out: list | None = None) -> dict[str, float] | None:
    """Positions per ticker, or None on ANY read failure. The caller MUST fail-CLOSED on None
    (halt + cancel-all) — mirroring KalshiClient.get_positions' raise-don't-report-flat contract, so
    a REST outage maps to CANNOT-VERIFY, never to "flat" (which would blind the loss cap + inventory
    cap). An empty dict {} = a confirmed-flat account (legit), which is DISTINCT from None.

    ⚠️ `targets` SCOPES the fail-closed. A whole-read failure is always None, but a
    single UNREADABLE record fails the whole read closed only when its ticker is a TARGET (or is
    unidentifiable) — a bad record for a market we do not quote cannot cause the phantom-flatten the
    None path prevents (`_mark_step` reads only targets), so it is logged and SKIPPED, not halted.
    `targets=None` = the conservative default: every record treated as a target ⇒ halt on any.

    ⚠️ CALLERS MUST READ THE RESULT PER-TARGET (`.get(t)` for `t in targets`), never aggregate the raw
    dict. A GOOD non-target record still lands in it, so a future `sum(cur.values())`-style total would
    both count unscoped holdings AND undo this fix's skip. Every consumer today is per-target; keep it so.

    ⚠️ `raw_out` is a PURE DIAGNOSTIC side-channel and must stay one. When given a list, the raw
    venue records are appended to it — the SAME response this function already fetched, so it costs
    **zero extra rate-limit units** (a second `get_positions()` would cost 10/cycle and, at
    `--markets 1`, push `(20+24)/requote_s` to `(30+24)/requote_s`, breaking the 8 u/s budget below
    `--requote-s 7`). It exists so a run can reconcile `market_exposure_dollars` /
    `realized_pnl_dollars` / `fees_paid_dollars` against fills EVERY cycle instead of only at
    teardown — those fields are the proposed P&L source, and they are NOT yet verified across
    enough captured markets to be trusted alone. It is filled AFTER the fail-closed checks, so a cycle that returns
    None contributes nothing and cannot make a halt look like data."""
    # A 429 is TRANSIENT, not "cannot verify". Kalshi rejects over-limit rather than throttling
    # (per its published rate-limit docs), and /portfolio/* list endpoints cost 10 units each (default_cost=10, vs 2
    # for a cancel) — so a busy cycle can trip the bucket. Failing closed on the FIRST 429 makes the
    # tool unusable for long runs; failing closed on a PERSISTENT one is still correct. Retry a
    # bounded number of times with backoff, then fall through to the existing halt path.
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
        # ⚠️ AN UNREADABLE RECORD IS CANNOT-VERIFY, NOT FLAT. This used to be
        # `float(p.get("position_fp") or 0.0)` inside `except (TypeError, ValueError): continue`,
        # which had TWO fail-OPEN paths: a MISSING/null/empty `position_fp` sailed through `or 0.0`
        # and read as **flat**, and an unparseable one was silently dropped from the dict — also
        # flat, because every consumer treats "absent from `cur`" as 0. Either way a held position
        # vanishes, and `_mark_step` then books the disappearance as a TRADE: measured, a held long
        # long on a record with no `position_fp` produced a phantom SELL at the ask — cash the
        # account never received, credited in the direction that DISARMS the loss cap. Same
        # consequence as a settled ticker vanishing, reached by a second route. Route both down the
        # proven fail-closed path instead of inventing a third.
        if not t:
            print("  ⚠️ position record with no ticker — CANNOT VERIFY, failing closed.")
            return None
        # ⚠️ SCOPE THE FAIL-CLOSED TO TARGETS. A bad record produces the phantom-flatten the None
        # path prevents ONLY for a ticker `_mark_step` reads — a TARGET. A bad record for a non-target
        # market (the account can hold hundreds) is invisible to the caps, so halting the run — and,
        # via the baseline read, REFUSING TO START — on it is a false positive: log loudly and skip.
        # This scope (like the vanish detection and the teardown flags) rests on EXACT string identity
        # between the venue `ticker` and the `targets` list — the same match already load-bearing for
        # normal marking, so a form mismatch fails VISIBLY (a target read as perpetually vanished), not
        # silently; keep ticker forms identical end to end.
        target = targets is None or t in targets
        raw_qty = p.get("position_fp")
        # ⚠️ BEHAVIOURALLY REDUNDANT, KEPT FOR THE MESSAGE — and recorded as such so nobody writes a
        # test that cannot fail. Removing this branch does NOT reintroduce the fail-open: with the
        # `or 0.0` gone, `float(None)` raises TypeError and `float("")` raises ValueError, so the
        # unparseable guard below already halts on both. Mutating this line away leaves the suite
        # green because the behaviour is unchanged — an EQUIVALENT mutant, not a coverage gap. What
        # it buys is an operator reading "has no position_fp" instead of "unparseable … None".
        if raw_qty is None or raw_qty == "":
            if target:
                print(f"  ⚠️ position record for {t} has no position_fp — CANNOT VERIFY, failing closed.")
                return None
            print(f"  ⚠️ non-target {t}: no position_fp — logged and SKIPPED (not read by the caps).")
            continue
        try:
            qty = float(raw_qty)                          # fixed-point; long +, short −
        except (TypeError, ValueError):
            if target:
                print(f"  ⚠️ unparseable position_fp for {t} ({raw_qty!r}) — CANNOT VERIFY, failing closed.")
                return None
            print(f"  ⚠️ non-target {t}: unparseable position_fp ({raw_qty!r}) — logged and SKIPPED.")
            continue
        # ⚠️ A NON-FINITE position is CANNOT-VERIFY, not a position. It parses silently
        # (float("NaN") succeeds), then poisons cash → pnl → and `nan < -loss_cap` is FALSE, which
        # SILENTLY DISABLES the loss kill-switch on a real-money run. Infinity is the mirror image:
        # it would trip the cap on the first cycle forever. A successfully-read GARBAGE value is
        # just as unverifiable as a failed read, so route it down the same proven fail-closed path
        # (None → halt + cancel-all) rather than inventing a second one.
        if not math.isfinite(qty):
            if target:
                print(f"  ⚠️ non-finite position for {t} ({qty!r}) — CANNOT VERIFY, failing closed.")
                return None
            print(f"  ⚠️ non-target {t}: non-finite position ({qty!r}) — logged and SKIPPED.")
            continue
        out[t] = qty
    if raw_out is not None:
        raw_out.extend(pos)          # diagnostic only; never read by the caps
    return out


def _quote_price(qty: float, quote, *, as_fill: bool = False) -> float | None:
    """The price at which a MAKER transacts a position or delta of this sign — bid for long/buy,
    ask for short/sell. `quote` is a `(bid, ask)` pair — **a bare scalar is NOT accepted** (it was
    briefly, and this line kept advertising it for a commit after the code stopped honouring it; a
    caller trusting that got `TypeError: cannot unpack non-iterable float object` inside the quote
    loop). Returns None when the needed side is absent, when the book is CROSSED, and — under
    `as_fill` — when the price is a fabricated placeholder outside (0,1).

    ⚠️ `as_fill` MAKES THIS TWO RULES, differing on the one input that matters most: a FILL refuses
    the fabricated `0.0`/`1.0` an empty ladder produces (booking a buy as free or a sell at $1.00 is
    the most optimistic price that exists, and it would be permanent), while a MARK accepts it as the
    settlement bound. Everything else below is shared.

    Otherwise one rule covers booking and marking, and that is the point. A maker BUYS at its own bid and
    SELLS at its own ask; a position liquidates the same way round — a long exits by selling into
    the bid, a short by buying back at the ask. Using the same side for the fill and for the
    inventory it created keeps P&L exactly 0 at the fill. Mixing them does not: marking at the exit
    while booking at the MID shows half a spread of phantom LOSS on every fill, and marking at the
    mid while booking there shows half a spread of phantom PROFIT that can never be realised —
    which is what this tool used to do, and it flattered every maker fill it ever took."""
    if quote is None:
        return None
    # ⚠️ NO SCALAR FALLBACK. It briefly accepted a bare float as a degenerate zero-spread book, purely
    # so the pre-existing tests kept passing unedited — and that is exactly what made them stop
    # testing production: every caller passes `(bid, ask)`, so every call site — including the
    # kill-switch tests — was exercising a shape the tool never sees. ⚠️ The removal did NOT change
    # anything for `Decimal` — an earlier version of this comment implied it did. `isinstance(
    # Decimal("0.5"), (int, float))` was already False, so a Decimal fell through to the unpack and
    # raised identically before and after. The one real behavioural difference: the scalar branch
    # returned BEFORE the `as_fill` placeholder guard, so a scalar 0.0/1.0 was bookable as a fill
    # price. Unreachable from production (every caller passes `_quote_of`), but not a pure type
    # narrowing either.
    bid, ask = quote
    # ⚠️ A CROSSED BOOK IS NOT A PRICE. `_mid_from` refuses `bid > ask`; this path had NO such check,
    # which put the money path on a WEAKER test than the one whose weakness caused the earlier
    # misvaluation. On a real captured crossed shape (yes_bid 0.65 / yes_ask 0.61) a long marks off
    # the INFLATED bid — a phantom profit, in the direction that disarms the cap. `bid == ask`
    # is a legitimate zero-spread book and stays allowed. (The empirical cause, orderbook float
    # dust, is closed at source by the Decimal migration; this is the belt to that braces.)
    if bid is not None and ask is not None and bid > ask:
        return None
    px = bid if qty > 0 else ask
    # ⚠️ THE FABRICATED PLACEHOLDER IS A VALID MARK AND AN INVALID FILL PRICE. `feed._derive` writes
    # `yes_bid = 0.0` for an empty YES ladder and `yes_ask = 1.0` for an empty NO ladder. Marking
    # held inventory there is exactly the settlement bound — conservative, and what the loss cap
    # wants. Using the SAME number as the price a fill transacted at is the opposite: it books a buy
    # as FREE and a sell at the full $1 payout, and since booking advances `last` it is PERMANENT,
    # with no later cycle to correct it — most of a contract's face value of phantom profit per
    # contract, in the direction that disarms the loss cap. Real prices live in [0.01, 0.99], so these bounds are unambiguous
    # sentinels. Refusing here re-inherits the OLD behaviour for fills — leave `last` unadvanced and
    # re-book once the ladder returns — while keeping the worst-case MARK.
    if as_fill and px is not None and not (0.0 < px < 1.0):
        return None
    return px


def _mark_step(targets, base, last, cur, quotes, cash):
    """One pure P&L/inventory step (unit-tested in tests/test_live_mm_pnl.py). `cur` is a fresh
    positions dict (the caller fail-closes on None BEFORE calling this). `quotes[t]` is a `(bid, ask)`
    pair. ⚠️ EXIT-SIDE, NOT MID — this docstring described the mid convention long after the code
    had stopped using it. Each part is priced on the side that transacts it, `cash −= Δ·_quote_price(Δ)`
    and `mark += inv·_quote_price(inv)`, so P&L is 0 at the fill and accrues only on an adverse move.
    Mid-booking made a maker round trip net exactly ZERO — it was blind to spread capture.
    `base` is the starting positions → only OUR fills count. A ticker with a NEW FILL this cycle whose
    transacting side is unpriceable is SKIPPED and its `last` is NOT advanced (so the fill re-books
    once the book is back). ⚠️ HELD inventory whose exit side is unpriceable is a different case and is
    **marked at its WORST CASE** (long → 0, short → 1), never skipped: skipping it left the cash leg
    unopposed, so an unmarkable SHORT read as a PROFIT and the loss cap could not fire.
    Returns (cash, inv, pnl, new_last, fills)."""
    new_last = dict(last)
    inv = {}
    fills = []
    mark = 0.0
    for t in targets:
        q = quotes.get(t)
        prev = last.get(t, base.get(t, 0.0))   # position as of the last cycle we could VALUE
        # ⚠️ ABSENT ≠ FLAT. A flat-but-LIVE ticker is returned at position_fp 0.00; a SETTLED one
        # is DROPPED from the read entirely. `cur.get(t, 0.0)` conflated them, so a vanished held
        # position booked `d = 0 − prev` — a phantom SELL of the whole position at the ask, inventing
        # cash the account never received and reporting a completed profitable round trip, in the
        # direction that disarms the loss cap. A disappearance is
        # NEVER a trade: carry the last-known position (`c = prev` ⇒ `d = 0`, no fill), and value it
        # WORST-CASE below — the ticker is gone from the venue, most likely settled, so its exit side
        # is unpriceable exactly as a one-sided book's is. The true settlement value is resolved
        # offline; `last` is not advanced, so a later read that lists the ticker can still correct it.
        # A present-at-0.0 ticker is a REAL flatten and still books normally — only absence is skipped.
        vanished = t not in cur
        c = prev if vanished else cur[t]
        d = c - prev
        inv[t] = c - base.get(t, 0.0)    # the POSITION is always known (even when the book isn't) —
        #                                  always populate inv so callers never KeyError on a skip
        # ⚠️ EXIT-SIDE, NOT MID. Each part is priced on the side that actually transacts it, so a
        # ONE-SIDED book is still valuable for the direction it can price: an empty NO ladder leaves
        # a real bid, so a LONG still marks — where mid-marking declared the whole ticker unvaluable.
        # And when the needed side is genuinely absent the feed already fabricates exactly the worst
        # case (`yes_bid=0.0` on an empty YES ladder, `yes_ask=1.0` on an empty NO ladder
        # [feed.py:_derive]), so the worst-case bound falls out of the normal path for free.
        fill_px = _quote_price(d, q, as_fill=True) if d else None
        mark_px = None if vanished else (_quote_price(inv[t], q) if inv[t] else None)
        if (d and fill_px is None) or (inv[t] and mark_px is None):
            # ⚠️ SPLIT THE UNVALUABLE PARTS, don't skip the whole ticker. `d` is a property of the
            # TICKER, so keying the skip on `d != 0` also suppressed the mark for inventory that was
            # ALREADY BOOKED — and since `last` is deliberately never advanced, `d` stayed non-zero
            # every subsequent cycle, so the cap stayed disarmed for the entire one-sided window
            # rather than one cycle. The two parts need opposite treatment:
            #   · BOOKED (`prev − base`): its cash leg is already in `cash`, so it MUST be marked or
            #     the total is a naked proceeds figure — worst case, per the note below.
            #   · UNBOOKED (`d = cur − prev`): in neither `cash` nor `mark`, and cannot be valued at
            #     all without a price. Leave `last` unadvanced so it books when the book returns.
            # WORST CASE is the settlement bound, not a guess: a long is worth 0 if it settles NO,
            # and a short YES (= long NO, fully collateralized) is worth 1 against us if it settles
            # YES. For a short of q at entry p this yields pnl = |q|·p − |q| = −|q|·(1−p), the NO
            # leg's cost basis. ⚠️ It BRACKETS `market_exposure_dollars`, it does not equal it:
            # `cash` accumulates at the book's TOUCH one cycle after the fill, not at the price we
            # actually transacted at. ⚠️ THE SIGN OF THAT ERROR IS OPTIMISTIC, NOT PESSIMISTIC — an
            # earlier version said "safe direction (pessimistic)", true only of the old MID
            # convention. ⚠️ It then justified that with "we quote INSIDE the touch", which is FALSE
            # at the shipped defaults: `_tick` FLOORS bids and CEILS asks, so with `--improve-ticks 0`
            # the inventory skew moves a quote OUTSIDE or leaves it AT the touch, never inside
            # (executed across inv ∈ [−3,+3]). The correct reason is structural: `_mark_step` books a
            # fill at the SAME price it marks the resulting inventory, so P&L is 0 at the fill BY
            # CONSTRUCTION — the tool cannot show the instantaneous adverse move the fill represents,
            # and that residual is the 0s markout, negative in expectation.
            # MEASURED against the venue's own fill prices on a real run: the tool's booked cash
            # was OPTIMISTIC by a material fraction of a cent per fill. ⚠️ SCOPE IT — that gap IS the
            # adverse move between our fill and the next observation, measured in the lane already
            # known worst for adverse selection, so it is lane-specific and must NOT be used as a
            # general discount. The comparison was only valid because that run had no offsetting legs
            # inside a valuation interval (Σ|d| over booked fills equalled Σ|count_fp| over venue
            # fills, per-ticker); without that check the two cash figures are not comparable at all.
            # The venue sends the exact figure on every position record, and `_positions` discards it.
            # Marking rather than HALTING is deliberate: a one-sided book is common and transient,
            # and halting on one would make the tool unusable in exactly the thin markets it exists
            # to quote.
            booked = prev - base.get(t, 0.0)
            if booked:
                mark += booked * (0.0 if booked > 0 else 1.0)
            continue
        if d:
            cash -= d * fill_px
            fills.append((t, d, fill_px))
        if inv[t]:
            mark += inv[t] * mark_px
        new_last[t] = c
    return cash, inv, cash + mark, new_last, fills


def _book_mid(okb: dict) -> float | None:
    """Yes-space mid from a Kalshi orderbook dict, or None if not a sane two-sided book. `yes`/`no`
    are bid ladders of [price, qty]; best YES bid = max(yes price), best YES ask = 1 − max(no price)
    (best_bid = max(price) per feed._derive)."""
    yes = okb.get("yes_dollars") or okb.get("yes") or []
    no = okb.get("no_dollars") or okb.get("no") or []
    try:
        yb = max(float(lvl[0]) for lvl in yes)          # best YES bid
        ya = 1.0 - max(float(lvl[0]) for lvl in no)      # best YES ask = 1 − best NO bid
    except (ValueError, TypeError, IndexError):
        return None
    if not (0.0 < yb <= ya < 1.0):                       # crossed / one-sided / empty → not quotable
        return None
    return (yb + ya) / 2.0


# ⛔ SERIES THE MAKER MUST NEVER QUOTE — defined HERE, on the money path, and imported by the scanner
# so there is exactly one copy. The hazard is STRUCTURAL, not a market-selection preference: these
# are short-dated crypto strikes. They are maker-fee-free and sit at the top of every raw-volume
# screen, which is precisely the danger — their fair value is a public sub-second reference feed, so
# a non-colocated maker is picked off BY CONSTRUCTION. Because they also rank near the top by
# contested flow, any volume-ranked selector over a widened universe reaches for them FIRST, which is
# why the denylist has to sit on the money path rather than in the discovery layer. Extend it with
# any series whose fair value is a public fast feed you cannot beat to.
TRAP_SERIES = frozenset({"KXBTCD", "KXBTC", "KXETHD", "KXXRPD"})
# Resolution floor for RAW REST orderbook levels. The WS path floors at feed._QTY_STEP; the REST
# snapshot does not, and the venue has been observed sending 5.68e-14 qty levels.
_QTY_STEP_REST = 1e-4


def _book_touch_depth(okb: dict) -> float | None:
    """Contracts resting AT the touch, worse side of the two, or None if unreadable.

    This is the queue a size-1 maker must get through before a trade reaches it, so it — not raw
    volume — is what decides whether we ever fill. At MATCHED flow, series differ by three orders of
    magnitude in touch depth, and raw volume does not predict which: a market can trade heavily and
    still be unreachable behind its own queue, or trade thinly with almost nothing resting in front
    of you. Takes the WORSE (deeper) side because we quote BOTH: the side that fills late is the side
    that decides the round trip.
    """
    yes = okb.get("yes_dollars") or okb.get("yes") or []
    no = okb.get("no_dollars") or okb.get("no") or []

    def _touch_qty(ladder) -> float | None:
        """Qty at the best REAL price. Sub-resolution levels are dropped BEFORE taking the max.

        ⚠️ Without that filter this fails OPEN, in the flattering direction: a dust level
        (the venue has sent 5.68e-14) at a better price wins `max(price)`, the qty sum then returns
        ~0, and a 16,000-deep book reports an EMPTY queue and sails through the gate. That is the same
        `max(price)`-selects-a-ghost pattern that produced the crossed-book phantom edges, and it is
        the exact inverse of this function's promise that an unknown queue is not a safe queue. The WS
        path already floors at `_QTY_STEP`; this reads the raw REST snapshot, which does not.
        """
        real = []
        for lvl in ladder:
            p, q = float(lvl[0]), float(lvl[1])
            if q >= _QTY_STEP_REST:
                real.append((p, q))
        if not real:
            return None
        best = max(p for p, _ in real)
        return sum(q for p, q in real if p == best)

    try:
        yq, nq = _touch_qty(yes), _touch_qty(no)
    except (ValueError, TypeError, IndexError):
        return None
    if yq is None or nq is None:
        return None            # one-sided/dust-only → unreadable → caller skips (fail-CLOSED)
    return max(yq, nq)


def _event_of(ticker: str) -> str:
    """Event key = ticker minus the final outcome segment (…CHUYOM-CHU and -YOM share one event)."""
    return ticker.rsplit("-", 1)[0]


# Kalshi MAKER-fee status per series. ⚠️ READ FROM THE VENUE, never inferred from the fee-schedule
# PDF. The PDF's Table 2 lists 76 maker-charged series; the live `/series` API lists 130 — the PDF is a
# strict SUBSET, so "absent from the PDF" does NOT mean maker-free. That inference mislabelled
# KXNBAGAME and KXNHLGAME (both `quadratic_with_maker_fees`) as free, and `--maker-free-only` would
# have selected them and paid the ~0.44c/fill it exists to avoid. The discriminator is `fee_type`,
# NOT `fee_multiplier` (which is 1 on both kinds). Populated by _load_maker_fees() before selection.
_MAKER_CHARGED: dict[str, bool] = {}


# A game's clock: `expected_expiration_time` is the expected END, so a market inside the last
# _GAME_LEN_S before it is a game IN PROGRESS. Beyond _LONG_DATED_S there is no game clock to speak of
# at all — that is a weather/election/"next team" market, or a game more than a day out.
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

    ⚠️ This replaces a boolean `_live()` that read `expiration − 3h ≤ now < expiration` as "game in
    progress". That is a SPORTS concept and it was applied to every market: on a weather, macro or
    "LeBron's next team" market (93 days out) there is no game to be in progress, so the old test was a
    category error that silently answered False and pushed every non-sports market into a fallback tier.
    Naming the long-dated case explicitly is the fix — it is a real phase, not a failed liveness check."""
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

    `feed._matches_prefix` is a HARD FILTER on inbound messages and defaults to config.KALSHI_SERIES —
    the 12 cross-arb SPORTS series. The MM universe is ~97% of Kalshi, so that default silently drops
    every book message for any non-arb target: the book never populates, `_quote` returns early on
    `not bid or not ask`, and the run places ZERO orders while logging a healthy connect+subscribe."""
    return sorted({_series_of(t) for t in targets})


async def wait_for_book(book, targets, *, tries: int = 5, delay: float = 3.0) -> bool:
    """True once ANY target has a two-sided book. Refusing to start blind on the BOOK mirrors the
    existing refusal to start blind on POSITIONS.

    ANY, not ALL: a genuinely one-sided quiet market is quotable-in-principle and must not abort the
    run, whereas the failure this guards (a filter dropping every message) takes out every target at
    once — so ANY separates the two cases while ALL would false-refuse."""
    for _ in range(tries):
        await asyncio.sleep(delay)
        if any(book.get_best_bid(t) and book.get_best_ask(t) for t in targets):
            return True
    return False


def eligible_tickers(markets, *, pregame_only: bool = False) -> list[str]:
    """Markets we are willing to quote, by phase.

    Excluded:
      • `closing` — already PAST its expected expiry. These stay `open` for hours (settlement lag / the
        postponement buffer) and a DRY preview picked one 14h expired: the outcome is effectively
        decided and it can settle out from under any inventory we hold.
      • `unknown` — timing UNREADABLE. Fail-closed on purpose: if `expected_expiration_time` ever goes
        missing or changes shape, every market classifies `unknown`, and treating that as eligible would
        make this gate (and --pregame-only) silently inert while still reading as armed — re-admitting
        the very expired market the gate exists to exclude. Cannot-verify is not permission.
      • `live`, only under --pregame-only.

    ⚠️ `live` means "within _GAME_LEN_S of expiry", which is exact for the sports series (Kalshi sets
    expected_expiration = start + 3h) but for a non-sports market just means "about to resolve" — also
    a lane worth avoiding for MM, so --pregame-only stays meaningful there, just for a different
    reason. A game running long (extra innings, rain delay) crosses into `closing` and drops out while
    still trading; that loses flow but only ever skips, never quotes."""
    return [m.ticker for m in markets
            if _phase(m) in (("pregame", "long-dated") if pregame_only
                             else ("pregame", "long-dated", "live"))]


def _series_of(ticker: str) -> str:
    """Series ticker = the segment before the first '-' (KXNPBGAME-26JUL…-CHU → KXNPBGAME)."""
    return ticker.split("-", 1)[0]


def _maker_mult(ticker: str) -> int:
    """1 = maker fee charged, 0 = maker-free — from the venue, with a FAIL-SAFE default.

    An unknown series returns 1 (CHARGED) on purpose: `--maker-free-only` must never select a market
    whose fee status we could not verify. Guessing "free" would be the expensive direction."""
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
    Guardrails learned the hard way:
      • contested band — mid in [min_price, max_price]; the tails of a near-resolved game are the worst
        place to measure MM economics (no two-sided flow, max adverse selection, huge %-per-tick).
      • one-per-event — never quote both complementary sides of the same game (same risk twice).
      • maker-fee preference — consider M=0 (maker-FREE) series FIRST: on an M=1 series the maker fee
        roughly CANCELS the favourable markout at the widths we quote, so the edge is arithmetic, not
        market. `maker_free_only` hard-excludes M=1 entirely.
      • FLOW ranking — within a fee class, take the most-traded market first. Without this the picker
        returned whatever the scanner happened to list first, which is how a throughput run ends up in
        the thinnest corner of the venue.
        ⚠️ `vol_of` is PER-MARKET (`volume_24h_fp` on ONE strike), while series-level volume figures
        are SUMS over every strike in the series — one series can show a few thousand while another
        shows millions on the same field. A displayed per-market vol24 is therefore far smaller than
        the series figure and the two are NOT comparable. Per-market is the right key here — it ranks
        the book we will actually rest in — but it does not by itself produce a series-level ordering:
        choosing the series is still the operator's job, via --series."""
    vol_of = vol_of or {}
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
        if mid is None or not (min_price <= mid <= max_price):
            continue
        # QUEUE GATE. Raw volume ranks a market by how much trades, not by whether any of it reaches
        # US. Behind a touch that is orders of magnitude deeper than our size, a back-of-queue maker
        # effectively never fills — so a HIGH-volume market can be strictly worse than a quiet one
        # with a shallow touch.
        # NARROWING ONLY: this can reject a candidate the old code would have taken, never admit one it
        # would have skipped — so it cannot widen what gets quoted. `None` (unreadable depth) is
        # treated as FAIL and skipped: an unknown queue is not a safe queue.
        if max_touch is not None:
            depth = _book_touch_depth(okb)
            if depth is None or depth > max_touch:
                continue
        out.append(t)
        seen_events.add(ev)
        if len(out) >= want:
            break
    return out


class MakerSession:
    """The maker run's order lifecycle and its state, in one testable object.

    ⚠️ WHY THIS CLASS EXISTS — it is not tidiness. These nine methods were closures inside a
    720-line `main()` (in a 1,264-line file holding 11 nested functions in total) [MEASURED by AST
    over `git show 44d93d0^`], which made them unreachable from a test except by string-matching
    their own source. Three "regression tests" written that way passed against inverted branches, and four
    consecutive attempts at a single P&L defect each shipped broken in a new direction, because
    nothing could execute the code that was being changed. The structure was the defect generator;
    every fix on top of it inherited that.

    Extracted with every method body AST-identical to the closure it came from, and captured
    variables re-bound from `self` at the top of each method rather than renamed through the body
    (which also preserves the comments, where most of the bug history lives).

    ⚠️ THAT IS NOT A PROOF OF BEHAVIOUR PRESERVATION, and calling it one hid a real regression for
    two review rounds. Re-binding from `self` preserves semantics for variables that are MUTATED; it
    does NOT for variables that are REBOUND. `inv` is rebound — `_mark_step` returns a fresh dict
    each cycle — so `self.inv` froze at all-zeros and both `--inv-cap` and `--inv-coef` went inert
    while the header still advertised the cap as a hard rail. `main()` now writes it back. The other
    shared names (`resting`, `order_meta`, `filled_oids`, `stats`, `seen_fills`, `markout_pending`)
    are mutate-only and were unaffected. Identical bodies are necessary, not sufficient.

    State is deliberately grouped: collaborators (`client`, `book`, `w`), config (`args`,
    `targets`), the order lifecycle (`resting`, `order_meta`, `filled_oids`) and the run's
    measurement accumulators (`inv`, `stats`, `seen_fills`, `markout_pending`).
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
        self.inv: dict[str, float] = {t: 0.0 for t in targets}
        self.stats = {"placed": 0, "rejected": 0}
        self.seen_fills: set[str] = set()
        # ⛔ THE FLATTEN'S OWN ORDERS, KEYED ON `order_id` AT PLACEMENT — NOT on a fill read.
        # `flatten_on_exit` deduped by adding each `fill_id` to `seen_fills`, which only works if its
        # `get_fills` read SUCCEEDS. That read is a 10-unit `/portfolio/*` call issued during a
        # teardown that fires ~7 of them back to back, and a real run has already 429'd at 15.3 u/s —
        # so on a `positions_cannot_verify` halt (429-driven by construction) the failure branch is
        # the LIKELY one. When it failed, `fills = []`, nothing reached `seen_fills`, and the teardown
        # catch-up re-booked our own forced exit as `real_fill` + `spread_capture` + `markout`.
        # That is not a neutral duplicate. The flatten sells at the ASK and buys at the BID, so its
        # `spread_capture` is POSITIVE BY CONSTRUCTION — a forced exit from a position the loss cap
        # just called a loser, recorded as a favourable maker-edge observation. And its `fill_ts` is
        # up to `--flatten-wait-s` (60s) stale by then, so all three markout horizons resolve off one
        # mid in a single pass — the exact defect that corrupted an earlier run's markout, reached
        # through a new door.
        # Keying on the order we PLACED is fail-closed: the id is known before any read happens.
        self.flatten_oids: set[str] = set()
        self.flatten_fill_ids: set[str] = set()
        self.markout_pending: list[dict] = []
        # WHY DID THE QUEUE AHEAD OF US CLEAR — did it trade away, or cancel away?
        # `qtrack` accumulates the tape and the L2 deltas at our own price levels; `tape` is
        # the trade feed supplying half of that and is None until main() wires it (DRY never does —
        # DRY places no orders, so there is no queue to attribute). `tape_ok` below is what keeps a
        # dead socket from reading as "the queue never traded", i.e. NOT_TRADE_THROUGH on every fill.
        self.qtrack = QueueTracker()
        self.tape = None
        self.verdicts: dict[str, int] = defaultdict(int)
        # ── crash-durable order record (bot/core/maker_state.py) ────────────────────────────────
        # None until main() attaches one, and ONLY on a real run: a DRY preview places nothing, so
        # letting it write would reset a crashed real run's order list — a preview destroying the
        # evidence of a crash. When attached, `quote()` records each order's INTENT before sending
        # it, so a SIGKILL between the send and the response still leaves a trace of what may be
        # resting. Optional rather than required so every existing MakerSession construction (the
        # unit tests, mm_fakes) keeps working unchanged.
        self.state: maker_state.MakerStateStore | None = None

    def sync_inventory(self, inv: dict[str, float]) -> None:
        """Adopt the inventory `_mark_step` just computed.

        ⚠️ THE REASON THIS IS A METHOD and not two inline lines: it is the fix for a regression the
        extraction introduced, and inline it could only be pinned by string-matching `main()`'s
        source. `_mark_step` RETURNS a fresh dict rather than mutating, so rebinding main's local
        `inv` left the copy `quote()` reads frozen at zeros — silently disabling `--inv-cap` and
        `--inv-coef` while the module header advertised the cap as a hard rail.

        Mutates in place rather than reassigning, so no other holder of the dict goes stale."""
        self.inv.clear()
        self.inv.update(inv)

    def mid(self, t):
        book = self.book

        return _mid_from(book.get_best_bid(t), book.get_best_ask(t))

    def quote_of(self, t):
        """`(bid, ask)` for P&L. ⚠️ Deliberately NOT `mid()`: a one-sided book still prices the side
        that can transact, and the feed's fabricated `0.0`/`1.0` on an empty ladder IS the worst
        case — so this degrades to the conservative bound without a special branch, where `mid()`
        returns None and forces one."""
        book = self.book
        return (book.get_best_bid(t), book.get_best_ask(t))

    @property
    def tape_ok(self) -> bool:
        """Is the trade tape CONFIRMED live — venue-acked, not merely connected?

        Read straight off `KalshiTradeFeed.subscribed`, which stays False until the venue acks and
        is reset before every re-subscribe. This flag went unread for a long time; it is now
        load-bearing, because `traded = 0` from a rejected subscription is arithmetically
        identical to a queue that pulled, and would report NOT_TRADE_THROUGH on 100% of fills."""
        return bool(self.tape is not None and self.tape.subscribed)

    def log_queue_dynamics(self, oid: str, *, outcome: str,
                           filled_qty=None, fill_ts: float | None = None) -> None:
        """Close out one order's queue attribution and write its `queue_dynamics` row.

        Called on BOTH fill and cancel, on purpose: the cancelled population is what says "we sat
        behind 35 contracts, 2 of them traded, and we never got there" — the survivorship half that
        a fills-only log cannot express.

        One row per order, by construction: `close()` removes the tracker, so the repeated
        `log_unfilled` that a failed-then-retried cancel produces (see `cancel_all`) writes a second
        `order_outcome` row but NOT a second row here."""
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
        # Running tally, printed each cycle by the caller. A run whose tape is dying returns ~100%
        # UNKNOWN — safe-direction, but at teardown that is a wasted session; visible per cycle it is
        # abortable at minute 5. (The 5s ping makes reconnects more likely on a jittery link, and any
        # order placed inside a gap is UNKNOWN for its whole life.)
        # ⚠️ FILLS ONLY. The tape guards fire BEFORE the `outcome != "fill"` check, so a cancelled
        # order carries UNKNOWN_TAPE_DOWN/GAP while a healthy cancel carries nothing — tallying both
        # mixes populations and reads as "96% unreadable" on a run whose fills are 1/1 readable,
        # arguing to abort a run that had already recovered. The question this tally answers is "of
        # the fills, how many can I read", and the CSV carries every row either way.
        if row["verdict"] and outcome == "fill":
            self.verdicts[row["verdict"]] += 1

    def log_unfilled(self, oid: str, cancel_status: str = "cancelled") -> None:
        """Record the fate of an order that is being cancelled without having filled.

        The survivorship point: a run that logs only its FILLS reports on the quotes that worked and
        is silent about the ones that didn't, which is the population that tells you whether the
        policy is reaching the front at all. `queue_ahead` on an unfilled order is the number that
        says "we sat behind a deep queue for the whole cycle and nothing came" — without it, a
        zero-fill market is indistinguishable from a dead one.

        Best-effort by construction: a cancel whose response was lost still lands here, because the
        order is gone from OUR tracking either way and the venue sweep is what reconciles reality."""
        filled_oids = self.filled_oids
        order_meta = self.order_meta
        w = self.w

        meta = order_meta.get(oid)
        if not meta or oid in filled_oids:
            return
        # `already_gone` means the venue had nothing to cancel — the order filled (or was already
        # cancelled) between our last fill poll and now. Recording that as `unfilled` would be the
        # same lie by a slower route, so it gets its own outcome and an analysis can exclude it.
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
        venue, and the caller MUST NOT place on top of it.

        ⚠️ A failed cancel used to still clear `resting[t] = []`, so the next cycle placed a NEW order
        on top of one still resting on the venue — unbounded stacking on a 429 storm, and invisible to
        the inv cap (which reads `inv[t]`, never resting size). Now, on ANY failure, RECONCILE
        `resting[t]` from the venue (`get_resting_orders([t])`) so still-live orders stay tracked and are
        re-cancelled next cycle; if that reconcile ALSO fails, KEEP the tracked oids rather than dropping
        them. Only the all-success path clears to []."""
        _log_unfilled = self.log_unfilled
        client = self.client
        resting = self.resting

        failed = False
        for oid in list(resting[t]):
            status = "cancelled"
            try:
                r = await client.cancel_order(oid)
                # READ THE VENUE, don't infer. `cancel_order` maps a 404 to
                # {"status": "already_gone"} — i.e. the order was no longer resting, which is the
                # venue telling us it filled (or was already gone). Inferring "unfilled" from our
                # own not-yet-updated fill set is how ~95% of fills got labelled as non-fills.
                if isinstance(r, dict) and r.get("status"):
                    status = str(r.get("status"))
            except Exception as e:
                # The order may still be RESTING — a failed cancel is not a cancelled order. `failed`
                # makes the whole method report that, so the caller skips placement and we reconcile
                # below instead of dropping a live order from tracking.
                status = "cancel_failed"
                failed = True
                print(f"  cancel {oid[:8]} failed: {e!r}")
            else:
                # Cancelled (or the venue said it was already gone) — it can no longer be resting,
                # so drop it from the crash record. Deliberately in the `else`, NOT after the
                # try/except: a FAILED cancel means the order may still be live, and forgetting it
                # here would hide it from recovery. Same asymmetry `resting[t]` itself uses.
                if self.state is not None:
                    with contextlib.suppress(Exception):
                        self.state.clear_order(oid)
            # Logging must never break cancellation. Uncaught, this propagates out of _cancel_all →
            # _quote → the outer finally → re-enters _cancel_all → raises again and escapes the
            # finally itself, skipping the venue stray sweep and leaving live orders resting.
            try:
                _log_unfilled(oid, status)
            except Exception as e:                        # noqa: BLE001 — logging is never fatal
                print(f"  (order_outcome logging error, ignored: {e!r})")
        if failed:
            try:
                live = await client.get_resting_orders([t])
                resting[t] = [o.get("order_id") for o in live if o.get("order_id")]
                print(f"  ⚠️ cancel_failed on {t}: reconciled {len(resting[t])} still-resting order(s) "
                      f"from the venue — skipping placement this cycle, retrying cancel next cycle.")
            except Exception as e:                        # noqa: BLE001 — keep tracked oids for retry
                # ⚠️ Keeping ALL original oids means an already-cancelled one is re-cancelled and
                # re-logged next cycle (two order_outcome rows for one order, distinct labels). It
                # self-heals once any cancel succeeds and the reconcile trims; instrumentation-only,
                # never the caps. Only reachable when BOTH cancel_order AND get_resting_orders fail —
                # a storm the positions fail-closed usually halts anyway. Left as-is deliberately —
                # noted here for anyone counting order_outcome rows.
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
        """Belt-and-suspenders after cancelling TRACKED orders: ask the venue what is STILL resting
        in our target markets and cancel it. This reaches the one residual _cancel_everything can't —
        an order whose create-response was LOST (booked on the venue, reply dropped) has no captured
        order_id, so a tracked-id cancel never sees it. Fail-LOUD: if the listing can't be read we
        cannot claim 'no strays', so we say exactly that (never pretend clean). ⚠️ Cancels ALL resting
        orders in the target markets — fine for this dedicated paper-MM process (we don't hand-trade
        these markets alongside it); it is not ticker-scoped to only THIS run's orders.

        RETURNS True only when the VENUE ITSELF is the evidence: the listing succeeded and either
        nothing was resting or every stray cancelled cleanly. False on an unreadable listing or any
        failed cancel. The return is what lets the teardown decide whether it may record a CLEAN
        EXIT in the crash state — a clean-exit flag written on our own belief rather than the
        venue's answer is precisely the fiction `bot/runner/reconcile.py` exists to prevent.
        Previously returned None and every caller ignored it; the return is additive."""
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
        """PASSIVE flatten of OUR inventory, on EVERY exit. The caller gates on
        `real and --flatten-on-exit`; live quotes are already cancelled by `cancel_everything`.
        ⚠️ It used to be CLEAN-EXIT-ONLY (`and not halted`). That gate was removed, because the
        process is exiting and the loss cap dies with it, so skipping the flatten on a halt walked
        away from an adverse position and left it unattended. This is passive/post-only/reducing-only
        and self-refuses on an unreadable venue read (step 1), which is why it is safe on a halt.
        This:
          1. reads the VENUE position (never local belief — a FAILED read does NOT flatten, so we
             never size a real order off the number the fail-closed exists to distrust);
          2. for each of OUR net positions (`venue − base`, so INHERITED `base` is left alone), posts
             ONE post-only reducing order AT THE TOUCH (long → sell at the ask, short → buy at the
             bid) — earns the half-spread, never crosses, so the post_only rail is PRESERVED;
          3. waits `wait_s`, then reads OUR OWN fills (matched by `order_id`) and records them as
             `flatten_fill` + a `flatten_summary`, adding each `fill_id` to `seen_fills` so the
             teardown catch-up does NOT re-book them as `real_fill`/`markout`/`spread_capture` and
             confound the maker edge.
        Any unfilled residual is left resting for the venue sweep to cancel, and the teardown's
        OPEN-POSITION warning still fires — passive often will NOT clear a thin long-dated book, and
        the operator must still be told. Never takes; `--flatten-cross` (the taker last resort) and
        the PREFLIGHT flatten are deliberately NOT in v1. Floats throughout, matching the existing
        fill parser `_record_fills_and_markout` and the float `_tick` price path.
        ⚠️ v1 limitations (accepted, narrow): a SECOND Ctrl-C during the wait runs the sweep but then
        truncates the rest of teardown (no final position summary); and a flatten fill whose
        create-response was LOST (no captured `order_id`) is not deduped, so the catch-up books it into
        the edge metrics. Both require an unusual event (double-force-kill; a dropped reply on a flatten
        order) and the money stays safe — the venue sweep cancels residual either way."""
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
                # ⚠️ ABSENT ≠ FLAT. A settled or transiently-dropped ticker is absent from the
                # read; `venue.get(t, 0.0)` would read it as flat and — with inherited `base` — FLIP
                # net's sign, placing a real reducing order in the WRONG direction. That is the
                # same phantom-trade class already closed in `_mark_step`, and it must not re-enter here. Never
                # flatten a ticker we cannot read.
                continue
            net = venue[t] - base.get(t, 0.0)            # OUR fills only; sized off the READ (t present)
            qty = math.floor(abs(net))                   # FLOOR: never OVER-sell into a flip if the
            if qty < 1:                                  # venue rounds a fractional count up. A flat or
                continue                                 # sub-contract residual is left for the sweep +
            #                                              the OPEN-POSITION warning — no dust order.
            bid, ask = book.get_best_bid(t), book.get_best_ask(t)
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
                    # ⛔ RECORD IT NOW, BEFORE ANY READ CAN FAIL. This is what makes the dedup
                    # fail-closed: the catch-up excludes fills by `order_id`, so a failed
                    # `get_fills` below can lose the flatten's ACCOUNTING but can no longer let its
                    # fills be re-booked as maker flow. See `self.flatten_oids`.
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
        total_n = total_cost = 0.0
        per_ticker: dict[str, float] = defaultdict(float)
        for f in fills:
            oid, fid = f.get("order_id"), f.get("fill_id")
            if oid not in placed or not fid or fid in seen_fills:
                continue
            seen_fills.add(fid)              # so the teardown catch-up does not re-book this fill
            self.flatten_fill_ids.add(fid)   # ...and so it is EXCLUDED from the run's fill COUNT
            try:
                raw_px = f.get("yes_price_dollars")
                px = float(raw_px) if raw_px not in (None, "") else None
                n = float(f.get("count_fp") or 0.0)
                fee = float(f.get("fee_cost") or 0.0)
            except (TypeError, ValueError):
                continue
            # Reject a missing/out-of-range price rather than `or 0.0`-booking a phantom flatten fill
            # (would understate the flatten cost; inventory itself is tracked via the venue read) — A2.
            if px is None or not (0 < px < 1):
                print(f"  ⚠️ flatten: skipping fill with unusable yes_price_dollars="
                      f"{f.get('yes_price_dollars')!r}")
                continue
            per_ticker[placed[oid]] += n
            total_n += n
            total_cost += px * n + fee
            w.writerow([f"{f.get('ts') or int(time.time())}", "flatten_fill",
                        f"{placed[oid]} {f.get('action')} n={n:g} px={px:.4f} fee={fee:.4f}",
                        "", "", ""])
        w.writerow([f"{time.time():.0f}", "flatten_summary",
                    f"filled {total_n:g} across {len(per_ticker)} ticker(s) "
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

        bid, ask = book.get_best_bid(t), book.get_best_ask(t)
        # ⚠️ QUOTE ONLY WHERE WE CAN PRICE BOTH SIDES. The old `bid>0 and ask>0` waved through the
        # feed's FABRICATED one-sided placeholders (empty NO ladder → yes_ask=1.0, empty YES ladder →
        # yes_bid=0.0 [feed._derive]) — so the maker quoted on a book its own `_mid_from` rejects,
        # placing a real order against a fabricated side (a systematic one-directional lean, since only
        # the real side can fill) whose inventory then cannot be two-sided-marked. Gate on the SAME
        # priceability contract the mark/selection path uses (`0.0 < bid <= ask < 1.0`), so quotable ==
        # markable and a one-sided OR crossed book is never quoted.
        if _mid_from(bid, ask) is None:
            return
        # ⚠️ get_depth(side) = "size available to BUY side", i.e. the OPPOSITE ladder: _depth["yes"]
        # comes from the NO bids (= yes ASK depth), _depth["no"] from the YES bids (= yes BID depth)
        # (see `feed.get_depth`). Mapping bid=get_depth("yes") INVERTS OBI, which makes the skew
        # lean INTO adverse flow instead of away from it. It did exactly that once.
        depth_b = book.get_depth(t, "no") or 0.0    # yes BID depth
        depth_a = book.get_depth(t, "yes") or 0.0   # yes ASK depth
        t_depth = time.time()                       # the reference time for `ahead` — see track_lag_s
        # ⚠️ THIS DEPTH INCLUDES OUR OWN LAST-CYCLE ORDERS. It is read BEFORE `_cancel_all` below (it
        # has to be — `quote_ctx`'s replay row wants the untouched pre-policy book), so in a thin
        # market our own resting size is part of it. Left uncorrected, `ahead` overstates the
        # EXTERNAL queue by our own size, and the queue attribution then reports a fill as
        # NOT_TRADE_THROUGH on the strength of our own requote churn. The
        # subtraction below is exact — we know precisely what we had resting and at what price —
        # which is why it is done from `order_meta` rather than by re-reading the book after the
        # cancel and racing our own WS delta. `quote_ctx` keeps reporting the RAW depth: it is the
        # book as it was, and the replay wants that.
        # ⚠️ SKIP ORDERS THAT ALREADY FILLED. `resting[t]` is cleared only by `_cancel_all` below,
        # and fills are polled BEFORE quoting, so at this point the list still names orders that
        # filled this cycle — which are gone from the venue book and therefore NOT in `depth_b` /
        # `depth_a`. Subtracting them anyway understates `ahead` by our size on exactly the cycles
        # that FOLLOW a fill, i.e. where the repeat fills are in a thin lane, and it understates it
        # toward TRADE_THROUGH: under-detecting adverse selection, which is the error that
        # green-lights more capital. (A partial fill leaves less than `args.size` resting and we do
        # not track how much, so skipping is also the right call there — not subtracting inflates
        # `ahead`, which errs toward the alarming verdict.)
        own_resting: dict[tuple[str, float], float] = defaultdict(float)
        for _oid in resting[t]:
            _m = order_meta.get(_oid)
            if _m and _oid not in self.filled_oids:
                own_resting[(_m["action"], _m["px"])] += float(args.size)
        obi = (depth_b - depth_a) / (depth_b + depth_a) if (depth_b + depth_a) > 0 else 0.0
        # Whole-tick lean, rounded ONCE from the combined OBI+inventory intent (see _skew_ticks) —
        # both quotes move by the same tick count, so the recenter is symmetric and there is no
        # sub-tick shift for _tick's floor/ceil to turn into a one-sided step function.
        shift = _skew_ticks(obi, inv[t], args.obi_coef, args.inv_coef) * _TICK
        touch_b, touch_a = bid, ask                 # the untouched BBO, before any policy applies
        bid, ask = _improve(bid, ask, args.improve_ticks)
        if not await _cancel_all(t):
            # A cancel FAILED, so an order may still be resting on the venue. Placing now would
            # stack a new order on top of it (unbounded on a 429 storm, and invisible to the inv cap,
            # which reads inv[t] not resting size). Skip placement this cycle; cancel_all reconciled
            # resting[t] from the venue, so next cycle re-attempts the cancel before quoting.
            w.writerow([f"{time.time():.0f}", "quote_ctx",
                        f"{t} SKIP=cancel_failed resting={len(resting[t])} — retry next cycle",
                        "", "", ""])
            return
        pairs = []
        if inv[t] < args.inv_cap:
            pairs.append(("buy", _tick(bid + shift, "buy")))
        if inv[t] > -args.inv_cap:
            pairs.append(("sell", _tick(ask + shift, "sell")))
        # THE REPLAY ROW. One account cannot run two arms at once, so a live run can never A/B its
        # own policy — unless it records enough of the world to reconstruct the counterfactual
        # afterwards. This is that record: the untouched touch, both queue depths, our inventory, and
        # the prices we actually sent. With it, an offline replay can ask "what would quoting AT the
        # touch have filled here?" against the same book, turning every live run into a retrospective
        # A/B. Without it, a null result is permanently unattributable between a thin market and a
        # policy that requoted its own priority away — which is exactly what a real run ran into.
        # Pure observation: written AFTER the prices are decided, and nothing downstream reads it.
        # ⚠️ NO `improved` FIELD ON THIS ROW, deliberately. Two attempts at one both failed: the
        # first tested the pre-shift bid only, the second used `any(...)` over both sides — but
        # `shift` moves both quotes the SAME way, so it improves one side and pushes the other a
        # full tick outside the book, and a single per-cycle boolean cannot describe that. Worse,
        # under the shipped defaults it was true iff |inv| >= 2, i.e. a relabelling of inventory
        # sign masquerading as a policy label. The honest per-SIDE value lives on the order rows
        # (`fill_attrib` / `order_outcome`), which is where an analysis should read it. Deleting a
        # field that cannot be made correct beats patching it a third time.
        w.writerow([f"{time.time():.0f}", "quote_ctx",
                    f"{t} touch={touch_b:.4f}/{touch_a:.4f} spread_ticks="
                    f"{round((touch_a - touch_b) / _TICK)} depth_bid={depth_b:g} "
                    f"depth_ask={depth_a:g} obi={obi:+.4f} inv={inv[t]:+.2f} shift={shift:+.4f} "
                    f"sent={'/'.join(f'{a}@{p}' for a, p in pairs) or 'NONE(cap)'}",
                    "", "", ""])
        for side_action, px in pairs:
            # ── DURABLE INTENT, WRITTEN BEFORE THE SEND ────────────────────────────────────────
            # The window nothing else can cover is "the create reached the venue and we died before
            # the response arrived": the order rests, and no in-process record of it exists. That
            # is not hypothetical here — the teardown comment ~80 lines below already names the
            # lost-create-response stray as the one `_cancel_everything` cannot reach, and SIGKILL
            # (an OOM kill is the realistic route to one) removes even the sweep.
            #
            # ⛔ FAIL-CLOSED, SKIP-NOT-FIRE. If the intent cannot be written we do NOT place the
            # order. An order we cannot durably record is an order nothing can ever cancel, which
            # is strictly worse than a missed quote — and skipping is the same safe direction every
            # other gate here takes (`_freeze_gate_reject`, the post_only reject). It costs one
            # cycle's quote on that side, not the run.
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
                    # It is a deterministic function of `improved`: inside the touch nobody else can
                    # hold our price so it is 0 by construction, at the touch it is the visible
                    # level. Regressing fills on `ahead` is regressing on the arm label, and
                    # "improvement buys priority" and "improvement is DEFINED as priority" produce
                    # identical rows. It is recorded because it makes the row self-describing, not
                    # because it tests anything.
                    # ⚠️ THIS APPLIES TO THE QUEUE ATTRIBUTION TOO, and it nearly ate it: `ahead`
                    # is the denominator of the fill-cause verdict, so an improved quote's
                    # `ahead == 0` made every fill "TRADE_THROUGH" — the arm label again, wearing a
                    # verdict's name. `queue_tracker._verdict` therefore returns ALONE_AT_PRICE for
                    # `ahead == 0` rather than a finding. Do not "simplify" that branch away.
                    #
                    # ⚠️ A post-ack `touch_depth` was tried here and REMOVED. `get_depth` returns
                    # the size at the CURRENT best level, so once our own order is the touch it
                    # returns our own size — self-inclusive, measuring a different price level in
                    # each arm, and racing our own WS delta against the REST ack so it is
                    # non-deterministically one or the other. It was sold as the observed covariate
                    # and was less trustworthy than what it replaced. The pre-quote book state is
                    # already recorded on `quote_ctx`; read it from there.
                    px_f = float(px)
                    # Net out our OWN last-cycle size at each touch price before asking how many
                    # contracts were in front of us — see the `own_resting` note above.
                    # ⚠️ Matched with the SAME half-tick tolerance `_queue_ahead` uses, not by dict
                    # equality on a float. Two spellings of one price comparison sitting three lines
                    # apart is how the shadow probe put 6 of 99 on-grid cent prices a tick off the
                    # touch; a miss here silently skips the subtraction.
                    ahead, improved = _queue_ahead(
                        side_action, px_f, touch_b, touch_a,
                        max(0.0, depth_b - _own_at(own_resting, "buy", touch_b)),
                        max(0.0, depth_a - _own_at(own_resting, "sell", touch_a)))
                    t_placed = time.time()
                    order_meta[oid] = {"tk": t, "action": side_action, "px": px_f,
                                       "t_placed": t_placed, "ahead": ahead,
                                       "improved": improved,
                                       "spread_ticks": round((touch_a - touch_b) / _TICK)}
                    # Start watching the queue in front of this order. `px` is
                    # passed as the SENT STRING, not `px_f`: the tracker keys levels by exact
                    # Decimal, and a float round-trip is the error that put 6 of 99 on-grid cent
                    # prices a tick off the touch in the shadow probe. `ahead` is forwarded as-is
                    # INCLUDING None — "we quoted where depth is unobserved" must not become "the
                    # queue was empty", which is the reading that would flatter TRADE_THROUGH.
                    # `t_sent` is the DEPTH read, not the create call: `ahead`'s reference time is
                    # when we measured the book, and between that and tracking sit `_cancel_all`'s
                    # round trip per resting order AND the create's. Reporting only the create's RTT
                    # would understate the blind window by most of its length.
                    self.qtrack.track(oid, t, side_action, px, args.size, ahead, t_placed,
                                      t_sent=t_depth, tape_ok=self.tape_ok)
                print(f"  {t[:32]:32} {side_action:4} {args.size}@{px} → {'rest '+oid[:8] if oid else 'dry/no-oid'}")
            except Exception as e:
                # post_only would-cross is the modal reject — it means the spread was takeable, i.e.
                # NO room to make. Counting rejects vs placed is real signal, not noise.
                stats["rejected"] += 1
                # Drop the durable intent. ⚠️ We do NOT know whether a raised create booked (the
                # design deliberately REJECTED modelling the venue's error taxonomy to answer
                # that), so this is not a claim that it didn't. It is bounded-file
                # housekeeping with a named backstop: post_only rejects are the MODAL outcome, and
                # a 24/7 maker keeping every one would grow this file without limit and re-fsync it
                # on every order — an IO/memory hazard in the process whose memory we are guarding.
                # What actually protects a lost create is the VENUE LISTING, in three places: the
                # next cycle's `cancel_all` reconcile, the teardown stray sweep, and — for the
                # SIGKILL case — `scripts/maker_recover.py`, which cancels every resting order the
                # venue reports whether or not we recorded it. The crash flag (`clean_exit=False`)
                # is what forces that last read to happen, and it is untouched here.
                if self.state is not None:
                    with contextlib.suppress(Exception):
                        self.state.clear_order(intent_id)
                print(f"  {t[:32]:32} {side_action:4} @{px} REJECTED: {e!r}")

    def resolve_markouts(self):
        """Value any markout horizon that has come due. Costs ZERO rate-limit units — `_mid` is a
        read of the local WS book cache — so it can run far more often than the quote loop.

        ⚠️ THIS MUST NOT BE TIED TO THE REQUOTE INTERVAL. Horizons used to resolve only inside the
        cycle loop, so at --requote-s 30 a 5s horizon could not be valued until age>=30 and it fired
        on the same pass as the 30s one, writing two rows with IDENTICAL mk under two different
        labels. That is the original defect at 1/5 scale: `h=5` looked like a measurement and was a
        copy of `h=30`. A dedicated ticker is what makes a short horizon actually short."""
        MK_HORIZONS = self.MK_HORIZONS
        _mid = self.mid
        markout_pending = self.markout_pending
        w = self.w

        now = time.time()
        keep = []
        for ob in markout_pending:
            age, m = now - ob["t"], _mid(ob["tk"])
            for h in MK_HORIZONS:
                # mark a horizon done ONLY when we could actually value it — if the mid is
                # unavailable this tick, leave it pending and retry (don't lose the observation).
                if age >= h and h not in ob["done"] and m is not None:
                    ob["done"].add(h)
                    mk = (m - ob["px"]) if ob["action"] == "buy" else (ob["px"] - m)
                    # `h` is the horizon this row is FOR; `age` is when we actually valued it.
                    # Logging both is what makes a late valuation detectable rather than invisible —
                    # an analysis should discard rows where they diverge materially.
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
            except Exception as e:                    # noqa: BLE001 — logging is never fatal
                print(f"  (markout resolve error, ignored: {e!r})")
            # Piggy-backed on the 1 Hz ticker rather than the requote loop, and for the same reason
            # the markouts are: a tape outage has to be noticed while the affected orders are still
            # LIVE. At --requote-s 30 a per-cycle poll could miss a whole reconnect between two
            # checks, and the orders it corrupted would summarize as clean.
            try:
                if self.tape is not None:
                    # `n_bad_trades`, NOT `n_dropped`: the wide counter includes every unrecognised
                    # control frame, and the ack types are a guess, so feeding it here would mark
                    # every order on every poll if the venue emits anything periodic.
                    # ⚠️ BOTH counters here are LOST-PRINT counters, which is what
                    # `note_tape_state` requires. `n_callback_errors` is a raising `on_trade` — a
                    # print the tape delivered and the tracker dropped — and it is deliberately NOT
                    # `errors`, which also accrues reconnect exceptions and venue `error` frames; an
                    # error storm fed in here would mark every live order on every poll and return a
                    # 100%-unreadable run, the very failure that method's docstring forbids.
                    self.qtrack.note_tape_state(
                        self.tape.n_reconnects,
                        self.tape.n_bad_trades + self.tape.n_callback_errors)
                self.qtrack.note_observer_errors(sum(self.book.level_observer_errors.values()))
            except Exception as e:                    # noqa: BLE001 — logging is never fatal
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
            # cancelled this cycle is written `cancelled_unfilled` — a failed read is indistinguish-
            # able from a market that did not trade. The likeliest cause is a 429, and the likeliest
            # moment is the fail-closed halt, i.e. the most interesting cycle of the run. A stdout
            # print does not survive into the CSV an analysis actually reads.
            print(f"  (fills read failed — outcomes this cycle are UNRELIABLE: {e!r})")
            w.writerow([f"{time.time():.0f}", "fills_read_failed", str(e)[:200], "", "", ""])
            recent = []
        # Our OWN filled quantity per order across this batch, summed BEFORE the loop starts marking
        # ids seen. The queue attribution subtracts it from the tape's print total to recover what
        # traded AHEAD of us; the venue partial-fills below one contract, so one order can appear as
        # several records in a single poll and every one of them printed.
        # ⚠️ DECIMAL, not float. The venue partial-fills below one contract, and
        # `float("0.33") + float("0.80")` is `1.1300000000000001` against the tape's exact `1.13` —
        # which trips the `traded < filled` guard and reports UNKNOWN_TAPE_INCOMPLETE, a label whose
        # docstring calls it ARITHMETIC PROOF that a print was dropped, on a perfectly healthy row.
        # (Enumerated over realistic two-partial combinations, a large minority disagree.) Parsed
        # from the venue's string form, per the standing rule.
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
            # ⛔ FAIL-CLOSED EXCLUSION OF OUR OWN FORCED EXIT, keyed on the order we PLACED rather
            # than on a fill read that can 429. A flatten fill booked here becomes `real_fill` +
            # a `spread_capture` that is POSITIVE BY CONSTRUCTION (the flatten sells at the ask,
            # buys at the bid) + three markout horizons resolved off one stale mid. That is our own
            # exit recorded as favourable market flow, on precisely the halts where the exit was
            # forced. `seen_fills` alone could not carry this: it is populated only if
            # `flatten_on_exit`'s `get_fills` succeeds.
            if f.get("order_id") in self.flatten_oids:
                seen_fills.add(fid)
                self.flatten_fill_ids.add(fid)
                # ⛔ WRITE IT, DO NOT JUST SKIP IT. Excluding the fill from the maker-edge columns is
                # only half the job: a bare `continue` made the fill vanish from the ledger entirely
                # on the path this exclusion exists for. When `flatten_on_exit`'s own `get_fills`
                # 429s it writes `flatten_summary "filled 0 across 0 ticker(s)"` — so the run then
                # asserted NOTHING FILLED while a real position move had happened, and
                # `reconcile_maker_pnl` saw a `positions_raw` with no fill behind it and blamed the
                # VENUE with `❌ NO CONVENTION MATCHES`. That relocated the false-verdict generator
                # onto the branch the change was built for, instead of removing it.
                # Everything needed is already in `f`. Marked UNCONFIRMED because the flatten's own
                # accounting pass never saw it, so `flatten_summary`'s totals exclude it.
                # ⚠️ `_fill_ts` returns (epoch, source) — the VENUE's stamp where available. Take [0];
                # using the tuple would write a Python repr into the ts column that
                # `reconcile_maker_pnl`'s `row[0].isdigit()` filter then silently DROPS, which would
                # reproduce the vanishing row this block exists to stop.
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
                # no_price_dollars, and a sell-YES books as side="no" (short YES == long NO), so
                # keying the price off `side` yielded a NO-space price (0.51) that would then be
                # compared against a YES-space mid (0.48) — a garbage markout.
                raw_px = f.get("yes_price_dollars")
                px = float(raw_px) if raw_px not in (None, "") else None
                n = float(f.get("count_fp") or 0.0)
                fee = float(f.get("fee_cost") or 0.0)
            except (TypeError, ValueError):
                continue
            # A missing/out-of-range price would `or 0.0` into a phantom fill at px≈0.0 → spread_capture
            # ≈ ±0.5 (a ~100× outlier one record dominates) → the maker-viability metric silently poisoned.
            # Reject it, the way _quote_price(as_fill=True) refuses the fabricated 0.0/1.0.
            if px is None or not (0 < px < 1):
                print(f"  ⚠️ skipping fill with unusable yes_price_dollars="
                      f"{f.get('yes_price_dollars')!r} (would poison spread_capture)")
                continue
            taker = bool(f.get("is_taker"))
            print(f"  ✎ real-fill {str(tk)[:26]:26} {side}/{action} {n:g}@{px:.3f} fee={fee:.4f}"
                  f"{'  ⚠️TAKER(post_only leaked!)' if taker else ''}")
            w.writerow([f"{f.get('ts') or int(time.time())}", "real_fill",
                        f"{tk} {side} {action} n={n:g} px={px:.4f} fee={fee:.4f} taker={taker}",
                        "", "", ""])
            # Direction comes from `action`, NOT `side`: we only ever send yes-space orders
            # (create_order(t, "yes", ...)), so action buy/sell IS our yes-space direction. Gating on
            # side == "yes" recorded NOTHING, because every sell-YES fill reports side="no".
            if action in ("buy", "sell"):
                # Anchor on the VENUE'S fill time, falling back to now only if it is unusable. A
                # fallback must be VISIBLE, not silent: `ts_src` records which anchor was used, so a
                # later analysis can drop detection-anchored rows instead of averaging them in with
                # true ones and quietly reproducing the defect this replaced.
                fill_ts, ts_src = _fill_ts(f)
                mid_at_fill = _mid(tk)
                # Join the fill back to the order that produced it. `order_id` is on the fill record,
                # so this is an exact join rather than a price/time guess.
                oid = f.get("order_id")
                meta = order_meta.get(oid or "")
                if meta:
                    # BEFORE the add, so the queue attribution fires once per ORDER rather than once
                    # per partial fill. `fill_attrib` deliberately still writes per fill.
                    # ⚠️ WRAPPED, unlike `fill_attrib` below. `seen_fills.add(fid)` has already
                    # stamped this fill, so a raise here would propagate out of the whole method and
                    # lose this fill's markout and spread_capture rows — AND every later fill in the
                    # batch — permanently, since their ids are now marked seen. The cap reads
                    # `_positions`, never this path, so nothing here can touch money; it can only
                    # destroy the run's primary data, which is reason enough.
                    if oid not in filled_oids:
                        try:
                            # `batch_filled` not `n`: the tape carries every partial's print, so
                            # subtracting only the FIRST partial leaves the rest counted as queue
                            # that traded ahead of us — inflating traded_ahead, in the same
                            # direction as the other biases.
                            self.log_queue_dynamics(
                                oid, outcome="fill", filled_qty=batch_filled.get(oid, n),
                                fill_ts=fill_ts if ts_src == "venue" else None)
                        except Exception as e:      # noqa: BLE001 — logging is never fatal
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
                    # response was lost so the id was never recorded. Say so rather than dropping it;
                    # an unattributed fill silently shrinks the denominator of every rate below.
                    w.writerow([f"{fill_ts:.0f}", "fill_attrib",
                                f"{tk} {action} px={px:.4f} UNATTRIBUTED order_id={oid[:8]}",
                                "", "", ""])
                markout_pending.append({"tk": tk, "action": action, "px": px, "t": fill_ts,
                                        "ts_src": ts_src, "done": set()})
                # REALIZED SPREAD CAPTURE — the maker edge itself, measured against the VENUE fill
                # price rather than a book-derived approximation. (Marked P&L used to book every
                # fill at the MID, which netted spread capture to exactly zero; it now books
                # exit-side, which sees capture but off the TOUCH, not off our sent limit. This
                # column is the one that is right either way.) Signed so POSITIVE = we captured
                # spread: bought below the mid, or sold above it. A round trip that opens and closes
                # at the same price captured nothing, and this is the column that says so.
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
    # block-buffered (~8 KB) — and every long run is redirected (`> logs/run.log`), which is exactly
    # when you need to watch it. On a real run the log stopped after "preflight: starting balance"
    # for MINUTES while the tool was quoting normally, and confirming that required querying the
    # venue. Progress you cannot see is indistinguishable from a hang — and this tool's own history
    # includes real runs that placed nothing and looked identical to a quiet market.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:                       # never fatal — see below for what can actually raise
        # ⚠️ NOT "captured in tests", as an earlier version of this comment said: under pytest
        # `sys.stdout` is a real TextIOWrapper and reconfigure SUCCEEDS. The genuine raisers are a
        # CLOSED stream (ValueError) and a replacement object without the method (AttributeError) —
        # both Exception subclasses, so both caught, while KeyboardInterrupt/SystemExit correctly are
        # not. `logging` is unaffected: reconfigure mutates the wrapper in place rather than
        # rebinding `sys.stdout`, so handlers already holding it keep a valid stream.
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=float, default=1200.0)
    ap.add_argument("--markets", type=int, default=3)
    ap.add_argument("--size", type=int, default=1)
    ap.add_argument("--inv-cap", type=float, default=3.0)
    # ⚠️ A RUNAWAY BACKSTOP, NOT A LOSS BOUND. It compares an OPTIMISTIC exit-side touch mark, so it
    # is wrong in BOTH directions: it can trip on nothing (unmarkable inventory is worst-case marked
    # at cost basis, and a transient one-sided ladder is ordinary in a thin book), and it cannot see
    # the loss it is named for — a real run fed the cap a POSITIVE number while realizing a loss, an
    # inventory ratchet the mark cannot represent. The venue's `market_exposure_dollars` is the actual cost basis
    # and the actual max loss, and every cycle already writes it on `positions_raw_cycle`; read that
    # for a true bound. Do not size this as if it caps a loss.
    ap.add_argument("--loss-cap", type=float, default=5.0,
                    help="RUNAWAY BACKSTOP: halt + cancel-all if the OPTIMISTIC marked P&L < -this "
                         "($). Not a loss bound — see market_exposure_dollars for the real one")
    # DEFAULT 0 (no OBI skew) — deliberately. The old nonzero default was tuned while the get_depth
    # mapping was INVERTED, and post-fix measurement shows NO usable OBI signal on Kalshi
    # (corr≈0, flat tail across bins; the signal is real on POLY, not here). Skewing real orders on an
    # unvalidated signal is unjustified risk — opt in explicitly once Kalshi data supports a sign.
    ap.add_argument("--obi-coef", type=float, default=0.0)
    ap.add_argument("--inv-coef", type=float, default=0.003)
    ap.add_argument("--improve-ticks", type=int, default=0,
                    help="step this many ticks INSIDE the touch on BOTH sides (needs enough spread, "
                         "else quotes at the touch). Buys queue priority, which is the binding "
                         "constraint for a size-1 maker; costs that much spread. 0 = quote at touch.")
    ap.add_argument("--requote-s", type=float, default=6.0)
    ap.add_argument("--flatten-on-exit", action="store_true",
                    help="on EVERY exit, INCLUDING a halt, post PASSIVE (post-only) reducing "
                         "orders to wind down OUR inventory before the sweep; any residual is left "
                         "for the sweep and still triggers the OPEN-POSITION warning. Never takes — "
                         "the post_only rail is preserved. Applies on a halt too: that is when "
                         "inventory most needs clearing, since the cap dies with the process.")
    ap.add_argument("--flatten-wait-s", type=float, default=60.0,
                    help="how long to let the --flatten-on-exit passive orders rest before recording "
                         "what filled and leaving the residual for the sweep")
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
    # ⚠️ `real` DOES NOT GATE ORDER PLACEMENT — it never has. `MakerSession.quote` calls
    # `create_order` unconditionally and the ONLY DRY short-circuit is `if config.DRY_RUN` inside
    # `KalshiClient.create_order`. So `config.DRY_RUN` alone decides whether money moves; `real`
    # decides only whether the run is INSTRUMENTED (fills, markout, preflight, stray sweep, run_id).
    #
    # That split makes one combination silently catastrophic: a `.env` carrying `DRY_RUN=false` with
    # no flag on the command line places REAL post-only quotes while printing "DRY PREVIEW — NO real
    # orders placed", stamping every CSV row `dry-`, logging zero fills, and skipping the teardown
    # stray sweep. Live money, mislabelled, uninstrumented, unswept. Latent only because the
    # deployed `.env` says True today — and the arb bot's go-live flip sets that same variable,
    # which the refusal below converts into a HARD STOP rather than an arming. Do NOT read this as
    # "the flip arms the maker" — it does the opposite.
    #
    # Refuse to start rather than paper over it. A startup refusal is checkable and total; a
    # per-order check would have to be threaded through the quote path and could be missed.
    flagged = args.i_understand_real_money       # plain attribute: a renamed dest must raise here,
    #                                              not silently degrade to permanent DRY
    refusal = _real_money_refusal(config.DRY_RUN, flagged)
    if refusal:
        raise SystemExit(f"REFUSING: {refusal}")
    real = flagged and not config.DRY_RUN
    # One CSV accumulates every run, and it already held 11 DRY previews interleaved with 3 real
    # runs with nothing to tell them apart — so any rate computed over the file silently mixed
    # modes. Stamp each row's run and mode instead of hoping timestamps disambiguate it.
    run_id = f"{'real' if real else 'dry'}-{int(time.time())}"
    # ⚠️ RATE-LIMIT GATE — this is a MONEY guard, not a tidiness one.
    # Kalshi bills /portfolio/* list reads at 10 units each. Polling positions AND fills every cycle
    # at a 6s requote is ~90 units/cycle, which 429'd a real-money run within minutes and tripped its
    # fail-closed halt. Thinning the fills poll was the fix for that incident; this
    # tool now polls fills every cycle so markout horizons mean what they say, which re-creates the
    # exposure unless the requote interval is long enough to pay for it.
    # The failure is not a lost log line: a 429 storm makes `cancel_order` fail, the order is dropped
    # from tracking while STILL RESTING on the venue, the stray sweep's listing also 429s, and the
    # process exits leaving live quotes with the loss cap dead. Refuse rather than warn.
    if real:
        # (1) The ordering fix REQUIRES per-cycle polling. `_cancel_all` runs every cycle, but
        # `filled_oids` is only populated by the fill poll — so with _FILLS_EVERY=N>1, N−1 out of
        # every N cancels have no preceding poll and their fills get written as `cancelled_unfilled`
        # again. Raising it is NOT a valid remedy for the rate limit, and an earlier version of this
        # message suggested exactly that. Assert it instead of trusting a comment.
        if _FILLS_EVERY != 1:
            raise SystemExit(
                f"REFUSING: _FILLS_EVERY={_FILLS_EVERY} breaks the fill/cancel ordering invariant — "
                f"cancels run every cycle, so a fill between polls is logged as cancelled_unfilled. "
                f"Lengthen --requote-s to pay for the rate limit; never thin the poll.")
        # (2) Rate-limit budget, and it is dominated by --markets, not by the requote interval.
        # Measured cost model (from the venue's endpoint-cost table): list reads 10 units, cancel 2, create 10.
        #     units/cycle = 20 (positions + fills) + markets · (2·10 create + 2·2 cancel)
        # The run that 429'd within minutes and tripped its fail-closed halt was markets=3 @ 6s =
        # ~15.3 u/s. Gating on requote-s ALONE let `--markets 18 --requote-s 30` reproduce that exact
        # rate while passing. The failure is not a lost log line: cancels 429 → orders dropped from
        # tracking while still resting → the stray sweep's listing also 429s → the process exits with
        # live quotes and the loss cap dead.
        # (3) A skew knob that never moves a quote must not look like it is working. The lean is
        # rounded to whole ticks (_skew_ticks), so refuse only a coef whose max effect at the cap
        # rounds to ZERO ticks — anything reaching a tick is honest management, the default 0.003
        # included (the old guard refused it on a false "sub-tick is inert" premise; see _skew_guard).
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

    # --ticker: quote EXACTLY the named markets (the supervisor's precise steering, since _two_sided ranks
    # by VOLUME not spread width). The series is derived from the tickers so --ticker works standalone. The
    # named set is still run through the FULL validation pipeline below (eligibility + maker-free + contested
    # + book), so a closing / charged / one-sided / out-of-band ticker is DROPPED, never force-quoted.
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

    # Annotate each target with its phase + flow, so the operator can see WHY it was picked rather than
    # decode the ticker date. LIVE single-name sports is the MEASURED-adverse lane — every markout
    # horizon came back against us there — while pregame is the clean one. So a LIVE pick is called
    # out, not merely labelled.
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
        # the same slot silently changed basis by 3h. For a game, start ≈ expiry − 3h; for a macro or
        # weather market there is no start at all, which is the whole point of the phase rework.
        return f"{ph} (expires in ~{(e - time.time()) / 3600.0:.1f}h)"

    print("  targets:")
    for t in targets:
        maker = "maker M=0 FREE ✅" if _maker_mult(t) == 0 else "maker M=1 ~0.4¢/fill ⚠️"
        v = band["vol_of"].get(t, 0.0)
        print(f"    {t}  [{_liveness(t)}]  {maker}  vol24={v:,.0f}")
    n_m1 = sum(1 for t in targets if _maker_mult(t))
    if n_m1:
        print(f"  ⚠️ {n_m1}/{len(targets)} target(s) charge the maker fee (M=1, ~0.4¢/fill) — that roughly "
              f"CANCELS the favorable markout on those. ~97% of Kalshi series are maker-FREE — including "
              f"the sports DERIVATIVES (KXMLBTOTAL/SPREAD/RFI) of charged moneylines; pass "
              f"--maker-free-only to force them. NOTE: NBA/NHL are CHARGED, not free.")
    if all(_phase(m_of[t]) in ("pregame", "long-dated") for t in targets if t in m_of):
        print("  ⚠️ NONE are in-progress. That is the CLEAN lane for markout, but flow is thinner — if "
              "this run is measuring THROUGHPUT, prefer a high-volume series (see vol24 above).")
    if not args.confirm:
        print("  preview only — add --confirm (and --i-understand-real-money for real orders)."); await client.close(); return

    # ⚠️ The prefix list is a HARD FILTER on inbound WS messages (`feed._matches_prefix`), and it
    # defaults to config.KALSHI_SERIES — the 12 cross-arb SPORTS series. Any target outside that set
    # had every book message dropped, so the book never populated, `_quote` returned early on
    # `not bid or not ask`, and the run placed ZERO orders for its full duration while looking healthy:
    # WS "connected"/"subscribed" logged normally and the only symptom was silence. Real-money runs
    # have ended `quotes=0 fills=0` exactly this way.
    # This is why --series must drive the feed too: the MM universe is ~97% of Kalshi, NOT the arb set.
    book = KalshiOrderBookCache(client, series_prefixes=feed_prefixes(targets))
    book.set_book_tickers(targets)
    book_task = asyncio.create_task(book.run_forever())
    # ── Order provenance, keyed by the venue's order_id (which fills carry, so the join is exact).
    # SIDE TABLE ON PURPOSE: `resting` and the cancel path are untouched, so nothing about what gets
    # placed or cancelled changes — this is pure observation bolted alongside.
    # It answers the three questions a fills-only ledger cannot:
    #   · how much depth was AHEAD of us when we quoted   (did improvement actually buy priority?)
    #   · how long the order sat before filling            (time-to-fill)
    #   · what happened to the ones that did NOT fill      (cancelled unfilled, and how old)
    # Without these a null result is unattributable between "the market is thin" and "our quote sat
    # behind a queue that never cleared" — which is exactly where a real run left us.
    halted = False
    # WHY the run stopped, for the heartbeat's exit status. The CSV already records each reason on
    # its own `halt` row; this carries it to the DEADMAN, where the distinction that matters is
    # "exited on purpose" vs "stopped beating" — a halt must not read as a clean exit, and a clean
    # exit must not page anyone. Set beside every `halted = True`.
    halt_reason = ""


    fh = open("logs/kalshi_live_mm.csv", "a", newline="")
    _raw_w = csv.writer(fh)
    if fh.tell() == 0:
        _raw_w.writerow(["ts", "event", "detail", "cash", "inv", "pnl", "run_id"])

    class _RunWriter:
        """csv.writer that stamps every row with this run's id and mode.

        Wrapped rather than threaded through ~20 call sites: a stamp that depends on remembering to
        pass it is a stamp that will be missing from the one row that matters. Rows predating this
        have 6 columns and no id — an analysis must treat a missing run_id as 'unknown run', not as
        part of the current one."""

        @staticmethod
        def writerow(row):
            _raw_w.writerow(list(row) + [run_id])
            # ⚠️ FLUSH EVERY ROW. Without this the CSV is block-buffered (~8 KB), so a 90-minute
            # real-money run is INVISIBLE while it runs — on a real run the file's mtime sat
            # unchanged for minutes and the only way to tell the tool was working was to query the
            # venue for resting orders. Two costs, not one: you cannot
            # monitor a live run from its own ledger, and a hard kill (SIGKILL/OOM — a graceful exit
            # still flushes via `fh.close()`) silently discards the tail. Both probes flush per row
            # already; the maker was the outlier. One line here covers ~20 call sites, which is why
            # the writer is wrapped in the first place.
            fh.flush()

    w = _RunWriter()







    # ── preflight (real runs): snapshot balance (CASH, ⚠️ not P&L) and clean the target markets
    #    of any leftover resting orders so a prior-run stray can't contaminate this run's fills.
    #    ⚠️ The balance is CASH, not P&L (see the header) — snapshotted for the delta, not as truth. ──
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

    # ⚠️ THE BASELINE IS READ **AFTER** THE PREFLIGHT STRAY CANCEL, and the order is deliberate.
    # Two reasons, one safety and one correctness:
    #   · Safety: this refusal is a bare `return` ahead of the `try`, so it does NOT run the
    #     teardown's venue sweep. Read before the preflight, a 429 here meant a PRIOR run's resting
    #     orders survived unlisted and uncancelled — precisely what the book refusal below is
    #     deliberately placed INSIDE the `try` to prevent. Refusing after the preflight means the
    #     strays have already been cancelled by the time we can refuse.
    #   · Correctness: the preflight cancels strays to "clean the baseline — a leftover stray would
    #     pollute fills/markout", which only holds if the cleaning happens BEFORE the baseline is
    #     taken. It did not. A stray that filled between the old baseline read and its cancellation
    #     was attributed to THIS run's inventory.
    # Nothing between the preflight and here reads `base`/`last`/`cash`, so the move is local.
    # ⚠️ RESIDUAL, and it is the one direction this move makes WORSE: a stray that FILLS during the
    # preflight is now absorbed into `base` instead of into `inv`. That is right for attribution and
    # markout, but `--inv-cap` and the loss cap are both `cur − base`, so that real position sits
    # OUTSIDE them. It is the already-documented inherited-inventory class (see the module header),
    # slightly widened — bounded by `--size` per stray, and only reachable when a stray's cancel
    # FAILED, since a successful cancel cannot fill afterwards.
    #
    # ⚠️ THE BALANCE SNAPSHOT MUST STAY ADJACENT TO THE BASELINE, on the SAME side of the preflight
    # cancel. Cancelling a resting order releases its held collateral back to available cash, so a
    # balance read taken BEFORE the cancel and a position baseline taken AFTER it do not describe
    # one instant: a single prior-run resting buy would make the teardown's reported `cash Δ` better
    # than the run actually did by that order's full collateral — the same order of magnitude as the
    # entire result it would be compared against.
    if real:
        try:
            start_bal = await client.get_balance()
            w.writerow([f"{time.time():.0f}", "balance_start", "", f"{start_bal:.4f}", "", ""])
            print(f"  preflight: starting balance ${start_bal:.2f}")
        except Exception as e:
            print(f"  ⚠️ preflight: balance read failed (CASH cross-check degraded — see the header: this is cash, not P&L): {e!r}")

    # ── SAY WHICH FILE THE KILL SWITCH IS WATCHING, resolved against THIS process's cwd ──
    # Wiring `is_paused()` into the loop is only half a fix if the operator cannot see which path
    # this run is watching. `KILL_SWITCH_FILE` is a RELATIVE default and this program is launched by
    # hand (no systemd `WorkingDirectory=`), so a maker started from ~ watches ~/pause.json while
    # `scripts/show_config`, run from the repo, prints the repo one — and an operator who touches
    # that gets no error and no effect, which is EXACTLY the pre-fix behaviour this change removed.
    # The empty-value case is louder still: `is_paused()` is then a constant False with no warning
    # anywhere, so the disabled state is otherwise indistinguishable from the armed one.
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
    # (market_exposure_dollars / realized_pnl_dollars / fees_paid_dollars) are LIFETIME per-market,
    # so the venue-derived `pnl = (R − R_base) − (F − F_base) + Σ[mark − (E − E_base)]` is unanchored without
    # it — and the first in-loop `positions_raw_cycle` row is written AFTER the first quote round, so
    # it is not a baseline. Captured here it costs nothing: same read, same units.
    base_raw: list = []
    base = await _positions(client, targets=targets, raw_out=base_raw)  # baseline: only OUR fills count
    if base is None:
        print("REFUSING: cannot read baseline positions (get_positions failed) — won't start blind.")
        await client.close(); return
    last = dict(base)
    cash = 0.0
    if real:
        # The pre-trade E/R/F baseline, written BEFORE any order is placed. Wrapped: logging must
        # never touch the money path. Not filtered to targets — the selected market is decided
        # below, so capture everything and let the reconciliation pick.
        try:
            w.writerow([f"{time.time():.0f}", "positions_raw_base", json.dumps(base_raw), "", "", ""])
        except Exception as e:
            print(f"  (baseline positions_raw log failed — non-fatal: {e!r})")

    # ── economics-measurement state. ADDITIVE and fully separate from the positions-based loss cap:
    #    a read failure here degrades LOGGING only, never the kill-switch. ──
    run_start = int(time.time())
    _cycle = 0

    # ── The order lifecycle lives in MakerSession, which is testable; main() keeps orchestration.
    #    The local names below are bound to its methods and shared state.
    #
    #    ⚠️ THE EXTRACTION WAS NOT BEHAVIOUR-PRESERVING, and a comment here claiming it was
    #    ("provably mechanical", "the REST of this function is untouched") hid a real regression for
    #    two review rounds. Method bodies ARE AST-identical to their closures — that part is true and
    #    was checked — but identical bodies are necessary, not sufficient: re-binding a captured
    #    variable from `self` preserves MUTATION and not REBINDING. `inv` is rebound (`_mark_step`
    #    returns a fresh dict each cycle), so `sess.inv` froze at zeros and both `--inv-cap` and
    #    `--inv-coef` went inert. The write-back below exists because of that; this function is NOT
    #    untouched. Before adding any binding here, check whether the session reads it and whether
    #    it is ever rebound.
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
    # Shared state. `resting`, `order_meta`, `filled_oids`, `stats`, `seen_fills` are the SAME
    # objects for the whole run (mutate-only), so main()'s reads track the session.
    # ⚠️ `inv` DOES NOT. `_mark_step` rebinds main's local to a fresh dict every cycle, after which
    # this alias and `sess.inv` are different objects with equal content — kept equal only by the
    # `sess.sync_inventory(inv)` call below. Listing `inv` under "same objects" is precisely the
    # belief that produced the inventory-cap regression.
    resting, order_meta, filled_oids = sess.resting, sess.order_meta, sess.filled_oids
    inv, stats, seen_fills = sess.inv, sess.stats, sess.seen_fills
    markout_pending = sess.markout_pending
    MK_HORIZONS = MakerSession.MK_HORIZONS
    # Markout horizons, measured from the VENUE'S FILL TIMESTAMP.
    # ⚠️ These were once measured from fill DETECTION, and a real run shows what that costs: EVERY
    # markout row came back at the same age. Polling fills every Nth cycle means a fill is discovered
    # up to N requotes late, and BOTH horizons then fire on the same pass — one number recorded
    # twice, labelled 5s and 30s. The single measurement that decides maker viability was the one
    # being mis-taken, and it disagreed in SIGN with the offline prediction — a gap the horizon
    # error alone could explain.
    # Anchoring to `f["ts"]` (which the fill record carries and the code used to discard) makes the
    # horizons mean what they say; polling every cycle keeps discovery lag under one requote.




    # Runs only on real runs — DRY places nothing, so there is nothing to mark out.
    mk_task = asyncio.create_task(_markout_ticker()) if real else None

    # ── QUEUE ATTRIBUTION ────────────────────────────────────────────────────────────────────────
    # Half the answer is the L2 delta stream (what LEFT our price level), half is the trade tape
    # (what TRADED there). Both are consumed on THIS process's clock, which is the whole reason this
    # lives in the maker: the offline scorer had to join our fills against a separately-collected
    # tape across the venue's whole-second fill timestamps, and that join is what made it
    # unsalvageable. Nothing here can change what gets placed, sized or cancelled — the tracker is
    # write-only from the quote path's point of view, and `feed._notify_level` swallows any fault.
    #
    # Real runs only, matching mk_task: DRY places no orders, so there is no queue to attribute, and
    # opening a second socket to observe nothing is pure cost.
    if real:
        book.set_level_observer(sess.qtrack.on_level)
        sess.tape = KalshiTradeFeed(client, targets, sess.qtrack.on_trade)
        tape_task = asyncio.create_task(sess.tape.run_forever())
    else:
        tape_task = None

    # ── OPERATIONAL RAILS (bot/core/{heartbeat,maker_state,memguard}.py) ────────────────────────
    # All three exist for ONE death: SIGKILL. It is not the exotic case — memory pressure kills a
    # long-running process this way, and the header of this file names SIGKILL as the one death
    # that always strands live orders. SIGKILL
    # runs no `finally`, so nothing written during teardown protects against it — the record has to
    # be on disk BEFORE it is needed, and the observer has to be a different process.
    #
    # ⚠️ THE DURABLE STATE IS REAL-RUNS ONLY. A DRY preview places nothing, so letting it call
    # `begin_run` would reset a crashed REAL run's order list — a preview destroying the evidence
    # of a crash. The HEARTBEAT beats either way: a DRY run is still a long-running process an
    # operator wants to see.
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
            # Venue-split path: this store opens the KALSHI file and adopts a
            # kalshi-prefixed legacy record once; a Poly record can no longer be reset here.
            state = maker_state.store_for_venue("kalshi", base_path=config.MAKER_STATE_FILE)
            # inventory={} ALWAYS on the Kalshi arm: this maker's inventory
            # is a DELTA from the venue baseline read (`_mark_step`: inv = venue − base),
            # so pre-existing positions are baseline to ignore — seeding them here would
            # double-count against the delta model. There is no carry flow on this venue.
            state.begin_run(run_id, mode="real", loss_cap=Decimal(str(args.loss_cap)),
                            tickers=targets, inventory={})
            sess.state = state
        except Exception as e:
            # Covers BOTH `PriorRunUnresolved` (a previous run died holding orders — starting would
            # erase the only record of what is resting) and `StateCorrupt` (a truncated file, which
            # is the SIGKILL-mid-write artefact). Refusing is the point: a maker that trades on top
            # of an unresolved crash turns a recoverable strand into an invisible one.
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
        # placing nothing, with no error anywhere — indistinguishable from a quiet market. Two
        # real-money runs have ended `quotes=0 fills=0` exactly this way.
        # Placed INSIDE the try, after the stray preflight, so a refusal still runs the full `finally`
        # (venue stray sweep, cancel-all, balance/position report) — a refusal must never be the reason
        # a prior run's resting order survives unswept and unmentioned.
        if not await wait_for_book(book, targets, tries=_BOOK_WARMUP_TRIES, delay=_BOOK_WARMUP_S):
            print(f"REFUSING: no two-sided WS book for ANY target after "
                  f"{_BOOK_WARMUP_TRIES * _BOOK_WARMUP_S:.0f}s — the quote path would place nothing "
                  f"all run. Check that the feed's series prefixes cover these tickers.")
            w.writerow([f"{time.time():.0f}", "refused", "no_two_sided_book", "", "", ""])
            halted, halt_reason = True, "no_two_sided_book"
        # ── Is the tape actually live? (the health flag that went unread for a long time) ──
        # Deliberately a WARNING, not a refusal: the tape is pure instrumentation, and refusing to
        # trade because a logging feed is down would let an observability fault dictate a money
        # decision. The run stays honest either way, because every `queue_dynamics` row records
        # `tape=DOWN` and its verdict degrades to UNKNOWN_TAPE_DOWN rather than reading the
        # resulting `traded=0` as "the queue in front of us pulled".
        # By here the book warmup has already burned several seconds, so a healthy subscribe has
        # long since acked; `subscribed` still False means it was rejected or never sent.
        if sess.tape is not None and not halted:
            tape_note = ("ok" if sess.tape_ok else
                         f"NOT_ACKED dropped={sess.tape.n_dropped} "
                         f"last_error={sess.tape.last_error or 'none'}")
            if not sess.tape_ok:
                print(f"  ⚠️ TRADE TAPE NOT CONFIRMED ({tape_note}) — trading proceeds, but the "
                      f"traded-vs-cancelled split will read UNKNOWN_TAPE_DOWN for this run.")
            # ⚠️ The queue attribution needs ORDERBOOK mode and fails SILENTLY without it: outside
            # it `get_depth` returns None, `quote()` coerces that with `or 0.0`, and "depth
            # unobserved" becomes "empty queue" — every fill would score ALONE_AT_PRICE with
            # matched_levels=0, which looks like data. The deployed value is `orderbook`, but
            # nothing asserted it, so say so out loud rather than relying on that staying true.
            if config.KALSHI_PRICE_SOURCE != "orderbook":
                tape_note += f" ⚠️NO_L2(KALSHI_PRICE_SOURCE={config.KALSHI_PRICE_SOURCE})"
                print(f"  ⚠️ KALSHI_PRICE_SOURCE={config.KALSHI_PRICE_SOURCE!r}, not 'orderbook' — "
                      f"there is NO level feed, so the queue attribution will collect nothing "
                      f"usable this run.")
            w.writerow([f"{time.time():.0f}", "tape_status", tape_note, "", "", ""])
        while time.time() - t0 < args.seconds and not halted:
            # ── THE OPERATOR'S KILL SWITCH, checked FIRST so no cycle can quote past it ──
            # `touch pause.json` (= config.KILL_SWITCH_FILE) is the documented way to stop trading
            # without a restart. For a long time `is_paused()` had exactly ONE caller — the arb bot,
            # which is deliberately stopped and has never traded — while THIS process, the only one
            # in the repo that has ever placed real orders, contained no reference to it at all. An
            # operator reaching for the documented control got no error and no effect. A kill switch
            # wired to the wrong process is worse than none: it reads as armed.
            #
            # `break`, not a bare `return`: it hands off to the SAME `finally` the loss-cap halt
            # takes, so the pause reuses the proven teardown instead of a second, unexercised one.
            # That teardown is **cancel-all → optional passive flatten → venue stray sweep**, in
            # that order — the sweep is LAST on purpose, so the flatten's own unfilled post-only
            # order gets swept rather than left resting with the loss cap dead. (This comment said
            # "cancel-all → sweep → flatten" for one review cycle. Getting it backwards invites a
            # maintainer to "remove the redundant sweep", which is the strand the sweep prevents.)
            # `_should_flatten` deliberately ignores `halted`, so `--flatten-on-exit` still flattens
            # here — right, because the process is exiting and the loss cap dies with it.
            #
            # ⚠️ SO A KILL-SWITCH HALT CAN PLACE AN ORDER AND THEN WAIT. Under the supervisor's
            # config (`--flatten-on-exit --flatten-wait-s 60`) the teardown posts a passive reducing
            # order and sleeps up to 60s. It is post-only and reduces OUR net, but with an inherited
            # `base` "reducing our net" can INCREASE the account's position, and can be a BUY (see
            # the block above `_should_flatten`'s call). The operator message below says so, because
            # an operator who reads "cancelling" and then sees a NEW resting order on the venue
            # reaches for `kill -9` — the one death that always strands.
            #
            # ⚠️ Latency is NOT one `--requote-s`. Worst case is that interval PLUS a full cycle
            # body (fills poll, N cancels, 2N creates, and `_positions`, which retries a 429 four
            # times at 1.5/3/6s), PLUS `--flatten-wait-s`. At the supervisor's 10s/60s that is ~70s+
            # from arming to "process gone, nothing resting". The arb bot's equivalent is ~5s.
            # ⚠️ KILL_SWITCH_FILE defaults to a RELATIVE path, and the maker is launched by hand
            # rather than under systemd — so the file must be created relative to the cwd the run
            # was started from. `python -m scripts.show_config` prints the resolved path (⚠️ against
            # ITS OWN cwd). `is_paused()` logs at WARNING, which also reaches Discord off-loop.
            if is_paused():
                print("  🛑 KILL SWITCH: pause file present — halting. Cancelling all resting "
                      "orders, then (if --flatten-on-exit) placing ONE passive reducing order and "
                      "waiting --flatten-wait-s for it, then sweeping the venue. A new resting "
                      "order appearing now is that flatten, not a failed halt. "
                      "Remove the file before restarting.")
                w.writerow([f"{time.time():.0f}", "halt", "kill_switch", f"{cash:.4f}", "", ""])
                halted, halt_reason = True, "kill_switch"
                break
            # ── MEMORY HEADROOM, checked beside the kill switch because it is the same question:
            # should this process still be running? The OOM killer sends SIGKILL — no teardown,
            # no cancel-all, live orders left resting on the venue. Halting here
            # takes the SAME proven teardown the kill switch and loss cap take, while there is
            # still memory left to run it.
            # ⚠️ Fail-OPEN, unlike every position read in this file: an unreadable /proc carries no
            # information about memory pressure, so it must not become the outage. `check` already
            # swallows its own reader failures; the wrapper covers the rest.
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
            # ⚠️ FILLS ARE POLLED BEFORE QUOTING, and the order matters.
            # `_quote` starts by cancelling last cycle's orders, and `_log_unfilled` decides
            # "did this order fill?" from `filled_oids`, which only `_record_fills_and_markout`
            # populates. Polling AFTER the cancel (the previous arrangement) meant an order that
            # filled at any point during its resting life was still absent from `filled_oids` when
            # it was cancelled — so it was written as `cancelled_unfilled` AND, moments later, as a
            # fill. At --requote-s 30 that mislabels ~95% of fills as non-fills, inverting the one
            # number this instrumentation exists to produce.
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
            # logged next to that cycle's fills. Diagnostic only: nothing below reads it, and it is
            # populated only after the fail-closed checks pass. This is what makes the venue-derived P&L decidable,
            # since those fields reconcile on one captured market and are unreachable on another.
            _cycle_raw: list = []
            cur = await _positions(client, targets=targets, raw_out=_cycle_raw if real else None)
            if cur is None:                              # CANNOT-VERIFY → fail CLOSED
                print("  🛑 positions unreadable — fail-closed: halt + cancel-all.")
                w.writerow([f"{time.time():.0f}", "halt", "positions_cannot_verify", f"{cash:.4f}", "", ""])
                halted, halt_reason = True, "positions_cannot_verify"
                break
            if real:
                # One row per cycle carrying only the TARGET markets' raw records — the reconciliation
                # input for the venue-derived P&L. Wrapped: a logging failure must never touch the cap.
                # ⚠️ GATED ON THE READ SUCCEEDING (`cur is not None`, guaranteed here), NOT on the list
                # being non-empty. `if _cycle_raw:` conflated "the account holds nothing" with "no row
                # written", and on a never-traded target it emitted NOTHING until the first fill —
                # suppressing the E/R/F BASELINE that is the whole point of the capture.
                try:
                    _tgt = set(targets)
                    w.writerow([f"{time.time():.0f}", "positions_raw_cycle",
                                json.dumps([p for p in _cycle_raw if p.get("ticker") in _tgt]),
                                "", "", ""])
                except Exception as e:
                    print(f"  (per-cycle positions_raw log failed — non-fatal: {e!r})")
            mids = {t: _quote_of(t) for t in targets}   # (bid, ask) — see _quote_price
            cash, inv, pnl, last, fills = _mark_step(targets, base, last, cur, mids, cash)
            # ⚠️ WRITE IT BACK. `_mark_step` RETURNS A NEW DICT rather than mutating, so rebinding
            # the local `inv` leaves `sess.inv` — which `quote()` reads for the inventory cap and
            # the skew — frozen at all-zeros for the life of the run. Pre-refactor this worked by
            # accident: `_quote` was a closure over main()'s `inv`, so the rebinding propagated
            # through the cell. Extraction broke that, silently, and it made BOTH `--inv-cap` and
            # `--inv-coef` inert while the module header still advertised the cap as a hard rail.
            # Mutate in place rather than reassigning, so every holder of the dict stays correct.
            sess.sync_inventory(inv)
            # ── HEARTBEAT + durable inventory, once per cycle ────────────────────────────────────
            # Placed HERE because this is the first point where all three operator-facing numbers
            # exist together, and BEFORE the loss-cap break so a halting cycle still beats — the
            # deadman must see the last live state, not a gap.
            #
            # ⚠️ `marked_pnl`, not `pnl`. This is `_mark_step`'s MARKED figure, and marking a fill
            # at the same touch that priced it makes P&L 0 at the fill by construction — a
            # venue-confirmed LOSS has been fed to the cap as a POSITIVE number, so the sign itself
            # is not safe. The field name says "marked" so nobody downstream reads it as realized.
            # Wrapped: instrumentation must never be able to stop trading.
            try:
                hb.beat(markets_quoted=len(targets),
                        inventory=sum(inv.values()),
                        inventory_by_ticker={t: round(q, 4) for t, q in inv.items()},
                        marked_pnl=pnl, cash=cash, cycle=_cycle,
                        fills=len(sess.seen_fills), run_id=run_id, phase="quoting")
            except Exception as e:
                print(f"  (heartbeat write failed — non-fatal: {e!r})")
            if state is not None:
                try:
                    state.set_inventory({t: Decimal(repr(q)) for t, q in inv.items()})
                except Exception as e:
                    print(f"  (durable inventory write failed — non-fatal: {e!r})")
            for t, d, m in fills:
                # 2dp not %.0f: position_fp comes back FRACTIONAL and %.0f masked it as clean ±1 /
                # "-0". ⚠️ NOT an anomaly — root-caused: the venue partial-fills BELOW one contract
                # (a resting 1-lot sell came back with a fill count of 0.11, costed at exactly that
                # fraction), and every snapshotted position reconciles to the signed
                # sum of count_fp. Never round the inventory display below the precision it arrives at.
                print(f"  FILL {t[:30]} Δ={d:+.2f} @~{m:.3f}")
                w.writerow([f"{time.time():.0f}", "fill", f"{t} {d:+.2f}", f"{cash:.4f}",
                            f"{inv.get(t, 0.0):.4f}", ""])
            _cycle += 1
            # ⚠️ SHOW THE VERDICT MIX WHILE THE RUN IS STILL ABORTABLE. A run whose tape is flaky
            # returns ~100% UNKNOWN_* — safe-direction, but discovering that at teardown wastes the
            # session, and the 5s ping makes reconnects likelier on a jittery link (any order placed
            # inside a gap is UNKNOWN for its whole life). Printed, not logged: the CSV already has
            # every row, this is for the operator watching.
            if sess.verdicts:
                print("  verdicts: " + " ".join(f"{k}={v}" for k, v in
                                                sorted(sess.verdicts.items())))
            # Defence in depth: the cap is a COMPARISON, and a non-finite pnl makes it silently
            # False (nan < -5.0 is False) — the cap would look armed while protecting nothing. A
            # position can no longer be non-finite (see _positions), but a mid still can, so assert
            # the number is usable BEFORE trusting the comparison rather than after.
            if not math.isfinite(pnl):
                print(f"  🛑 marked P&L is not finite ({pnl!r}) — cannot evaluate the loss cap; "
                      f"failing closed: halt + cancel-all.")
                w.writerow([f"{time.time():.0f}", "halt", "pnl_not_finite", f"{cash:.4f}", "", ""])
                halted, halt_reason = True, "pnl_not_finite"
                break
            if pnl < -args.loss_cap:
                print(f"  🛑 LOSS KILL-SWITCH: marked P&L ${pnl:+.2f} < −${args.loss_cap:.2f} — halting.")
                w.writerow([f"{time.time():.0f}", "halt", "loss_cap", f"{cash:.4f}", "", f"{pnl:.4f}"])
                halted, halt_reason = True, "loss_cap"
                break
            await asyncio.sleep(args.requote_s)
    finally:
        # ⚠️ POLL FILLS BEFORE THE FINAL CANCEL, for the same reason the loop does.
        # Teardown used to cancel first and poll last, which is exactly the ordering the loop fix
        # removed — so the FINAL cycle's orders were still written as `cancelled_unfilled` even when
        # they had filled. On a 20-cycle run that is 5% of orders; on a run that exits early via the
        # loss-cap break it is the most interesting cycle of the lot. It also has to happen before
        # `book_task.cancel()`, or every spread_capture/markout row written here is marked against a
        # dead book — and spread capture IS the maker edge, so marking it off a frozen mid is worse
        # than not recording it.
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
        # Optional passive flatten of OUR inventory, BEFORE the sweep so its residual passive
        # orders are cancelled by the sweep. ⚠️ The sweep is in a `finally` so a SECOND
        # Ctrl-C/SIGTERM during the flatten's wait cannot skip it (a skipped sweep would strand the
        # flatten's own resting orders).
        #
        # ⛔ THE `not halted` GATE IS GONE. It read: "a loss-cap halt means the market moved
        # against us — the worst moment to place new orders — so we do NOT flatten there." That
        # inverts the risk. The process is EXITING, so the loss cap dies with it: declining to
        # flatten does not avoid the adverse move, it walks away from an adverse position and leaves
        # it unattended with no cap at all. **The branch that fired was the branch that left
        # inventory**, on exactly the exits that produced inventory worth worrying about.
        #
        # It is safe to drop because of what `flatten_on_exit` actually is: PASSIVE, post-only, AT
        # the touch, and reduces OUR NET (`venue − base`) — it cannot cross, so "placing new orders in a
        # bad moment" overstates it (worst case it does not fill, which is the state the gate
        # guaranteed anyway). It also reads the VENUE position with its own fail-closed and does NOT
        # flatten on an unreadable read — so the one halt cause where flattening blind would be
        # genuinely wrong (`positions_cannot_verify`) is already handled inside, not by this gate.
        #
        # Per halt cause: `no_two_sided_book` is pre-trade (nothing held; no-op), `pnl_not_finite`
        # is a MARK fault with a known position (must flatten — the old gate skipped it purely
        # because it shared the `halted` flag), `positions_cannot_verify` self-refuses inside, and
        # `loss_cap` is the case above. The teardown's OPEN-POSITION warning still fires either way.
        #
        # ⚠️ "REDUCING" MEANS *OUR NET*, NOT THE ACCOUNT'S POSITION. With inherited `base` — which
        # this tool permits — returning our net to zero can INCREASE the account's absolute position:
        # base +5, run sold 4 (venue +1), halt → the flatten posts a passive BUY 4, back to +5 long.
        # That is the intended "return to base" contract, but do not read this as "can never add
        # exposure"; it cannot add to OUR net, which is a narrower claim.
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
            # `swept_clean` is True only when the listing succeeded and nothing is left resting. If
            # the venue could not be read, or a stray would not cancel, the record stays OPEN
            # (`clean_exit=False`) so the next start REFUSES and `scripts/maker_recover.py` runs.
            # Writing "clean" off our own belief instead of the venue's answer is exactly the
            # fiction `bot/runner/reconcile.py` exists to prevent — and it is the fiction that
            # would matter most, because this flag is what tells recovery there is nothing to do.
            if state is not None:
                try:
                    if swept_clean:
                        for _iid in list(state.snapshot().orders):
                            state.clear_order(_iid)
                        state.end_run(f"halted:{halt_reason}" if halted else "clean")
                    else:
                        print("  ⚠️ the venue could not confirm that nothing is resting — leaving "
                              "the crash record OPEN. The next maker start will refuse until "
                              "`python -m scripts.maker_recover` resolves it.")
                except Exception as e:
                    print(f"  (durable state teardown failed — non-fatal: {e!r})")
        # Catch-up poll for fills that landed DURING teardown. It must run before `book_task.cancel()`
        # — it writes spread_capture and markout rows, and a mid read from a dead book is not a mark.
        # It cannot fix outcome labels (those were already decided by the pre-cancel poll above), so
        # its job is purely to complete the fill record.
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
            # `matched` is the "is this actually wired?" pair. Zero on both across a run that took
            # fills means the plumbing is broken, NOT that the market was quiet — and the two are
            # indistinguishable without the counter, which is the failure mode this whole feature
            # exists to avoid reproducing. `lvl_obs_errors` is the feed-side half.
            health = (f"acked={sess.tape.subscribed} prints={sess.tape.n_trades} "
                      f"dropped={sess.tape.n_dropped} bad_trades={sess.tape.n_bad_trades} "
                      f"reconnects={sess.tape.n_reconnects} "
                      f"matched_prints={n_tr} matched_levels={n_lv} "
                      f"lvl_obs_errors={sum(book.level_observer_errors.values())} "
                      f"last_error={sess.tape.last_error or 'none'} "
                      f"errors={sess.tape.errors or '{}'}")
            print(f"  tape: {health}")
            w.writerow([f"{time.time():.0f}", "tape_health", health, "", "", ""])
            # The captured control frames settle the open "the ack types are guessed, not captured".
            # Written only when we have them, so a clean run does not carry noise.
            if sess.tape.unknown_frames:
                w.writerow([f"{time.time():.0f}", "tape_unknown_frames",
                            json.dumps(sess.tape.unknown_frames), "", "", ""])
        # Final reconciliation: read venue positions (catches any FILLED unexpected/leaked fill). ⚠️ An
        # UNFILLED resting order whose create-response was LOST (create reached the venue, the reply
        # dropped) has no captured order_id, so _cancel_everything can't reach it. At size=1 its exposure
        # is bounded by one contract's face value, but it can't be auto-cancelled here — verify the
        # venue UI for stray resting orders.
        venue = await _positions(client, targets=targets)
        # Capture the RAW target-ticker position items so quantities are measurable next run instead
        # of rounded away — read-only, real runs only. Diagnostic, never gates anything.
        # ⚠️ There is NO integer `position` key. An earlier version of this comment claimed the dicts
        # carry "`position` int alongside `position_fp`"; across every positions_raw row ever
        # captured the key set is exactly {ticker, position_fp, market_exposure_dollars,
        # realized_pnl_dollars, fees_paid_dollars, total_traded_dollars, last_updated_ts}.
        # `position_fp` is the ONLY quantity, and it is genuinely fractional: the venue partial-fills
        # below one contract (a 1-lot maker sell came back with a fill count of 0.11), confirmed
        # against prod — real venue behaviour, not the anomaly it was first read as.
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
        # convention, by construction. Every inline re-derivation of this number has been wrong in a
        # different direction: `_mid(t) or 0.0` marked an unmarkable position at ZERO (a long read as
        # a total loss, a short as a total win); EXCLUDING unmarkable tickers left a short's proceeds
        # unopposed and printed a WIN; and marking the FULL `inv` while `_mark_step` marks only the
        # BOOKED part let the two disagree in SIGN inside one run — sell high, the book goes
        # one-sided, our resting buy fills back to flat, and the cap's last evaluation reads a small
        # LOSS while the headline reads a large WIN on a round trip that made about a tick, with NO
        # warning printed because a flat ticker is in neither the marked nor the unmarkable list.
        # Routing through `_mark_step` also re-books a pending fill for free when the book recovered
        # between the final loop cycle and teardown.
        final_mids = {t: _quote_of(t) for t in targets}   # (bid, ask) — see _quote_price
        cur_pos = venue if venue is not None else last          # venue unreadable → last known
        cash, inv, final_pnl, _fl, _ff = _mark_step(targets, base, last, cur_pos, final_mids, cash)
        # ⚠️ TWO DIFFERENT INCOMPLETENESS SIGNALS, and they stopped being the same thing the moment
        # a fill began refusing the fabricated bound while a mark still accepts it:
        #   · UNMARKABLE — held inventory whose EXIT side is absent. Marked at the worst case, so
        #     the total is a bound, not a hole. Now rare: `_derive` almost always writes both sides,
        #     so this fires mainly when the feed never saw the ticker at all.
        #   · UNBOOKED — a fill we could not price, so it is in NEITHER `cash` NOR `mark`. Detected
        #     by `_mark_step` having declined to advance `last` to the current position.
        # The warning text describes the SECOND, and an earlier version of this predicate tested only
        # the first — so a run whose last fill landed on an empty ladder printed a confident
        # zero marked P&L while holding a real SHORT, with no warning at all.
        unmarkable = [t for t in targets if inv.get(t, 0.0)
                      and _quote_price(inv[t], final_mids.get(t)) is None]
        # ⚠️ KNOWN GAP (only partly closed): when the venue read FAILED, `cur_pos` IS `last`, and `_fl`
        # derives from `last`, so this UNBOOKED predicate is identically empty — it genuinely cannot
        # detect an unbooked fill through a failed read (there is nothing fresh to compare against).
        # What it CAN do, and now does, is STAMP the `end` row `VENUE_READ=FAILED` and warn (below), so
        # an analysis can tell a venue-backed headline from one computed against stale local belief.
        unbooked = [t for t in targets
                    if _fl.get(t, base.get(t, 0.0)) != cur_pos.get(t, 0.0)]
        # A target the (successful) venue read DROPPED is settled-and-gone (or a transient drop).
        # `_mark_step` worst-cased it, but `unmarkable` above re-derives markability from the BOOK, so a
        # vanished ticker whose book still prices is worst-cased WITHOUT being flagged — the operator
        # would see a pessimistic headline with no reason. Flag it, so the bound is not misread as loss.
        vanished = [t for t in targets
                    if venue is not None and t not in venue and inv.get(t, 0.0)]
        # ⛔ MAKER FILLS ONLY — the flatten's own fills are excluded. `seen_fills` doubles as the
        # dedup set and the advertised count, so once the flatten started adding to it the count rose
        # on orders the maker never QUOTED: `stats['placed']` increments only inside `quote()`, so the
        # flatten inflated the numerator and not the denominator, and the implied fill rate with it.
        # A run's stated purpose includes comparing real fill counts against a FIFO model's
        # prediction, so a count inflated by our own forced exit argues the model
        # under-predicts — the exact shape that would justify a larger next rung on a false reading.
        # Reported separately rather than hidden: the flatten count is real, it is just not maker edge.
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
            # The teardown position read failed, so the headline was computed against `last`
            # (stale local belief), not the venue. Stamp it so an analysis can never mistake this for a
            # venue-backed number, and warn — the UNBOOKED check above could not run through a failed read.
            note += " VENUE_READ=FAILED(headline-vs-stale-belief)"
            print(f"  ⚠️ the FINAL venue position read FAILED — this marked-P&L headline is "
                  f"computed against STALE LOCAL BELIEF (last known), NOT a venue read, and the UNBOOKED "
                  f"check could not run. The `end` row is stamped VENUE_READ=FAILED. CHECK THE KALSHI UI.")
        # ⚠️ WRAPPED, because everything below it is the operator's only warning. This was the one
        # `w.writerow` after the venue sweep with no guard, and a raise here (ENOSPC on a long run is
        # the realistic trigger) skipped `fh.close()`, `client.close()`, the DONE / cash-Δ lines and —
        # the line that actually matters — the "⚠️ OPEN POSITION — NOT auto-closed and NOT covered by
        # the loss cap once this process exits" block. The money would be fine (cancel-all and the
        # sweep already ran) while the operator is told the run ended clean and never learns inventory
        # is riding to settlement unwatched. Pre-existing; the per-row flush only moved the failure
        # one line earlier, from `fh.close()` to here. Never swallow this silently — a truncated
        # ledger must be loud — but never let it cost the warning either.
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
                else {t: venue.get(t, 0.0) - base.get(t, 0.0)
                      for t in targets if venue.get(t, 0.0) - base.get(t, 0.0)})
        # ⚠️ get_balance() is AVAILABLE CASH, not portfolio value. With inventory open, cash is down
        # by what the position cost while the position itself still has value — so the cash delta is
        # NOT the P&L (the Kalshi app's portfolio figure = cash + position mark is). Labelling it
        # "GROUND TRUTH" was wrong, and it got a merely-OPEN run reported as a losing one: the cash
        # delta and the true mark-to-market were not even the same order of magnitude.
        # LEAD WITH THE RIGHT NUMBER. `final_pnl` is an EXIT-SIDE (touch) mark — it books each fill at
        # the FAVOURABLE touch (buys at the bid, sells at the ask), so it is optimistically biased BY
        # CONSTRUCTION (`pnl_exit − pnl_mid ≥ 0`, see the module header and `_mark_step`) and carries
        # NO forward-adverse term. A real run showed it POSITIVE against a realized LOSS — an
        # inventory ratchet the mark cannot see. The truth was already in hand: on a FLAT exit the
        # position is closed, so `cash Δ` IS realized P&L, and it has matched the venue's own
        # settlements record to the cent. (⚠️ A net-zero but UNSETTLED yes+no pair relies on the venue
        # releasing the hedged collateral back to cash — confirmed, but not across every series, hence
        # the confirm-via-settlements caveat.) So: FLAT ⇒ headline the cash Δ and demote the touch
        # mark; OPEN or venue-UNREADABLE ⇒ keep the touch mark as the best estimate along with the
        # "cash Δ is not P&L" caveat, and never claim cash Δ is realized without confirming flat.
        # ⚠️ The mid-run LOSS CAP reads this SAME optimistic touch mark — so a run ratcheting into a
        # loss while the cash leg still reads positive is UNDER-PROTECTED. That is the more dangerous
        # half of the defect and it is still open.
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
        # exits, nothing is watching it: the loss cap only runs inside the loop. Say so explicitly
        # rather than leaving the operator to notice a position in the app afterwards.
        if isinstance(held, dict) and held:
            print(f"  ⚠️ OPEN POSITION — NOT auto-closed and NOT covered by the loss cap once this "
                  f"process exits. It settles with the game:")
            for _t, _q in held.items():
                _m = _mid(_t)
                _side = "SHORT yes / long no" if _q < 0 else "LONG yes"
                _mk = f", marked ~${abs(_q) * (1 - _m if _q < 0 else _m):.2f}" if _m else ""
                print(f"       {_t}  {_q:+.2f} ({_side}{_mk})")
            print(f"     Flatten manually in the Kalshi UI if you don't want the exposure "
                  f"(exiting crosses the spread, so it costs the taker fee + spread).")
        print(summary_line(stats, seen_fills, sess.flatten_fill_ids))
        print("  post_only make-only; loss-capped; tracked orders cancelled + venue swept for strays. "
              "If the sweep reported it could NOT list, verify the Kalshi UI manually.")
        # ── STAMP THE EXIT ON THE HEARTBEAT — the last thing main() does ────────────────────────
        # Its ABSENCE is the SIGKILL signature: the deadman reads a heartbeat with no exit stamp
        # and beats that simply stopped as an unclean death, and (via the recorded PID) can tell
        # "killed" from "hung". A HALT is stamped with its reason so it does not read as `clean` —
        # a guard-triggered stop is an exit an operator must see, while a planned finish must not
        # page anyone. Both properties fail if this line is moved anywhere reachable by SIGKILL,
        # which is to say: anywhere at all. It is `mark_exit`'s only caller in this file.
        try:
            hb.mark_exit(f"halted:{halt_reason}" if halted else "clean",
                         markets_quoted=len(targets), fills=len(seen_fills), run_id=run_id)
        except Exception as e:
            print(f"  (heartbeat exit stamp failed — non-fatal: {e!r})")

