"""CLI entry point for the Polymarket US maker. The implementation is `bot/poly_us/maker.py`.

⛔ **THIS FILE IS THE ONLY PLACE REAL MONEY CAN BE ARMED.** It exists to do ONE thing that cannot be
done anywhere else: decide whether real orders are enabled, and set `DRY_RUN` in the environment
BEFORE `bot.core.config` is imported. `config` snapshots the environment at import time, so the
decision cannot move into `main()` — and it must not live in the library, because a module-level
`sys.argv` sniff under `bot/` fires on ANY process that happens to import the module with that
string in its argv, flipping DRY_RUN off as a side effect of an import. Reading argv is a CLI's job.
This mirrors `scripts/kalshi_live_mm.py` exactly, for exactly those reasons.

⚠️ WHAT ACTUALLY GATES REAL ORDERS IS `config.DRY_RUN`, AND ONLY THAT. `PolyUSClient` snapshots it
at construction and every order method short-circuits on it. The `--i-understand-real-money` flag
does not enable anything by itself — it sets the environment variable below. So the dangerous
combination is a `.env` already carrying `DRY_RUN=false` with no flag here: that would place REAL
maker quotes while this run reported a preview. `maker.arming_refusal` makes that a HARD STOP rather
than a silent downgrade to DRY, and the refusal is checked before any venue object is built.

THREE MODES, and they are not interchangeable:

  --shadow            RUNG 0. Runs the complete cycle — cache-busted book reads, tick reads, quote
                      selection, the requote/hold decision against a VIRTUAL resting book — and
                      places nothing. `PolyMaker._place` raises rather than returning, so "places
                      nothing" is structural. This is what answers "can one process hold 33 books
                      inside the rate budget without missing cycles?".
  (default)           DRY. The real cycle against the real client, whose order methods
                      short-circuit on DRY_RUN. Exercises the placement path without money.
  --i-understand-real-money   REAL, and only when `DRY_RUN` is genuinely false.

Run (detached — nothing here launches itself):
    setsid nohup .venv/bin/python -m scripts.poly_live_mm --shadow \\
        --slugs a,b,c --seconds 21600 > logs/poly_live_mm.log 2>&1 &
"""
from __future__ import annotations

import argparse
import asyncio
import math
import os
import sys
import time
from decimal import Decimal, InvalidOperation

# ⛔ MUST PRECEDE THE MAKER IMPORT (which imports bot.core.config). Nothing in this file may import
# anything from `bot` above this line — see the module docstring.
if "--i-understand-real-money" in sys.argv:
    os.environ["DRY_RUN"] = "false"

from bot.core import config                                    # noqa: E402 — ordering is the point
from bot.core.heartbeat import Heartbeat                       # noqa: E402
from bot.core.maker_state import (DEFAULT_LANE, LegacyStateLive,  # noqa: E402
                                  LegacyStateUnclassifiable, MakerStateStore,
                                  PriorRunUnresolved, account_carried, assess_recovery,
                                  live_sibling_lanes, load_all_lanes, load_state_for_venue,
                                  store_for_venue, venue_account_loss, venue_path)
from bot.poly_us import maker                                  # noqa: E402
from bot.poly_us.client import PolyUSClient                    # noqa: E402

_HEARTBEAT_NAME = "poly_live_mm"


# The one certified-flat verdict, as a constant: the post-teardown retry loop compares against it
# rather than substring-matching a sentence that could drift.
VERDICT_FLAT = "  teardown/verify: venue confirms FLAT on this run's market(s)."


def _flatness_verdict(venue_pos: dict[str, tuple[str, str]] | None, run_slugs: set[str],
                      local_inventory: dict) -> str:
    """The post-teardown flatness line — read from the VENUE, never self-certified.

    ⛔ The engine's own final-inventory print is a LOCAL BELIEF assembled from order read-backs; a
    create whose response carried no id produces a fill that belief never sees, so it can print
    FLAT over a real position. The run's gate is flatness, so the gate reads the venue.
    None ≠ {}: unreadable is NOT flat — the reconciler bug this repo already shipped once.
    """
    if venue_pos is None:
        return ("  teardown/verify: ⚠️ venue positions UNREADABLE — flatness NOT certified; "
                "read /portfolio/positions manually before trusting this run's inventory.")
    # A junk quantity is CANNOT-VERIFY, not a traceback: this runs inside the shim's `finally`,
    # so raising here would skip both the verdict and client.close().
    try:
        # Values are (qty, updateTime); the verdict is decided on qty alone, and updateTime is
        # carried into the DISAGREE line so a human can reconcile divergent replica reads.
        residue = {s: qu for s, qu in venue_pos.items()
                   if s in run_slugs and Decimal(str(qu[0])) != 0}
    except (InvalidOperation, IndexError, TypeError):
        return (f"  teardown/verify: ⚠️ venue position quantity UNPARSEABLE ({venue_pos}) — "
                f"flatness NOT certified; read /portfolio/positions manually.")
    if residue:
        local = {s: str(q) for s, q in local_inventory.items() if q} or "FLAT"
        return (f"  teardown/verify: ⛔ VENUE DISAGREES — still holding {residue} on this run's "
                f"market(s); local belief was {local}. Flatten manually; do not trust the tape's "
                f"inventory columns.")
    return VERDICT_FLAT


async def _venue_positions(client: PolyUSClient) -> dict[str, tuple[str, str]] | None:
    """Every open Poly position, or None = CANNOT VERIFY.

    ⛔ None and {} must never be confused: {} is "confirmed flat" and may start, None is "we do not
    know" and must refuse. Collapsing the two is precisely how a position reconciler can report
    "confirmed flat" for a month while you hold positions. Paginates to the end, because stopping at
    page one reproduces the same bug in a different shape.
    """
    out: dict[str, str] = {}
    cursor, pages = "", 0
    while pages < 50:
        params: dict = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            # ⛔ The continuation key below is `nextCursor`, NOT `cursor` — reading the wrong key
            # made this loop always break after page one while the docstring above claimed it
            # paginates to the end. Fail direction was OPEN: unseen positions read as absent,
            # i.e. flat.
            resp = await client._sdk.portfolio.positions(params)
        except Exception as exc:
            print(f"  positions read FAILED: {exc!r} — CANNOT VERIFY", flush=True)
            return None
        if not isinstance(resp, dict):
            print(f"  positions shape unexpected ({type(resp).__name__}) — CANNOT VERIFY")
            return None
        positions = resp.get("positions")
        if not isinstance(positions, dict):
            print(f"  `positions` not a dict ({type(positions).__name__}) — CANNOT VERIFY")
            return None
        for slug, item in positions.items():
            if not isinstance(item, dict):
                print(f"  position for {slug} is {type(item).__name__}, not an object — "
                      f"CANNOT VERIFY")
                return None
            # `dict.get(k, default)` does NOT consult the default when the key is PRESENT AND
            # NULL — check presence explicitly; an unreadable position is cannot-verify,
            # never absent. ⛔ `netPositionDecimal` is REQUIRED: it carries the EXACT
            # holding ("-10.6000") beside the ROUNDED display field `netPosition` ("-11").
            # This read feeds recovery, carry verification and the teardown venue-verify —
            # a rounded value here is the fractional-books incident from the other side.
            # No fallback (rounded or qtyAvailable): missing = CANNOT VERIFY.
            raw = item.get("netPositionDecimal")
            if isinstance(raw, dict):
                raw = raw.get("value")
            if raw is None:
                print(f"  position for {slug} has no netPositionDecimal — CANNOT VERIFY "
                      f"(the rounded netPosition is not a substitute; refusing to read "
                      f"it as flat)")
                return None
            # `updateTime` rides along as evidence for the operator: the endpoint serves
            # divergent replicas — a minutes-stale row is observable — and when reads
            # disagree the venue's own row timestamps are what a human reconciles with.
            out[str(slug)] = (str(raw), str(item.get("updateTime") or "?"))
        cursor = resp.get("nextCursor") or ""
        pages += 1
        if not cursor or resp.get("eof"):
            break
    else:
        # Ran out of pages with a cursor still live: we have NOT seen every position.
        print("  positions pagination hit the 50-page ceiling with more to read — CANNOT VERIFY")
        return None
    return out


def _announce_halt(reason: str | None, run_id: str, real: bool) -> None:
    """Belt to the deadman's braces: a memory halt pages NOBODY until the watchdog's next
    timer tick notices the stale heartbeat — minutes later, and all it can say is "maker
    gone", not WHY. A REAL run posts its halt
    reason at halt time. DRY previews stay quiet (a development halt paging the operator
    channel is alarm fatigue). Best-effort: an announce failure must never break the
    teardown that follows — the deadman remains the backstop for exactly that case."""
    if not real:
        return
    try:
        from scripts.poly_fill_watch import _notify
        _notify(f"🛑 maker HALT ({run_id}): {reason}")
    except Exception:  # noqa: BLE001
        print("  (halt announce failed — the deadman remains the backstop)", flush=True)


def parse_carry(specs: list[str], slugs: list[str]) -> dict[str, tuple[Decimal, Decimal]]:
    """`['slug:170@0.8474', 'slug:-10@0.13']` → {slug: (qty, avg_px)}. FAIL-CLOSED on every
    ambiguity, the `parse_pull_at` discipline verbatim: unknown slug / malformed / duplicate
    / zero qty / basis outside (0,1) each REFUSE — a wrong carry is a wrong BASIS on real
    money, and the no-basis tripwire this seeding disarms only stays honest if the basis is
    right. ⚠️ SHORT basis is the maker's OWN avg_entry semantics (the yes-space price the
    shorts were SOLD at, e.g. hormuz 0.13) — NEVER the venue UI's complement collateral
    (0.868): the two look equally plausible on a screen and differ by 1−2p.
    """
    out: dict[str, tuple[Decimal, Decimal]] = {}
    for spec in specs:
        slug, sep, rest = spec.rpartition(":")
        qty_s, at, px_s = rest.partition("@")
        if not sep or not slug or not at:
            raise SystemExit(f"REFUSING: --carry {spec!r} is not slug:qty@avg_px")
        if slug not in slugs:
            raise SystemExit(f"REFUSING: --carry names {slug!r}, not on --slugs — a carried "
                             f"book the run does not quote cannot be worked down")
        if slug in out:
            raise SystemExit(f"REFUSING: duplicate --carry for {slug!r}")
        try:
            qty, px = Decimal(qty_s), Decimal(px_s)
        except Exception:                               # noqa: BLE001
            raise SystemExit(f"REFUSING: --carry {spec!r} — unparseable qty or basis")
        if qty == 0 or not qty.is_finite() or not px.is_finite():
            raise SystemExit(f"REFUSING: --carry {spec!r} — zero or non-finite")
        if not (Decimal("0") < px < Decimal("1")):
            raise SystemExit(f"REFUSING: --carry {spec!r} — basis {px} outside (0,1); for a "
                             f"SHORT use the yes-space sale price, not the complement cost")
        out[slug] = (qty, px)
    return out


#: Candidate basis fields on a venue position, in the order they are REPORTED to the operator.
#: `avgPx` is used; the rest are printed beside it so a wrong pick is visible rather than silent.
_BASIS_FIELDS = ("avgPx", "costPerShare")


def _money(item: dict, key: str) -> Decimal | None:
    """A venue money field is `{"value": "24.0160", "currency": "USD"}`, never a scalar.

    Parsed from the STRING form (`Decimal("0.5460")`) — `Decimal(float)` would launder the
    float's error into the Decimal and defeat the point [repo rule: prices are Decimal]."""
    raw = item.get(key)
    if isinstance(raw, dict):
        raw = raw.get("value")
    if raw is None:
        return None
    try:
        return Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return None


async def derive_carries(client: PolyUSClient, slugs: list[str]) -> list[str]:
    """Venue positions → `--carry` specs, for `--adopt-existing`. Raises SystemExit on anything
    it cannot derive SAFELY; returns [] when the account is flat.

    ⛔ WHY THIS IS A SAFETY FEATURE, NOT A CONVENIENCE. `parse_carry` verifies the QUANTITY
    against the venue and refuses on a mismatch — but it only RANGE-CHECKS the basis (0 < px < 1).
    A mistyped basis therefore passes silently and seeds a wrong `avg_entry`, which feeds the mark
    tripwire, the reduce-only clamp, and every P&L figure the run reports. A basis
    hand-transcribed from an operator's screen is close enough to look right, and nothing in the
    system could say otherwise if it were not.

    ⚠️ THE VENUE REPORTS FOUR PLAUSIBLE BASES AND THEY DISAGREE — probed live on a real position;
    illustrative shape, the exact gap is per-position:

        avgPx        0.4460     cost/qty      0.4458      ← fee-NET family
        costPerShare 0.4490     baseCost/qty  0.4490      ← fee-GROSS family (cost = baseCost + fees)

    `avgPx` is used, because it is the venue's own average execution price and the field this repo
    already treats as authoritative for the rebate. But the maker's `avg_entry` is built from
    `order.price` — our LIMIT, gross of fees — so the gross family is arguably the closer analogue,
    and the two families separate by a fraction of a cent per contract, which is a non-trivial
    share of the mark tripwire's per-contract threshold. UNRESOLVED: settling it needs a book where
    our own `avg_entry` is known independently and can be matched. Until then every candidate is
    PRINTED beside the choice, so a wrong pick is visible rather than silent.

    ⛔ REFUSES TO ADOPT A SHORT. For a short the basis must be the yes-space SALE price, never the
    UI's complement collateral — the two "look equally plausible on a screen and differ by 1−2p",
    and there is a recorded wire-price-vs-avgPx divergence on exactly that confusion.
    Which family `avgPx` reports for a short is UNVERIFIED (the probe account held only a long),
    so a short must be declared explicitly with `--carry`.

    ⛔ REFUSES TO ADD A BOOK TO THE SLATE. A position on a book not on `--slugs` refuses rather
    than quietly extending the slate: a slate ADDITION bypasses preflight, registration, freshness
    and the calendar gate — the same reason `hot_settings` is removal-direction only.
    """
    positions = await _venue_positions(client)
    if positions is None:
        raise SystemExit("REFUSING: --adopt-existing could not read venue positions — "
                         "CANNOT VERIFY is not 'flat'")
    if not positions:
        print("  --adopt-existing: venue is flat, nothing to adopt", flush=True)
        return []

    raw = await client._sdk.portfolio.positions({"limit": 100})
    items = raw.get("positions") if isinstance(raw, dict) else None
    if not isinstance(items, dict):
        raise SystemExit("REFUSING: --adopt-existing got an unreadable positions envelope")

    specs: list[str] = []
    for slug, (qty_s, _update) in sorted(positions.items()):
        qty = Decimal(qty_s)
        if qty == 0:
            continue
        if slug not in slugs:
            raise SystemExit(
                f"REFUSING: venue holds {qty} on {slug!r}, which is NOT on --slugs. Adopting it "
                f"would extend the slate past preflight, registration, freshness and the calendar "
                f"gate. Add it to --slugs deliberately, or flatten it first.")
        if qty < 0:
            raise SystemExit(
                f"REFUSING: {slug!r} is SHORT ({qty}). The basis for a short must be the yes-space "
                f"SALE price, not the complement collateral, and which one the venue reports is "
                f"UNVERIFIED. Declare it explicitly: --carry {slug}:{qty}@<yes-space-price>")
        item = items.get(slug)
        if not isinstance(item, dict):
            raise SystemExit(f"REFUSING: no readable position payload for {slug!r}")
        basis = _money(item, "avgPx")
        if basis is None or not (Decimal("0") < basis < Decimal("1")):
            raise SystemExit(f"REFUSING: {slug!r} has no usable avgPx ({basis!r})")
        alts = "  ".join(f"{f}={_money(item, f)}" for f in _BASIS_FIELDS)
        print(f"  --adopt-existing: {slug} qty {qty} @ avgPx {basis}   [candidates: {alts}]",
              flush=True)
        specs.append(f"{slug}:{qty}@{basis}")
    return specs


async def _preflight_recovery(client: PolyUSClient, state_path: str | None,
                              carries: dict[str, tuple[Decimal, Decimal]] | None = None,
                              *, lane: str = DEFAULT_LANE,
                              slate: set[str] | None = None) -> bool:
    """Refuse to start a REAL run over an unresolved crash. True = clear to proceed.

    `load_state` rather than a fresh store's `snapshot()`: an empty store snapshots to a state with
    `clean_exit=False`, which reads as "the previous run died" on the very first run. Absent state
    is None, and None is what `assess_recovery` means by "no prior run".
    """
    try:
        resting = await client.get_open_orders()
    except Exception as exc:
        # An unreadable venue is None, never [] — see `_venue_positions`.
        print(f"  open-orders read FAILED: {exc!r} — CANNOT VERIFY")
        resting = None
    positions = await _venue_positions(client)
    # ⛔ UNWRAP THE (qty, updateTime) TUPLES — `assess_recovery` expects bare quantities, and its
    # `_dec` helper swallows the parse error on a stringified tuple and returns ZERO, so passing
    # the tuples through read EVERY inherited position as flat and silently disabled this gate
    # (None survives untouched, because None is CANNOT-VERIFY and must reach the assessment
    # as itself rather than being unwrapped into a zero).
    qty_only = ({k: v[0] for k, v in positions.items()} if positions is not None else None)
    # Venue-split read: the POLY file, falling back to the legacy shared file
    # only when its run_id classifies poly — so this preflight can no longer certify a KALSHI
    # crash record from a Poly venue read (the symmetric hole).
    # Slate-scoped when a slate is given: sibling lanes come from the SHARED poly
    # ledger (own lane excluded); assess_recovery refuses a slate collision, never plans to
    # cancel a sibling's order, and reports (not refuses) foreign positions. slate=None keeps
    # the historical account-global assessment.
    others = None
    if slate is not None:
        vpath = venue_path(state_path or config.MAKER_STATE_FILE, "poly")
        others = {ln: st for ln, st in load_all_lanes(vpath).items() if ln != lane}
    plan = assess_recovery(load_state_for_venue("poly", base_path=state_path, lane=lane),
                           resting, qty_only, slate=slate, other_lanes=others)
    if carries:
        from bot.core.maker_state import apply_carry
        plan = apply_carry(plan, {s: q for s, (q, _px) in carries.items()}, qty_only)
    print(plan.report(), flush=True)
    return plan.action == "start"


WS_SHADOW_MARKER = "logs/ws_shadow_clean.json"
WS_MARKER_MIN_S = 7200.0        # "multi-hour": ≥2h of clean ws shadow
WS_MARKER_MAX_AGE_S = 7 * 86400.0


WS_MARKER_MIN_WS_FRACTION = 0.5    # a "clean ws shadow" must have actually SERVED from WS


def write_ws_shadow_marker(path: str, *, run_id: str, seconds: float, cycles: int,
                           ended_ts: float, ws_fraction: float = 0.0,
                           feed_deaths: int = 0) -> None:
    """Record a clean ws shadow session — the evidence the real-money ws gate demands. Called ONLY after a shadow
    ws run's teardown with no halt_reason and ≥ WS_MARKER_MIN_S of runtime."""
    import json as _json
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as fh:
        _json.dump({"run_id": run_id, "seconds": seconds, "cycles": cycles,
                    "ended_ts": ended_ts, "ws_fraction": ws_fraction,
                    "feed_deaths": feed_deaths}, fh)


def ws_marker_verdict(path: str, now: float) -> str | None:
    """None = the marker permits a real-money ws start; else the human-readable refusal reason.
    Fail-closed: missing, unreadable, too-short, or stale all REFUSE."""
    import json as _json
    try:
        with open(path) as fh:
            m = _json.load(fh)
        seconds = float(m.get("seconds") or 0.0)
        ended = float(m.get("ended_ts") or 0.0)
    except (OSError, ValueError, TypeError):
        return "no readable clean-shadow marker"
    if seconds < WS_MARKER_MIN_S:
        return (f"the recorded ws shadow ran {seconds:.0f}s < the required "
                f"{WS_MARKER_MIN_S:.0f}s (multi-hour)")
    # ⛔ POSITIVE WS evidence required: halt-absence is not the same as feed-worked —
    # a feed that never delivered a single book still "ran clean" over pure REST fallback.
    # Missing field (an old marker) fails closed.
    try:
        wf = float(m["ws_fraction"])
    except (KeyError, ValueError, TypeError):
        return "marker records no ws_fraction (older marker format) — re-earn it"
    if wf < WS_MARKER_MIN_WS_FRACTION:
        return (f"the recorded shadow served only {wf:.0%} of reads from WS "
                f"(< {WS_MARKER_MIN_WS_FRACTION:.0%}) — the WS path did not carry the run")
    if now - ended > WS_MARKER_MAX_AGE_S:
        return (f"the clean-shadow marker is {(now - ended) / 86400.0:.1f} days old "
                f"(> {WS_MARKER_MAX_AGE_S / 86400.0:.0f}d) — re-earn it, the feed/venue drifts")
    return None


def banner_budget(loss_cap: Decimal, carried: Decimal) -> tuple[Decimal, Decimal]:
    """(binding, lifetime_room), matching the ENGINE's two axes exactly.

    `binding` is the number the operator plans around: min(session cap, ledger room), never
    above the cap. Printing cap + carried AS the budget is the trap: the engine halts on
    SESSION loss, so an operator planning around the larger number reads a correct halt as a bug.

    ⚠️ `lifetime_room` is the LEDGER's true remaining room, cap + carried: carried PROFIT
    genuinely extends the multi-session ratchet, so printing cap − loss instead would understate
    the max-loss figure. The session axis binds first, which is why `binding` never exceeds the
    cap. Two axes, two numbers, and the operator needs both."""
    lifetime_room = loss_cap + carried
    return min(loss_cap, lifetime_room), lifetime_room


def parse_pull_at(specs: list[str], slugs: list[str], now: float) -> dict[str, float]:
    """`['slug:1785900000', …]` → {slug: epoch}. FAIL-CLOSED on every ambiguity.

    Refusals, and why each is a refusal rather than a default [the --size discipline: an
    unnamed book refuses rather than inheriting a setting nobody chose]:
      · unknown slug — a typo silently means "this book is NEVER pulled", i.e. it keeps
        quoting straight through the information window the operator wrote the flag to avoid.
        That is the exact failure the flag exists to prevent, arriving via a keystroke.
      · already-past epoch — the operator believes that book is pulled; launching it into a
        normal quote is a config error, not a request to pull immediately (that is a slate
        edit, made by removing the slug).
      · unparseable / duplicate slug — never guess between two deadlines for one book.
    """
    out: dict[str, float] = {}
    for spec in specs:
        slug, sep, raw = str(spec).rpartition(":")
        if not sep or not slug:
            raise SystemExit(f"REFUSING: --pull-at {spec!r} is not `slug:<unix-epoch>`")
        try:
            epoch = float(raw)
        except ValueError:
            raise SystemExit(f"REFUSING: --pull-at {spec!r} — {raw!r} is not an epoch")
        if not math.isfinite(epoch):
            # ⛔ `float()` accepts inf/nan and BOTH slip the past-check in opposite, silent
            # directions: `inf` is never pulled (the unknown-slug failure by
            # another route), `nan` compares False everywhere so the book pulls on cycle 1.
            raise SystemExit(f"REFUSING: --pull-at {spec!r} — {raw!r} is not a finite epoch")
        if slug not in slugs:
            raise SystemExit(
                f"REFUSING: --pull-at names {slug!r}, which is not in --slugs. A typo here "
                f"means that book is never pulled and quotes straight through the window "
                f"this flag exists to avoid.")
        if slug in out:
            raise SystemExit(f"REFUSING: --pull-at names {slug!r} twice — one deadline per book")
        if epoch <= now:
            raise SystemExit(
                f"REFUSING: --pull-at {slug} epoch {epoch:.0f} is already past (now "
                f"{now:.0f}). To not quote a book at all, remove it from --slugs.")
        out[slug] = epoch
    return out


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Polymarket US maker. --shadow places nothing; real money needs the flag.")
    p.add_argument("--slugs", required=True,
                   help="comma-separated Poly market slugs (bare slugs, not side-suffixed tokens)")
    p.add_argument("--lane", default=DEFAULT_LANE,
                   help="ledger lane in the SHARED per-venue state file (default 'main'). Two "
                        "concurrent maker processes MUST use different lanes AND disjoint slates "
                        "— recovery refuses a slate collision; the loss ratchet stays account-"
                        "wide across lanes")
    p.add_argument("--order-ws", action="store_true",
                   help="run the private-WS fill accelerator (bot/poly_us/order_feed.py): order "
                        "events are booked the moment the venue pushes them, through the same "
                        "cum-idempotent path the REST poll verifies. Built after a REST poll "
                        "missed an entire order's worth of fills — a full cap of invisible "
                        "exposure. Off = polling exactly as before")
    p.add_argument("--shadow", action="store_true",
                   help="RUNG 0: run the full cycle and place NOTHING (structurally enforced)")
    p.add_argument("--i-understand-real-money", action="store_true", dest="real_flag",
                   help="arm real orders; requires DRY_RUN=false in the environment too")
    p.add_argument("--size", default=str(maker.MIN_SIZE),
                   help=f"contracts per quote: ONE integer for every book, or PER-BOOK "
                        f"`slug-a:25,slug-b:12` naming EVERY slug (an unnamed book refuses "
                        f"rather than defaulting — size scales the rebate AND the adverse tail). "
                        f"Minimum {maker.MIN_SIZE}; below that the venue rounds the rebate to "
                        f"zero at every price. Per-book caps follow per-book size.")
    p.add_argument("--cap-fills", default="3",
                   help="per-market inventory REDUCE-ONLY TRIGGER expressed in FILLS "
                        "(⚠️ NOT a bound: reach = size x fills + size − 1, e.g. 251 at "
                        "size 84 / 2 fills) (contracts = size x "
                        "fills): ONE integer for every book, or PER-BOOK `slug-a:2,slug-b:1` "
                        "naming EVERY slug — an unnamed book refuses rather than inheriting "
                        "a headroom the operator never chose (same discipline as --size)")
    p.add_argument("--max-total-contracts", type=int, default=None,
                   help="global gross-exposure REDUCE-ONLY TRIGGER in contracts (⚠️ NOT a "
                        "bound — see below); default is the sum of the per-market caps, "
                        "which is NON-BINDING — set it for a multi-market run. ⛔ Crossing it "
                        "stops the ADDING side on every book; it cannot unwind a fill already "
                        "in flight, so gross REACHES up to this + one order size per book "
                        "before reduce-only bites — observed live, inside the documented reach "
                        "and reduced as designed. Budget the REACH, never this number.")
    p.add_argument("--carry", action="append", default=[], metavar="SLUG:QTY@AVGPX",
                   help="CONTINUOUS OPERATION: declare a clean-exit position deliberately "
                        "carried from the previous run — the venue must hold EXACTLY this "
                        "qty (sign included), or the start refuses. The engine seeds it as "
                        "inventory at the declared basis (for a SHORT: the yes-space sale "
                        "price, NEVER the UI's complement collateral). A book at/over its "
                        "cap starts reduce-only and works down — expected. Undeclared live "
                        "positions still refuse: a carry is explicit, per book, per run.")
    p.add_argument("--adopt-existing", action="store_true",
                   help="derive --carry from the venue instead of transcribing it by hand. "
                        "Longs only (a SHORT's basis is unverified and must be declared "
                        "explicitly); refuses any position on a book not already on --slugs. "
                        "Still an explicit act of consent — never silent adoption.")
    p.add_argument("--pull-at", action="append", default=[], metavar="SLUG:EPOCH",
                   help="PER-BOOK calendar exit, repeatable: `slug:<unix-epoch>` — after that "
                        "instant THAT book goes reduce-only (clamped to its inventory) while "
                        "the rest of the slate keeps quoting normally. For a book whose own "
                        "information window opens before the run ends: a temp strike's "
                        "heating window, a game's first pitch. Wall-clock UTC epoch, because "
                        "the deadlines are venue calendar events. An unknown slug or an "
                        "already-past epoch REFUSES the launch rather than quoting a book "
                        "the operator believes is pulled.")
    p.add_argument("--book-source", choices=("rest", "ws"),
                   default=config.MAKER_BOOK_SOURCE,
                   help="per-cycle book READ source. 'rest' (default) polls each book every "
                        "cycle; 'ws' reads the WS order-book cache (freshness-gated, REST "
                        "backstop) to remove the per-book req/s cost. ⛔ 'ws' needs the feed "
                        "wired (next brick); with no feed it safely degrades to REST.")
    p.add_argument("--requote-s", type=float, default=10.0, help="seconds between quote cycles")
    p.add_argument("--seconds", type=float, default=3600.0, help="how long to run")
    p.add_argument("--passive-exit-s", type=float, default=0.0,
                   help="WIND-DOWN window after --seconds expires with inventory: keep the "
                        "normal quote loop running with every book REDUCE-ONLY at a size "
                        "clamped to remaining inventory (the reduce-side order keeps the "
                        "session's queue seniority — the reason this can succeed where the "
                        "teardown's 0-for-3 cold flatten cannot), until flat or this deadline; "
                        "the normal teardown follows either way. 0 = off. NEVER runs on a "
                        "kill-switch/memory/loss halt — those keep the fast teardown.")
    p.add_argument("--max-req-per-s", type=float, default=maker.DEFAULT_MAX_REQ_PER_S,
                   help=f"self-imposed request ceiling (venue limit {maker.VENUE_REQ_PER_S:g}/s; "
                        f"over-limit is THROTTLED, not rejected)")
    p.add_argument("--stale-cancel-cycles", type=int, default=3,
                   help="consecutive un-quotable cycles before a market's resting quotes are "
                        "PULLED (default 3 = 30s at a 10s requote). Lower forfeits queue position "
                        "on routine hiccups; higher leaves orders on a market nothing is reading")
    p.add_argument("--flatten-wait-s", type=float, default=30.0,
                   help="how long the passive teardown flatten is given before the residual is "
                        "reported rather than force-sold")
    p.add_argument("--loss-cap", type=Decimal, default=Decimal("3.00"),
                   help="durable PRICE-REALIZED loss halt, in dollars (0 disables). Counts net "
                        "realized at round-trip completion ONLY: rebates — the strategy's whole "
                        "edge — are NOT credited, so the cap can trip on a rebate-profitable "
                        "lifetime; marks are NOT counted, so an open drawdown is invisible until "
                        "it realizes. The ratchet survives restarts. The default is a CHOSEN "
                        "BUDGET, not a measured tail — set it BELOW your plan's single-book tail "
                        "and only modestly above the worst per-book loss you have on record, so "
                        "EXPECT it to fire on a bad night; that is its job")
    p.add_argument("--mark-trip-per-ct", type=Decimal,
                   default=maker.DEFAULT_MARK_TRIP_PER_CONTRACT,
                   help="per-book MARKED-P&L reduce-only tripwire, in dollars PER CONTRACT "
                        "(0 disables; per-contract so the rail means the same thing at every "
                        "size — a fixed-dollar threshold tightens as headroom grows, "
                        "backwards for the runs it guards). The realized loss cap and the "
                        "post-trip cooldown are both blind to an ACCUMULATING position "
                        "(over a long recorded run the cooldown never fired once); this "
                        "marks inventory "
                        "against the liquidation side of the live touch, arms the same "
                        "reduce-only machinery, and tapes its cycles as status=mark_trip so "
                        "experiment mechanism reads can exclude them. An INHERITED position "
                        "has no basis from this run and arms reduce-only until it exits")
    p.add_argument("--adverse-cooldown-s", type=float, default=900.0,
                   help="after a round trip realizes worse than "
                        f"{maker.ADVERSE_RT_PER_CONTRACT}/contract, quote that book REDUCE-ONLY "
                        "for this long (0 disables). Replay-backed: it removes the re-entry that "
                        "bought the top of a deflating news spike seconds after the adverse close")
    p.add_argument("--enable-probe-retirement", action="store_true",
                   dest="probe_retirement",
                   help="let a cancel-probe's structured not_found RETIRE a zombie order "
                        "(and re-quote its side). OFF by default: the verdict has never been "
                        "observed on a real purge, and the corroborating reads share one store "
                        "with hours of documented lag — enable only after you have captured and "
                        "reviewed the venue's own not-found body.")
    p.add_argument("--quote-csv", default=None, help="override the quote tape path")
    p.add_argument("--cycle-csv", default=None, help="override the per-cycle tape path")
    p.add_argument("--fill-csv", default=None, help="override the fill tape path")
    return p


async def main() -> None:
    args = _build_parser().parse_args()
    if not args.loss_cap.is_finite() or args.loss_cap < 0:
        # `Decimal("Infinity")` parses and would arm an infinite cap with no DISABLED warning;
        # `nan < x` is False everywhere, the shape that poisoned the Kalshi cap once already;
        # a NEGATIVE cap silently disables (the engine requires > 0) — 0 is the one deliberate
        # OFF switch.
        raise SystemExit(f"REFUSING: --loss-cap {args.loss_cap} is not a finite non-negative "
                         f"number. Use 0 to disable the cap deliberately.")

    # ── the arming gate, before anything is constructed ───────────────────────────────────────
    refusal = maker.arming_refusal(config.DRY_RUN, args.real_flag)
    if refusal:
        raise SystemExit(f"REFUSING: {refusal}")
    real = maker.is_real_money(config.DRY_RUN, args.real_flag)
    if real and args.shadow:
        raise SystemExit("REFUSING: --shadow and --i-understand-real-money are contradictory. "
                         "A shadow run places nothing by construction; asking for both means one "
                         "of the two is not what you meant.")
    # ⛔ THE WS-FEED GATE: REAL money + --book-source ws refuses unless a
    # MARKER records a clean multi-hour ws SHADOW session — a hurried morning must not be able to
    # point real money at an unproven feed. The marker is written only by a shadow ws run that
    # ended cleanly (no halt) after ≥2 h; it expires after 7 days (the feed/venue can drift).
    # ⚠️ This gate is NECESSARY, not sufficient: the flip still needs its own money-path review.
    if real and args.book_source == "ws":
        verdict = ws_marker_verdict(WS_SHADOW_MARKER, time.time())
        if verdict is not None:
            raise SystemExit(f"REFUSING: --book-source ws with real money — {verdict}. "
                             f"Shadow-run --book-source ws for ≥2h to earn the marker "
                             f"({WS_SHADOW_MARKER}), then re-review the flip.")
        print(f"  ws shadow mk : {WS_SHADOW_MARKER} OK (clean ws shadow on record)", flush=True)

    slugs = [s.strip() for s in args.slugs.split(",") if s.strip()]
    if not slugs:
        raise SystemExit("REFUSING: --slugs is empty.")
    # Parse + validate the size spec BEFORE anything is constructed, so a typo'd per-book spec
    # dies at the CLI rather than inside the engine. [per-book sizing]
    try:
        sizes = maker.parse_sizes(str(args.size), slugs)
    except ValueError as exc:
        raise SystemExit(f"REFUSING: {exc}")
    for slug, sz in sorted(sizes.items()):
        refusal = maker.size_refusal(sz)
        if refusal:
            raise SystemExit(f"{slug}: {refusal}")
    suffixed = [s for s in slugs if "::" in s]
    if suffixed:
        raise SystemExit(f"REFUSING: {suffixed} are side-suffixed tokens. One slug carries ONE "
                         f"book, in yes space; an ask is expressed as a short at the complement "
                         f"inside the client, not as a suffixed token here.")
    # BEFORE the client is constructed. The first cut of this sat after `_preflight_recovery`,
    # so the "never touches the venue" claim it made was false — positions and open orders had
    # already been read. Parsing needs nothing but argv and the clock, so it belongs up here.
    pull_at = parse_pull_at(args.pull_at, slugs, time.time())

    # ── the kill switch, with its path RESOLVED and announced ──────────────────────────────────
    # A relative operational path is a known, expensive defect in this repo: a launch from another
    # cwd silently makes the guard inert. Print what will actually be checked.
    kill_path = os.path.abspath(config.KILL_SWITCH_FILE) if config.KILL_SWITCH_FILE else None
    mode = "SHADOW (places nothing)" if args.shadow else ("REAL MONEY" if real else "DRY")
    print("=" * 78)
    print(f"Poly US maker — mode: {mode}")
    print(f"  markets      : {len(slugs)}  requote {args.requote_s:g}s")
    cap_fills_by_slug = maker.parse_sizes(str(args.cap_fills), slugs)
    for slug, sz in sorted(sizes.items()):
        cf = cap_fills_by_slug[slug]
        print(f"    {slug[:52]:52s} size {sz:>3d}  cap "
              f"{maker.cap_contracts(sz, cf):>3d} ({cf} fills)")
    print(f"  adverse cooldown: {args.adverse_cooldown_s:g}s after a round trip worse than "
          f"−{maker.ADVERSE_RT_PER_CONTRACT}/contract"
          + ("  (DISABLED)" if args.adverse_cooldown_s <= 0 else ""))
    print(f"  mark tripwire: reduce-only at −${args.mark_trip_per_ct}/contract marked "
          f"(liquidation-side; the realized cap and post-trip cooldown cannot see an "
          f"accumulating position; inherited no-basis inventory arms until it exits)"
          + ("  ⚠️ DISABLED" if args.mark_trip_per_ct <= 0 else ""))
    print(f"  loss cap     : ${args.loss_cap} durable realized (REALIZED only, marks invisible)"
          + ("  ⚠️ DISABLED" if args.loss_cap <= 0 else ""))
    print(f"  kill switch  : {kill_path or '⚠️ NONE CONFIGURED — there is no pause file'}")
    if kill_path:
        print(f"                 (currently {'ACTIVE' if os.path.exists(kill_path) else 'clear'})")
    print(f"  DRY_RUN      : {config.DRY_RUN}   real-money flag: {args.real_flag}")
    print("=" * 78, flush=True)

    if args.max_total_contracts is None and len(slugs) > 1 and not args.shadow:
        print("⚠️ --max-total-contracts not set: the global cap defaults to the SUM of the "
              "per-market caps and is therefore non-binding. Set it explicitly for a "
              "multi-market run.", flush=True)

    client = PolyUSClient()
    if args.adopt_existing:
        if args.carry:
            raise SystemExit("REFUSING: --adopt-existing with --carry is ambiguous — one of them "
                             "is wrong and there is no way to tell which. Pick one.")
        # Derived specs go through `parse_carry` VERBATIM, so every interlock the manual path has
        # (venue-qty match, duplicate, range, on-slate) applies identically. Deriving must not
        # become a second, weaker entrance.
        args.carry = await derive_carries(client, slugs)
    carries = parse_carry(args.carry, slugs)
    store: MakerStateStore | None = None
    effective_cap = args.loss_cap      # tightened to the account-wide binding budget below (real)
    if real:
        state_path = getattr(config, "MAKER_STATE_FILE", None)
        # Slate scoping activates exactly when a PAIR is possible: a non-main lane, or a LIVE
        # sibling lane (unclean exit, maybe-live orders, or recorded inventory). A solo main run
        # keeps the historical ACCOUNT-GLOBAL gate — and ⛔ a long-DEAD lane record must not
        # activate scoping: lane records are never deleted, so keying on lane-key EXISTENCE
        # would latch scoping ON forever after a single probe, permanently demoting main's
        # unknown_position refusal to a printed note.
        try:
            _vpath = venue_path(state_path or config.MAKER_STATE_FILE, "poly")
            _live_sibs = live_sibling_lanes(_vpath, args.lane)
        except Exception:
            _live_sibs = {}        # unreadable ledger surfaces via the preflight itself
        _scoped = args.lane != DEFAULT_LANE or bool(_live_sibs)
        if not await _preflight_recovery(client, state_path, carries, lane=args.lane,
                                         slate=(set(slugs) if _scoped else None)):
            await client.close()
            # ⛔ Do NOT name `scripts.maker_recover` here — it is KALSHI-only (builds
            # KalshiClient unconditionally) and was demonstrated certifying a false clean from
            # the wrong venue over a live Poly order.
            # Confirmation is by API, not the UI:
            # this branch includes cannot_verify (the read just failed), where "nothing in the
            # UI" is also what a wrong-account login shows.
            raise SystemExit("REFUSING to start — resolve the recovery plan above on the POLY "
                             "venue (confirm what is resting with `.venv/bin/python -m "
                             "scripts.poly_us_orders`, read-only, same credentials; close "
                             "positions with `scripts.poly_close`). Do NOT use "
                             "scripts.maker_recover — it is Kalshi-only and reads the wrong "
                             "venue.")
        # Constructed only AFTER the assessment: the constructor raises on a corrupt record, and
        # `begin_run` refuses to clobber an unresolved crash — both of which the plan above already
        # explains in operator terms. `store_for_venue` opens the POLY file and adopts a
        # poly-classified legacy record once (ledger rides along; legacy renamed, not deleted).
        try:
            store = store_for_venue("poly", base_path=state_path, lane=args.lane)
        except (LegacyStateLive, LegacyStateUnclassifiable) as exc:
            await client.close()
            raise SystemExit(f"REFUSING to start — {exc}")
        # The RATCHET CARRIES: an operator relaunching with the same --loss-cap after a losing
        # session has only the REMAINDER of it, not a fresh one — print the carried number and
        # the true remaining budget so an immediate halt reads as "the ratchet", never "a bug".
        # The ratchet is ACCOUNT-WIDE: the budget is priced
        # off Σ per-lane LOSS (each floored at zero — a profitable lane must NOT buy another
        # lane's loss budget), and the binding number is ENFORCED, not just printed:
        #   · binding ≤ 0 → REFUSE to start (the old banner said "⛔ ALREADY BREACHED" and then
        #     started an engine that would never halt, because the engine's own-lane ledger was
        #     clean — the looks-protected-but-isn't inversion);
        #   · the ENGINE's cap input becomes `binding`, so this session halts where the ACCOUNT
        #     ratchet says, not where this lane's private history says. The engine additionally
        #     subtracts its own lane's carried loss from that number — double-counting our own
        #     lane, which only makes the halt EARLIER (safe direction, documented here).
        account_lost = venue_account_loss("poly", state_path)
        carried = account_carried(store.path)      # signed, for the operator's context line
        effective_cap = args.loss_cap
        if args.loss_cap > 0:
            binding, lifetime_room = banner_budget(args.loss_cap, -account_lost)
            lane_note = "" if args.lane == DEFAULT_LANE else f" [lane {args.lane!r}]"
            print(f"  loss-cap ledger: account loss ${account_lost} (Σ lanes, losses only; "
                  f"signed carried {carried:+}) → binding budget ${binding} "
                  f"(session ${args.loss_cap}, lifetime ${lifetime_room}){lane_note}", flush=True)
            if binding <= 0:
                await client.close()
                raise SystemExit(
                    f"REFUSING to start — the account-wide loss ratchet is exhausted "
                    f"(Σ lane losses ${account_lost} ≥ cap ${args.loss_cap}). Clear the ledger "
                    f"deliberately (note realized first) or raise --loss-cap deliberately.")
            # ⛔ The engine's lifetime axis now reads the ACCOUNT-WIDE loss LIVE from the
            # shared ledger every check — a startup-only subtraction lets N concurrent lanes
            # each price a full cap at launch, so the account's real exposure is N times what
            # the operator set. The engine therefore receives the RAW cap; subtracting siblings
            # here as well would double-count them. binding remains the operator's printed
            # planning number; the refusal above and the engine's live read do the enforcing.
            effective_cap = args.loss_cap

    # Mode-stamped name: the exit verdict (`record_open:` after a dirty
    # sweep) lives in this FILE, and one shared name let a later --shadow peek stamp "clean"
    # into it — erasing the alarm over a still-open crash record. Same displacement class as
    # the dry-tapes incident; same fix as `_default()`'s tape stamping.
    _hb_mode = "" if real else (".shadow" if args.shadow else ".dry")
    heartbeat = Heartbeat(_HEARTBEAT_NAME + _hb_mode, interval_s=args.requote_s,
                          directory=getattr(config, "HEARTBEAT_DIR", None),
                          # The teardown's passive flatten legitimately blocks, so the budget has
                          # to cover it or the deadman trips on a correct shutdown.
                          # +120s: the teardown's delayed-verify phase (4×20s) legitimately blocks
                          # between cancel-all and the flatten, and the post-teardown
                          # flatness verify may re-read twice 20s apart on a stale positions row —
                          # without this headroom the deadman reports stale on a CORRECT shutdown.
                          stale_after_s=max(args.requote_s * 3.0 + args.flatten_wait_s + 120.0,
                                            90.0))

    feed = None
    if args.order_ws:
        from bot.poly_us.order_feed import OrderFeed
        feed = OrderFeed()
        feed.start()
        print("  order-ws    : ACCELERATOR ON (REST poll still verifies; a dead feed degrades "
              "to polling, never to silence)", flush=True)

    # WS book feed: when --book-source ws, construct + prime + run the market
    # order-book cache and hand it to the maker. Its run_forever task is cancelled on BOTH exit
    # paths (the PriorRunUnresolved refusal below and the finally), mirroring order_feed — a WS
    # task must never outlive the run. With no feed the seam degrades to REST, so this is the
    # ONLY place "ws" becomes live. Two guards stand behind it: the shadow-marker gate
    # (real+ws refuses without a clean multi-hour ws shadow carrying ws_fraction evidence) and
    # the whole-connection fallback→halt. ⛔ Neither is sufficient on its own — pointing real
    # money at a new feed earns a deliberate review, not a flag flip.
    book_feed = None
    book_feed_task = None
    if args.book_source == "ws":
        from bot.poly_us.feed import PolyUSOrderBookCache
        book_feed = PolyUSOrderBookCache(client._sdk)
        for _slug in slugs:
            book_feed.prime(_slug, 1.0)          # 1.0 sentinel until the first WS book arrives
        book_feed.retain_slugs(set(slugs))
        book_feed_task = asyncio.create_task(book_feed.run_forever())
        print(f"  book-ws     : ON — maker reads the WS book cache for {len(slugs)} book(s) "
              f"(freshness-gated {maker.WS_BOOK_STALE_S:.0f}s, single-book REST backstop, "
              f"re-read cap {maker.WS_STALE_REREAD_MAX}/cycle)", flush=True)

    async def _cancel_book_feed() -> None:
        """Cancel the book-feed task idempotently — safe to call on any exit path."""
        if book_feed_task is None or book_feed_task.done():
            return
        book_feed_task.cancel()
        try:
            await book_feed_task
        except asyncio.CancelledError:
            pass    # our own cancellation echoing back — expected; anything else propagates

    try:
        engine = maker.PolyMaker(
            client=client, slugs=slugs, size=args.size, cap_fills=args.cap_fills,
            requote_s=args.requote_s, max_total_contracts=args.max_total_contracts,
            max_req_per_s=args.max_req_per_s, shadow=args.shadow, real=real,
            flatten_wait_s=args.flatten_wait_s, stale_cancel_cycles=args.stale_cancel_cycles,
            adverse_cooldown_s=args.adverse_cooldown_s,
            mark_trip_per_ct=args.mark_trip_per_ct, loss_cap=effective_cap,
            heartbeat=heartbeat, order_feed=feed, state_store=store,
            quote_csv=args.quote_csv, cycle_csv=args.cycle_csv, fill_csv=args.fill_csv,
            probe_retirement=args.probe_retirement,
            book_source=args.book_source, book_feed=book_feed)
        # `prepare()` belongs INSIDE this handler: `begin_run` refuses an unresolved crash from
        # there, not from the constructor — and the record-open-plus-flat-venue state that
        # reaches it is a ROUTINE teardown outcome since the sweep-clear (an unattributable
        # stray leaves the record open while the preflight sees a flat venue and plans start).
        # With `prepare()` outside, that refusal was a raw traceback with the client left open.
        engine.pull_at = pull_at        # validated pre-client, above
        # CONTINUOUS OPERATION: seed the declared carries as inventory WITH basis — the
        # preflight has already verified the venue holds exactly these. Basis seeding is
        # what keeps the no-basis mark-tripwire honest instead of armed-by-default; a book
        # at/over its cap starts reduce-only and works down (the plan said so).
        for _slug, (_qty, _px) in carries.items():
            engine.inventory[_slug] = _qty
            engine.avg_entry[_slug] = _px
            print(f"  carry seeded: {_slug} qty {_qty} @ basis {_px}", flush=True)
        if engine.pull_at:
            for slug, epoch in sorted(engine.pull_at.items(), key=lambda kv: kv[1]):
                print(f"  📕 pull-at    : {slug} goes reduce-only in "
                      f"{(epoch - time.time()) / 60:.0f} min (epoch {epoch:.0f})", flush=True)
        await engine.prepare()
    except PriorRunUnresolved as exc:
        if feed is not None:
            await feed.stop()
        await _cancel_book_feed()
        await client.close()
        raise SystemExit(f"REFUSING: {exc}")
    if not engine.ticks:
        await _cancel_book_feed()
        await client.close()
        raise SystemExit("REFUSING: no market had a readable tick — nothing to quote. A defaulted "
                         "tick misprices silently, so this is a refusal, not a fallback.")
    print(f"quoting {len(engine.ticks)}/{len(slugs)} market(s): "
          + ", ".join(f"{s} tick={t}" for s, t in sorted(engine.ticks.items())), flush=True)

    # BEFORE any order: which markets cannot earn a rebate at this size. The rebate is the ONLY
    # positive term in this lane, so quoting a market where it rounds to zero is taking the risk
    # for none of the reward. Read-only (`read_touches`, not `run_cycle`) so the warning arrives
    # before the money does.
    zero = engine.zero_rebate_markets(await engine.read_touches())
    if zero:
        print(f"⚠️ {len(zero)}/{len(engine.ticks)} market(s) earn NO rebate at their size — "
              f"the venue rounds the credit to the nearest cent and these fall under half a cent:",
              flush=True)
        for line in zero:
            print(f"     {line}", flush=True)

    # `monotonic`, not wall time: a clock step (NTP, a VM resume) must not extend or truncate a
    # timed run, and must never make a cadence sleep negative.
    started = time.monotonic()
    last_report = 0.0
    _loop_completed = False    # True ONLY when the run loop exits normally — gates the shadow marker
    try:
        while time.monotonic() - started < args.seconds:
            cycle_start = time.monotonic()
            # `run_cycle` polls fills itself, at the TOP of the cycle — the cap and reduce-only
            # rules read inventory, so it has to be current before any quote is decided.
            stats = await engine.run_cycle()
            if engine.should_stop:
                print(f"\n🛑 HALTING: {engine.halt_reason}", flush=True)
                _announce_halt(engine.halt_reason, engine.run_id, engine.real)
                break
            elapsed = time.monotonic() - started
            if elapsed - last_report >= 60.0 or stats.cycle == 1:
                last_report = elapsed
                # Two rates, because they answer different questions: `peak` is the in-cycle burst
                # the venue's limiter actually sees, `sust` is the cadence-averaged load. A run
                # can be comfortably under the sustained budget and still be throttled on the peak.
                print(f"  … {elapsed:6.0f}s  cycle={stats.cycle}  wall={stats.wall_s:.2f}s  "
                      f"req={stats.requests} (peak {stats.req_per_s:.1f}/s, "
                      f"sust {stats.requests / args.requote_s:.1f}/s)  "
                      f"quoted={stats.markets_quoted}  skipped={stats.markets_skipped}  "
                      f"missed={stats.missed}  "
                      f"stale_max={stats.max_staleness_s or 0:.1f}s  "
                      f"actions={stats.actions}  rss={stats.rss_mb or 0:.0f}MB  "
                      f"repl_blocked={engine.replace_blocked}"
                      + (f"  ws={'up' if feed.subscribed else 'DOWN'} deaths={feed.deaths} "
                         f"anom={feed.echo_anomalies}" if feed is not None else ""),
                      flush=True)
            # Hold the CADENCE at --requote-s rather than adding the cycle's own duration to it,
            # so the sampling interval is the one the operator asked for. An overrun is COUNTED
            # (stats.missed), not absorbed.
            await asyncio.sleep(max(0.0, args.requote_s - (time.monotonic() - cycle_start)))
        # ── WIND-DOWN [--passive-exit-s] ───────────────────────────────────────────────────
        # Runs ONLY after a NORMAL clock expiry: `halt_reason is None` excludes kill-switch,
        # memory and loss-cap halts (a memory halt lingering with live orders is the exact
        # hazard the floor exists to prevent; a kill switch means GONE). Inside the same
        # `try`, so every halt path and exception still reaches the teardown `finally`.
        if (args.passive_exit_s > 0 and engine.halt_reason is None
                and engine.gross_exposure > 0):
            engine.winddown = True
            print(f"\n⏳ WIND-DOWN: gross {engine.gross_exposure} — reduce-only on every book "
                  f"(size clamped to inventory, the flip guard) for up to "
                  f"{args.passive_exit_s:g}s; the reduce orders carry this session's queue "
                  f"seniority. Teardown follows at flat or deadline.", flush=True)
            wd_end = time.monotonic() + args.passive_exit_s
            while (time.monotonic() < wd_end and engine.halt_reason is None
                   and engine.gross_exposure > 0):
                cycle_start = time.monotonic()
                await engine.run_cycle()
                if engine.should_stop:
                    print(f"\n🛑 HALTING (wind-down): {engine.halt_reason}", flush=True)
                    _announce_halt(engine.halt_reason, engine.run_id, engine.real)
                    break
                await asyncio.sleep(max(0.0, args.requote_s
                                        - (time.monotonic() - cycle_start)))
            print(f"  wind-down end: gross {engine.gross_exposure} "
                  f"({'FLAT — nothing for the teardown to flatten' if engine.gross_exposure == 0 else 'residual remains — the teardown reports it'})",
                  flush=True)
        _loop_completed = True
    finally:
        # ⛔ cancel-all → reconcile-pending → flatten → sweep. The order is the library's
        # (`TEARDOWN_PHASES`), not this file's. The feed drains INSIDE teardown (its queue may
        # hold the fill that reconcile-pending would otherwise wait 80s to verify) and stops
        # after, so a WS task never outlives the run.
        for phase, outcome in await engine.teardown():
            print(f"  teardown/{phase}: {outcome}", flush=True)
        # The ws shadow marker: a SHADOW ws session that ran long enough and ended with NO
        # halt earns the marker the real-money ws gate requires. Written after teardown so a
        # teardown crash never certifies; a halted/short/interrupted run writes nothing.
        _elapsed = time.monotonic() - started
        # ⛔ `_loop_completed` (set ONLY after the while-loop exits normally) — the bare
        # `finally` also runs on Ctrl-C and unhandled exceptions, and halt_reason is None on
        # every exception path, so without this flag a CRASHED long shadow certifies the feed.
        if (args.shadow and args.book_source == "ws" and engine.halt_reason is None
                and _loop_completed and _elapsed >= WS_MARKER_MIN_S):
            _wf = (engine.ws_reads / engine.book_reads) if engine.book_reads else 0.0
            write_ws_shadow_marker(WS_SHADOW_MARKER, run_id=engine.run_id,
                                   seconds=_elapsed, cycles=engine.cycle_index,
                                   ended_ts=time.time(), ws_fraction=_wf,
                                   feed_deaths=engine.ws_feed_deaths)
            print(f"  ws shadow mk : clean {_elapsed / 3600.0:.1f}h ws shadow recorded → "
                  f"{WS_SHADOW_MARKER}", flush=True)
        # Final value, unconditionally: on a halt the last 60s status line can be a minute
        # stale, and this counter's whole point is being seeable.
        print(f"  repl_blocked : {engine.replace_blocked} refused-cancel REPLACE(s) left a "
              f"side stale/dark for a cycle", flush=True)
        if engine.cum_regressions:
            print(f"  ⛔ cum_regressions: {engine.cum_regressions} — a cumQuantity went "
                  f"BACKWARDS (some read lied); run scripts.poly_order_diff before trusting "
                  f"this run's fills", flush=True)
        # Per-run belief-recovery obligations, each one a defect a review turned up: the probe
        # verdict distribution (the interim substitute for instrumenting the venue's own
        # answer), commission-less recovered orders (BLANK is not zero and must be named), and
        # the through-zero counter (the loss-cap corruption window, visible without
        # forensics).
        if engine.recovery_probe_verdicts:
            print(f"  probe verdicts: {engine.recovery_probe_verdicts} "
                  f"(retirement {'ENABLED' if engine.probe_retirement else 'disabled'}; "
                  f"an all-'ok' distribution under a real purge means the venue cancels "
                  f"idempotently and the probe is uninformative)", flush=True)
        if engine.recovery_commission_absent:
            print(f"  ⚠️ recovered orders with NO commission snapshot (rebate column BLANK, "
                  f"not zero): {engine.recovery_commission_absent}", flush=True)
        if engine.recovered_through_zero:
            print(f"  ⚠️ recovered_through_zero: {engine.recovered_through_zero} recovered "
                  f"booking(s) touched/crossed flat — the durable realized ratchet may be "
                  f"pairing-corrupted; compare against the session report's chronological "
                  f"pairing before trusting either", flush=True)
        if feed is not None:
            await feed.stop()
            print(f"  order-ws     : stopped — events={feed.events} dropped={feed.dropped} "
                  f"connects={feed.connects} errors={feed.errors} deaths={feed.deaths} "
                  f"anomalies={feed.echo_anomalies} "
                  f"unattributed={engine.order_feed_unattributed} "
                  f"last_data_age={feed.data_age_s():.0f}s", flush=True)
        # Book WS task cancelled AFTER teardown (the teardown flatten reads fresh REST, not the
        # cache, so the feed is not needed during teardown) — a WS task never outlives the run.
        if book_feed_task is not None:
            await _cancel_book_feed()
            print("  book-ws     : stopped", flush=True)
        # ⛔ "TEARDOWN FLAT" IS CERTIFIED BY THE VENUE, NOT BY OUR OWN BOOKKEEPING. The engine's
        # final-inventory line is a LOCAL BELIEF assembled from order read-backs; a create whose
        # response carried no id produces a fill this belief never sees, and the flatten is then
        # sized against a number that is simply wrong while printing "FLAT". The run's gate is
        # flatness, so the gate reads the venue. None ≠ {}: unreadable is NOT flat.
        # ⚠️ The positions endpoint serves DIVERGENT REPLICAS — a one-shot read has returned a
        # minutes-stale row while a steadily-polling guard stayed fresh — so one
        # read must not decide the run's gate — but the retries are EVIDENCE, never an upgrade:
        # once any read disagrees, a later FLAT can itself be the stale row (an old snapshot
        # predating the position), and certifying on it would print "confirms FLAT" over real
        # inventory nothing is watching. A disagreeing sequence therefore always refuses; every
        # read is printed with the venue's own per-position updateTime for a human to reconcile.
        # (the first version of this loop broke out on the first FLAT it saw, which is exactly
        # the stale-snapshot case above.)
        verdict = _flatness_verdict(await _venue_positions(client), set(engine.ticks),
                                    dict(engine.inventory))
        if verdict == VERDICT_FLAT:
            print(verdict, flush=True)
        else:
            reads = [verdict]
            for _ in range(2):
                await asyncio.sleep(20.0)
                reads.append(_flatness_verdict(await _venue_positions(client),
                                               set(engine.ticks), dict(engine.inventory)))
            for i, r in enumerate(reads):
                print(f"  [verify read {i + 1}/{len(reads)}]{r}", flush=True)
            print("  teardown/verify: ⛔ NOT certified — the first read disagreed, and a later "
                  "FLAT cannot overrule it (a stale replica can serve an old flat row). "
                  "Reconcile the reads above against the venue before trusting this run's "
                  "inventory.", flush=True)
        await client.close()

    print(f"\n{engine.cycle_index} cycles over {len(engine.ticks)} market(s).")
    print(f"tapes → {engine._cycle_path}\n        {engine._quote_path}")
    if not args.shadow:
        print(f"        {engine._fill_path}")


if __name__ == "__main__":
    asyncio.run(main())
