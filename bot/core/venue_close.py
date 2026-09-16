"""
bot/core/venue_close.py
───────────────────────
HAND CLOSES — a belief the venue does not hold, priced from the venue's own activities ledger.

⛔ WHY. A believed book with NO venue row whose market has NOT resolved was, until now, the one
permanent refusal: the reconcile tool printed `⛔ NO MATCH` and `_preflight_recovery` carried the
wrong inventory forward forever. But the shape has a mundane cause — the operator closed the
position BY HAND on the UI, and an out-of-process close never books. The remedy was a hand edit
of the durable state file. That ends: **a belief the venue does not hold never blocks a
launch** [OPERATOR-DECIDED].

So the absent row is PRICED before it is refused, from the only venue record of the close that
exists: the trade-activities ledger. This module owns that walk and both of its landings, and
the reconcile tool, the launch shim (`_preflight_recovery`) and the settle step all call it —
one mechanism, three entry points.

  · the activities NET the belief to exactly zero → `MakerStateStore.add_hand_close` folds
    `qty·(exit_avg − basis)` into `realized_pnl` (the cap ratchet: an out-of-process close is as
    real as a fill), keyed on the venue's own oldest trade id.
  · anything else — an unreadable ledger, a net that is not zero, a basis this run cannot
    replay — PARKS the belief in `closed_unpriced` and zeroes it. Parking is not booking: no
    money moves, the quantity stays visible, and `poly_settle_book --auto` re-walks it later.

⛔ THE PARSER IS NOT A SECOND ONE. `bot.core.poly_activities.poly_transactions` is the shipped
activities parser (its three named traps: `effectiveRealizedPnl` over `realizedPnl`, the
`isAggressor` leg selection, the per-execution commission) and it is CALLED, never re-implemented.
Direction comes from `side`/`outcomeSide` together — never `intent`, which reads `SELL_SHORT`
while REDUCING a short the private design notes.

⛔ IT MOVES REAL MONEY (`realized_pnl` → the loss cap), so every unknown parks instead of booking.
Nothing here can book on silence.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Container, MutableMapping, Sequence

from bot.core.money import dec_or_none

log = logging.getLogger(__name__)

_ZERO = Decimal("0")
_ONE = Decimal("1")

#: A pause between paginated activity reads — the CDN politeness lever: the ban came from
#: unpaced bursts, never a rate.
PACE_S = 1.0  # placeholder — production value withheld
#: A hand close sits within a page or two of the run's own last fill; the page bound is far
#: more ledger than one night produces. It bounds a cursor that never dies.
MAX_PAGES = 1  # placeholder — production value withheld
#: The walk stops once a page's OLDEST trade predates the run's last taped fill by this margin.
#: The tape stamp is our local receipt time and the venue's is `createTime`; 60 s covers the skew
#: in the safe direction (one extra page, never a missed close).
SINCE_MARGIN_S = 60.0


@dataclass(frozen=True)
class FillReplay:
    """This run's own replay of one book: (basis, why, the last taped fill's ts)."""
    basis: Decimal | None
    reason: str
    last_fill_ts: float | None


@dataclass(frozen=True)
class HandClose:
    """A priced hand close. `amount` FOLDS INTO `realized_pnl`; nothing else here does."""
    slug: str
    qty: Decimal            # the belief that was closed — signed, YES space
    basis: Decimal
    exit_avg: Decimal
    amount: Decimal
    event_id: str
    n_acts: int


@dataclass
class HandCloseResult:
    booked: dict[str, HandClose]
    parked: dict[str, str]          # slug → the reason it could not be priced
    lines: list[str]


def replay_run_fills(run_id: str, slug: str, believed_qty: Decimal,
                     *, manifest_path: str | None = None,
                     fills_paths: list[str] | None = None) -> FillReplay:
    """This run's OWN `avg_entry` for `slug` (+ its last taped fill's ts), or None with the
    reason it is not established.

    ⛔ THE DURABLE RECORD HAS NO PER-BOOK BASIS, BUT THE FILLS TAPE DOES. Recording every carve
    `basis_source="reset"` would defeat the carve-out: `poly_settle_book --auto` prices only a
    `"run"` row, so a settled book's payout would sit unbooked forever waiting on a hand price.
    The basis is REPLAYED here through the same path `poly_night`'s derive phase ships as
    `--carry` — `basis_chain` for the window, `chain_rows` to select, `poly_teardown_row
    .price_realized` (the SHIPPED `fill_accounting`) to walk it — never a second replay of our
    own, and never the venue's cost basis (that field died with the position row).

    ⛔ PARITY WITH THE DERIVE PHASE'S GUARD SET [MPR round 2, BLOCKING 1]. `poly_night` refuses a
    DERIVED label on each of these, and this path stamps a number `poly_settle_book --auto`
    auto-books into `realized_pnl` — i.e. into the loss cap — so a weaker gate here is a wrong
    cap. Each returns "reset" and its own reason:
      · no manifest row for the run — `load_carries` returns `{}` for "no row" and for "no
        carries" alike, so an absent row would read as "started flat" and derive from nothing;
      · a `carries` statement for this slug whose `source` is NOT `"recorded"` — the run
        INHERITED the position on an operator's DECLARATION, which this process never verified;
        ⛔ but a `source == "recorded"` entry is NOT a refusal [2026-09-03,
        ]: its qty and basis were derived by `maker_state.recorded_carries`
        from the lane's durable record and replayed under this same guard set, so the replay
        ANCHORS ON IT and walks this run's fills on top through the same `fill_accounting` —
        reductions keep the basis, a through-zero flip resets it, additions re-average. Refusing
        it too capped a recorded carry at ONE cycle: cycle N+1's manifest tapes the carry, so
        cycle N+2 could not replay a basis and the evening still stopped on a second consecutive
        unfilled flatten. Every other guard below is unchanged, and the FLAT-ANCHOR guard is
        replaced by the recorded carry itself (see `derived`);
      · an ABSENT or UNREADABLE tape sibling — `load_all_fills` swallows `OSError` per file and
        its own comment says readability is refused UPSTREAM; this caller is that upstream, and
        a silently truncated window whose dropped fills net to zero passes the quantity guard
        with a wrong `avg_entry` (the measured `<run-id>` class);
      · a row whose `ts` cannot be PLACED IN TIME — the same silent-drop shape, per row;
      · `DerivedCarry.derived` false — the shipped predicate (flat anchor AND no broken
        checkpoint), CALLED not restated: a flat anchor alone was the B1 hazard;
      · `end_inv != believed_qty`, or a basis outside (0,1) — the quantity guard is what says
        the replay saw every fill, and `poly_night` refuses the same out-of-range basis.

    ⛔ AND THE WINDOW IS BOUNDED ABOVE, WHICH `chain_rows` IS NOT. `basis_chain` walks back from
    the NEWEST manifest row, not from this run's, and `chain_rows` selects `ts >= since_ts` with
    no upper bound — so a LATER run's fills on the same slug would fold into this run's basis.
    Rows are restricted to this run's own `run_id` AND to `[started_ts, next real run's
    started_ts)`. A blank `run_id` (the two oldest frozen tape siblings carry no such column) is
    excluded, which fails CLOSED: fewer fills, a quantity mismatch, a "reset".

    ⛔ `last_fill_ts` IS THE HAND-CLOSE WALK'S LOWER BOUND [2026-09-03] and is reported for the
    SELECTED rows only — the same window, so an activity at or before it is already inside the
    basis and must never be counted a second time as an exit.
    """
    from bot.core import fill_replay
    from bot.core import run_manifest

    man_path = manifest_path or run_manifest.DEFAULT_PATH
    man_rows = fill_replay.real_manifest_rows(man_path)
    idx = next((i for i, r in enumerate(man_rows)
                if str(r.get("run_id") or "") == run_id), None)
    if idx is None:
        return FillReplay(None, "no real-money manifest row for the run", None)
    mine_row = man_rows[idx]
    # ⛔ A `"recorded"` CARRY IS THE ANCHOR; A `"declared"` ONE IS STILL THE REFUSAL
    #. Provenance is the whole difference: a RECORDED entry's
    # basis was replayed from this lane's own fills under this same guard set, while a DECLARED
    # one is an operator string this process never verified (it can be the venue UI's complement
    # collateral) — anchoring on it would relabel an assertion as a derivation and
    # `poly_settle_book --auto` would BOOK it into `realized_pnl`. A row without the key, a
    # malformed entry and an unreadable column all arrive as declared/absent, which refuses.
    entry = fill_replay.load_carries_with_source(man_path, run_id).get(slug)
    if entry is not None and entry[2] != run_manifest.CARRY_SOURCE_RECORDED:
        return FillReplay(None, "carried into the run", None)
    seed = None if entry is None else (entry[0], entry[1])
    # ⛔ THE CHAIN IS BUILT UP TO THIS RUN, NOT TO THE NEWEST ROW. `basis_chain` walks BACKWARDS
    # from the last row it is given, so handing it the whole manifest anchors on whatever ran
    # LAST — `since_ts` then opens after this run ended and the replay sees none of its fills
    # (measured: "no fills replayed" on a run with three).
    chain = fill_replay.basis_chain(slug, man_rows[:idx + 1])
    paths = fills_paths or fill_replay.tape_paths(fill_replay.DEFAULT_FILLS)
    # ⛔ `_tape_readability` is `poly_night`'s, private only because it has had one caller.
    # Copying it here is the duplication the derive phase's own comment warns against.
    readable = fill_replay._tape_readability(paths)
    if not readable.present:
        return FillReplay(None, "the fills tape is ABSENT — no basis can be replayed", None)
    if readable.unreadable:
        return FillReplay(None, (f"{len(readable.unreadable)} tape file(s) EXIST but cannot be "
                                 f"read ({', '.join(readable.unreadable)}) — their fills would "
                                 f"be dropped silently"), None)
    # ⛔ AN UNPARSEABLE `started_ts` IS A NAMED REFUSAL, NOT A ValueError. It bounds the replay
    # window, so a row we cannot place in time cannot bound anything — and this function's
    # contract is (None, reason), never an exception into the carve loop.
    try:
        started = float(mine_row.get("started_ts") or 0)
        stamps = [float(r.get("started_ts") or 0) for r in man_rows]
    except (TypeError, ValueError):
        return FillReplay(None, "manifest started_ts unparseable — the replay window cannot be "
                                "bounded", None)
    later = [t for t in stamps if t > started]
    until = min(later) if later else float("inf")
    scoped = [r for r in fill_replay.load_all_fills(paths)
              if str(r.get("run_id") or "") == run_id]
    bad_ts: list[str] = []
    rows = [r for r in fill_replay.chain_rows(slug, chain, scoped, unparseable=bad_ts)
            if float(dec_or_none(r.get("ts")) or 0) < until]
    if bad_ts:
        return FillReplay(None, f"{len(bad_ts)} fill row(s) cannot be placed in time "
                                f"({bad_ts[0]})", None)
    stamps_seen = [float(dec_or_none(r.get("ts")) or 0) for r in rows]
    last_ts = max(stamps_seen) if stamps_seen else None
    book = fill_replay.price_realized(
        rows, {slug: seed} if seed is not None else {}).get(slug)
    # ⛔ A SEEDED BOOK WITH NO FILLS IS THE CARRY ITSELF, NOT AN EMPTY REPLAY: a cycle that
    # inherited a residual and never traded it must still hand the SAME basis to the next cycle,
    # or the chain breaks on the first quiet run.
    if book is None or (book.fills == 0 and seed is None):
        return FillReplay(None, "no fills replayed", last_ts)
    # ⛔ THE SHIPPED PREDICATE, CALLED, not restated — `DerivedCarry.derived`. ⛔ Its checkpoint
    # half is EMPTY BY CONSTRUCTION here and the single-run window bound REPLACES it: the chain
    # is built to THIS run's row and a carried book is anchored on its own row's declaration, so
    # the window holds no intermediate declaration to check. Checkpoints exist to police a window
    # that spans runs; this one cannot. `derived` therefore reduces to the flat anchor.
    carry = fill_replay.DerivedCarry(
        slug=slug, venue_qty=believed_qty, replayed_qty=book.end_inv, basis=book.avg_entry,
        n_fills=book.fills, contracts=book.contracts, round_trips=book.trips, chain=chain,
        checkpoints=(), durable_qty=believed_qty, status="ok", detail="")
    # ⛔ THE FLAT-ANCHOR GUARD APPLIES TO AN UNSEEDED REPLAY ONLY. With a manifest carry the
    # anchor is DECLARED (qty + basis), which is what the guard was asking for — it exists to
    # refuse a replay that ASSUMES flat over an inherited position, not one that reads the
    # inheritance off the row.
    if seed is None and not carry.derived:
        return FillReplay(None, f"chain anchor {chain.anchor!r}, not a flat start", last_ts)
    if book.end_inv != believed_qty:
        return FillReplay(None, f"replay end_inv {book.end_inv} ≠ belief {believed_qty}", last_ts)
    # ⛔ NO `abs()` — `fill_accounting`'s `avg_entry` is a YES-SPACE PRICE and stays positive on
    # a short (the sign lives on `end_inv`); verified −5 @ 0.44 on three ask fills. The bound is
    # `poly_night`'s, unchanged.
    if not (_ZERO < book.avg_entry < _ONE):
        return FillReplay(None, f"replayed basis {book.avg_entry} outside (0,1)", last_ts)
    anchor = "" if seed is None else f" on the carried anchor {seed[0]}@{seed[1]}"
    return FillReplay(book.avg_entry, f"replayed from {book.fills} fill(s){anchor}", last_ts)


def resolve_basis_run(slug: str, believed_qty: Decimal, *,
                     fills_paths: list[str] | None = None) -> tuple[str | None, str]:
    """MIGRATION ONLY: which run's fills left `slug` at `believed_qty`?

    A pre-fix durable record carries an off-slate position with no `carried_basis_run`, and the
    record itself says nothing about which run traded it. The fills tape does: walk every fill
    for the slug in time order (`bid` adds, `ask` subtracts — `fill_accounting`'s own
    vocabulary). The walk must END at `believed_qty`, and its LAST fill is then the one that
    left the position we still hold; its `run_id` is the basis run.

    ⛔ THE TERMINAL CHECK IS THE WHOLE GUARD. Without it, an
    intermediate visit to `believed_qty` answers: run A shorts 10 @0.40, run B shorts 5 @0.80
    (position −15), the operator closes 5 on the UI (an out-of-process close never books) — both
    the venue and the record read −10, the walk ends at −15, and the intermediate hit hands back
    RUN A, whose single-run replay ends at −10 and seeds 0.40 for a position whose true basis
    mixes 0.40 and 0.80. Ending elsewhere means the tape does not explain the position we hold.

    ⛔ AND IT IS WHAT MAKES TRUNCATION FAIL CLOSED. The walk is `load_all_fills` — account-global
    across lanes, and `include_gz=False`, so gzipped archives are invisible to it; a window that
    misses fills simply does not end at `believed_qty` and refuses.

    ⛔ NEVER A GUESS. A walk that ends elsewhere, or a final fill with a blank `run_id` (the two
    oldest frozen siblings have no such column) → (None, reason), and the caller keeps the
    existing NOT-carryable refusal rather than replaying a wrong run.
    """
    from bot.core import fill_replay

    paths = fills_paths or fill_replay.tape_paths(fill_replay.DEFAULT_FILLS)
    readable = fill_replay._tape_readability(paths)
    if not readable.present:
        return None, "the fills tape is ABSENT — no basis run can be resolved"
    if readable.unreadable:
        return None, (f"{len(readable.unreadable)} tape file(s) EXIST but cannot be read "
                      f"({', '.join(readable.unreadable)}) — the walk would be truncated")
    pos = _ZERO
    last: dict | None = None
    for row in fill_replay.load_all_fills(paths):
        if str(row.get("slug") or "") != slug:
            continue
        qty = dec_or_none(row.get("filled_qty"))
        side = str(row.get("side") or "")
        if qty is None or qty <= _ZERO or side not in ("bid", "ask"):
            continue
        pos += qty if side == "bid" else -qty
        last = row
    if last is None or pos != believed_qty:
        return None, (f"the fills tape does not explain {slug} at {believed_qty} — the walk "
                      f"ends at {pos}")
    run = str(last.get("run_id") or "")
    if not run:
        return None, f"the last fill on {slug} carries no run_id"
    return run, (f"resolved from the fills tape: the walk ends at {believed_qty} and its last "
                 f"fill ran under {run}")


def carried_basis_run(state: Any, slug: str, believed_qty: Decimal, *,
                      fills_paths: list[str] | None = None) -> tuple[str, str]:
    """(the run id whose fills to replay for `slug`, why) — THE ONE EXPRESSION.

    ⛔ `state.run_id` IS THE WRONG ANSWER FOR A CARRIED BOOK and it is what a probe evening
    replayed: the position filled in an earlier run, later runs replaced `run_id`, and replaying the
    current run's fills for a book it never traded returned "no fills replayed" → the carry was
    refused → the recovery gate found an UNDECLARED position and the evening stopped early. A
    recorded position stays carryable across any number of later runs [OPERATOR-DECIDED].

    Off-slate and on-slate carries take this same call: whether this run's pick happens to seat
    the book changes nothing about which run holds its fills.

    Order: the record's own `carried_basis_run` → the tape resolution for a pre-fix record →
    `state.run_id` for a book that was never carried (this run traded it itself).
    """
    run = (getattr(state, "carried_basis_run", None) or {}).get(slug)
    if run:
        return str(run), f"basis run {run} from the durable record"
    if slug not in (getattr(state, "carried_off_slate", None) or {}):
        return state.run_id, "this run's own fills"
    got, why = resolve_basis_run(slug, believed_qty, fills_paths=fills_paths)
    if got is None:
        # ⛔ Fail into the EXISTING refusal, never into `state.run_id`: a pre-fix carried book
        # whose basis run cannot be resolved has no replayable basis at all.
        return "", why
    return got, why


def replayed_basis(run_id: str, slug: str, believed_qty: Decimal,
                   *, manifest_path: str | None = None,
                   fills_paths: list[str] | None = None) -> tuple[Decimal | None, str]:
    """`replay_run_fills` without the timestamp — the shape `poly_cert_reconcile`'s settled
    carve-out has always called, kept public and kept named."""
    out = replay_run_fills(run_id, slug, believed_qty, manifest_path=manifest_path,
                           fills_paths=fills_paths)
    return out.basis, out.reason


def signed_yes(txn: Any) -> tuple[Decimal, Decimal] | None:
    """(signed YES-space quantity, YES-space price) for one parsed activity, or None.

    ⛔ THE SIGN IS `side`, WHICH IS THE YES-SPACE SIDE, and `outcomeSide` only has to be a value
    the venue actually ships. The recorded population the private design notes carries exactly three combinations — `BUY`/`YES` (BUY_LONG,
    +), `SELL`/`YES` (SELL_LONG, −) and `SELL`/`NO` (BUY_SHORT, −, "a resting ask is a BUY_SHORT
    of NO at 1−p, not a sale") — and the fourth, `BUY`/`NO`, is the SELL_SHORT that REDUCES a
    short, i.e. a YES-space buy, +. `side` alone reproduces all four; `intent` reproduces none
    of them [§3.2's ⚠️].

    ⛔ `trade.price` is YES-space on EVERY leg (`PolyMaker._merge_activities` §0), so a NO leg
    needs no complement. None on anything unrecognised — the caller PARKS on None.
    """
    side_s, _, outcome_s = str(getattr(txn, "side", "") or "").partition("/")
    if side_s == "ORDER_SIDE_BUY":
        sign = _ONE
    elif side_s == "ORDER_SIDE_SELL":
        sign = -_ONE
    else:
        return None
    if outcome_s not in ("OUTCOME_SIDE_YES", "OUTCOME_SIDE_NO"):
        return None
    qty, price = getattr(txn, "qty", None), getattr(txn, "price", None)
    if qty is None or price is None or qty <= _ZERO or not (_ZERO <= price <= _ONE):
        return None
    return sign * qty, price


async def walk_slug_trades(client: Any, slug: str, *, since_ts: float,
                           pace_s: float = PACE_S, max_pages: int = MAX_PAGES,
                           sleep: Any = None) -> tuple[list[Any] | None, str]:
    """This slug's own trade activities STRICTLY AFTER `since_ts`, newest-first, or None + why.

    ⛔ None IS NOT AN EMPTY LEDGER. A raised read, a client with no activities reader, a page
    whose shape the shipped parser cannot resolve to our leg — all return None, and None parks.
    `[]` means the walk genuinely reached back past the run's last fill and found no close.

    Paging stops at the first page whose OLDEST trade predates `since_ts - SINCE_MARGIN_S`, at
    an exhausted cursor, or at `max_pages`; a cursor still live at `max_pages` is None (we did
    not reach the bound, so absence proves nothing).
    """
    from bot.core import poly_activities

    reader = getattr(client, "get_activities_page", None)
    if reader is None:
        return None, "the client has no activities reader"
    naptime = sleep if sleep is not None else asyncio.sleep
    floor = since_ts - SINCE_MARGIN_S
    cursor, mine, pages = "", [], 0
    while pages < max_pages:
        try:
            acts, cursor = await reader(cursor)
        except Exception as exc:                       # unreadable is None, per §
            return None, f"activities read FAILED ({exc!r})"
        pages += 1
        txns = poly_activities.poly_transactions(acts)
        for t in txns:
            if str(getattr(t, "market", "") or "") != slug:
                continue
            # ⛔ AN UNPLACEABLE `ts` PARKS, IT DOES NOT READ AS EPOCH ZERO [MPR nit]. `or 0.0`
            # would drop this slug's own close below `since_ts` (excluded from the exit set)
            # AND read as older than the floor (stopping the walk) — a silent under-count in
            # the booking direction, on the one row that matters.
            if getattr(t, "ts", None) is None:
                return None, (f"activity {getattr(t, 'txn_id', '?')} cannot be placed in time "
                              f"(no createTime)")
            if float(t.ts) <= since_ts:
                continue
            if signed_yes(t) is None:
                return None, (f"activity {getattr(t, 'txn_id', '?')} has no readable own leg "
                              f"(side {getattr(t, 'side', '')!r})")
            mine.append(t)
        # Paging coverage is judged on the rows that CAN be placed; an unplaceable one on
        # another slug is not this walk's business and must not end it either.
        oldest = min((float(t.ts) for t in txns if getattr(t, "ts", None) is not None),
                     default=None)
        if oldest is not None and oldest < floor:
            return mine, f"walked {pages} page(s) back to the run's last fill"
        if not cursor:
            return mine, f"walked {pages} page(s) to the end of the ledger"
        await naptime(pace_s)
    return None, (f"the activities cursor was still live after {max_pages} pages — the walk "
                  f"never reached the run's last fill")


def price_close(slug: str, believed_qty: Decimal, basis: Decimal,
                txns: Sequence[Any]) -> tuple[HandClose | None, str]:
    """Price a netted-to-zero close: `qty·(exit_avg − basis)`, or None + why not.

    ⛔ THE NET-TO-ZERO CHECK IS THE WHOLE GATE. `belief + Σ signed_qty == 0` (exactly, in
    Decimal) is what says these activities ARE this belief's close and not a partial, a
    re-open, or another run's traffic on the same slug. A partial nets to something else and
    PARKS — booking it would fold a realized number against a basis for a position that is
    still open.

    ⛔ SIGN-SYMMETRIC, and the SAME sign as `poly_teardown_row.price_realized`: profit is
    positive on both legs. A long +5 @ 0.58 closed at 0.96 books +1.90; a short −5 @ 0.44
    covered at 0.20 books +1.20 (`qty·(exit_avg − basis)` = −5·(0.20 − 0.44)). It is the same
    form `poly_settlement_join.settlement_pnl` uses for a settlement, with the venue's
    `long_value` replaced by the price the operator actually got.
    """
    exits: list[tuple[Decimal, Decimal]] = []
    for t in txns:
        parsed = signed_yes(t)
        if parsed is None:
            return None, f"activity {getattr(t, 'txn_id', '?')} is unparseable"
        signed, price = parsed
        exits.append((-signed, price))          # the quantity REMOVED from the belief
    closed = sum((q for q, _ in exits), _ZERO)
    if closed != believed_qty:
        return None, (f"{len(exits)} activity(ies) close {closed} against a belief of "
                      f"{believed_qty} — not a complete close")
    exit_avg = sum((q * p for q, p in exits), _ZERO) / believed_qty
    amount = believed_qty * (exit_avg - basis)
    oldest = min(txns, key=lambda t: float(getattr(t, "ts", 0.0) or 0.0))
    raw_id = str(getattr(oldest, "txn_id", "") or "").split(":")[-1]
    if not raw_id:
        return None, "the oldest activity carries no venue id — nothing to deduplicate on"
    return HandClose(slug=slug, qty=believed_qty, basis=basis, exit_avg=exit_avg,
                     amount=amount, event_id=f"poly:handclose:{slug}:{raw_id}",
                     n_acts=len(exits)), "netted to zero"


def booked_line(hc: HandClose, run_id: str = "") -> str:
    tag = f"[{run_id}] " if run_id else ""
    return (f"{tag}hand close booked: {hc.slug} {hc.qty} @ basis {hc.basis} → exit "
            f"{hc.exit_avg} = {'+' if hc.amount >= _ZERO else '-'}${abs(hc.amount)} "
            f"({hc.n_acts} activities)")


def parked_line(slug: str, qty: Decimal, reason: str, run_id: str = "") -> str:
    tag = f"[{run_id}] " if run_id else ""
    return (f"{tag}hand close parked: {slug} {qty} — {reason}; price later with "
            f"the settle step of the operator runbook")


def fold_hand_close(store: Any, hc: HandClose, *, run_id: str = "",
                    since_ts: float | None = None) -> bool:
    """Park the belief, fold the money, then clear the park — booked?

    ⛔ THE ORDER IS PARK → BOOK → CLEAR, exactly as `settle_pending_books` books before it
    clears. The park is what takes the quantity out of `inventory`; a crash between the park and
    the fold leaves a `closed_unpriced` row that `poly_settle_book --auto` prices on the next
    pass, while folding first could zero nothing and leave the belief live against booked money.
    """
    store.record_closed_unpriced(slug=hc.slug, qty=hc.qty, avg_entry_yes=hc.basis,
                                 basis_source="run", reason="hand close, pricing",
                                 run_id=run_id, since_ts=since_ts)
    _total, did = store.add_hand_close(
        event_id=hc.event_id, slug=hc.slug, qty=hc.qty, basis=hc.basis,
        exit_avg=hc.exit_avg, amount=hc.amount,
        note=f"venue_close run_id={run_id} n_acts={hc.n_acts}")
    store.clear_closed_unpriced(hc.slug)
    return did


async def book_hand_closes(client: Any, run_id: str, *,
                           believed: MutableMapping[str, Decimal],
                           venue_held: Container[str],
                           store: Any,
                           manifest_path: str | None = None,
                           fills_paths: list[str] | None = None,
                           pace_s: float = PACE_S,
                           fold: bool = True,
                           nets_only: bool = False,
                           walk_cache: MutableMapping[str, tuple[list | None, str]] | None = None,
                           sleep: Any = None) -> HandCloseResult:
    """Retire every row-less belief: book it or park it.

    ⛔ RUN THIS BEFORE `maker.carve_out_settled_books`, IN `nets_only` MODE. The venue serves no
    position row for a hand-closed
    book OR for a resolved one, so status alone cannot tell them apart when the market resolves
    minutes after the operator's UI close — and a settled carve that goes first books a
    settlement payout against a position the venue did not hold at resolution (measured:
    −<n> against a real ≈ −<n> close). The activities ledger decides: a net to EXACTLY
    zero after this run's last taped fill is a hand close whatever the status later became.
    `nets_only=True` retires only those and leaves everything it cannot price UNTOUCHED in
    `believed` — no park, no zero — for the settled carve to claim. The caller then runs this
    again with `nets_only=False` to park the remainder.

    ⛔ `walk_cache` IS HOW THE TWO PASSES SHARE ONE WALK. A belief that neither the `nets_only` pass nor the settled carve claimed reaches the
    second pass, and `since_ts` is derived from the same tape for the same slug and run — so
    the second walk pages `get_activities_page` again for a byte-identical answer. A mapping
    passed to both calls caches `(txns, why)` per slug. It is NOT a cross-run cache: the caller
    builds it fresh per reconcile, so a stale ledger can never be read as a current one.

    ⛔ NO STORE, NO CARVE, and the same rule as the settled carve-out: a retired belief has to
    land somewhere durable or it is deleted. Every failure direction PARKS (durable, unpriced),
    and only an exact net-to-zero against a run-replayed basis books money.

    ⛔ `fold=False` IS THE RECOVERY GATE'S MODE, for the
    same reason `settle_pending_books(write=False)` is: the gate runs at EVERY supervisor child
    launch, and the evening loss gate captures `baseline_realized` ONCE and diffs the same
    `realized_pnl` scalar — a fold at cycle 2 would shrink the evening's delta at cycle 3 (a
    gain hiding price loss under the ceiling) or stop a healthy night early (a carried loss).
    The gate PARKS every hand close, priced or not; `_close_evening`, `poly_settle_book --auto`
    and the operator-run `poly_cert_reconcile` do the folding. Parking still retires the belief,
    so the launch is still not blocked — which is the whole decision.
    """
    out = HandCloseResult(booked={}, parked={}, lines=[])
    if store is None:
        log.warning("hand close inert: no durable store — every row-less belief stays "
                    "uncorroborated.")
        return out
    for slug, qty in sorted(believed.items()):
        if qty == _ZERO or slug in venue_held:
            continue
        replay = replay_run_fills(run_id, slug, qty, manifest_path=manifest_path,
                                  fills_paths=fills_paths)
        if replay.basis is None or replay.last_fill_ts is None:
            reason = replay.reason if replay.basis is None else "no taped fill to walk from"
            if nets_only:
                continue
            store.record_closed_unpriced(slug=slug, qty=qty, avg_entry_yes=None,
                                         basis_source="reset", reason=reason, run_id=run_id,
                                         since_ts=replay.last_fill_ts)
            believed[slug] = _ZERO
            out.parked[slug] = reason
            out.lines.append(parked_line(slug, qty, reason, run_id))
            continue
        if walk_cache is not None and slug in walk_cache:
            txns, why = walk_cache[slug]
        else:
            txns, why = await walk_slug_trades(client, slug, since_ts=replay.last_fill_ts,
                                               pace_s=pace_s, sleep=sleep)
            if walk_cache is not None:
                walk_cache[slug] = (txns, why)
        hc: HandClose | None = None
        if txns is not None:
            hc, why = price_close(slug, qty, replay.basis, txns)
        if hc is None or not fold:
            reason = why if hc is None else "priced, NOT booked here (the gate never folds)"
            # ⛔ `nets_only` DEFERS EVERY UNPRICED BELIEF TO THE SETTLED CARVE, it does not park
            # it: parking would zero the belief and the carve (which skips a zero) could never
            # claim a book that genuinely resolved. Only an EXACT net-to-zero retires here.
            if hc is None and nets_only:
                continue
            store.record_closed_unpriced(slug=slug, qty=qty, avg_entry_yes=replay.basis,
                                         basis_source="run", reason=reason, run_id=run_id,
                                         since_ts=replay.last_fill_ts)
            believed[slug] = _ZERO
            out.parked[slug] = reason
            out.lines.append(parked_line(slug, qty, reason, run_id))
            continue
        did = fold_hand_close(store, hc, run_id=run_id, since_ts=replay.last_fill_ts)
        believed[slug] = _ZERO
        out.booked[slug] = hc
        out.lines.append(booked_line(hc, run_id) + ("" if did else " — already booked"))
    return out
