"""
bot/core/maker_state.py
───────────────────────
Crash-durable maker state, and the recovery decision a SEPARATE process makes from it.

DESIGNED FOR SIGKILL, WHICH IS THE ONLY DEATH THAT MATTERS HERE — it runs no `finally`, no
`atexit`, no signal handler, so a design where cleanup writes the record writes nothing precisely
when it counts. Two consequences shape everything below:

  1. **State is durable BEFORE it is needed, not after.** An order intent is fsync'd to disk before
     the order is sent, because the uncoverable window is "sent, then died before the response
     arrived".
  2. **Recovery is a different process.** `assess_recovery` is a pure function over
     (durable state, venue orders, venue positions) and `scripts/maker_recover.py` runs it.

The `clean_exit` flag is written PESSIMISTICALLY: `begin_run` sets it False, `end_run` sets it
True. A crash therefore leaves the correct value on disk *by doing nothing*, which is the only
behaviour SIGKILL reliably permits. Never invert this.

TWO RULES GOVERN THE RECOVERY DECISION
──────────────────────────────────────
**Cannot-verify is not flat** — a venue read that fails is `None` here and always refuses; an empty
list means confirmed-flat and is allowed to start. Never collapse those two (`bot/runner/
reconcile.py`'s Kalshi half returned `[]` on an unparsed response for a month and so reported
"confirmed flat" every poll while we held positions).

**Cancelling is safe, flattening is not.** A cancel only removes exposure, so even an
unattributable resting order is listed for automatic cancellation — including under a `refuse`. A
flatten MOVES MONEY against a basis we may not own, so a venue position with no matching record
halts for an operator instead.

⚠️ SCOPE OF THE LOSS CAP HERE. This makes the cap SURVIVE PROCESS DEATH; it does **not** make the
cap's INPUT correct (the private design notes addendum 1.2 is still open). Whatever
number the process computes is what persists.
"""
from __future__ import annotations

import json
import logging
import os
import time
import dataclasses
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Mapping, Sequence

from bot.core import durable
from bot.core.money import CENTICENT, ZERO as _ZERO

log = logging.getLogger(__name__)

DEFAULT_PATH = durable.repo_path("logs", "maker_state.json")

# Below venue wire resolution is not a position. Both venues report holdings to 2dp and quote to
# the centicent, so anything under that is residue. Same compare-to-zero class as the crossed-book
# float dust — and here a false positive would REFUSE TO START, forever, over nothing.
_DUST = CENTICENT

# The venue's whole-contract unit. A holding strictly under it ("dust") is residue the maker quotes
# below by design, and it bounds exposure below <n> per position (a contract settles in [0, 1]).
# Dust warns LOUDLY and proceeds; a whole contract or more still refuses.
#
# ⛔ ONE PREDICATE, TWO OWNERS — these MUST stay equal to `scripts.poly_prelaunch.DUST_QTY_LIMIT`
# and `.DUST_CEILING_USD`. They are duplicated rather than imported because `bot/` may not import
# `scripts/`; `tests/test_maker_state.py` pins the equality.
_ONE = Decimal("1")
_DUST_AGGREGATE_LIMIT_USD = Decimal("5")

# ⛔ OWN OFF-SLATE RESIDUAL: CARRIED, NOT REFUSED [operator decision 2026-09-03]. When the durable
# belief EQUALS the venue's answer — sign and quantity, Decimal-exact — nothing is in dispute, so
# the start proceeds and the book is CARRIED: unmanaged by the new run (never quoted, never
# flattened) and left in the lane's durable record so the next teardown's carve-out /
# `settled_pending` pricing books it. A DISAGREEMENT still refuses `own_position_off_slate`.
#
# Σ|qty| over off-slate dust + carried is the worst-case dollars (a contract settles in [0, 1]).
# The ceiling stops residue the size of the whole account, never a night's leftovers.
OWN_OFFSLATE_CARRY_LIMIT_USD = Decimal("1")  # placeholder — production value withheld


def is_carryable_off_slate(recorded: Decimal, venue_qty: Decimal) -> bool:
    """May an own off-slate position be carried past the recovery gate?

    ONE predicate, exact equality — the durable belief IS the venue's answer. Anything else
    (opposite sign, a different quantity, a belief we do not hold) is a divergence, and a
    divergence is precisely what `own_position_off_slate` refuses over."""
    return _is_real(recorded) and _is_real(venue_qty) and recorded == venue_qty


@dataclasses.dataclass(frozen=True)
class OffSlateResidual:
    """How our OWN off-slate positions split: dust, carried, or refused — and what they cost."""
    dust: tuple[tuple[str, Decimal], ...]
    carried: tuple[tuple[str, Decimal], ...]
    refused: tuple[str, ...]
    worst_case: Decimal          # Σ|qty| over dust AND carried; a contract settles in [0, 1]
    over_ceiling: bool


def classify_own_off_slate(recorded: Mapping[str, Any],
                           venue_off_slate: Mapping[str, Any]) -> OffSlateResidual:
    """⛔ THE ONE OWNER of the off-slate residual rule — the recovery gate AND the pre-launch
    screen call this, never their own copy. A screen that summed only the carried half would bless
    a launch the gate then refuses.

    `recorded` is THIS lane's durable inventory; `venue_off_slate` is the venue's own answer for
    books off this run's slate. A book we have no record of is not ours to classify here (it is
    an orphan or a sibling's) and is skipped."""
    dust: list[tuple[str, Decimal]] = []
    carried: list[tuple[str, Decimal]] = []
    refused: list[str] = []
    for t, raw_q in sorted(venue_off_slate.items()):
        q = _dec(raw_q)
        rec = _dec(recorded.get(t, _ZERO))
        if not _is_real(rec) or not _is_real(q):
            continue
        if q.copy_abs() < _ONE and rec.copy_abs() < _ONE:
            dust.append((str(t), q))
        elif is_carryable_off_slate(rec, q):
            carried.append((str(t), q))
        else:
            refused.append(str(t))
    worst = sum((q.copy_abs() for _t, q in dust + carried), _ZERO)
    return OffSlateResidual(tuple(dust), tuple(carried), tuple(refused), worst,
                            worst > OWN_OFFSLATE_CARRY_LIMIT_USD)


class PriorRunUnresolved(Exception):
    """A previous run died holding orders and nothing has resolved it yet.

    Raised by `begin_run` so a fresh run cannot erase the crash record it needs. The maker catches
    this and refuses to start, pointing at the VENUE-APPROPRIATE remedy (`scripts/maker_recover.py`
    for a Kalshi record; `scripts/poly_cancel` + `scripts/poly_close` for a Poly one — the tool is
    Kalshi-only), branched on `poly_names`.
    """


class LegacyStateUnclassifiable(Exception):
    """The shared-file record's run_id matches no venue prefix — refuse, never guess.

    Guessing routes a crash record to the wrong venue's tooling, which is the exact
    certified-false-clean failure the venue split exists to end."""


class LegacyStateLive(Exception):
    """The shared-file record's process appears ALIVE — migrating underneath it would split one
    run's writes across two files. If you are certain no maker is running, the pid has been
    recycled: resolve the crash record first (cancel its strays on the venue), then retry."""


# ── the per-venue state-path split (TODO ) ────────────────────────
# Venue is decided by WHICH FILE a store opens, never by the record's CONTENTS — the name-shape
# heuristics are defense-in-depth, not the primary discriminator.

VENUES = ("kalshi", "poly")
# The Poly maker stamps `polymm-…` (old `polymm-<ts>-<hex>` and the 2026-07-31 mode-stamped
# `polymm-<mode>-<ts>-<hex>`); the Kalshi maker's documented mode stamp is `real-`/`dry-`.
_RUN_ID_PREFIXES = {"polymm-": "poly", "real-": "kalshi", "dry-": "kalshi"}


def venue_path(base_path: str, venue: str) -> str:
    """`logs/maker_state.json` + "poly" → `logs/maker_state_poly.json`."""
    stem, ext = os.path.splitext(base_path)
    return f"{stem}_{venue}{ext}"


def venue_of_run_id(run_id: str) -> str | None:
    for prefix, venue in _RUN_ID_PREFIXES.items():
        if str(run_id).startswith(prefix):
            return venue
    return None


def _pid_alive(pid: int) -> bool:
    """Could `pid` be a LIVE MAKER OF OURS? Makers run as this user, so a pid we lack
    permission to signal is some OTHER user's process — a recycled pid, not our maker
    (mapping PermissionError→True froze adoption forever on any record whose dead pid was
    recycled to a root/systemd process, pid 1 included)."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return True


def _maybe_adopt_legacy(venue: str, base: str, vpath: str) -> None:
    """One-time migration of the shared legacy file into this venue's file.

    Runs only while the venue file does NOT exist. A corrupt legacy raises StateCorrupt (the
    SIGKILL-mid-write artefact must never read as "nothing to migrate"); an unclassifiable
    run_id refuses; a record whose pid is alive with no clean exit refuses (a live maker is
    still writing it). The other venue's record is left in place for its own store to adopt.
    On adoption the ledger rides along wholesale and the legacy is RENAMED (evidence
    preserved), never deleted."""
    if os.path.exists(vpath):
        # Crash-window repair: a crash between the venue-file write and the legacy rename leaves
        # BOTH readable with the same record — opswatch then double-reports it and maker_recover's
        # legacy fallback re-sees a migrated record. Finish the rename.
        # NO pid check here ON PURPOSE: equal run_ids in both files can only be produced by an
        # adopting process that died between its write and its rename, and a pid check would
        # reintroduce the recycled-pid deadlock. The equality guard does the real work.
        if os.path.exists(base):
            try:
                v_raw = durable.read_json_strict(vpath)
                b_raw = durable.read_json_strict(base)
            except durable.StateCorrupt:
                return                      # surfaced by the corrupt-handling paths, not here
            # v2-aware [review nit 1]: once the venue file is v2 its run_id lives in lanes.main,
            # so a top-level read is None and the repair never fires — the legacy file then
            # lingers, double-reported by opswatch forever.
            v_run = (v_raw.get("run_id") if isinstance(v_raw, dict) else None)
            if v_run is None and _is_v2(v_raw):
                v_run = (v_raw["lanes"].get(DEFAULT_LANE) or {}).get("run_id")
            if (isinstance(b_raw, dict) and v_run and v_run == b_raw.get("run_id")):
                dest = f"{base}.migrated-{int(time.time())}"
                os.replace(base, dest)
                log.info(f"maker_state: finished interrupted adoption — {base} → {dest}")
        return
    if not os.path.exists(base):
        return
    raw = durable.read_json_strict(base)
    if not isinstance(raw, dict) or not raw:
        return
    owner = venue_of_run_id(str(raw.get("run_id") or ""))
    if owner is None:
        raise LegacyStateUnclassifiable(
            f"{base}: run_id {raw.get('run_id')!r} matches no venue prefix "
            f"({sorted(_RUN_ID_PREFIXES)}) — refusing to migrate it by guesswork. Inspect the "
            f"record and move it to {venue_path(base, 'poly')} or "
            f"{venue_path(base, 'kalshi')} by hand.")
    if owner != venue:
        return
    pid = int(raw.get("pid") or 0)
    if _pid_alive(pid) and not bool(raw.get("clean_exit")):
        raise LegacyStateLive(
            f"{base}: record {raw.get('run_id')!r} (pid {pid}) appears to belong to a maker "
            f"that is STILL RUNNING — refusing to migrate underneath it. If you are CERTAIN "
            f"no maker is running, the pid was recycled: resolve the crash on the venue "
            f"(cancel its strays), then move the file to {vpath} by hand — retrying alone "
            f"hits this same check until that pid dies.")
    durable.write_json_durable(vpath, raw)
    dest = f"{base}.migrated-{int(time.time())}"
    os.replace(base, dest)
    log.info(f"maker_state: adopted legacy {base} (run {raw.get('run_id')!r}) into {vpath}; "
             f"legacy preserved as {dest}")


# ── lanes: one SHARED per-venue ledger, multiple concurrent runs [2026-08-12, operator-chosen
#    over per-process files so the loss ratchet stays ACCOUNT-WIDE] ─────────────────────────────
#
# Schema v2: `{"schema": 2, "lanes": {"main": <record>, "park": <record>, …}}`. A legacy flat
# record reads as `lanes = {"main": <record>}` and is rewritten as v2 on the first mutation. Each
# store binds to ONE lane: `_write()` re-reads the file under an exclusive flock and replaces ONLY
# its own lane's subtree, so two processes in DIFFERENT lanes never clobber each other. Two
# processes in the SAME lane still last-writer-win: that is the slate-collision case, which
# `assess_recovery` refuses at startup.

DEFAULT_LANE = "main"


class StateLocked(RuntimeError):
    """The shared ledger's lock could not be acquired within the bound. Raised rather than
    blocking forever: an un-timed LOCK_EX inside the asyncio loop would freeze the whole maker
    (no quotes, no cancels, no kill-switch check) behind a HUNG sibling — a crash releases the
    flock instantly, but SIGSTOP/stalled-fsync does not. A
    raise lands in the shim's finally → teardown, the safe direction."""


class LaneChanged(RuntimeError):
    """A lane re-check inside the write lock found a different run or a live holder — the
    caller's evidence belongs to another run, and nothing was written."""


class _FileLock:
    """Exclusive advisory lock on `<path>.lock` with a BOUNDED wait, held across a
    read-modify-write. Advisory is enough: every writer comes through MakerStateStore (the
    store_for_venue rule), and the durable write itself stays atomic (os.replace) so an
    out-of-band reader is never torn. Normal hold time is one read + one durable write (~ms);
    the 5 s bound is ~1000× that."""

    _TIMEOUT_S = 5.0
    _POLL_S = 0.05

    def __init__(self, path: str) -> None:
        self._lock_path = f"{path}.lock"
        self._fd: int | None = None

    def __enter__(self) -> "_FileLock":
        import fcntl
        os.makedirs(os.path.dirname(self._lock_path) or ".", exist_ok=True)
        try:
            self._fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        except PermissionError as exc:
            # A root-owned .lock (a once-sudo'd tool) would otherwise kill every mutation.
            raise StateLocked(f"{self._lock_path}: cannot open lock file ({exc}) — fix its "
                              f"ownership (chown) before the maker can write state") from exc
        deadline = time.monotonic() + self._TIMEOUT_S
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except (BlockingIOError, InterruptedError):
                if time.monotonic() >= deadline:
                    os.close(self._fd)
                    self._fd = None
                    raise StateLocked(
                        f"{self._lock_path}: held for >{self._TIMEOUT_S}s — a sibling maker "
                        f"is hung mid-write (a crashed one releases instantly). Refusing to "
                        f"block the event loop; this mutation fails loudly instead.")
                time.sleep(self._POLL_S)

    def __exit__(self, *exc: Any) -> None:
        import fcntl
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


def _is_v2(doc: Any) -> bool:
    return isinstance(doc, dict) and doc.get("schema") == 2 and isinstance(doc.get("lanes"), dict)


def _doc_lanes(doc: Any) -> dict[str, dict]:
    """The lanes of a state file's parsed content, migrating a legacy flat record in MEMORY.
    Empty/absent → {}. A non-empty legacy record becomes `{"main": record}`.

    ⛔ FAIL-CLOSED on a malformed v2: a document that CLAIMS the schema (`"schema"` present) but
    whose lanes are not all dicts is the SIGKILL-mid-write artefact, and reading it as "fewer
    lanes" silently deletes a lane — its carried loss, its slate claim, its crash record. A filter
    (`if isinstance(v, dict)`) is the fail-open shape this module's own header forbids."""
    if isinstance(doc, dict) and "schema" in doc:
        if not _is_v2(doc):
            raise durable.StateCorrupt(
                f"state document claims schema {doc.get('schema')!r} but lanes is "
                f"{type(doc.get('lanes')).__name__} — corrupt, refusing to reinterpret")
        bad = [k for k, v in doc["lanes"].items() if not isinstance(v, dict)]
        if bad:
            raise durable.StateCorrupt(
                f"lane record(s) {bad} are not objects — a torn write; refusing to read the "
                f"file as if those lanes did not exist")
        return {str(k): v for k, v in doc["lanes"].items()}
    if isinstance(doc, dict) and doc:
        return {DEFAULT_LANE: doc}
    return {}


def load_all_lanes(path: str | None = None) -> dict[str, "MakerState"]:
    """Every lane's snapshot from one venue file — the account-wide view the ratchet and the
    collision check read. Missing file → {}. Corrupt raises (never read corruption as empty)."""
    p = path or DEFAULT_PATH
    if not os.path.exists(p):
        return {}
    doc = durable.read_json_strict(p)
    return {lane: _to_state(rec) for lane, rec in _doc_lanes(doc).items() if rec}


#: How long a RESOLVED unresolved-exposure row is kept before `begin_run` drops it. Two weeks is
#: long enough to read last night's clear at the next launch and short enough that the record does
#: not grow forever. ⛔ Applies to RESOLVED rows only — an open row is kept at any age.
RESOLVED_ROW_TTL_S = 14 * 24 * 3600.0


def _carry_unresolved(raw: Any, now: float | None = None) -> dict[str, dict]:
    """The unresolved-exposure map `begin_run` carries into the new run: every OPEN row, plus
    resolved rows younger than `RESOLVED_ROW_TTL_S`."""
    ts = time.time() if now is None else now
    out: dict[str, dict] = {}
    for slug, row in (raw or {}).items():
        if not isinstance(row, Mapping):
            continue
        resolved = row.get("resolved")
        if resolved and ts - float((resolved or {}).get("ts") or 0.0) > RESOLVED_ROW_TTL_S:
            continue
        out[str(slug)] = dict(row)
    return out


def open_operator_unresolved(path: str | None = None) -> list[dict]:
    """Every OPEN unresolved-exposure row across every lane of a venue file, oldest first.

    The one read shared by opswatch's page and `poly_night`'s launch print, so the two cannot
    disagree about what is outstanding. Each row already names its own lane. Missing file → [];
    a corrupt one RAISES, because "no unresolved exposure" is the answer that must never come
    from a file we could not read.
    """
    rows = [r for s in load_all_lanes(path).values() for r in s.operator_unresolved_open.values()]
    return sorted(rows, key=lambda r: (float(r.get("first_ts") or 0.0), str(r.get("slug"))))


def operator_unresolved_cleared_by(row: Mapping[str, Any], venue_qty: Decimal | None) -> bool:
    """Does this FRESH venue quantity resolve `row`? ⛔ `None` is cannot-verify, never a clear.

    Flat always clears. A quantity INSIDE the reach the row recorded clears too — the exposure
    the row exists for is "beyond what a lawful launch can work down", and a position back
    inside that reach is carried by the ordinary carry path. A row with NO recorded reach (the
    flatten-refused condition, where reach was never the problem) clears on FLAT only.
    """
    if venue_qty is None:
        return False
    if venue_qty.copy_abs() < _DUST:
        return True
    reach = row.get("reach")
    return reach is not None and venue_qty.copy_abs() <= _dec(reach)


def recorded_close_spec(row: Mapping[str, Any]) -> str:
    """`side:qty` of the row's OPEN-time close intent, for printing as history only — never the
    runnable command. `sell` reduces a long, `buy` a short
    (`scripts.poly_live_mm.poly_close_command`'s rule)."""
    q = _dec(row.get("qty"))
    return f"{'sell' if q > _ZERO else 'buy'}:{q.copy_abs()}"


def account_carried(path: str | None = None) -> Decimal:
    """Σ realized_pnl across ALL lanes of a venue file (SIGNED — for reporting)."""
    return sum((s.realized_pnl for s in load_all_lanes(path).values()), _ZERO)


def account_loss(path: str | None = None, *, overall: bool = False) -> Decimal:
    """Σ of per-lane LOSS-TO-DATE (each floored at zero) — the number the lifetime ratchet
    prices budgets from. ⛔ NOT the signed sum: netting a profitable lane against a losing one
    would let one lane's profit BUY another lane's loss budget. One account, one ratchet, losses
    only.

    `overall=True` sums `overall_loss_to_date` instead — price + realized rebate, the axis the
    cap enforces [AMENDMENT 30]. The default stays PRICE-ONLY: existing readers report the price
    channel and must keep reporting it."""
    return sum(((s.overall_loss_to_date if overall else s.loss_to_date)
                for s in load_all_lanes(path).values()), _ZERO)


def venue_account_loss(venue: str, base_path: str | None = None, *,
                       overall: bool = False) -> Decimal:
    """Account-wide loss for a venue, with the LEGACY fallback: if the venue file does not
    exist yet but the legacy shared file classifies to this venue, ITS loss counts — otherwise
    a park lane launched before the first adoption prices its budget off a $0 ledger while a
    real carried loss sits in the legacy file."""
    base = base_path or DEFAULT_PATH
    vpath = venue_path(base, venue)
    if os.path.exists(vpath):
        return account_loss(vpath, overall=overall)
    st = load_state_for_venue(venue, base_path=base)
    if st is None:
        return _ZERO
    return st.overall_loss_to_date if overall else st.loss_to_date


CLOSED_LANES_SUFFIX = ".closed_lanes.jsonl"


def closed_lanes_path(state_path: str | None = None) -> str:
    """The ARCHIVE beside a venue state file: `maker_state_poly.json` →
    `maker_state_poly.closed_lanes.jsonl`. Never rotated, never trimmed — it is the only place a
    closed lane's lifetime realized P&L survives after `close_lane` zeroes the live record."""
    p = state_path or DEFAULT_PATH
    base = p[:-len(".json")] if p.endswith(".json") else p
    return base + CLOSED_LANES_SUFFIX


def read_closed_lanes(state_path: str | None = None, *,
                      lane: str | None = None) -> list[dict]:
    """Archived lane closes, oldest first (absent file → []). `lane` filters.

    ⛔ A malformed line RAISES `durable.StateCorrupt` rather than being skipped: every line is money
    the live record no longer carries.
    ⛔ DEDUPED ON `(lane, run_id, closed_ts)`: a close whose archive append succeeded and whose state
    write then failed re-runs, and counting it twice would DOUBLE the lane's lifetime loss. Safe
    only because `closed_ts` is the instant of the append."""
    return read_closed_lanes_at(closed_lanes_path(state_path), lane=lane)


def read_closed_lanes_at(path: str, *, lane: str | None = None) -> list[dict]:
    """`read_closed_lanes` addressed by the ARCHIVE path itself (the append path's own reader)."""
    if not os.path.exists(path):
        return []
    seen: set[tuple[str, str, str]] = set()
    out: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for n, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except ValueError as exc:
                raise durable.StateCorrupt(f"{path}:{n}: unreadable archive line ({exc})")
            if not isinstance(rec, dict):
                raise durable.StateCorrupt(
                    f"{path}:{n}: archive line is {type(rec).__name__}, not an object")
            key = (str(rec.get("lane") or ""), str(rec.get("run_id") or ""),
                   str(rec.get("closed_ts") or ""))
            if key in seen:
                continue
            seen.add(key)
            if lane is None or key[0] == lane:
                out.append(rec)
    return out


class LaneCloseRaced(RuntimeError):
    """A sibling lane went LIVE between the close's venue read and its write. The write is
    abandoned; nothing on the live record moves."""


def _archived_close_key(record: Mapping[str, Any]) -> tuple[str, str, str]:
    """The IDENTITY of a close for append-idempotence: lane, run, and the exact amount retired.
    ⛔ `closed_ts` is deliberately NOT in it — the re-run after a failed state write stamps a new
    instant, and keying on it would append a second line for the same money.
    ⛔ IT RESTS ON `nothing_to_close`: the key is only unique-per-close because a lane whose
    `realized_pnl` is already 0 REFUSES, so no lane can ever be closed twice at two different
    amounts under one run_id. Drop that refusal and this key stops identifying a close."""
    return (str(record.get("lane") or ""), str(record.get("run_id") or ""),
            str(record.get("realized_pnl") or ""))


def _append_closed_lane(path: str, record: Mapping[str, Any]) -> bool:
    """Append ONE archive line durably (read + rewrite through `write_bytes_durable`, which is
    temp+fsync+replace). A file whose last line lacks its newline gains one first, so a torn
    prior write never swallows the new record into it.

    Returns False and appends NOTHING when a line with the same `_archived_close_key` is already
    there: the close's OWN retry (archive succeeded, state write failed) must re-run to
    completion without recording the money twice."""
    if any(_archived_close_key(prior_rec) == _archived_close_key(record)
           for prior_rec in read_closed_lanes_at(path)):
        return False
    prior = b""
    if os.path.exists(path):
        with open(path, "rb") as fh:
            prior = fh.read()
        if prior and not prior.endswith(b"\n"):
            prior += b"\n"
    line = (json.dumps(dict(record), sort_keys=True) + "\n").encode("utf-8")
    durable.write_bytes_durable(path, prior + line)
    return True


def live_sibling_lanes(path: str | None, own_lane: str) -> dict[str, "MakerState"]:
    """Sibling lanes that are LIVE — an unclean exit, maybe-live orders, or recorded inventory.
    A cleanly-exited, flat, order-free lane record is history, not a sibling: scoping (and the solo
    main run's account-global gate) keys on THIS, never on the mere existence of a lane key.

    ⛔ A CARRIED-ONLY INVENTORY IS NOT A LIVE LANE — such a lane has no process, no orders and
    nothing it quotes, and by the carry rule nobody is managing that position anyway."""
    out: dict[str, MakerState] = {}
    for lane, s in load_all_lanes(path).items():
        if lane == own_lane:
            continue
        managed = {t: q for t, q in s.inventory.items() if t not in s.carried_off_slate}
        if (not s.clean_exit) or s.maybe_live_orders or any(
                _is_real(q) for q in managed.values()):
            out[lane] = s
    return out


def store_for_venue(venue: str, base_path: str | None = None,
                    lane: str = DEFAULT_LANE) -> "MakerStateStore":
    """This venue's own store, adopting the legacy shared file once if it belongs here.

    ⛔ EVERY maker/tool must come through here (or `load_state_for_venue`) rather than opening
    `MAKER_STATE_FILE` directly — the shared path is how a Kalshi start reset a Poly record's
    orders and how the Poly preflight certified a Kalshi crash from a Poly venue read."""
    if venue not in VENUES:
        raise ValueError(f"unknown venue {venue!r}")
    base = base_path or DEFAULT_PATH
    vpath = venue_path(base, venue)
    _maybe_adopt_legacy(venue, base, vpath)
    return MakerStateStore(path=vpath, lane=lane)


def load_state_for_venue(venue: str, base_path: str | None = None,
                         lane: str = DEFAULT_LANE) -> "MakerState | None":
    """Read-only counterpart for tools (opswatch, recover, preflights): prefers the venue
    file; falls back to READING the legacy in place when it classifies to this venue —
    without migrating, so a read-only tool never mutates operator state."""
    if venue not in VENUES:
        raise ValueError(f"unknown venue {venue!r}")
    base = base_path or DEFAULT_PATH
    vpath = venue_path(base, venue)
    if os.path.exists(vpath):
        return load_state(vpath, lane=lane)
    if os.path.exists(base):
        state = load_state(base, lane=lane)
        if state is not None and venue_of_run_id(state.run_id) == venue:
            return state
    return None


def _dec(value: Any, default: Decimal = _ZERO) -> Decimal:
    """Decimal from a wire/JSON value, never from a float literal path. Unparseable → default."""
    if isinstance(value, Decimal):
        return value
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


def _is_real(qty: Decimal) -> bool:
    return qty.copy_abs() >= _DUST


@dataclass(frozen=True)
class MakerState:
    """One snapshot of the durable record."""
    run_id: str
    pid: int
    mode: str                            # "real" | "dry"
    started_ts: float
    updated_ts: float
    clean_exit: bool
    tickers: tuple[str, ...]
    orders: dict[str, dict]              # intent_id → {order_id, ticker, side, price, count, ts}
    inventory: dict[str, Decimal]        # ticker → signed qty (zeros are not stored)
    realized_pnl: Decimal                # signed; PRICE CHANNEL ONLY (+ booked settlements)
    loss_cap: Decimal                    # 0 = disabled, and it binds on OVERALL (see below)
    exit_status: str | None = None
    # WHY the crash record was left open, written by the maker's own teardown branches
    #. Two open records look identical from a venue read and
    # must NOT be closed by the same evidence:
    #   "sweep_unverified"      — the sweep/verify could not confirm nothing is RESTING. A venue
    #                             read showing zero orders IS the answer, so it is closable by
    #                             `scripts/poly_cert_reconcile.py`.
    #   "unreconciled_realized" — the C8 branch: the venue IS swept clean, but a queued/
    #                             unresolved recovery entry may carry a fill the realized ledger
    #                             never saw. The venue read says nothing about that, and closing
    #                             on it writes clean_exit over an UNDERSTATED loss ratchet.
    # `""` = a record written before this marker existed (or a clean one): treated as unknown,
    # which refuses the same way `unreconciled_realized` does unless the operator arms it.
    record_open_reason: str = ""
    # REALIZED REBATE, its own field beside `realized_pnl` and never folded into it
    # [AMENDMENT 30, 2026-09-08]. Every reader of `realized_pnl` means the PRICE channel by it and
    # keeps meaning that; the cap reads `overall_realized`. The value is the venue's own per-fill
    # commission read-back (`commission_order_total`, last-per-order, sign-flipped), NEVER the
    # modelled `predicted_rebate_on_cum` — a modelled credit would buy real loss budget.
    # ⛔ `None` = a PRE-FIX state file with no field at all: arithmetic treats it as 0 (a missing
    # credit never inflates headroom) and every operator line labels it UNKNOWN-pre-fix.
    rebate_realized: Decimal | None = None
    # slug → wall-clock time the POLY maker's adverse-round-trip rail latched that book
    # reduce-only. Durable for the same reason the loss ratchet is: the rail exists because a
    # book's FLOW turned against us, a crash-restart does not change the flow, and an
    # in-memory-only latch silently re-arms the book on the next OOM. It survives restarts on
    # purpose.
    # ⛔ THREE CLEAR PATHS, not one: (1) the dated operator ack (hot_settings
    # `clear_adverse_latch`) — the only MID-RUN clear on the main lane; (2) expiry at a
    # venue-confirmed CLEAN run end; (3) on the PROBE lane, the timed ban in
    # `adverse_ban_until` below reaching its deadline. A crash-restart is not any of them.
    adverse_latched: dict[str, float] = dataclasses.field(default_factory=dict)
    # slug → wall-clock DEADLINE at which a probe-lane TIMED ban re-admits the book
    # the private design notes. Durable for exactly the reason
    # `adverse_latched` is: a crash-restart mid-ban must not clear the ban, and must not silently
    # convert a timed ban into a run-lifetime one either. A latched slug with NO entry here is a
    # run-LIFETIME latch, which is why absence is meaningful and must not be defaulted.
    adverse_ban_until: dict[str, float] = dataclasses.field(default_factory=dict)
    # Books whose MARKET SETTLED while we held them. The venue's positions endpoint DROPS a
    # settled book, so belief ≠ 0 against no venue row is not a divergence there — it is a
    # position that has already paid out and cannot be flattened, priced or read back.
    # ⛔ THIS IS NOT PRICE-REALIZED P&L. The rows carry quantity and OUR OWN mean entry price
    # (`avg_entry_yes`, never the venue's cost basis); the settlement channel books through
    # `add_settlement` with the VENUE's own event_id. Rows: {slug, qty, avg_entry_yes, status,
    # run_id, ts, iso}.
    settled_pending: tuple[dict, ...] = ()
    # Books the venue does NOT hold and whose market has NOT resolved — an out-of-process HAND
    # CLOSE, retired here when its money could not be priced on the spot (`bot/core/venue_close.py`).
    # ⛔ PARKING IS NOT BOOKING: no money moves here, and a `basis_source != "run"` row is never
    # auto-priced. `poly_settle_book --auto` re-walks the ledger and folds it through
    # `add_hand_close`. Rows: {slug, qty, avg_entry_yes, basis_source, reason, since_ts, run_id,
    # ts, iso}.
    closed_unpriced: tuple[dict, ...] = ()
    # Own OFF-SLATE books this run CARRIED past the recovery gate (record == venue, under the
    # ceiling): recorded, unquoted, unflattened. They stay in `inventory` — that is what makes
    # the next gate re-carry them and the settled carve-out book them — but they are NOT
    # evidence that this lane is LIVE (`live_sibling_lanes`), and they are not judged by the
    # runtime venue-inventory guard (`PolyMaker._venue_breach`).
    carried_off_slate: dict[str, Decimal] = dataclasses.field(default_factory=dict)
    # slug → THE RUN ID WHOSE FILLS HOLD THAT SLUG'S BASIS.
    # ⛔ NOT `run_id`. A carried position keeps its basis in the run that last traded it, and a
    # record survives any number of later runs — replaying THIS run's fills for a book this run
    # never touched returns "no fills replayed", the carry is refused, and the recovery gate
    # stops the evening over a position that was always carryable (a probe evening).
    # Written by `begin_run` from the PRIOR record (its own entry if it had one, else its
    # run_id) and carried forward untouched; pre-fix records have no entry and are resolved
    # from the fills tape by `venue_close.carried_basis_run`.
    carried_basis_run: dict[str, str] = dataclasses.field(default_factory=dict)
    # slug → the UNRESOLVED-EXPOSURE record. A position this
    # process could NOT hand back: a recorded carry beyond the launch's lawful reach (the start
    # refused), or a teardown residual the flatten could not clear. ⛔ AN ORDINARY WITHIN-REACH
    # CARRY IS NOT IN HERE — that is `carried_off_slate`, which the next launch picks up by
    # design. Rows are the durable half of the operator handoff: opswatch pages while any row
    # has `resolved is null`, and the ONLY two clears are a venue read showing the book flat (or
    # inside the recorded reach) and an explicit operator acceptance of the carry. Schema in
    # the private design notes § maker_state_poly.json.
    operator_unresolved_exposure: dict[str, dict] = dataclasses.field(default_factory=dict)
    # slug → the last TWO-SIDED touch seen on that book, `(bid, ask, ts)` [continuous maker P1b].
    # ⛔ A PRICE MEMORY, NOT A POSITION: it exists so a pinned non-sports seat whose book went
    # EMPTY can be re-seeded at the prices the market last showed, across a restart. Written only
    # when both sides existed, so an entry is never half a book; the maker's own
    # `EMPTY_BOOK_TOUCH_MAX_S` decides whether an entry is still usable, never this loader.
    last_touch: dict[str, tuple[Decimal, Decimal, float]] = dataclasses.field(
        default_factory=dict)
    # The post-exit recovery trail, written by
    # `scripts/poly_cert_reconcile.py --recover-orders` over an open `sweep_unverified` record.
    # `recovery_attempts` counts venue-ANSWERED passes only (a box-ban refusal never left the box);
    # `recovery_last` is the last pass's trail; `recovery_escalated_ts` is stamped once, when the
    # bound paged, and stops every later pass. All three are reset by `begin_run`.
    recovery_attempts: int = 0
    recovery_last: dict[str, Any] = dataclasses.field(default_factory=dict)
    recovery_escalated_ts: float | None = None

    @property
    def operator_unresolved_open(self) -> dict[str, dict]:
        """The rows still demanding a human — `resolved is null`. What opswatch pages on."""
        return {s: r for s, r in self.operator_unresolved_exposure.items() if not r.get("resolved")}

    @property
    def maybe_live_orders(self) -> int:
        """Orders that MIGHT still be resting: confirmed ones AND unresolved intents.

        An intent with no `order_id` is the ambiguous case — we sent it and never saw the answer.
        Counting it as live is the safe direction: it costs a redundant cancel, whereas counting
        it as dead costs an unattended resting order on the money path.
        """
        return len(self.orders)

    @property
    def order_ids(self) -> tuple[str, ...]:
        return tuple(o["order_id"] for o in self.orders.values() if o.get("order_id"))

    @property
    def loss_to_date(self) -> Decimal:
        """PRICE-CHANNEL realized LOSS, floored at zero. A profitable run does not earn extra
        budget. ⛔ NOT what the cap compares against since Amendment 30 — `overall_loss_to_date`
        is. Left price-only on purpose: every existing reader (metrics_tracker, opswatch,
        poly_lane_close, the account ratchet) means the price channel by it."""
        return max(_ZERO, -self.realized_pnl)

    @property
    def rebate_booked(self) -> Decimal:
        """Realized rebate as ARITHMETIC: an absent (pre-fix) field is 0, never an error and
        never a guess. Ask `rebate_realized is None` for the UNKNOWN-pre-fix label."""
        return self.rebate_realized if self.rebate_realized is not None else _ZERO

    @property
    def overall_realized(self) -> Decimal:
        """OVERALL = price-realized (incl. booked settlements) + REALIZED rebate. Rewards are
        EXCLUDED — they are venue-paid D+1 against the whole account and are not attributable to
        a lane or a run. Marks never enter."""
        return self.realized_pnl + self.rebate_booked

    @property
    def overall_loss_to_date(self) -> Decimal:
        """OVERALL realized loss, floored at zero — the number the cap compares."""
        return max(_ZERO, -self.overall_realized)

    @property
    def cap_breached(self) -> bool:
        """⛔ THE CAP BINDS ON OVERALL [operator-decided]. A trip once fired on price alone with the run's
realized rebates unseen. Rewards excluded."""
        return self.loss_cap > _ZERO and self.overall_loss_to_date > self.loss_cap


def cap_line(state: "MakerState") -> str:
    """The ONE labelled rendering of the cap's three numbers, for every operator line that names
    it (the launch refusal, the sibling-lane refusal, the engine's halt, the banner). Both
    channels are always shown: a run read as "losing" on price alone had its rebates unseen, and
    the enforced number is the OVERALL one."""
    reb = ("UNKNOWN-pre-fix" if state.rebate_realized is None
           else f"{state.rebate_realized:+}")
    return (f"price-channel {state.realized_pnl:+} · rebate {reb} · "
            f"OVERALL {state.overall_realized:+} vs cap ${state.loss_cap}")

def _basis_runs(prior: MakerState | None,
                carried: Mapping[str, Any] | None) -> dict[str, str]:
    """slug → the run whose fills hold its basis, for the carried set `begin_run` is writing.

    The PRIOR record's own entry wins — the book has been carried before, and the run that
    traded it is further back than the run that merely carried it. Otherwise the prior run is
    the one that last held it non-flat. No prior record (or a prior with no run id) leaves the
    slug unmapped, and `venue_close.carried_basis_run` resolves it off the tape.

    ⚠️ A carried slug that round-trips OFF-TAPE back to the same quantity keeps its old entry.
    Bounded: the maker never quotes a carried book, and any change of quantity fails
    `replay_run_fills`'s `end_inv != believed_qty` guard, which refuses rather than seeds.
    """
    if prior is None:
        return {}
    out: dict[str, str] = {}
    for slug, qty in (carried or {}).items():
        if not _is_real(_dec(qty)):
            continue
        run = prior.carried_basis_run.get(str(slug)) or prior.run_id
        if run:
            out[str(slug)] = str(run)
    return out


def poly_names(state: MakerState) -> tuple[str, ...]:
    """The record's POLY-shaped names, sorted (truthiness = 'this record touches Poly').

    Poly slugs carry lowercase; Kalshi tickers never do. ONE copy on purpose: this heuristic gates
    three venue-branched operator remedies (begin_run's refusal, opswatch's problem strings,
    maker_recover's certification guard)."""
    names = set(state.tickers) | set(state.inventory) | {
        str(o.get("ticker") or "") for o in state.orders.values()}
    return tuple(sorted(n for n in names if n and any(ch.islower() for ch in str(n))))



def unresolved_prior_message(prior: MakerState, path: str) -> str:
    """The refusal text `begin_run` raises over an UNRESOLVED prior record — one owner.

    Extracted so a pre-spawn echo can print the operator the SAME paragraph instead of a second
    spelling of it (`scripts/poly_night.probe_ledger_findings`, the probe lane's phase-0b read
    of this very record). The predicate stays in `begin_run`; this is only the words.
    """
    # The remedy is VENUE-SPECIFIC and this module serves both makers. Naming
    # `scripts.maker_recover` to a POLY operator sends them to a Kalshi-only tool; the
    # heuristic lives ONCE in `poly_names`.
    is_poly = bool(poly_names(prior))
    # ⚠️ This message has NO refused-count context (the record does not say WHY the
    # sweep failed), so the terminal-but-listed ghost may be offered only as a
    # POSSIBILITY here, never asserted the way the teardown's gated copy can.
    remedy = (
        "confirm what is actually resting with a read-only listing (see the operator "
        "runbook; the same credentials that placed the "
        "orders — never the UI alone, which a wrong-account login renders blind); "
        "cancel strays by hand and close positions by hand (see the operator runbook). If it "
        "shows nothing resting and positions are flat, the prior teardown may have "
        "hit the terminal-but-listed case (its listing carried an already-dead "
        "order) — but an unattributable or unreadable listing leaves the SAME "
        "record, so treat ghost as a possibility to confirm, not a diagnosis. "
        "Then CLOSE THE RECORD from that venue read — the reconcile tool (see the operator runbook) for run "
        f"{prior.run_id} reads the lane's open "
        "orders and, on zero resting, records the exit while KEEPING the loss ledger "
        "and the carries' basis. "
        "Do NOT use the Kalshi recovery tool — it is "
        "Kalshi-only and reads the wrong venue." if is_poly else
        "the Kalshi recovery tool (see the operator runbook) "
        "with its arming flag")
    return (
        f"the previous maker run {prior.run_id!r} never recorded an exit and left "
        f"{prior.maybe_live_orders} order(s) that may still be RESTING on the venue. "
        f"Starting now would erase that record. Resolve it first:\n"
        f"  {remedy}\n"
        f"(LAST RESORT ONLY, once the venue is confirmed flat and the reconcile "
        f"tool has been tried: delete {path} — ⚠️ that also discards the durable "
        f"loss ledger AND every carry's basis record, which turns the carries into "
        f"unknown positions the next launch must flatten; note realized/loss_to_date "
        f"first)")

def _to_state(raw: Mapping[str, Any]) -> MakerState:
    inv = {str(k): _dec(v) for k, v in (raw.get("inventory") or {}).items()}
    return MakerState(
        run_id=str(raw.get("run_id") or ""),
        pid=int(raw.get("pid") or 0),
        mode=str(raw.get("mode") or ""),
        started_ts=float(raw.get("started_ts") or 0.0),
        updated_ts=float(raw.get("updated_ts") or 0.0),
        clean_exit=bool(raw.get("clean_exit")),
        record_open_reason=str(raw.get("record_open_reason") or ""),
        tickers=tuple(str(t) for t in (raw.get("tickers") or ())),
        orders={str(k): dict(v) for k, v in (raw.get("orders") or {}).items()
                if isinstance(v, dict)},
        inventory={k: v for k, v in inv.items() if _is_real(v)},
        realized_pnl=_dec(raw.get("realized_pnl")),
        loss_cap=_dec(raw.get("loss_cap")),
        # ⛔ ABSENCE IS MEANINGFUL: a pre-Amendment-30 file has no field, which is UNKNOWN, not 0.
        rebate_realized=(_dec(raw.get("rebate_realized"))
                         if raw.get("rebate_realized") is not None else None),
        exit_status=raw.get("exit_status"),
        adverse_latched=_latch_map(raw.get("adverse_latched")),
        adverse_ban_until=_ban_map(raw.get("adverse_ban_until")),
        last_touch=_touch_map(raw.get("last_touch")),
        settled_pending=tuple(dict(r) for r in (raw.get("settled_pending") or ())
                              if isinstance(r, Mapping)),
        closed_unpriced=tuple(dict(r) for r in (raw.get("closed_unpriced") or ())
                              if isinstance(r, Mapping)),
        carried_off_slate={str(k): _dec(v)
                           for k, v in (raw.get("carried_off_slate") or {}).items()
                           if _is_real(_dec(v))},
        carried_basis_run={str(k): str(v)
                           for k, v in (raw.get("carried_basis_run") or {}).items() if v},
        # Absent on every pre- record — {} is "nothing unresolved", which is
        # what a file written before the section existed actually means.
        operator_unresolved_exposure={str(k): dict(v)
                             for k, v in (raw.get("operator_unresolved_exposure") or {}).items()
                             if isinstance(v, Mapping)},
        recovery_attempts=int(raw.get("recovery_attempts") or 0),
        recovery_last=dict(raw.get("recovery_last") or {}),
        recovery_escalated_ts=(float(raw["recovery_escalated_ts"])
                               if raw.get("recovery_escalated_ts") is not None else None),
    )


def _latch_map(raw: Any) -> dict[str, float]:
    """Parse the durable latch map, DROPPING nothing that is readable.

    An unreadable stamp is kept, NOT dropped: the slug is what makes the book reduce-only, and
    losing an entry because its timestamp was mangled would re-arm the book.

    ⛔ It is re-stamped to NOW, not to 0.0: zero is older than every timestamp, so a spent ack still
    sitting in the level-triggered hot-settings file would clear a mangled latch on the next
    unrelated edit. `now` fails in the direction "the latch stays".
    """
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, float] = {}
    for slug, ts in raw.items():
        try:
            out[str(slug)] = float(ts)
        except (TypeError, ValueError):
            out[str(slug)] = time.time()
    return out


#: How far AHEAD of our clock a recorded touch's stamp may be and still be read. Small on purpose,
#: like `hot_settings`' ack skew: it absorbs host/operator clock skew, never a future date.
_TOUCH_FUTURE_SKEW_S = 60.0


def _touch_map(raw: Any) -> dict[str, tuple[Decimal, Decimal, float]]:
    """Parse the durable last-two-sided-touch map, DROPPING anything unreadable.

    ⛔ THE OPPOSITE FAIL DIRECTION FROM `_latch_map`: that map's entries RESTRICT, so a mangled
    stamp is kept. An entry here is a PRICE a seat would quote at, so a mangled row must vanish —
    a re-stamped or half-parsed touch would put a real order at a made-up price.
    """
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, tuple[Decimal, Decimal, float]] = {}
    for slug, row in raw.items():
        if not isinstance(row, (list, tuple)) or len(row) != 3:
            continue
        try:
            bid, ask, ts = _dec(row[0]), _dec(row[1]), float(row[2])
        except (TypeError, ValueError, ArithmeticError):
            continue
        # ⛔ A PROBABILITY, STRICTLY INSIDE (0, 1), AND A TIMESTAMP THAT IS NOT IN THE FUTURE
        #. These are prices a seat will POST at, so every
        # implausible row is dropped rather than clamped: 0 or 1 is a resolved book, >1 is a
        # cents/dollars unit error, and a future stamp is a clock jump that would keep a stale
        # touch inside the maker's age bound for as long as the skew lasts.
        if not (bid.is_finite() and ask.is_finite()):
            continue
        if not (0 < bid < 1 and 0 < ask < 1) or ts > time.time() + _TOUCH_FUTURE_SKEW_S:
            continue
        out[str(slug)] = (bid, ask, ts)
    return out


def _ban_map(raw: Any) -> dict[str, float]:
    """Parse the durable BAN-DEADLINE map. ⛔ Deliberately NOT `_latch_map`.

    `_latch_map` re-stamps an unreadable value to `now` because for a LATCH that fails toward "the
    latch stays". Here the sign is inverted — a deadline of `now` means the ban is already OVER — so
    an unreadable deadline is DROPPED instead, and the slug then reads as a run-LIFETIME latch. Both
    maps fail in the same direction: the book stays reduce-only.
    """
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, float] = {}
    for slug, ts in raw.items():
        try:
            out[str(slug)] = float(ts)
        except (TypeError, ValueError):
            continue
    return out


def load_state(path: str | None = None, lane: str = DEFAULT_LANE) -> MakerState | None:
    """ONE LANE's durable record, or None if there is none (legacy flat file = the main lane).

    RAISES `durable.StateCorrupt` if the file exists but cannot be parsed. That is not pedantry: a
    truncated state file is the SIGKILL-mid-write artefact, and reading it as "no prior run" is the
    single most dangerous available interpretation — it says "nothing to recover" at exactly the
    moment there is something to recover.
    """
    raw = durable.read_json_strict(path or DEFAULT_PATH)
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise durable.StateCorrupt(f"{path}: state is {type(raw).__name__}, not an object")
    rec = _doc_lanes(raw).get(lane)
    return _to_state(rec) if rec else None


class MakerStateStore:
    """The writer, bound to ONE LANE of the file. Every mutation is a durable write — there is
    no `flush()` to forget. `_raw` is this lane's record only; `_write()` merges it into the
    shared file under a lock, preserving every other lane byte-for-byte."""

    def __init__(self, path: str | None = None, lane: str = DEFAULT_LANE) -> None:
        self.path = path or DEFAULT_PATH
        self.lane = str(lane or DEFAULT_LANE)
        self._raw: dict[str, Any] = {}
        try:
            existing = durable.read_json_strict(self.path)
            rec = _doc_lanes(existing).get(self.lane)
            if isinstance(rec, dict):
                self._raw = rec
        except durable.StateCorrupt as exc:
            # Do NOT clobber it. Preserving the corrupt file is what lets the recovery path see
            # `state_corrupt` and refuse; overwriting it with a fresh run would erase the evidence
            # of the crash that produced it.
            log.error(f"maker_state: existing {self.path} is CORRUPT ({exc}) — refusing to "
                      f"overwrite; run the Kalshi recovery tool (see the operator runbook) before starting.")
            raise

    # ── run lifecycle ────────────────────────────────────────────────────────────────────────

    def begin_run(self, run_id: str, *, mode: str, loss_cap: Decimal,
                  tickers: Iterable[str], inventory: Mapping[str, Any],
                  carried_off_slate: Mapping[str, Any] | None = None,
                  allow_unresolved: bool = False) -> None:
        """Open a run. `clean_exit=False` is written FIRST and cleared only by `end_run`.

        ⛔ `inventory` is REQUIRED and is THE RECOVERY-VERIFIED VIEW [design v2 §B], never a
        wholesale copy of the prior run's map: accepted carries → exactly the declared quantities (a
        flat record over live contracts is the night22 class), clean or unclean-but-flat → {}, the
        Kalshi maker → {} always. Keyword-only, so no caller falls back to the old copy silently.

        ⛔ `carried_off_slate` IS THE ONE EXCEPTION, AND IT IS NOT INVENTORY: own positions the
        record and the venue agree on exactly, off this run's slate. They stay in the DURABLE record
        while staying OUT of the quoting book, so they are recorded under their own key and MERGED
        into the written inventory here — and `set_inventory` re-merges them on every later write.

        Carries `realized_pnl` forward: the cap is a lifetime ratchet across processes. ⛔ REFUSES to
        clobber an UNRESOLVED prior crash (this call resets the order list, which would destroy the
        only record of what is resting); `allow_unresolved=True` is the deliberate override.
        """
        prior = _to_state(self._raw) if self._raw else None
        # ⛔ SAME-LANE LIVE PROCESS: an open record whose pid is STILL ALIVE is a running maker,
        # and two writers on one lane last-writer-win the ledger (losses silently understated).
        # Refused regardless of the order count — a maker between cancel and next place has zero
        # recorded orders and is no less alive. No allow_unresolved override: that flag
        # acknowledges a CRASH, not a live sibling.
        if (prior is not None and not prior.clean_exit
                and prior.pid != os.getpid() and _pid_alive(prior.pid)):
            raise PriorRunUnresolved(
                f"lane {self.lane!r}: record {prior.run_id!r} (pid {prior.pid}) belongs to a "
                f"maker that appears to be STILL RUNNING — two processes must not share a "
                f"lane. Use a different --lane, or stop that process first. (If you are "
                f"certain it is dead, the pid was recycled; wait for it or clear deliberately.)")
        if (prior is not None and not allow_unresolved
                and not prior.clean_exit and prior.maybe_live_orders):
            raise PriorRunUnresolved(unresolved_prior_message(prior, self.path))
        self._raw = {
            "run_id": str(run_id),
            "pid": os.getpid(),
            "mode": str(mode),
            "started_ts": time.time(),
            "clean_exit": False,
            "exit_status": None,
            "tickers": [str(t) for t in tickers],
            "orders": {},
            "carried_off_slate": {str(k): str(_dec(v))
                                  for k, v in (carried_off_slate or {}).items()
                                  if _is_real(_dec(v))},
            # ⛔ THE BASIS RUN, CARRIED FORWARD — never this run's id.
            "carried_basis_run": _basis_runs(prior, carried_off_slate),
            # The verified view, normalized exactly as set_inventory writes (zeros drop), with
            # the carried off-slate belief merged in (the verified view wins any collision).
            "inventory": {**{str(k): str(_dec(v))
                             for k, v in (carried_off_slate or {}).items()
                             if _is_real(_dec(v))},
                          **{str(k): str(_dec(v)) for k, v in inventory.items()
                             if _is_real(_dec(v))}},
            "realized_pnl": str(_dec(self._raw.get("realized_pnl"))),
            # ⛔ CARRIED FORWARD ONLY IF IT EXISTS [AMENDMENT 30]. The cap is a lifetime ratchet
            # on OVERALL, so the realized rebate ratchets with the price channel — but writing
            # "0" onto a pre-fix record would convert UNKNOWN into a claim that the prior runs
            # earned nothing, which is a credit-side guess. Absent stays absent.
            **({"rebate_realized": str(_dec(self._raw.get("rebate_realized")))}
               if self._raw.get("rebate_realized") is not None else {}),
            "loss_cap": str(loss_cap),
            # ⛔ CARRIED FORWARD, like realized_pnl and for the same reason: a book the adverse
            # rail latched is a book whose flow beat us, and a relaunch does not change the flow.
            # The operator's dated `clear_adverse_latch` ack is the ONE clear.
            "adverse_latched": dict(_latch_map(self._raw.get("adverse_latched"))),
            # ⛔ CARRIED WITH the latch map, never separately. Keeping the latch and dropping the
            # deadline promotes a probe-lane TIMED ban into a run-lifetime one; the reverse
            # re-arms the book. They are one record.
            "adverse_ban_until": dict(_ban_map(self._raw.get("adverse_ban_until"))),
            # ⛔ CARRIED FORWARD [continuous maker P1b]: the whole point of persisting the last
            # two-sided touch is that a RESTART finds a book that reopened empty, and `begin_run`
            # replaces `_raw` wholesale — so an entry not carried here is an entry that only ever
            # survives inside one process. Age is judged at the read, not here.
            "last_touch": {s: [str(b), str(a), ts]
                           for s, (b, a, ts) in _touch_map(
                               self._raw.get("last_touch")).items()},
            # ⛔ CARRIED FORWARD ACROSS RUNS, for the reason the whole record exists: an
            # unresolved exposure is a position no process is managing, and a new run starting
            # is not a resolution. It is cleared by a venue read or an operator acceptance,
            # never by a relaunch.
            # ⚠️ RESOLVED rows are pruned here after `RESOLVED_ROW_TTL_S` — history, not state,
            # and the file is read on every launch. An OPEN row is never pruned at any age.
            "operator_unresolved_exposure": _carry_unresolved(
                self._raw.get("operator_unresolved_exposure")),
        }
        self._write()

    def end_run(self, status: str = "clean") -> None:
        """Record a real exit. Only reachable when cleanup runs — so its ABSENCE is the signal."""
        self._raw["clean_exit"] = True
        self._raw["exit_status"] = str(status)
        self._write()

    def set_record_open_reason(self, reason: str) -> None:
        """Stamp WHY this teardown is leaving the record open [review B1].

        ⛔ THE ONLY DISCRIMINATOR THE NEXT TOOL HAS. `unreconciled_realized` and
        `sweep_unverified` are indistinguishable from a venue read (both read flat), and only the
        second is closable by one — see `record_open_reason` on `MakerState`. Called by the
        maker's teardown branches; the reason is history once the record closes and is left in
        place.
        """
        self._raw["record_open_reason"] = str(reason)
        self._write()

    def note_recovery_attempt(self, *, expect_run_id: str, answered: bool, outcome: str,
                              listed: int, cancelled: int, verdicts: Mapping[str, str],
                              ban_until: float | None, escalated: bool = False) -> bool:
        """One `--recover-orders` pass's trail. `answered` is the
        ONLY thing that moves `recovery_attempts` — a pass the box budget refused writes its trail
        and nothing else. `escalated` stamps `recovery_escalated_ts`, once.

        ⛔ GUARDED, like `close_record_from_venue`: the caller's venue round-trip is long enough
        for a hand tool (`--run-id` close, `--accept-carry`, a settlement booking) to write this
        lane meanwhile. The write is abandoned — returns False, nothing written — when the lane
        under the lock is no longer `expect_run_id`'s OPEN record."""
        def _still_open(lanes: dict[str, "MakerState"]) -> None:
            cur = lanes.get(self.lane)
            if cur is None or cur.run_id != expect_run_id or cur.clean_exit:
                raise LaneChanged(
                    f"lane {self.lane!r}: record changed during the venue round-trip (now "
                    f"{None if cur is None else cur.run_id!r}, clean_exit="
                    f"{None if cur is None else cur.clean_exit}) — attempt not written")
        if answered:
            self._raw["recovery_attempts"] = int(self._raw.get("recovery_attempts") or 0) + 1
        self._raw["recovery_last"] = {
            "ts": time.time(), "outcome": str(outcome), "listed": int(listed),
            "cancelled": int(cancelled), "verdicts": {str(k): str(v) for k, v in verdicts.items()},
            "ban_until": ban_until,
        }
        if escalated:
            self._raw["recovery_escalated_ts"] = time.time()
        try:
            self._write(_still_open)
        except LaneChanged as exc:
            log.warning(str(exc))
            return False
        return True

    def close_record_from_venue(self, *, status: str) -> None:
        """Close an uncertified CRASH record after a VENUE READ proved nothing is resting.

        Why: a teardown whose sweep/verify hit venue 503s never reaches
        `end_run`, so the record keeps `clean_exit=False` and its order intents — and `begin_run`
        then refuses every later launch. The only remedy that existed was deleting the state file,
        which also discards the lifetime loss ledger and the carries' basis records. This closes
        the LIFECYCLE ONLY: `orders` → {} plus exactly what `end_run` writes. Inventory, carried
        basis, settled/unpriced rows, realized, rebate, loss cap, adverse counters, the
        unresolved-exposure handoff and the slate are UNTOUCHED — the next launch picks the
        carries up exactly as it would have.

        ⛔ CALLER-PROVEN FLATNESS: this writes on the caller's venue read (a raise or an
        unreadable shape is CANNOT VERIFY, never zero orders). `scripts/poly_cert_reconcile.py`
        is the caller of record.

        Refuses (`PriorRunUnresolved`) on no record, on a record that already recorded a clean
        exit, and on a LIVE pid — the same sibling rule `begin_run` enforces, because a maker
        still writing this lane would have its orders forgotten underneath it.
        """
        prior = _to_state(self._raw) if self._raw else None
        if prior is None:
            raise PriorRunUnresolved(f"lane {self.lane!r}: no record to close")
        if prior.clean_exit:
            raise PriorRunUnresolved(
                f"lane {self.lane!r}: record {prior.run_id!r} already recorded a clean exit "
                f"({prior.exit_status!r}) — nothing to close")
        # `!= os.getpid()` for the same reason `begin_run` carries it: this tool's own pid can
        # be the dead maker's, RECYCLED — and a record we are the pid of is not a live sibling.
        if prior.pid != os.getpid() and _pid_alive(prior.pid):
            raise PriorRunUnresolved(
                f"lane {self.lane!r}: record {prior.run_id!r} (pid {prior.pid}) belongs to a "
                f"maker that appears to be STILL RUNNING — two processes must not share a "
                f"lane. Stop that process first, or wait for its teardown.")
        # ⛔ ONE WRITE, RE-CHECKED INSIDE THE LOCK [review C2]. The preconditions above were read
        # before the caller's venue round-trip; `_write(guard=…)` is the last point at which a
        # sibling that went live meanwhile can still stop us. Same two lines `end_run` writes,
        # because `end_run` takes no guard.
        def _still_closable(lanes: dict[str, "MakerState"]) -> None:
            cur = lanes.get(self.lane)
            if cur is None or cur.run_id != prior.run_id:
                raise PriorRunUnresolved(
                    f"lane {self.lane!r}: the record changed under us (now "
                    f"{None if cur is None else cur.run_id!r}, expected {prior.run_id!r}) — "
                    f"nothing written")
            if cur.clean_exit:
                raise PriorRunUnresolved(
                    f"lane {self.lane!r}: record {cur.run_id!r} recorded a clean exit while we "
                    f"read the venue — nothing written")
            if cur.pid != os.getpid() and _pid_alive(cur.pid):
                raise PriorRunUnresolved(
                    f"lane {self.lane!r}: record {cur.run_id!r} (pid {cur.pid}) belongs to a "
                    f"maker that appears to be STILL RUNNING — two processes must not share a "
                    f"lane. Nothing written.")
        self._raw["orders"] = {}
        self._raw["clean_exit"] = True
        self._raw["exit_status"] = str(status)
        self._write(_still_closable)

    # ── orders ───────────────────────────────────────────────────────────────────────────────

    def record_intent(self, intent_id: str, *, ticker: str, side: str,
                      price: Decimal, count: Any) -> None:
        """⛔ CALL THIS BEFORE SENDING THE ORDER. See the module docstring: the window this covers
        is 'sent, then died before the response', and it cannot be covered afterwards."""
        self._raw.setdefault("orders", {})[str(intent_id)] = {
            "order_id": None,
            "ticker": str(ticker),
            "side": str(side),
            "price": str(price),
            "count": str(count),
            "ts": time.time(),
        }
        self._write()

    def record_placed(self, intent_id: str, order_id: str | None) -> None:
        """Attach the venue's id once it answers. A 2xx with NO order_id leaves it None — still
        counted as maybe-live, which is what makes that case recoverable at all."""
        o = self._raw.setdefault("orders", {}).get(str(intent_id))
        if o is None:
            return
        o["order_id"] = str(order_id) if order_id else None
        self._write()

    def clear_order(self, key: str) -> None:
        """Forget an order, by venue order_id OR by intent id (a create that returned no id still
        has to be forgettable, or the file grows a permanent phantom)."""
        orders = self._raw.setdefault("orders", {})
        if str(key) in orders:
            del orders[str(key)]
        else:
            for iid, o in list(orders.items()):
                if o.get("order_id") == str(key):
                    del orders[iid]
                    break
            else:
                return
        self._write()

    # ── inventory + P&L ──────────────────────────────────────────────────────────────────────

    def set_inventory(self, inv: Mapping[str, Any]) -> None:
        """Replace the recorded inventory. ⚠️ Every live caller passes the maker's FILL-DERIVED
        BELIEF (`self.inventory`), not a venue read. Zeros are dropped — a zero position is not a
        position, and keeping them makes `unknown` comparisons noisy.

        ⛔ THE CARRIED OFF-SLATE BELIEF SURVIVES THIS REPLACE. The maker never quotes a carried
        book, so it is never in the fill-derived belief this receives — and a plain replace would
        DELETE it from the durable record at the first fill, leaving the position real, held, and
        recorded nowhere. The caller's own map still wins any collision."""
        self._raw["inventory"] = {**self._carried_off_slate(),
                                  **{str(k): str(_dec(v)) for k, v in inv.items()
                                     if _is_real(_dec(v))}}
        self._write()

    def set_inventory_slug(self, slug: str, qty: Any, *, expect_run_id: str, reason: str,
                           by: str, evidence: str) -> str | None:
        """Correct ONE book's recorded quantity, returning the quantity replaced (None = absent).

        ⛔ THE ONLY FIELD IT TOUCHES IS `inventory[slug]`. `realized_pnl` and `rebate_realized`
        are RUNNING TOTALS written per fill by `add_realized`/`add_rebate` at fill time, and the
        record holds no per-fill ledger to re-derive them from — so this path never re-books
        realized, rebate or basis, and never touches `carried_off_slate`, `carried_basis_run`,
        `settled_pending`, `settlements`, the order intents or the crash record. Every other slug
        is preserved as written.

        ⛔ NOT `set_inventory`: that one REPLACES the whole map from the maker's fill-derived
        belief, which is exactly the write that lost this quantity. Callers here hold venue
        evidence for ONE book and no belief about the others.

        `reason`/`by`/`evidence` are required and go to the log — the durable record gains no new
        field (the caller's own audit receipt is the journal).

        ⛔ RE-CHECKED INSIDE THE WRITE LOCK: the caller's evidence was gathered over venue
        round-trips, and a lane that changed hands meanwhile (a new run began — `run_id` is no
        longer `expect_run_id` — or an open record's pid is alive) makes that evidence another
        run's. `LaneChanged` is raised and NOTHING is written."""

        def guard(lanes: dict[str, "MakerState"]) -> None:
            cur = lanes.get(self.lane)
            if cur is None:
                return
            if cur.run_id != expect_run_id:
                raise LaneChanged(f"lane {self.lane!r} now records run {cur.run_id!r}, not "
                                  f"{expect_run_id!r}")
            if not cur.clean_exit and cur.pid != os.getpid() and _pid_alive(cur.pid):
                raise LaneChanged(f"lane {self.lane!r} is held by a LIVE maker (run "
                                  f"{cur.run_id!r}, pid {cur.pid})")

        inv = {str(k): str(v) for k, v in (self._raw.get("inventory") or {}).items()}
        before = inv.get(str(slug))
        q = _dec(qty)
        if _is_real(q):
            inv[str(slug)] = str(q)
        else:
            inv.pop(str(slug), None)
        self._raw["inventory"] = inv
        log.warning(f"maker_state: inventory[{slug}] {before} → {q} on lane {self.lane!r} "
                    f"by {by} ({reason}); evidence: {evidence}")
        self._write(guard=guard)
        return before

    def set_tickers(self, tickers: Iterable[str]) -> None:
        """Replace the recorded slate [continuous maker P1 — the hot slate's add/drop].

        ⛔ WHY IT HAS TO BE WRITABLE MID-RUN: `begin_run` wrote the LAUNCH slate, and the recovery
        path reads `tickers` to know whose position to go looking for. A book added at 03:00Z and
        absent from this list is a position no relaunch would ever ask the venue about — the
        night22 class, one axis over. A DROPPED book leaves the list only once it is flat on the
        venue, so the list is never narrower than the exposure it describes."""
        self._raw["tickers"] = [str(t) for t in tickers]
        self._write()

    def set_last_touch(self, touches: Mapping[str, tuple[Decimal, Decimal, float]]) -> None:
        """Replace the recorded last-TWO-SIDED-touch map [continuous maker P1b].

        ⛔ THE CALLER PASSES ONLY TWO-SIDED TOUCHES. A one-sided book has no entry to write, and
        writing half of one would hand the empty-book seed a price for a side the market never
        showed. Merged, never replaced wholesale: the maker only carries entries for the books it
        seeds, and a slug it stopped tracking keeps its last recorded touch rather than losing it
        to a cycle that had nothing to say about it.
        """
        merged = _touch_map(self._raw.get("last_touch"))
        merged.update({str(k): (_dec(v[0]), _dec(v[1]), float(v[2]))
                       for k, v in touches.items()})
        self._raw["last_touch"] = {s: [str(b), str(a), ts]
                                   for s, (b, a, ts) in merged.items()}
        self._write()

    def _drop_basis_run(self, slug: str) -> None:
        """Retire a slug's basis-run entry with the carried entry itself. A stale entry would
        name an ancient run if the same book were carried again later."""
        runs = dict(self._raw.get("carried_basis_run") or {})
        if runs.pop(slug, None) is not None:
            self._raw["carried_basis_run"] = runs

    def _carried_off_slate(self) -> dict[str, str]:
        """The durable carried-off-slate map (slug → qty string), or {}."""
        raw = self._raw.get("carried_off_slate")
        if not isinstance(raw, Mapping):
            return {}
        return {str(k): str(_dec(v)) for k, v in raw.items() if _is_real(_dec(v))}

    def set_adverse_latched(self, latched: Mapping[str, float],
                            ban_until: Mapping[str, float]) -> None:
        """Replace the recorded adverse-latch map (slug → wall-clock latch time) AND its
        ban-deadline companion (slug → wall-clock re-admission time), in ONE durable write.

        Written on every latch and every operator clear, so the file is the authority a restarted
        process reads — written when it CHANGES, never at exit.

        ⛔ `ban_until` is REQUIRED, not optional: the two maps are one record, and updating only the
        latches would leave a stale deadline behind or drop a live one (turning a timed ban into a
        lifetime one). It is EMPTY on every non-probe lane.
        """
        self._raw["adverse_latched"] = {str(k): float(v) for k, v in latched.items()}
        self._raw["adverse_ban_until"] = {str(k): float(v) for k, v in ban_until.items()}
        self._write()

    # ── unresolved exposure ─────────────────────────────────────────────

    def record_operator_unresolved_exposure(self, slug: str, *, run_id: str, qty: Decimal,
                                   venue_verified: bool, orders: str, reason: str,
                                   recovery_cmd: str, reach: Decimal | None = None) -> bool:
        """Record a position this process could not hand back. True ⇒ the row is NEW (page).

        ⛔ IDEMPOTENT ON (lane, slug) — this store is bound to one lane, and the slug is the key.
        A second failure on the same book UPDATES the row (a newer quantity, a newer reason) and
        keeps `first_ts`; it never appends a second row and never re-pages. A row that was
        RESOLVED and then breaks again re-opens and DOES page: that is a new fact.

        ⛔ RECORDS ONLY. Nothing here trades, cancels or flattens, and no field of this row is
        read by any sizing, cap or quoting path — `inventory`, `carried_off_slate` and the loss
        ledger are untouched. It is the durable half of the operator page.

        `venue_verified=False` means `qty` is BELIEF (no venue read succeeded) and the row says
        so; `orders` is the last verified open-order statement for the slug, or "UNVERIFIED".
        `reach` is this launch's lawful reach, kept so a later venue read can clear the row on
        "back inside reach" rather than only on flat.
        """
        rows = {str(k): dict(v)
                for k, v in (self._raw.get("operator_unresolved_exposure") or {}).items()
                if isinstance(v, Mapping)}
        prior = rows.get(str(slug))
        is_new = prior is None or bool(prior.get("resolved"))
        now = time.time()
        rows[str(slug)] = {
            "slug": str(slug),
            "lane": self.lane,
            "first_ts": now if is_new else float(prior.get("first_ts") or now),
            "ts": now,
            "iso": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "run_id": str(run_id),
            "qty": str(_dec(qty)),
            "qty_source": "venue_verified" if venue_verified else "belief_UNVERIFIED",
            "orders": str(orders),
            "reason": str(reason),
            "reach": None if reach is None else str(_dec(reach)),
            "recovery_cmd": str(recovery_cmd),
            "resolved": None,
            # the re-record is the one legitimate rewrite of
            # qty/recovery_cmd; the fresh-read history survives it.
            **({} if is_new else {"corrections": list(prior.get("corrections") or [])}),
        }
        self._raw["operator_unresolved_exposure"] = rows
        self._write()
        return is_new

    def append_exposure_correction(self, slug: str, *, venue_qty: Decimal, recovery_cmd: str,
                                   ts: float | None = None, source: str) -> bool:
        """Append a FRESH-read correction to an open row. True ⇒ appended.

        The row's `qty`/`recovery_cmd`/`first_ts` are incident evidence and are never rewritten;
        the newest entry of `corrections` is the command a printer may show as current.
        Idempotent: a read equal to the newest correction appends nothing.
        """
        rows = {str(k): dict(v)
                for k, v in (self._raw.get("operator_unresolved_exposure") or {}).items()
                if isinstance(v, Mapping)}
        row = rows.get(str(slug))
        if row is None or row.get("resolved"):
            return False
        corrections = [dict(c) for c in (row.get("corrections") or []) if isinstance(c, Mapping)]
        # Decimal against Decimal: the producer is 4-dp `netPositionDecimal` and a format
        # change ("-5.39" vs "-5.3900") must not duplicate.
        if corrections and _dec(corrections[-1].get("venue_qty")) == _dec(venue_qty):
            return False
        now = time.time() if ts is None else float(ts)
        corrections.append({
            "ts": now,
            "iso": datetime.fromtimestamp(now, timezone.utc).isoformat(timespec="seconds"),
            "venue_qty": str(_dec(venue_qty)),
            "recovery_cmd": str(recovery_cmd),
            "source": str(source),
        })
        row["corrections"] = corrections
        rows[str(slug)] = row
        self._raw["operator_unresolved_exposure"] = rows
        self._write()
        return True

    def resolve_operator_unresolved_exposure(self, slug: str, *, how: str,
                                    venue_qty: Decimal | None = None,
                                    by: str | None = None) -> bool:
        """Close a row. True ⇒ this call closed an OPEN row. Two `how` values and no others.

        ⛔ `venue_verified` — a FRESH venue read showed the book flat, or inside the reach the
        row recorded. ⛔ `operator_accepted_carry` — a human typed the acceptance and is named in
        `by`. There is no third clear: no supervisor, no relaunch, and no acknowledgement of the
        PAGE closes this row (acking a page answers the alert, not the exposure).
        """
        if how not in ("venue_verified", "operator_accepted_carry"):
            raise ValueError(f"unresolved exposure clears on a venue read or an operator "
                             f"acceptance, never on {how!r}")
        if how == "operator_accepted_carry" and not (by or "").strip():
            raise ValueError("an accepted carry must name WHO accepted it (`by`)")
        rows = {str(k): dict(v)
                for k, v in (self._raw.get("operator_unresolved_exposure") or {}).items()
                if isinstance(v, Mapping)}
        row = rows.get(str(slug))
        if row is None or row.get("resolved"):
            return False
        now = time.time()
        row["resolved"] = {
            "ts": now,
            "iso": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "how": how,
            "venue_qty": None if venue_qty is None else str(_dec(venue_qty)),
            "by": str(by) if by else None,
        }
        rows[str(slug)] = row
        self._raw["operator_unresolved_exposure"] = rows
        self._write()
        return True

    def add_realized(self, delta: Decimal) -> Decimal:
        """Fold a PRICE-CHANNEL realized P&L delta (signed; negative = loss) into the durable
        running total. ⛔ REBATES DO NOT COME THROUGH HERE — `add_rebate` owns its own field, and
        folding the two would silently redefine `realized_pnl` for every reader of it."""
        total = _dec(self._raw.get("realized_pnl")) + _dec(delta)
        self._raw["realized_pnl"] = str(total)
        self._write()
        return total

    def add_rebate(self, delta: Decimal) -> Decimal:
        """Fold a REALIZED rebate delta (signed; positive = credit received) into the durable
        `rebate_realized` [AMENDMENT 30]. Caller supplies the VENUE's commission read-back only;
        a fill whose read-back is missing books nothing, so it contributes 0."""
        total = _dec(self._raw.get("rebate_realized")) + _dec(delta)
        self._raw["rebate_realized"] = str(total)
        self._write()
        return total

    def close_lane(self, *, reason: str, venue_flat_read_ts: float,
                   accept_unpriced: bool = False, venue_txns: str = "",
                   live_check: Callable[[dict[str, "MakerState"]], list[str]] | None = None) -> dict:
        """LEDGER-PRESERVING LANE CLOSE [M0.5c]: archive this lane's carried P&L, then zero it.

        `account_loss` sums EVERY lane's loss-to-date forever, so a finished lane's realized loss
        prices every later run's budget on this venue. The record is appended to the durable archive
        FIRST, and only then are `realized_pnl` and `loss_to_date` written to "0" with a
        `closed_from` pointer — never "delete the state file", which zeroes the ratchet by
        destroying the ledger.

        ⛔ WHO DECIDES: this method does NOT check whether the lane is closable — inventory, carries,
        crash records, live pids and the venue-flat read are `scripts/poly_lane_close.py`'s refusals.
        The ordering is deliberate: a crash between append and write leaves an archive line with the
        money still on the live record (re-runnable), never a zeroed record with no archive.
        """
        # ⛔ TOCTOU: the caller's sibling check ran BEFORE a venue round-trip that takes seconds,
        # and a maker can be launched inside it. Re-check UNDER THE LOCK before the archive append
        # (so a caught race writes nothing at all), and again inside the write itself (the residual
        # window, where the archive line is already down — re-runnable).
        if live_check is not None:
            raced = live_check(self.locked_lanes())
            if raced:
                raise LaneCloseRaced("; ".join(raced))

            def _guard(lanes: dict[str, "MakerState"]) -> None:
                late = live_check(lanes)
                if late:
                    raise LaneCloseRaced("; ".join(late))
        else:
            _guard = None       # type: ignore[assignment]

        snap = self.snapshot()
        record = {
            "lane": self.lane,
            "run_id": snap.run_id,
            "realized_pnl": str(snap.realized_pnl),
            "loss_to_date": str(snap.loss_to_date),
            # Archived beside the price channel, never inside it [AMENDMENT 30].
            "rebate_realized": ("" if snap.rebate_realized is None
                                else str(snap.rebate_realized)),
            "overall_realized": str(snap.overall_realized),
            "closed_ts": time.time(),
            "reason": str(reason),
            "venue_flat_read_ts": float(venue_flat_read_ts),
        }
        record["iso"] = datetime.fromtimestamp(
            record["closed_ts"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        # ⛔ ACCEPTING THE VENUE LEDGER AS TRUTH for money this record cannot price [operator
        # decision 2026-09-04]. The parked rows are MOVED, not copied: a book that stays in
        # `closed_unpriced` after its lane is closed is a claim on a ledger that no longer carries
        # the loss it belongs beside. `poly_lane_close --accept-unpriced` has already checked that
        # the operator named EVERY parked book.
        record["unpriced_closes"] = []
        if accept_unpriced:
            for row, kind in ([(r, "settlement") for r in snap.settled_pending]
                              + [(r, "hand_close") for r in snap.closed_unpriced]):
                note = str(row.get("reason") or row.get("status") or "")
                record["unpriced_closes"].append({
                    "slug": str(row.get("slug") or ""),
                    "qty": str(row.get("qty") if row.get("qty") is not None else ""),
                    "kind": kind,
                    "venue_ref": f"{venue_txns or '(no ledger named)'}: {note}" if note
                                 else (venue_txns or "(no ledger named)"),
                })
            record["accepted_by"] = "operator"
            record["venue_txns"] = venue_txns
        record["archive_appended"] = _append_closed_lane(closed_lanes_path(self.path), record)
        if accept_unpriced:
            self._raw["settled_pending"] = []
            self._raw["closed_unpriced"] = []
        self._raw["realized_pnl"] = "0"
        self._raw["loss_to_date"] = "0"
        # Zeroed WITH the price channel: the archive above carries both, and a lane whose price
        # ledger was retired must not keep a rebate credit that would buy the next run budget.
        self._raw["rebate_realized"] = "0"
        self._raw["closed_from"] = {"run_id": snap.run_id,
                                    "archive_ts": record["closed_ts"],
                                    "archive": closed_lanes_path(self.path)}
        self._write(guard=_guard)
        return record

    def record_settled_pending(self, *, slug: str, qty: Decimal, avg_entry_yes: Decimal | None,
                               status: str, run_id: str = "",
                               basis_source: str = "reset") -> bool:
        """Move a SETTLED book's belief out of `inventory` and into `settled_pending`. Booked?

        The venue's positions endpoint DROPS a resolved market, so a book that settled while we held
        it reads as belief ≠ 0 against no venue row — the shape of a real divergence. It is not one,
        and it is not flattenable. The belief is retired HERE, with its quantity, THIS RUN's mean
        entry price and the venue's own status string.

        ⛔ `avg_entry_yes` IS NOT A COST BASIS — it is the maker's own `avg_entry` in YES space, and
        it is PROVENANCE: `poly_settle_book` prices a settlement from the venue's own transaction row.
        ⛔ `realized_pnl` DOES NOT MOVE **HERE**: a settled book whose payout is unknown reads as
        UNPRICED, never as a realized zero. `poly_settle_book.settle_pending_books` prices the row
        and books it through `add_settlement`, which FOLDS into `realized_pnl` — the cap bounds real
        loss and a settlement loss is real. A `basis_source != "run"` row is never auto-booked.
        ⛔ IDEMPOTENT ON `slug`. `avg_entry_yes=None` records an EMPTY string, never a zero.
        """
        rows = list(self._raw.get("settled_pending") or [])
        if any(isinstance(r, Mapping) and str(r.get("slug")) == str(slug) for r in rows):
            return False
        now = time.time()
        rows.append({"slug": str(slug), "qty": str(_dec(qty)),
                     "avg_entry_yes": "" if avg_entry_yes is None else str(avg_entry_yes),
                     "basis_source": str(basis_source),
                     "status": str(status), "run_id": str(run_id), "ts": now,
                     "iso": datetime.fromtimestamp(now, timezone.utc).isoformat()})
        self._raw["settled_pending"] = rows
        inv = dict(self._raw.get("inventory") or {})
        inv.pop(str(slug), None)
        self._raw["inventory"] = inv
        # ⛔ AND OUT OF THE CARRIED MAP, or `set_inventory` would re-merge this slug on the next
        # write and resurrect a settled position in the record the pending row just retired.
        carried = self._carried_off_slate()
        if carried.pop(str(slug), None) is not None:
            self._raw["carried_off_slate"] = carried
        self._drop_basis_run(str(slug))
        self._write()
        return True

    def clear_settled_pending(self, slug: str) -> bool:
        """Drop a `settled_pending` row once its settlement is BOOKED. Removed?

        ⛔ ONLY AFTER `add_settlement` HAS RETURNED for that slug's event id
        (`poly_settle_book.settle_pending_books` is the one caller). The pending row is the ONLY
        record that the lane held the book when the market resolved. The order is book-then-clear
        on purpose: `add_settlement` is idempotent on the venue event id, so a crash between the
        two leaves a pending row that re-books to `booked=False` and clears on the next pass, while
        clear-then-book could lose the row entirely.

        An unknown slug is False, not an error: a pass that already cleared it is a no-op.
        """
        rows = list(self._raw.get("settled_pending") or [])
        kept = [r for r in rows
                if not (isinstance(r, Mapping) and str(r.get("slug")) == str(slug))]
        if len(kept) == len(rows):
            return False
        self._raw["settled_pending"] = kept
        self._write()
        return True

    def record_closed_unpriced(self, *, slug: str, qty: Decimal,
                               avg_entry_yes: Decimal | None, basis_source: str, reason: str,
                               run_id: str = "", since_ts: float | None = None) -> bool:
        """Park a HAND-CLOSED book: belief out of `inventory`, quantity into `closed_unpriced`.

        A belief the venue does not hold never blocks a launch. When the operator closes a position
        by hand on the UI the venue serves no row for it and the market has not resolved, so the
        belief would refuse every reconcile and every start. It is retired HERE, with its quantity,
        this run's basis and the REASON its money is not yet priced.

        ⛔ `realized_pnl` DOES NOT MOVE HERE — parking is not booking. `add_hand_close` is the one
        method that folds a hand close, and a `basis_source != "run"` row is never auto-priced.
        ⛔ IDEMPOTENT ON `slug` and it KEEPS THE FIRST ROW: a second row would double-price one close.
        """
        rows = list(self._raw.get("closed_unpriced") or [])
        if any(isinstance(r, Mapping) and str(r.get("slug")) == str(slug) for r in rows):
            return False
        now = time.time()
        rows.append({"slug": str(slug), "qty": str(_dec(qty)),
                     "avg_entry_yes": "" if avg_entry_yes is None else str(avg_entry_yes),
                     "basis_source": str(basis_source), "reason": str(reason),
                     "since_ts": "" if since_ts is None else float(since_ts),
                     "run_id": str(run_id), "ts": now,
                     "iso": datetime.fromtimestamp(now, timezone.utc).isoformat()})
        self._raw["closed_unpriced"] = rows
        inv = dict(self._raw.get("inventory") or {})
        inv.pop(str(slug), None)
        self._raw["inventory"] = inv
        # ⛔ AND OUT OF THE CARRIED MAP, or `set_inventory` re-merges the slug on the next write
        # and resurrects a position this row just retired (the `record_settled_pending` trap).
        carried = self._carried_off_slate()
        if carried.pop(str(slug), None) is not None:
            self._raw["carried_off_slate"] = carried
        self._drop_basis_run(str(slug))
        self._write()
        return True

    def clear_closed_unpriced(self, slug: str) -> bool:
        """Drop a `closed_unpriced` row once `add_hand_close` has booked it. Removed?

        ⛔ ONLY AFTER the fold has returned, book-then-clear — `add_hand_close` is idempotent on
        the event id, so a crash between the two leaves a row that re-books to False and clears
        on the next pass, while clear-then-book could lose the position from every channel.
        """
        rows = list(self._raw.get("closed_unpriced") or [])
        kept = [r for r in rows
                if not (isinstance(r, Mapping) and str(r.get("slug")) == str(slug))]
        if len(kept) == len(rows):
            return False
        self._raw["closed_unpriced"] = kept
        self._write()
        return True

    def add_hand_close(self, *, event_id: str, slug: str, qty: Decimal, basis: Decimal,
                       exit_avg: Decimal, amount: Decimal,
                       note: str = "") -> tuple[Decimal, bool]:
        """Book an OUT-OF-PROCESS CLOSE into the durable ledger: (new realized total, booked?).

        The maker never saw the fill, so nothing folded it (memory
        `out-of-process-closes-dont-book`). `qty·(exit_avg − basis)` FOLDS INTO `realized_pnl`
        because it is PRICE-realized P&L in the ordinary sense: the cap must see a hand close exactly
        as it sees a maker round trip. The per-event row keeps the un-netted-buckets rule.

        ⛔ IDEMPOTENT ON `event_id`, the VENUE's own oldest trade id for the close
        (`poly:handclose:<slug>:<trade id>`) — never synthetic, and never the slug alone: a slug can
        be closed by hand, re-opened and closed again.

        ⛔ LOCKED READ-MODIFY-WRITE, for the reason `add_settlement` documents. The liveness half
        stays the caller's (`poly_settle_book.live_lane_refusal`).
        """
        eid = str(event_id).strip()
        if not eid:
            raise ValueError("add_hand_close requires the venue's own trade id — a blank id "
                             "cannot deduplicate and must refuse upstream, not here")
        with _FileLock(self.path):
            current: Any = None
            if os.path.exists(self.path):
                current = durable.read_json_strict(self.path)
            lanes = _doc_lanes(current)
            disk_raw = dict(lanes.get(self.lane) or {})
            booked = list(disk_raw.get("hand_closes") or [])
            if any(isinstance(r, dict) and r.get("event_id") == eid for r in booked):
                self._raw = disk_raw
                return _dec(disk_raw.get("realized_pnl")), False
            booked.append({"event_id": eid, "slug": str(slug), "qty": str(_dec(qty)),
                           "basis": str(_dec(basis)), "exit_avg": str(_dec(exit_avg)),
                           "amount": str(_dec(amount)), "note": str(note),
                           "booked_ts": time.time()})
            disk_raw["hand_closes"] = booked
            total = _dec(disk_raw.get("realized_pnl")) + _dec(amount)
            disk_raw["realized_pnl"] = str(total)
            disk_raw["updated_ts"] = time.time()
            lanes[self.lane] = disk_raw
            durable.write_json_durable(self.path, {"schema": 2, "lanes": lanes})
            self._raw = disk_raw
        return total, True

    def add_settlement(self, *, event_id: str, slug: str, amount: Decimal,
                       note: str = "") -> tuple[Decimal, bool]:
        """Book a SETTLEMENT into the durable ledger: (new realized total, booked?).

        The amount FOLDS INTO `realized_pnl` — the cap bounds REAL loss and a settlement is as real
        as a fill — while the per-event record keeps the un-netted-buckets rule [risk audit
        2026-08-24 §2.1: settlement never reached this ledger, so a resolved position's loss was
        invisible to the cap].

        ⛔ IDEMPOTENT ON `event_id` (the venue's own identifier), and ⛔ NEVER a synthetic one — if
        the venue row carries no id the caller must refuse to book. A GAIN also folds; "profit earns
        nothing back" is enforced ACROSS lanes (`account_loss` floors per lane), never within one.

        ⛔ LOCKED READ-MODIFY-WRITE, unlike every other mutator here: the snapshot-replace `_write`
        let this tool and a live maker erase each other's money in BOTH directions, so this re-reads
        its own lane UNDER the lock and applies the delta to the DISK state. `poly_settle_book`
        REFUSES to book while a live pid holds the lane — this is the integrity half.
        """
        eid = str(event_id).strip()
        if not eid:
            raise ValueError("add_settlement requires the venue's own event_id — a blank id "
                             "cannot deduplicate and must refuse upstream, not here")
        with _FileLock(self.path):
            current: Any = None
            if os.path.exists(self.path):
                current = durable.read_json_strict(self.path)
            lanes = _doc_lanes(current)
            disk_raw = dict(lanes.get(self.lane) or {})
            booked = list(disk_raw.get("settlements") or [])
            if any(isinstance(r, dict) and r.get("event_id") == eid for r in booked):
                self._raw = disk_raw
                return _dec(disk_raw.get("realized_pnl")), False
            booked.append({"event_id": eid, "slug": str(slug), "amount": str(_dec(amount)),
                           "note": str(note), "booked_ts": time.time()})
            disk_raw["settlements"] = booked
            total = _dec(disk_raw.get("realized_pnl")) + _dec(amount)
            disk_raw["realized_pnl"] = str(total)
            disk_raw["updated_ts"] = time.time()
            lanes[self.lane] = disk_raw
            durable.write_json_durable(self.path, {"schema": 2, "lanes": lanes})
            self._raw = disk_raw
        return total, True

    def snapshot(self) -> MakerState:
        return _to_state(self._raw)

    def locked_lanes(self) -> dict[str, "MakerState"]:
        """Every lane, re-read UNDER THE WRITE LOCK — the close's TOCTOU re-check. A `begin_run`
        racing us takes the same lock, so a lane that is live in this view was live before our
        write could have started."""
        with _FileLock(self.path):
            current: Any = durable.read_json_strict(self.path) if os.path.exists(self.path) else None
            return {lane: _to_state(rec) for lane, rec in _doc_lanes(current).items() if rec}

    def _write(self, guard: Callable[[dict[str, "MakerState"]], None] | None = None) -> None:
        """Merge THIS LANE's record into the shared file under an exclusive lock.

        `guard` (the lane close's only caller) sees the CURRENT file's lanes inside the lock and may
        raise to abandon the write — the last point at which a sibling that went live during our
        venue round-trip can still stop us.

        Read-modify-write, re-reading the file each time so another lane's writes are preserved. A
        corrupt current file REFUSES rather than clobbering (same rule as the ctor); missing file →
        we create the v2 shape. Own-lane concurrent writers are NOT defended here — that is the
        slate-collision case, refused at startup."""
        self._raw["updated_ts"] = time.time()
        with _FileLock(self.path):
            current: Any = None
            if os.path.exists(self.path):
                current = durable.read_json_strict(self.path)   # StateCorrupt propagates — never clobber
            lanes = _doc_lanes(current)
            if guard is not None:
                # Raises to abandon: nothing has been written yet at this point.
                guard({lane: _to_state(rec) for lane, rec in lanes.items() if rec})
            lanes[self.lane] = self._raw
            durable.write_json_durable(self.path, {"schema": 2, "lanes": lanes})


# ── the recovery decision ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RecoveryPlan:
    """`action` answers exactly ONE question: may the maker start?

      start   — yes.
      recover — not until the listed cleanup is done (then re-assess).
      refuse  — no, and cleanup alone will not fix it; an operator must look.

    `cancel_order_ids` is safe to execute under ANY action, including `refuse` — cancelling only
    removes exposure. `flatten` may be executed only under `recover`; `unknown` is never acted on.
    """
    action: str
    reason: str
    detail: str
    cancel_order_ids: tuple[str, ...] = ()
    flatten: tuple[tuple[str, Decimal], ...] = ()
    unknown: tuple[tuple[str, Decimal], ...] = ()
    # Own OFF-SLATE positions the record and the venue agree on exactly, carried past the gate
    # (see `is_carryable_off_slate`). NOT this run's inventory: the caller must keep them out of
    # the quoting book and hand them to `begin_run(carried_off_slate=…)` so the durable record
    # keeps them. Never flattened, never quoted.
    carried_off_slate: tuple[tuple[str, Decimal], ...] = ()
    # (slug, qty, basis) triples this start seeds as THIS run's inventory because the durable
    # record — not an operator declaration — matched the venue exactly and a basis was
    # recoverable (`recorded_carries` + `--carry-recorded`). ON-slate and QUOTED, unlike
    # `carried_off_slate`: the caller seeds them exactly as it seeds a declared `--carry`.
    seeded_carries: tuple[tuple[str, Decimal, Decimal], ...] = ()

    def report(self) -> str:
        lines = [f"RECOVERY: {self.action.upper()} ({self.reason}) — {self.detail}"]
        if self.cancel_order_ids:
            lines.append("  cancel (always safe): " + ", ".join(self.cancel_order_ids))
        if self.flatten:
            lines.append("  flatten (recorded, attributable): "
                         + ", ".join(f"{t} {q}" for t, q in self.flatten))
        if self.unknown:
            lines.append("  ⚠️ UNKNOWN venue positions — operator decision, NOT auto-flattened: "
                         + ", ".join(f"{t} {q}" for t, q in self.unknown))
        return "\n".join(lines)


def _venue_order_ids(venue_orders: Sequence[Any]) -> tuple[str, ...]:
    out: list[str] = []
    for o in venue_orders:
        oid = o.get("order_id") or o.get("id") if isinstance(o, Mapping) else o
        if oid:
            out.append(str(oid))
    return tuple(out)


def assess_recovery(
    state: MakerState | None,
    venue_orders: Sequence[Any] | None,
    venue_positions: Mapping[str, Any] | None,
    *,
    slate: Iterable[str] | None = None,
    other_lanes: Mapping[str, MakerState] | None = None,
) -> RecoveryPlan:
    """Decide, from the durable record and the venue's own answer, whether the maker may start.

    `venue_orders=None` / `venue_positions=None` mean CANNOT VERIFY and are checked FIRST; `[]` /
    `{}` mean confirmed flat and are a different answer entirely.

    ⛔ SLATE SCOPING. `slate=None` is ACCOUNT-GLOBAL (the historical behaviour, and the default).
    With a slate + `other_lanes` (sibling snapshots from the SHARED ledger, own lane excluded):
      · SLATE COLLISION refuses first — a sibling claiming any of OUR slugs means two processes
        would quote one book.
      · ORDERS are partitioned by the order's own market slug; foreign and slug-unreadable orders
        are REPORTED, never cancelled.
      · POSITIONS on our slate keep the unknown/attributable logic below. A foreign-slug position
        attributed to a sibling is that lane's business; one attributed to NO lane is an ORPHAN,
        reported LOUDLY.
      · THE CAP RATCHET STAYS ACCOUNT-WIDE — one account, one ratchet.
    """
    if venue_orders is None or venue_positions is None:
        which = "orders" if venue_orders is None else "positions"
        return RecoveryPlan(
            "refuse", "cannot_verify",
            f"could not read venue {which} — CANNOT VERIFY. An unreadable venue is not a flat "
            f"venue; starting now could double an inherited position or requote against a book "
            f"we are already resting in.")

    notes: list[str] = []
    own_carried: list[tuple[str, Decimal]] = []
    positions = {str(k): _dec(v) for k, v in venue_positions.items()}

    if slate is not None:
        slate_set = {str(s) for s in slate}
        for lane_name, ls in sorted((other_lanes or {}).items()):
            claimed = {t for t, q in ls.inventory.items() if _is_real(q)}
            claimed |= {str(o.get("ticker")) for o in ls.orders.values() if o.get("ticker")}
            if not ls.clean_exit:
                claimed |= set(ls.tickers)   # a crashed lane may hold unrecorded state anywhere on its slate
            hit = sorted(claimed & slate_set)
            if hit:
                return RecoveryPlan(
                    "refuse", "slate_collision",
                    f"lane {lane_name!r} (run {ls.run_id!r}) claims {hit} from this run's slate — "
                    f"two processes must never quote one book. Change the slate, or resolve that "
                    f"lane first.")
        scoped: list[Any] = []
        n_foreign = n_unattributable = 0
        for o in venue_orders:
            oslug = (o.get("marketSlug") or o.get("market_slug") or o.get("ticker")
                     ) if isinstance(o, Mapping) else None
            if not oslug:
                n_unattributable += 1        # cannot establish it is ours → report, never cancel
            elif str(oslug) in slate_set:
                scoped.append(o)
            else:
                n_foreign += 1
        if n_foreign or n_unattributable:
            notes.append(f"{n_foreign} foreign-slug and {n_unattributable} slug-unreadable "
                         f"order(s) resting on the account — NOT ours to cancel; verify a "
                         f"sibling run owns them (see the operator runbook).")
        venue_orders = scoped
        foreign_pos = {t: q for t, q in positions.items()
                       if t not in slate_set and _is_real(q)}
        positions = {t: q for t, q in positions.items() if t in slate_set}
        if foreign_pos:
            # ⛔ OUR OWN lane's record explaining an off-slate position is NOT an orphan and NOT a
            # sibling's business — it is a position WE hold that this run's slate would leave
            # unmanaged, so it refuses: put it on --slugs or flatten it first. Sign-AGNOSTIC on
            # purpose (a venue −5 against our recorded +20 is a DISAGREEMENT, strictly worse).
            # ⛔ DUST CARVE-OUT: the teardown flatten cannot place a sub-whole-contract residue, so
            # refusing here made it PERMANENT; exposure is bounded under <n>. It requires BOTH sides
            # sub-1 — venue dust against a recorded WHOLE-contract belief still refuses.
            # ⛔ CARRY-THROUGH — see OWN_OFFSLATE_CARRY_LIMIT_USD; the split and the worst case are
            # `classify_own_off_slate`'s.
            residual = classify_own_off_slate(
                state.inventory if state is not None else {}, foreign_pos)
            own_off_slate = list(residual.refused)
            own_dust = list(residual.dust)
            own_carried = list(residual.carried)
            if own_off_slate:
                return RecoveryPlan(
                    "refuse", "own_position_off_slate",
                    f"THIS lane's record holds {own_off_slate}, which is not on this run's "
                    f"slate — starting would leave our own position unmanaged. Add it to "
                    f"--slugs (or --carry), or flatten it first.")
            if own_dust:
                # ⛔ AGGREGATE CEILING. Per-position boundedness is not portfolio boundedness: N
                # dust positions carve out N × <<n> and a carve-out with no ceiling is a slow leak
                # that only ever grows. Worst-case dollars = Σ|qty|, because a contract settles in
                # [0, 1]. Same intent — and the same <n> — as `poly_prelaunch.DUST_CEILING_USD`.
                worst_case = sum((q.copy_abs() for _t, q in own_dust), _ZERO)
                if worst_case > _DUST_AGGREGATE_LIMIT_USD:
                    return RecoveryPlan(
                        "refuse", "own_dust_aggregate",
                        f"THIS lane's record holds {len(own_dust)} off-slate DUST position(s) "
                        f"worth up to ${worst_case} worst case, over the "
                        f"${_DUST_AGGREGATE_LIMIT_USD} aggregate ceiling: "
                        f"{[f'{t}:{q}' for t, q in own_dust]}. Each one is individually tiny, "
                        f"which is exactly how they accumulated — clear them with "
                        f"a passive hand close (see the operator runbook) before "
                        f"starting.")
                notes.append(
                    f"⚠️ OFF-SLATE DUST carried past the recovery gate: "
                    f"{[f'{t}:{q}' for t, q in own_dust]} — THIS lane's record holds these and "
                    f"this run's slate does not manage them. Under one contract each, "
                    f"${worst_case} worst case in aggregate (ceiling "
                    f"${_DUST_AGGREGATE_LIMIT_USD}), so the start PROCEEDS rather than dying on "
                    f"residue the teardown flatten could not place. Clear it with "
                    f"a passive hand close (see the operator runbook).")
            if own_carried:
                # ⛔ ONE CEILING OVER BOTH CARVE-OUTS. Dust and carried residuals are the same
                # exposure to the account, so they are priced together: Σ|qty| worst case, a
                # contract settling in [0, 1]. Above it the refusal stands, and it NAMES the
                # ceiling so the operator is not left guessing which number to get under.
                carried_worst = residual.worst_case
                if residual.over_ceiling:
                    return RecoveryPlan(
                        "refuse", "own_position_off_slate",
                        f"THIS lane's record holds {[t for t, _q in own_carried]}, which is not "
                        f"on this run's slate — the record and the venue agree, but off-slate "
                        f"residue worth ${carried_worst} worst case is over the "
                        f"${OWN_OFFSLATE_CARRY_LIMIT_USD} carry ceiling. Add it to --slugs (or "
                        f"--carry), or flatten it first.")
                notes.append("off-slate carried: "
                             + " ".join(f"{t} {q}" for t, q in own_carried))
            attributed, orphans = [], []
            own_dust_slugs = {t for t, _q in own_dust} | {t for t, _q in own_carried}
            for t, q in sorted(foreign_pos.items()):
                if t in own_dust_slugs:
                    continue        # already named above as OURS — not an orphan, not a sibling's
                owner = next((ln for ln, ls in (other_lanes or {}).items()
                              if _is_real(ls.inventory.get(t, _ZERO))
                              and (ls.inventory[t] > 0) == (q > 0)
                              and q.copy_abs() <= ls.inventory[t].copy_abs() + _DUST), None)
                (attributed if owner else orphans).append(f"{t}:{q}" + (f"→{owner}" if owner else ""))
            if attributed:
                notes.append(f"foreign positions attributed to sibling lanes: {attributed}")
            if orphans:
                notes.append(f"⚠️ ORPHAN foreign position(s) NO lane's record explains: "
                             f"{orphans} — with slate scoping no start will ever block on "
                             f"these; resolve them deliberately (flatten by hand or restart "
                             f"the owning run).")
        for lane_name, ls in sorted((other_lanes or {}).items()):
            if ls.cap_breached:
                return RecoveryPlan(
                    "refuse", "loss_cap",
                    f"sibling lane {lane_name!r} has breached its ${ls.loss_cap} cap "
                    f"({cap_line(ls)}). ⚠️ SCOPE [lanes r3]: this scan runs only on SCOPED "
                    f"starts (a pair in play); the always-on enforcement is the ENGINE's live "
                    f"account-loss read vs THIS run's cap, every check. Clear deliberately.")

    cancel = _venue_order_ids(venue_orders)
    live = {t: q for t, q in positions.items() if _is_real(q)}

    plan = _assess_scoped(state, cancel, live)
    if notes:
        plan = dataclasses.replace(plan, detail=plan.detail + "\n  " + "\n  ".join(notes))
    if own_carried:
        plan = dataclasses.replace(plan, carried_off_slate=tuple(own_carried))
    return plan


def _assess_scoped(state: MakerState | None, cancel: tuple[str, ...],
                   live: dict[str, Decimal]) -> RecoveryPlan:
    """The core verdict over the (possibly slate-filtered) orders + positions — the historical
    assess_recovery tail, factored out so the scoping layer above cannot drift from it."""
    recorded = dict(state.inventory) if state is not None else {}
    unknown: list[tuple[str, Decimal]] = []
    attributable: list[tuple[str, Decimal]] = []
    for ticker, qty in sorted(live.items()):
        known = recorded.get(ticker, _ZERO)
        if not _is_real(known):
            # NO RECORD (or a dust-level one): a real venue position with no basis of ours is
            # unknown BY DEFINITION, whatever its sign. A sign clause here read a NEGATIVE
            # dust-sized no-record position as attributable and planned an AUTO-FLATTEN against a
            # basis we do not own.
            unknown.append((ticker, qty))
            continue
        # `venue > known` is unknown exposure; `venue < known` is ordinary settlement/partial fill
        # and is NOT escalated — the same one-directional call reconcile.py makes. ⛔ But the
        # shrink exemption applies only when the SIGNS AGREE: a venue −12 against a recorded +12
        # passes a magnitude-only check, and the record then describes the OPPOSITE of what the
        # venue holds — a basis that is fiction, exactly as unknown as no record at all.
        if qty.copy_abs() > known.copy_abs() + _DUST or (qty > 0) != (known > 0):
            unknown.append((ticker, qty))
        else:
            attributable.append((ticker, qty))

    if unknown:
        return RecoveryPlan(
            "refuse", "unknown_position",
            "the venue holds a position with no matching durable record. Flattening it would move "
            "real money against a basis we do not own, so this is an operator decision — "
            "acknowledge it or flatten it by hand, then restart.",
            cancel_order_ids=cancel, unknown=tuple(unknown))

    if cancel or attributable:
        why = ("the previous run did not exit cleanly (no exit was recorded — SIGKILL / OOM-kill / "
               "power loss)" if state is not None and not state.clean_exit
               else "live venue state remains from a previous run")
        return RecoveryPlan(
            "recover", "live_venue_state",
            f"{why}; cancel the listed orders and flatten the listed positions, then re-assess.",
            cancel_order_ids=cancel, flatten=tuple(attributable))

    if state is not None and state.cap_breached:
        return RecoveryPlan(
            "refuse", "loss_cap",
            f"durable realized loss has breached the ${state.loss_cap} cap "
            f"({cap_line(state)}). "
            f"This is a LIFETIME ratchet across processes — a crash-restart loop must not reset "
            f"the budget. Clear it deliberately before restarting.")

    if state is not None and not state.clean_exit:
        return RecoveryPlan(
            "start", "unclean_exit_but_flat",
            "the previous run did not exit cleanly, but the venue confirms flat — nothing resting, "
            "no inventory. Safe to start.")

    return RecoveryPlan("start", "clean", "no live venue state and no prior unclean exit.")


def apply_carry(plan: RecoveryPlan, carries: Mapping[str, Any],
                venue_positions: Mapping[str, Any] | None) -> RecoveryPlan:
    """CONTINUOUS OPERATION [operator-directed]: convert a `recover` plan whose ONLY cleanup is
    flattening positions the operator has EXPLICITLY DECLARED as carries into a start.

    Fail-closed on every mismatch, because a wrong carry is a wrong BASIS on real money:
      · any resting venue order → the plan stands (a carry declares positions, never orders);
      · a declared book the venue does not hold, or holds at ANY other quantity or sign → REFUSE
        outright (the declaration is stale, and every other declaration is now suspect);
      · a live venue book NOT declared → the plan stands for it (a carry is per-book and explicit);
      · an `unknown_position` refusal is NEVER converted (no durable record = no basis to carry);
      · a carry declared against a `start` plan REFUSES unless the venue holds it exactly — see the
        phantom-carry gate below.
    Quantities compare EXACTLY (Decimal, sign included). This function only decides whether starting
    is LEGAL; the basis travels separately to the engine.
    """
    if not carries:
        return plan
    if plan.action == "start":
        # ⛔ PHANTOM-CARRY GATE. A `start` plan means the venue holds nothing REAL on the assessed
        # scope — `_assess_scoped` routes every real position to recover or refuse — so a carry
        # declared here describes a venue that does not exist, and `poly_live_mm`'s seeding loop is
        # UNCONDITIONAL: the engine would start believing it held contracts it did not (reduce-side
        # quotes resting against nothing, the mark tripwire and every P&L line running off the
        # phantom). Fail-closed and NAME THE NUMBERS — the operator's remedy is to re-read the
        # venue. ⚠️ Refuses rather than silently DROPS the carry: dropping is also a wrong belief
        # and it would hide the stale line.
        if venue_positions is None:
            return RecoveryPlan(
                "refuse", "carry_cannot_verify",
                f"--carry declares {sorted(carries)} but the venue positions could not be read "
                f"— CANNOT VERIFY. An unreadable venue is not a confirmation; seeding a carry "
                f"here would put a position into belief on the operator's word alone.")
        venue = {str(k): _dec(v) for k, v in venue_positions.items()}
        for slug, declared in sorted(carries.items()):
            held = venue.get(slug, _ZERO)
            if held != _dec(declared):
                return RecoveryPlan(
                    "refuse", "carry_not_held",
                    f"--carry declares {slug} at {declared} but the venue holds {held} and the "
                    f"recovery plan is a clean start (nothing of ours is live) — the declaration "
                    f"is stale. Seeding it would be PHANTOM inventory: reduce-side orders resting "
                    f"against nothing, and a fill on one OPENS a real position. Re-read the venue "
                    f"(scripts.poly_us_positions) and re-declare, or drop the --carry.")
        return plan
    if plan.action != "recover":
        return plan
    if plan.cancel_order_ids:
        return plan
    if venue_positions is None:
        return plan
    venue = {str(k): _dec(v) for k, v in venue_positions.items()}
    flatten = dict(plan.flatten)
    for slug, declared in carries.items():
        held = venue.get(slug, _ZERO)
        if held != _dec(declared):
            return RecoveryPlan(
                "refuse", "carry_mismatch",
                f"--carry declares {slug} at {declared} but the venue holds {held} — the "
                f"declaration is stale, and a stale declaration means every carry on this "
                f"line is suspect. Re-read the venue and re-declare.")
        if slug not in flatten:
            return RecoveryPlan(
                "refuse", "carry_mismatch",
                f"--carry declares {slug} but the recovery plan does not list it as an "
                f"attributable position (venue {held}) — carries may only name positions "
                f"the durable record already owns.")
        del flatten[slug]
    # ⛔ CARRY THE ORIGINAL PLAN'S NOTES FORWARD: the scoped assessment appends orphan/foreign
    # warnings to plan.detail, and carries are the NORMAL start mode — a fresh detail string here
    # would silently delete the one printing of an orphan.
    inherited = ""
    if "\n  " in plan.detail:
        inherited = "\n  " + plan.detail.split("\n  ", 1)[1]
    if flatten:
        remaining = ", ".join(f"{s}:{q}" for s, q in flatten.items())
        return RecoveryPlan(
            "recover", "live_venue_state",
            f"carries accepted for {sorted(carries)} but the venue still holds UNDECLARED "
            f"positions ({remaining}) — declare or flatten them, then re-assess." + inherited,
            flatten=tuple(flatten.items()),
            carried_off_slate=plan.carried_off_slate)
    # ⛔ FORWARDED, not re-derived: this builds a NEW plan, and dropping the carried set here
    # would lose the off-slate belief at `begin_run` on every launch that also declares a
    # `--carry` — the record would be wiped for a position we still hold.
    return RecoveryPlan(
        "start", "carried_inventory",
        f"clean start WITH declared or recorded carries: {sorted(carries)} match the venue "
        f"exactly; "
        f"no resting orders; the engine seeds these as inventory with the declared or "
        f"replayed basis. "
        f"A book at or above its cap starts REDUCE-ONLY until it works down — expected."
        + inherited,
        carried_off_slate=plan.carried_off_slate)


def recorded_carries(
    plan: RecoveryPlan,
    state: MakerState | None,
    venue_positions: Mapping[str, Any] | None,
    basis_of: Callable[[str, Decimal], tuple[Decimal | None, str]],
) -> tuple[dict[str, tuple[Decimal, Decimal]], list[str]]:
    """RECORDED ON-SLATE CARRY [`--carry-recorded`, ].

    The declaration a `--carry` makes — "this book is ours, at this quantity, at this basis" —
    DERIVED FROM THIS LANE'S OWN DURABLE RECORD instead of typed by the operator. Every such
    residual is the previous cycle's own, so the record already holds what the operator would have
    transcribed.

    Returns (carries, skipped-reasons). `apply_carry` is still the ONE authority on whether the
    start is legal; this only decides which books may be DECLARED, and it fails closed to "none":
      · not a `recover` plan, any resting order, an unreadable venue → nothing;
      · the record and the venue must agree EXACTLY, sign and Decimal quantity (`is_carryable_off_
        slate` — one predicate for both carve-outs);
      · the basis must be RECOVERABLE and a price (`venue_close.replayed_basis`); an unknown/reset/
        out-of-(0,1) answer leaves the book on the flatten list, because a seeded position with a
        wrong basis is a wrong number on every P&L line and on the mark tripwire.
    """
    if (plan.action != "recover" or plan.cancel_order_ids or state is None
            or venue_positions is None):
        return {}, []
    venue = {str(k): _dec(v) for k, v in venue_positions.items()}
    out: dict[str, tuple[Decimal, Decimal]] = {}
    skipped: list[str] = []
    for slug, _q in plan.flatten:
        rec = _dec(state.inventory.get(slug, _ZERO))
        held = venue.get(slug, _ZERO)
        if not is_carryable_off_slate(rec, held):
            skipped.append(f"{slug}: record {rec} ≠ venue {held}")
            continue
        px, why = basis_of(slug, rec)
        if px is None or not (_ZERO < _dec(px) < _ONE):
            skipped.append(f"{slug}: no usable basis ({why})")
            continue
        out[slug] = (rec, _dec(px))
    return out, skipped
