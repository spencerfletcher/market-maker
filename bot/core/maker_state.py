"""
bot/core/maker_state.py
───────────────────────
Crash-durable maker state, and the recovery decision a SEPARATE process makes from it.

DESIGNED FOR SIGKILL, WHICH IS THE ONLY DEATH THAT MATTERS HERE. SIGKILL is the one death that
always strands live orders, and on a memory-constrained host it is also the death that actually
happens. SIGKILL runs no `finally`, no `atexit`, no signal handler — so a design where cleanup
writes the record is a design that writes nothing precisely when it counts. Two consequences shape
everything below:

  1. **State is durable BEFORE it is needed, not after.** An order intent is fsync'd to disk before
     the order is sent, because the uncoverable window is "sent, then died before the response
     arrived". Recording after the venue answers leaves that window with no trace at all.
  2. **Recovery is a different process.** `assess_recovery` is a pure function over
     (durable state, venue orders, venue positions), and a separate recovery tool runs it. The
     dying process cannot participate.

The `clean_exit` flag is written PESSIMISTICALLY: `begin_run` sets it False, `end_run` sets it
True. A crash therefore leaves the correct value on disk *by doing nothing*, which is the only
behaviour SIGKILL reliably permits. Never invert this.

TWO RULES GOVERN THE RECOVERY DECISION
──────────────────────────────────────
**Cannot-verify is not flat** — the position reconciler learned this the hard way: its venue read
returned `[]` on an unparsed response, so it reported "confirmed flat" every poll for a month while
we held positions. A venue read that fails is `None` here and always refuses; an empty list
means confirmed-flat and is allowed to start. Never collapse those two.

**Cancelling is safe, flattening is not.** A cancel only removes exposure, so even an
unattributable resting order is listed for automatic cancellation — including under a `refuse`,
because a refusal must not leave live orders resting overnight. A flatten MOVES MONEY against a
basis we may not own, so a venue position with no matching record halts for an operator instead,
exactly as the reconciler treats unknown exposure.

⚠️ SCOPE OF THE LOSS CAP HERE. This makes the cap SURVIVE PROCESS DEATH — today it is a local
variable, so a crash-restart loop silently resets the budget to zero every crash and bounds
nothing. It does **not** make the cap's INPUT correct: the maker's marked-P&L input is a separate,
still-open defect — a real venue-confirmed LOSS has been fed to the cap as a PROFIT, sign and all.
Whatever number the process computes is what persists, correct or not.
"""
from __future__ import annotations

import logging
import os
import time
import dataclasses
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence

from bot.core import durable
from bot.core.money import CENTICENT

log = logging.getLogger(__name__)

DEFAULT_PATH = durable.repo_path("logs", "maker_state.json")

_ZERO = Decimal(0)

# Below venue wire resolution is not a position. Both venues report holdings to 2dp and quote to
# the centicent, so anything under that is residue. Same compare-to-zero class as the crossed-book
# float dust — and here a false positive would REFUSE TO START, forever, over nothing.
_DUST = CENTICENT


class PriorRunUnresolved(Exception):
    """A previous run died holding orders and nothing has resolved it yet.

    Raised by `begin_run` so a fresh run cannot erase the crash record it needs. The maker catches
    this and refuses to start, pointing at the VENUE-APPROPRIATE remedy — the recovery tool for a
    Kalshi record, the cancel/close tools for a Poly one — branched on `poly_names`.
    """


class LegacyStateUnclassifiable(Exception):
    """The shared-file record's run_id matches no venue prefix — refuse, never guess.

    Guessing routes a crash record to the wrong venue's tooling, which is the exact
    certified-false-clean failure the venue split exists to end."""


class LegacyStateLive(Exception):
    """The shared-file record's process appears ALIVE — migrating underneath it would split one
    run's writes across two files. If you are certain no maker is running, the pid has been
    recycled: resolve the crash record first (cancel its strays on the venue), then retry."""


# ── the per-venue state-path split ───────────────────────────────────────────
# Both makers shared one file; venue was inferred from the record's CONTENTS (name shapes),
# which left a no-names record unclassifiable and let one venue's preflight certify the
# other's crash record. Post-split, venue is decided by WHICH FILE a store opens — the
# heuristics become defense-in-depth instead of the primary discriminator.

VENUES = ("kalshi", "poly")
# The Poly maker stamps `polymm-…` (both the plain `polymm-<ts>-<hex>` and the mode-stamped
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
        # Crash-window repair: a crash between the venue-file write and the legacy rename
        # leaves BOTH readable with the same record — the watchdog then double-reports it and
        # the recovery tool's legacy fallback re-sees a migrated record. Finish the rename.
        # NO pid check here ON PURPOSE: equal run_ids in both files can only be produced by an
        # adopting process that died between its write and its rename — a LIVE maker writes
        # `base` alone (vpath would carry a DIFFERENT run, failing the equality below), and its
        # adoption was already refused by LegacyStateLive. Adding a pid check here would
        # reintroduce the recycled-pid deadlock; the equality guard is what does the real work.
        if os.path.exists(base):
            try:
                v_raw = durable.read_json_strict(vpath)
                b_raw = durable.read_json_strict(base)
            except durable.StateCorrupt:
                return                      # surfaced by the corrupt-handling paths, not here
            # v2-aware [review nit 1]: once the venue file is v2 its run_id lives in lanes.main,
            # so a top-level read is None and the repair never fires — the legacy file then
            # lingers, double-reported by the watchdog forever.
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


# ── lanes: one SHARED per-venue ledger, multiple concurrent runs — chosen over per-process
#    files so the loss ratchet stays ACCOUNT-WIDE ───────────────────────────────────────────────
#
# Schema v2: `{"schema": 2, "lanes": {"main": <record>, "park": <record>, …}}`. A legacy flat
# record (schema-less, `run_id` at top level) reads as `lanes = {"main": <record>}` and is
# rewritten as v2 on the first mutation. Each store binds to ONE lane: `_raw` is that lane's
# record and every mutation method is unchanged; `_write()` re-reads the file under an exclusive
# flock, replaces ONLY its own lane's subtree, and durable-writes — so two processes in
# DIFFERENT lanes never clobber each other. Two processes in the SAME lane still last-writer-win:
# that is the slate-collision case, which `assess_recovery` refuses at startup, not something
# the file layer can referee.

DEFAULT_LANE = "main"


class StateLocked(RuntimeError):
    """The shared ledger's lock could not be acquired within the bound. Raised rather than
    blocking forever: an un-timed LOCK_EX inside the asyncio loop would freeze the whole maker
    (no quotes, no cancels, no kill-switch check) behind a HUNG sibling — a crash releases the
    flock instantly, but a SIGSTOP or a stalled fsync does not. A raise lands in the shim's
    finally → teardown, the safe direction."""


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
    Empty/absent → {}. A non-empty legacy record becomes `{"main": record}` — the historical
    single-runner is by definition the main lane.

    ⛔ FAIL-CLOSED on a malformed v2: a document that
    CLAIMS the schema (`"schema"` present) but whose lanes are not all dicts is the
    SIGKILL-mid-write artefact, and reading it as "fewer lanes" silently deletes a lane — its
    carried loss, its slate claim, its crash record. Corruption raises here exactly as it does
    at the file layer; a filter (`if isinstance(v, dict)`) is the fail-open shape this module's
    own header forbids."""
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


def account_carried(path: str | None = None) -> Decimal:
    """Σ realized_pnl across ALL lanes of a venue file (SIGNED — for reporting)."""
    return sum((s.realized_pnl for s in load_all_lanes(path).values()), _ZERO)


def account_loss(path: str | None = None) -> Decimal:
    """Σ of per-lane LOSS-TO-DATE (each floored at zero) — the number the lifetime ratchet
    prices budgets from. ⛔ NOT the signed sum: netting a profitable lane against a losing
    one would let one lane's profit BUY another
    lane's loss budget, the exact "profit earns nothing back" rule the cap already enforces
    within a lane. One account, one ratchet, losses only."""
    return sum((s.loss_to_date for s in load_all_lanes(path).values()), _ZERO)


def venue_account_loss(venue: str, base_path: str | None = None) -> Decimal:
    """Account-wide loss for a venue, with the LEGACY fallback: if the venue file does not
    exist yet but the legacy shared file classifies to this venue, ITS loss counts — otherwise
    a park lane launched before the first adoption prices its budget off an EMPTY ledger while
    a real carried loss sits in the legacy file."""
    base = base_path or DEFAULT_PATH
    vpath = venue_path(base, venue)
    if os.path.exists(vpath):
        return account_loss(vpath)
    st = load_state_for_venue(venue, base_path=base)
    return st.loss_to_date if st is not None else _ZERO


def live_sibling_lanes(path: str | None, own_lane: str) -> dict[str, "MakerState"]:
    """Sibling lanes that are LIVE — an unclean exit, maybe-live orders, or recorded inventory.
    A cleanly-exited, flat, order-free lane record is history, not a sibling: scoping (and the
    solo main run's account-global gate) keys on THIS, never on the mere existence of a lane
    key, because lane records are never deleted — an existence test latches scoping ON forever
    after a single one-off probe lane."""
    out: dict[str, MakerState] = {}
    for lane, s in load_all_lanes(path).items():
        if lane == own_lane:
            continue
        if (not s.clean_exit) or s.maybe_live_orders or any(
                _is_real(q) for q in s.inventory.values()):
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
    """Read-only counterpart for tools (watchdog, recover, preflights): prefers the venue
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
    realized_pnl: Decimal                # signed; negative = loss
    loss_cap: Decimal                    # 0 = disabled
    exit_status: str | None = None

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
        """Realized LOSS, floored at zero. A profitable run does not earn extra budget."""
        return max(_ZERO, -self.realized_pnl)

    @property
    def cap_breached(self) -> bool:
        return self.loss_cap > _ZERO and self.loss_to_date > self.loss_cap


def poly_names(state: MakerState) -> tuple[str, ...]:
    """The record's POLY-shaped names, sorted (truthiness = 'this record touches Poly').

    Poly slugs carry lowercase; Kalshi tickers never do — checked across the whole log corpus
    and differentially tested over every recorded record shape. ONE copy on purpose: this
    heuristic gates several venue-branched operator remedies (begin_run's refusal, the
    watchdog's problem strings, the recovery tool's certification guard), and three inline
    copies of a safety heuristic is how a correction reaches only one of them."""
    names = set(state.tickers) | set(state.inventory) | {
        str(o.get("ticker") or "") for o in state.orders.values()}
    return tuple(sorted(n for n in names if n and any(ch.islower() for ch in str(n))))


def _to_state(raw: Mapping[str, Any]) -> MakerState:
    inv = {str(k): _dec(v) for k, v in (raw.get("inventory") or {}).items()}
    return MakerState(
        run_id=str(raw.get("run_id") or ""),
        pid=int(raw.get("pid") or 0),
        mode=str(raw.get("mode") or ""),
        started_ts=float(raw.get("started_ts") or 0.0),
        updated_ts=float(raw.get("updated_ts") or 0.0),
        clean_exit=bool(raw.get("clean_exit")),
        tickers=tuple(str(t) for t in (raw.get("tickers") or ())),
        orders={str(k): dict(v) for k, v in (raw.get("orders") or {}).items()
                if isinstance(v, dict)},
        inventory={k: v for k, v in inv.items() if _is_real(v)},
        realized_pnl=_dec(raw.get("realized_pnl")),
        loss_cap=_dec(raw.get("loss_cap")),
        exit_status=raw.get("exit_status"),
    )


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
                      f"overwrite; run scripts/maker_recover.py before starting.")
            raise

    # ── run lifecycle ────────────────────────────────────────────────────────────────────────

    def begin_run(self, run_id: str, *, mode: str, loss_cap: Decimal,
                  tickers: Iterable[str], inventory: Mapping[str, Any],
                  allow_unresolved: bool = False) -> None:
        """Open a run. `clean_exit=False` is written FIRST and cleared only by `end_run`.

        ⛔ `inventory` is REQUIRED and is THE RECOVERY-VERIFIED VIEW. It supersedes the old
        wholesale copy of the prior run's map, which is how a dead run's BELIEF rode into a
        relaunch's record with no venue standing behind it. The
        caller passes what its recovery path certified: accepted carries → exactly the
        declared quantities (a flat record over live contracts is the dangerous case — the
        carry path must never seed empty); clean or unclean-but-flat start → {} (the
        venue said flat; a copied belief would contradict the verdict that authorized the
        start); the Kalshi maker → {} always (its inventory is a DELTA from a venue
        baseline read, so pre-existing positions are baseline to ignore, and seeding them
        would double-count). Required keyword-only so no caller can silently fall back to
        the old copy.

        Carries `realized_pnl` forward from any prior run: the cap is a lifetime ratchet across
        processes, which is the entire point (see `is_exec_cost_cap_hit`'s "no auto-resume").

        ⛔ REFUSES to clobber an UNRESOLVED prior crash. This call resets the order list, so if the
        previous run died holding orders, proceeding would destroy the only record of what is
        resting on the venue — converting a recoverable crash into an invisible one, which is
        strictly worse than the crash. `allow_unresolved=True` is the deliberate override.
        """
        prior = _to_state(self._raw) if self._raw else None
        # ⛔ SAME-LANE LIVE PROCESS: an open record whose pid
        # is STILL ALIVE is a running maker, and two writers on one lane last-writer-win the
        # ledger (losses silently understated). Refused regardless of the order count — a maker
        # between cancel and next place has zero recorded orders and is no less alive. No
        # allow_unresolved override: that flag acknowledges a CRASH, not a live sibling.
        if (prior is not None and not prior.clean_exit
                and prior.pid != os.getpid() and _pid_alive(prior.pid)):
            raise PriorRunUnresolved(
                f"lane {self.lane!r}: record {prior.run_id!r} (pid {prior.pid}) belongs to a "
                f"maker that appears to be STILL RUNNING — two processes must not share a "
                f"lane. Use a different --lane, or stop that process first. (If you are "
                f"certain it is dead, the pid was recycled; wait for it or clear deliberately.)")
        if (prior is not None and not allow_unresolved
                and not prior.clean_exit and prior.maybe_live_orders):
            # The remedy is VENUE-SPECIFIC and this module serves both makers. Pointing a
            # POLY operator at the Kalshi-only recovery tool sends them to something that was
            # demonstrated certifying a false clean from the WRONG venue; the venue heuristic
            # lives ONCE, in `poly_names`.
            is_poly = bool(poly_names(prior))
            # ⚠️ This message has NO refused-count context (the record does not say WHY the
            # sweep failed): an unattributable order — listed, but no readable id — leaves
            # the same open record, so the terminal-but-listed ghost may be offered only as
            # a POSSIBILITY here, never asserted the way the teardown's gated copy can.
            # The preflight's id-extraction silently drops id-less entries too, so a prior
            # "venue confirms flat" plan does NOT rule the unattributable case out.
            remedy = (
                "confirm what is actually resting with `.venv/bin/python -m "
                "scripts.poly_us_orders` (read-only, the same credentials that placed the "
                "orders — never the UI alone, which a wrong-account login renders blind); "
                "cancel strays by hand and close positions with `scripts.poly_close`. If it "
                "shows nothing resting and positions are flat, the prior teardown may have "
                "hit the terminal-but-listed case (its listing carried an already-dead "
                "order) — but an unattributable or unreadable listing leaves the SAME "
                "record, so treat ghost as a possibility to confirm, not a diagnosis. "
                "Do NOT use scripts.maker_recover — it is "
                "Kalshi-only and reads the wrong venue." if is_poly else
                ".venv/bin/python -m scripts.maker_recover --execute "
                "--i-understand-real-money")
            raise PriorRunUnresolved(
                f"the previous maker run {prior.run_id!r} never recorded an exit and left "
                f"{prior.maybe_live_orders} order(s) that may still be RESTING on the venue. "
                f"Starting now would erase that record. Resolve it first:\n"
                f"  {remedy}\n"
                f"(or, once the venue is confirmed flat, delete {self.path} — ⚠️ that also "
                f"discards the durable loss ledger; note realized/loss_to_date first)")
        self._raw = {
            "run_id": str(run_id),
            "pid": os.getpid(),
            "mode": str(mode),
            "started_ts": time.time(),
            "clean_exit": False,
            "exit_status": None,
            "tickers": [str(t) for t in tickers],
            "orders": {},
            # The verified view, normalized exactly as set_inventory writes (zeros drop).
            "inventory": {str(k): str(_dec(v)) for k, v in inventory.items()
                          if _is_real(_dec(v))},
            "realized_pnl": str(_dec(self._raw.get("realized_pnl"))),
            "loss_cap": str(loss_cap),
        }
        self._write()

    def end_run(self, status: str = "clean") -> None:
        """Record a real exit. Only reachable when cleanup runs — so its ABSENCE is the signal."""
        self._raw["clean_exit"] = True
        self._raw["exit_status"] = str(status)
        self._write()

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
        BELIEF (`self.inventory`), not a venue read — this docstring claimed "the latest VENUE
        read" for a month and the mislabel became load-bearing when reporting started quoting
        it. Zeros are dropped — a zero position is not a position,
        and keeping them makes `unknown` comparisons noisy."""
        self._raw["inventory"] = {str(k): str(_dec(v)) for k, v in inv.items()
                                  if _is_real(_dec(v))}
        self._write()

    def add_realized(self, delta: Decimal) -> Decimal:
        """Fold a realized P&L delta (signed; negative = loss) into the durable running total."""
        total = _dec(self._raw.get("realized_pnl")) + _dec(delta)
        self._raw["realized_pnl"] = str(total)
        self._write()
        return total

    def snapshot(self) -> MakerState:
        return _to_state(self._raw)

    def _write(self) -> None:
        """Merge THIS LANE's record into the shared file under an exclusive lock.

        Read-modify-write, re-reading the file each time so another lane's writes since our last
        one are preserved — the ctor's snapshot of other lanes would be stale within seconds of a
        sibling process filling. A corrupt current file REFUSES (raises) rather than clobbering:
        overwriting it would erase the crash evidence, same rule as the ctor. Missing file → we
        create the v2 shape. Own-lane concurrent writers are NOT defended here (see the lane
        design note: that is the slate-collision case, refused at startup)."""
        self._raw["updated_ts"] = time.time()
        with _FileLock(self.path):
            current: Any = None
            if os.path.exists(self.path):
                current = durable.read_json_strict(self.path)   # StateCorrupt propagates — never clobber
            lanes = _doc_lanes(current)
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
    removes exposure. `flatten` may be executed only under `recover`. `unknown` is never acted on
    automatically.
    """
    action: str
    reason: str
    detail: str
    cancel_order_ids: tuple[str, ...] = ()
    flatten: tuple[tuple[str, Decimal], ...] = ()
    unknown: tuple[tuple[str, Decimal], ...] = ()

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

    `venue_orders=None` / `venue_positions=None` mean CANNOT VERIFY and are checked FIRST: without
    venue truth we can neither start safely nor clean up correctly, so there is nothing else worth
    evaluating. `[]` / `{}` mean confirmed flat and are a different answer entirely.

    ⛔ SLATE SCOPING — the prerequisite for running two isolated lanes at once. With `slate=None` the
    assessment is ACCOUNT-GLOBAL — byte-identical to the historical behaviour, and the default so
    no caller changes semantics by upgrading. With a slate (this run's slugs) + `other_lanes`
    (sibling lanes' snapshots from the SHARED per-venue ledger, own lane excluded):

      · SLATE COLLISION refuses first: a sibling lane whose record claims any of OUR slugs
        (inventory, recorded orders, or — for an uncleanly-exited lane — its whole slate) means
        two processes would quote one book. Nothing else is worth evaluating past that.
      · ORDERS are partitioned by the order's own market slug. Only OUR slate's orders enter the
        cancel plan — planning to cancel a sibling's live order is how "recovery" becomes an
        attack on a healthy run. Foreign and slug-unreadable orders are REPORTED, never
        cancelled (the teardown-sweep rule: we cannot establish they are ours).
      · POSITIONS on our slate keep the exact unknown/attributable logic below. Foreign-slug
        positions never refuse: one attributed to a sibling lane's record is that lane's
        business (info); one attributed to NO lane is an ORPHAN — reported LOUDLY in the plan
        detail, because with scoping nobody's start would ever block on it again.
      · THE CAP RATCHET STAYS ACCOUNT-WIDE: any sibling lane's breached cap refuses this start
        too — one account, one ratchet.
    """
    if venue_orders is None or venue_positions is None:
        which = "orders" if venue_orders is None else "positions"
        return RecoveryPlan(
            "refuse", "cannot_verify",
            f"could not read venue {which} — CANNOT VERIFY. An unreadable venue is not a flat "
            f"venue; starting now could double an inherited position or requote against a book "
            f"we are already resting in.")

    notes: list[str] = []
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
                         f"sibling run owns them (scripts.poly_us_orders).")
        venue_orders = scoped
        foreign_pos = {t: q for t, q in positions.items()
                       if t not in slate_set and _is_real(q)}
        positions = {t: q for t, q in positions.items() if t in slate_set}
        if foreign_pos:
            # ⛔ OUR OWN lane's record explaining an off-slate position is NOT an orphan and NOT
            # a sibling's business — it is a position WE hold that this run's slate would leave
            # unmanaged. Pre-scoping that was attributable→recover; scoped it must refuse, same
            # remedy as the adopt gate: put it on --slugs or flatten it first.
            # Sign-AGNOSTIC on purpose: a venue −5 against our recorded +20
            # is a belief-venue DISAGREEMENT — strictly worse than a matching sign, and the one
            # case a sign-equality filter would have demoted to an orphan note.
            own_off_slate = sorted(
                t for t, q in foreign_pos.items()
                if state is not None and _is_real(state.inventory.get(t, _ZERO)))
            if own_off_slate:
                return RecoveryPlan(
                    "refuse", "own_position_off_slate",
                    f"THIS lane's record holds {own_off_slate}, which is not on this run's "
                    f"slate — starting would leave our own position unmanaged. Add it to "
                    f"--slugs (or --carry), or flatten it first.")
            attributed, orphans = [], []
            for t, q in sorted(foreign_pos.items()):
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
                    f"sibling lane {lane_name!r} has breached its ${ls.loss_cap} cap (realized "
                    f"{ls.realized_pnl}). ⚠️ SCOPE: this scan runs only on SCOPED "
                    f"starts (a pair in play); the always-on enforcement is the ENGINE's live "
                    f"account-loss read vs THIS run's cap, every check. Clear deliberately.")

    cancel = _venue_order_ids(venue_orders)
    live = {t: q for t, q in positions.items() if _is_real(q)}

    plan = _assess_scoped(state, cancel, live)
    if notes:
        plan = dataclasses.replace(plan, detail=plan.detail + "\n  " + "\n  ".join(notes))
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
            # unknown BY DEFINITION, whatever its sign. The sign clause below read a NEGATIVE
            # dust-sized no-record position as attributable and planned an AUTO-FLATTEN
            # against a basis we do not own. Sign is not a licence: no record means no basis.
            unknown.append((ticker, qty))
            continue
        # `venue > known` is unknown exposure; `venue < known` is ordinary settlement/partial fill
        # and is NOT escalated — the same one-directional call the position reconciler makes, for the same
        # false-positive-storm reason. ⛔ But the shrink exemption applies only when the SIGNS
        # AGREE: a venue −12 against a recorded +12 passes a magnitude-only check, and the record
        # then describes the OPPOSITE of what the venue holds — a basis that is fiction, exactly
        # as unknown as a position with no record at all. This is not hypothetical: a belief of
        # −N against a venue +N has reached the recovery tool for real.
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
            f"durable realized loss ${state.loss_to_date} has breached the ${state.loss_cap} cap. "
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
    """CONTINUOUS OPERATION: convert a `recover` plan whose ONLY cleanup is flattening
    positions the operator has EXPLICITLY DECLARED as carries into a start — so a long-horizon
    position stops paying the measured rejoin tax (flatten + re-buy, back of the queue) nightly.

    Fail-closed on every mismatch, because a wrong carry is a wrong BASIS on real money:
      · any resting venue order → the plan stands (a carry declares positions, never orders);
      · a declared book the venue does not hold, or holds at ANY other quantity or sign →
        REFUSE outright (the declaration is stale — the operator is describing a venue that
        no longer exists, and every other declaration is now suspect);
      · a live venue book NOT declared → the plan stands for it (undeclared exposure keeps
        the original flatten/refuse verdict — a carry is per-book and explicit);
      · an `unknown_position` refusal is NEVER converted (no durable record = no basis to
        carry; the operator's number cannot substitute for a record we do not hold).
    Quantities compare EXACTLY (Decimal, sign included). `carries` maps slug → qty as
    declared on the CLI; the basis travels separately to the engine and is not this
    function's concern — this function only decides whether starting is LEGAL.
    """
    if plan.action != "recover" or not carries:
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
    # ⛔ CARRY THE ORIGINAL PLAN'S NOTES FORWARD: the scoped
    # assessment appends orphan/foreign warnings to plan.detail, and carries are the NORMAL
    # start mode — a fresh detail string here silently deleted the one printing of an orphan.
    inherited = ""
    if "\n  " in plan.detail:
        inherited = "\n  " + plan.detail.split("\n  ", 1)[1]
    if flatten:
        remaining = ", ".join(f"{s}:{q}" for s, q in flatten.items())
        return RecoveryPlan(
            "recover", "live_venue_state",
            f"carries accepted for {sorted(carries)} but the venue still holds UNDECLARED "
            f"positions ({remaining}) — declare or flatten them, then re-assess." + inherited,
            flatten=tuple(flatten.items()))
    return RecoveryPlan(
        "start", "carried_inventory",
        f"clean start WITH declared carries: {sorted(carries)} match the venue exactly; "
        f"no resting orders; the engine seeds these as inventory with the declared basis. "
        f"A book at or above its cap starts REDUCE-ONLY until it works down — expected."
        + inherited)


def assess_recovery_from_disk(
    path: str | None,
    venue_orders: Sequence[Any] | None,
    venue_positions: Mapping[str, Any] | None,
) -> RecoveryPlan:
    """`assess_recovery`, reading the durable record itself. A CORRUPT record refuses — it is the
    SIGKILL-mid-write artefact, and "unparseable" must never degrade to "no prior run"."""
    try:
        state = load_state(path)
    except durable.StateCorrupt as exc:
        return RecoveryPlan(
            "refuse", "state_corrupt",
            f"the durable maker state is unreadable ({exc}). That is what a SIGKILL mid-write "
            f"leaves behind, so it must not be read as 'no prior run'. Inspect the venue by hand "
            f"before restarting.",
            cancel_order_ids=_venue_order_ids(venue_orders or ()))
    return assess_recovery(state, venue_orders, venue_positions)
