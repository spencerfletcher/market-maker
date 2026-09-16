"""
bot/core/fill_replay.py
───────────────────────
The READ-ONLY tape / manifest / basis-chain helpers behind `bot/core/venue_close.py`'s basis
replay, moved VERBATIM out of `scripts/poly_teardown_row.py`, `scripts/poly_night.py` and
`scripts/poly_mark_trip_episodes.py` [2026-09-15] so the maker's import closure stays inside
`bot/`. Each of those scripts re-imports its names from here — same objects, same call paths.

Nothing here selects markets, sizes a seat or builds a slate. `manifest_slugs`/`hot_slate_adds`
read which books a run DECLARED (the manifest `slugs` column plus the hot-slate change tape) so
`basis_chain` can find a run that started FLAT on a book; that is bookkeeping, not selection.

⛔ Module constants (`DEFAULT_FILLS`, `HOT_SLATE_CHANGES`) are cwd-relative and resolved at CALL
time; the test sandbox (`tests/conftest.py`) redirects THIS module's binding, not only the
scripts' re-exported copies.
"""
from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping, Sequence

from bot.core import run_manifest
from bot.core import tape_paths as _tape_paths   # module; `tape_paths` below is this file's own fn
from bot.core.money import dec_or_none
from bot.core.run_manifest import HEADER as RUNS_HEADER
from bot.poly_us.maker import fill_accounting

_ZERO = Decimal(0)
_ONE = Decimal(1)

#: The hot-slate change tape. ⛔ Resolved at CALL time through `hot_slate_adds`' `None` default,
#: never bound as an argument default — the test sandbox redirects module constants.
HOT_SLATE_CHANGES = os.path.join("logs", "hot_slate_changes.csv")
DEFAULT_FILLS = os.path.join("logs", "poly_live_mm_fills.csv")
#: `--carry` basis precision: 4dp, matching the tape's own prices and every carry this repo has
#: declared. `derive_carries` range-checks the QUANTIZED value, because that is what ships.
_BASIS_QUANTUM = Decimal("0.0001")


# --------------------------------------------------------------------------- tapes

def tape_paths(live_csv: str) -> list[str]:
    """The live tape plus its rotated siblings. ⚠️ Rotation-folding is not optional: the live
    fills file holds ~59% of rows and a live-file-only read undercounts, sometimes by 99%.
    Columns are mapped BY HEADER per file (`csv.DictReader`), so a schema bump still folds.

    ⛔ …AND EVERY PER-LANE TAPE [2026-08-27]. `live + rotated` names a FROZEN legacy file since
    the maker's tapes went per-lane, so a probe-lane teardown would print UNKNOWN/zero for every
    cell. `tape_paths.fold_paths` is the one fold rule; the caller's own path stays in the list
    even when absent, because `load_tape` reads its presence as the fail-closed signal."""
    return _tape_paths.fold_paths(live_csv) or [live_csv]


def real_manifest_rows(path: str | None = None) -> list[dict]:
    """Every REAL-money manifest row, oldest first."""
    p = path or run_manifest.DEFAULT_PATH
    try:
        with open(p, newline="") as fh:
            rows = [r for r in csv.DictReader(fh) if (r.get("mode") or "") == "real"]
    except OSError:
        return []
    rows.sort(key=lambda r: float(r.get("started_ts") or 0))
    return rows


def load_all_fills(paths: list[str]) -> list[dict]:
    """EVERY fill row across the live tape and its rotated siblings, ts-ordered.

    ⛔ NOT run-scoped [mm-review round 2, B1]. `teardown_row.load_tape` filters by `run_id`, right
    for a teardown ledger row and wrong here: the replay window is a TIME window, and a run whose
    manifest write failed has fills on this tape and no run row to match.

    Rows are mapped BY HEADER per file (`csv.DictReader`), so a schema bump still folds; the fills
    tape has six frozen `pre-<tag>` siblings whose columns differ, and the two oldest carry no
    `run_id` column at all.
    """
    out: list[dict] = []
    for path in paths:
        if not os.path.exists(path):
            continue
        try:
            with open(path, newline="") as fh:
                out += [dict(r) for r in csv.DictReader(fh)]
        except OSError:
            # Unreadable files are REFUSED upstream by `_tape_readability`, which runs before
            # this. Skipping here would be the silent-truncation path that check exists to
            # close, so this branch is unreachable in `main` and deliberately not a fallback.
            continue
    out.sort(key=lambda r: float(dec_or_none(r.get("ts")) or 0))
    return out


@dataclass(frozen=True)
class TapeReadability:
    present: bool
    unreadable: tuple[str, ...]


def _tape_readability(paths: Sequence[str]) -> TapeReadability:
    """Which tape files exist, and which exist but cannot be READ.

    ⛔ THE DISTINCTION IS THE POINT. `teardown_row.load_tape` sets
    `present = True` from `os.path.exists` and then swallows `OSError` per file, so an
    existing-but-unreadable rotated tape contributes zero rows while the presence flag still says
    the tape is there. The replay then walks a TRUNCATED history — and if the dropped fills net to
    zero inventory change, the quantity guard does not fire and a wrong `avg_entry` is emitted as a
    `--carry`. Absence is loud; unreadability was silent.
    """
    present = False
    bad: list[str] = []
    for path in paths:
        if not os.path.exists(path):
            continue
        present = True
        try:
            with open(path, "rb") as fh:
                fh.read(1)
        except OSError:
            bad.append(os.path.basename(path))
    return TapeReadability(present=present, unreadable=tuple(bad))


# ------------------------------------------------------------------- price-realized

@dataclass
class BookOut:
    realized: Decimal = _ZERO
    trips: int = 0
    end_inv: Decimal = _ZERO
    #: The replayed `avg_entry` at the end of the walk — the maker's OWN basis semantics, gross
    #: of fees, built from `order.price`. Reported beside `end_inv` so a caller needing a carry
    #: BASIS (`scripts/poly_night.py`) reads it out of this one walk of `fill_accounting`
    #: instead of writing a second replay that could drift from this one.
    avg_entry: Decimal = _ZERO
    fills: int = 0
    contracts: Decimal = _ZERO
    per_trip: list[tuple[Decimal, Decimal]] = field(default_factory=list)


def price_realized(fill_rows: list[dict],
                   carries: dict[str, tuple[Decimal, Decimal]]) -> dict[str, BookOut]:
    """Per book, replay the run's own fills through the SHIPPED `fill_accounting` and
    accumulate realized **per reducing fill** — the exact form `bot/poly_us/maker.py` feeds the
    durable loss cap (`fill_pnl = (completed[0] - prev_rt) if completed is not None else
    (rt_r - prev_rt)`).

    ⛔ Do NOT "simplify" this to Σ over completed trips. On any book that ends on a fractional
    or partial residue the trip never completes and the per-trip sum silently reports ~0 —
    measured **+<n> against a true −<n>** on arm B. Σ per-fill deltas equals the per-trip
    sum whenever trips DO complete, so this is strictly the same notion of realized, learned
    earlier.

    `carries` SEEDS the position from the manifest's own `carries` column — starting a carried
    book flat pairs its buy-backs against phantom opens (the hormuz fiction: 6 real trips read
    as 9). Passive fills execute AT our resting price, so the row's `price` is the execution
    price, matching `fill_accounting`'s own contract."""
    out: dict[str, BookOut] = {}
    inv: dict[str, Decimal] = {}
    avg: dict[str, Decimal] = {}
    rt_r: dict[str, Decimal] = {}
    rt_c: dict[str, Decimal] = {}
    for slug, (qty, basis) in carries.items():
        out.setdefault(slug, BookOut())
        inv[slug] = qty
        avg[slug] = basis if qty != _ZERO else _ZERO
    for r in sorted(fill_rows, key=lambda r: float(dec_or_none(r.get("ts")) or 0)):
        slug = r.get("slug") or ""
        px = dec_or_none(r.get("price"))
        qty = dec_or_none(r.get("filled_qty"))
        side = r.get("side")
        if px is None or qty is None or qty <= _ZERO or side not in ("bid", "ask"):
            continue
        bk = out.setdefault(slug, BookOut())
        bk.fills += 1
        bk.contracts += qty
        prev_rt = rt_r.get(slug, _ZERO)
        new_inv, new_avg, r_r, r_c, completed = fill_accounting(
            inv.get(slug, _ZERO), avg.get(slug, _ZERO), prev_rt, rt_c.get(slug, _ZERO),
            side, px, qty)
        inv[slug], avg[slug], rt_r[slug], rt_c[slug] = new_inv, new_avg, r_r, r_c
        bk.realized += (completed[0] - prev_rt) if completed is not None else (r_r - prev_rt)
        if completed is not None:
            bk.trips += 1
            bk.per_trip.append(completed)
    for slug, bk in out.items():
        bk.end_inv = inv.get(slug, _ZERO)
        bk.avg_entry = avg.get(slug, _ZERO)
    return out


def parse_carries(raw: str) -> dict[str, tuple[Decimal, Decimal]]:
    """The manifest's `carries` column: `[{"slug":…,"qty":…,"basis":…}]`. A basis is the
    yes-space entry price the launch declared — never the venue UI's complement collateral."""
    if not raw:
        return {}
    try:
        items = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    out: dict[str, tuple[Decimal, Decimal]] = {}
    for it in items or []:
        q, b = dec_or_none(it.get("qty")), dec_or_none(it.get("basis"))
        if it.get("slug") and q is not None and b is not None:
            out[it["slug"]] = (q, b)
    return out


def hot_slate_adds(run_id: str, changes_path: str | None = None) -> set[str]:
    """Books this run ADDED mid-flight, from `logs/hot_slate_changes.csv` [continuous maker P0].

    ⛔ THE MANIFEST CANNOT KNOW THEM. It is written once at launch, and a day-long child changes
    its book set at runtime — so without this every hot add reads as a slug the manifest never
    declared and `slug_crosscheck` ⛔-flags a healthy run's whole fills column.

    A missing tape is an empty set: a run with no adds and a run whose tape is gone both leave
    the declared slate exactly as the manifest wrote it, and the cross-check's own
    manifest-only/tape-only split is what separates them.
    """
    path = changes_path if changes_path is not None else HOT_SLATE_CHANGES
    out: set[str] = set()
    if not run_id or not os.path.exists(path):
        return out
    try:
        with open(path, newline="") as fh:
            for row in csv.DictReader(fh):
                if (row.get("run_id") == run_id and row.get("action") == "add"
                        and (row.get("slug") or "").strip()):
                    out.add(row["slug"].strip())
    except OSError:
        return out
    return out


def manifest_slugs(m: dict, changes_path: str | None = None) -> set[str]:
    """The slate the run DECLARED — the manifest's `slugs` column (JSON list, written by
    `run_manifest.write_run_row`) UNIONED with the hot-slate adds for the same run_id. An
    unreadable/absent column with no adds returns empty, which the cross-check reads as "cannot
    compare" rather than "the slate was empty"."""
    added = hot_slate_adds(str(m.get("run_id") or ""), changes_path)
    raw = (m.get("slugs") or "").strip()
    if not raw:
        return added
    try:
        items = json.loads(raw)
    except (ValueError, TypeError):
        # tolerate a hand-edited comma list rather than silently claiming no slate
        return {s.strip() for s in raw.split(",") if s.strip()} | added
    if isinstance(items, str):
        return {items} | added
    return {str(s) for s in (items or []) if s} | added


# 7. assembly
# ─────────────────────────────────────────────────────────────────────────────
#: The manifest's carry JSON keys, as `scripts/poly_live_mm` writes them (`carries=[{"slug": …,
#: "qty": …, "basis": …}]`). ⛔ A LIST of objects, not a slug→spec map: the first cut of this
#: reader assumed a map, and the real manifest raised on the very first row it met. Pinned in
#: `tests/test_poly_mark_trip_episodes.py` against the launcher's own literal.
CARRY_KEYS = ("slug", "qty", "basis")
#: ⛔ PROVENANCE, ADDED 2026-09-03 — `bot/core/run_manifest.CARRY_ENTRY_KEYS`. Absent on every
#: row written before that build, and absent READS AS `"declared"`: an operator's number, which
#: no later basis replay may anchor on.
CARRY_SOURCE_KEY = "source"


def load_carries(runs_path: str, run_id: str) -> dict:
    """`{slug: (qty, avg_px)}` — `load_carries_with_source` without the provenance."""
    return {s: (q, b) for s, (q, b, _src) in
            load_carries_with_source(runs_path, run_id).items()}


def load_carries_with_source(runs_path: str, run_id: str) -> dict:
    """`{slug: (qty, avg_px, source)}` from the run manifest's `carries` column, or `{}`.

    ⛔ `source` IS `"recorded"` OR `"declared"`, and an entry WITHOUT the key is `"declared"`
    — every row written before the key existed was an
    operator's `--carry`. Only a `"recorded"` entry may be anchored on by a later basis replay
    (`bot/core/venue_close.py:replay_run_fills`); a declared basis is an unverified assertion and
    must not become an auto-booked `basis_source="run"`.

    ⛔ Read by header NAME off the manifest, and MISSING is not empty: a run whose manifest row
    does not exist carries no statement about its inherited positions, so its books fall into the
    unknown-basis arm rather than being asserted flat at start.

    ⛔ AND A MALFORMED ENTRY IS SKIPPED, NOT GUESSED. A carry is the BASIS of a real inherited
    position; inventing one from a shape we do not recognise would put a fabricated `avg_entry`
    into the decomposition, which is the flattering-direction error this whole tool exists to
    avoid. An unreadable entry simply leaves that book in the unknown-basis arm.
    """
    import csv
    import os

    if not os.path.exists(runs_path):
        return {}
    with open(runs_path, newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [c for c in ("run_id", "carries") if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{runs_path}: run manifest is missing {missing} "
                             f"(expected {RUNS_HEADER})")
        for row in reader:
            if str(row.get("run_id", "") or "").strip() != run_id:
                continue
            raw = str(row.get("carries", "") or "").strip()
            if not raw:
                return {}
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {}
            out: dict = {}
            for entry in (parsed or []):
                if not isinstance(entry, dict):
                    continue
                slug = str(entry.get(CARRY_KEYS[0], "") or "").strip()
                qty = dec_or_none(entry.get(CARRY_KEYS[1]))
                basis = dec_or_none(entry.get(CARRY_KEYS[2]))
                if not slug or qty is None or basis is None or qty == _ZERO:
                    continue
                # ⛔ ABSENT (and anything not exactly "recorded") IS "declared" — fail-closed.
                src = str(entry.get(CARRY_SOURCE_KEY, "") or "").strip()
                out[slug] = (qty, basis,
                             src if src == run_manifest.CARRY_SOURCE_RECORDED
                             else run_manifest.CARRY_SOURCE_DECLARED)
            return out
    return {}



def _qty_str(q: Decimal) -> str:
    """A quantity as the maker's own `parse_carry` will read it back. `normalize()` on an
    integral Decimal yields exponent notation (`3E+1`), which parses to the right number but
    reads wrong in a journal and in a command an operator inspects."""
    s = format(q, "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


@dataclass(frozen=True)
class Checkpoint:
    """One intermediate manifest declaration, checked against the replay at that instant."""
    run_id: str
    iso: str
    ts: float
    declared_qty: Decimal
    declared_basis: Decimal
    replayed_qty: Decimal
    replayed_basis: Decimal

    @property
    def agrees(self) -> bool:
        # Quantity EXACTLY (the venue's own unrounded convention); basis at the 4dp the carry
        # was declared in — comparing an unrounded replay against a 4dp declaration would flag
        # every book on rounding alone.
        return (self.replayed_qty == self.declared_qty
                and self.replayed_basis.quantize(_BASIS_QUANTUM)
                == self.declared_basis.quantize(_BASIS_QUANTUM))

    def as_dict(self) -> dict[str, Any]:
        return {"run_id": self.run_id, "iso": self.iso,
                "declared": f"{_qty_str(self.declared_qty)}@{self.declared_basis}",
                "replayed": f"{_qty_str(self.replayed_qty)}@{self.replayed_basis}",
                "agrees": self.agrees}


@dataclass(frozen=True)
class BasisChain:
    """How far back the replay reaches for ONE book, what anchors it, and what checks it.

    ⛔⛔ **THIS EXISTS BECAUSE THE FIRST CUT LAUNDERED A DECLARATION INTO A DERIVATION**
    [mm-review 2026-08-22, BLOCKING]. Scoped to the OUTGOING run alone, a carried book that did not
    trade has `fills == 0`, so `price_realized` returns the manifest seed — the previous launch's
    `--carry` string — while the printout read *"from 0 fill(s) replayed through
    maker.fill_accounting"*. So the chain walks BACKWARDS through the real-money run manifest to the
    most recent run that started **FLAT** on this book and replays every fill from there.

    ⛔⛔ **AND THE REPLAY IS BY TIME WINDOW, NOT RUN-ID MEMBERSHIP** [mm-review round 2, BLOCKING].
    `r["run_id"] in ch.run_ids` drops a run's fills whenever the run is absent from the MANIFEST —
    and `run_manifest.write_run_row` is explicitly allowed to fail, so a gap is a documented state.
    Reproduced: a middle run that bought 10 @0.90 and sold 10 @0.90 leaves inventory unchanged, so
    the QUANTITY guard cannot fire, and the tool printed `0.3000 DERIVED` against a true 0.60.
    `since_ts` closes that by construction.

    ⛔ **`checkpoints` IS THE SELF-CHECK.** Every intermediate manifest row declaring a `carries`
    entry is a free checkpoint: the replay's `(inv, avg)` at that row's `started_ts` must equal the
    declared `(qty, basis)`. A disagreement means the window is missing fills (a tape archive
    outside `tape_paths`' glob — the measured `<run-id>` case cost 42% of the tape) or a past
    declaration was wrong. Either way the number may NOT be labelled DERIVED.

    `anchor`:
      · `"flat"`      — a run in the chain started flat on this book: a real zero.
      · `"inherited"` — the chain bottoms out on a declaration. **PROVISIONAL, never derived.**
      · `"none"`      — no chain at all; the book has no manifest history.
    """
    slug: str
    run_ids: tuple[str, ...]
    seed: tuple[Decimal, Decimal] | None
    anchor: str
    anchor_run_id: str = ""
    anchor_iso: str = ""
    #: Replay every tape row for this slug with `ts >= since_ts`. NOT a run-id filter.
    since_ts: float = 0.0
    #: (run_id, iso, ts, declared_qty, declared_basis) for each intermediate declaration.
    declarations: tuple[tuple[str, str, float, Decimal, Decimal], ...] = ()

    @property
    def anchored_flat(self) -> bool:
        return self.anchor == "flat"


def _row_ts(row: Mapping[str, str]) -> float:
    try:
        return float(row.get("started_ts") or 0)
    except (TypeError, ValueError):
        return 0.0


def _declarations_from(slug: str, rows: Sequence[Mapping[str, str]]
                       ) -> tuple[tuple[str, str, float, Decimal, Decimal], ...]:
    out = []
    for r in rows:
        got = parse_carries(r.get("carries") or "").get(slug)
        if got is not None:
            out.append((r.get("run_id", ""), r.get("started_iso", ""), _row_ts(r),
                        got[0], got[1]))
    return tuple(out)


def basis_chain(slug: str, rows: Sequence[Mapping[str, str]]) -> BasisChain:
    """`rows` = the REAL-money manifest rows, oldest first. Walks back to a flat start.

    ⛔ REAL ROWS ONLY — a `dry` run places nothing, so it moves no inventory and its declared
    carries describe a simulation. Including one would anchor the chain on fiction.
    """
    for i in range(len(rows) - 1, -1, -1):
        row = rows[i]
        carries = parse_carries(row.get("carries") or "")
        if slug in carries:
            continue                          # started this run holding it — keep walking back
        if slug in manifest_slugs(row):
            # Quoted it and declared NO carry ⇒ started FLAT. A real zero to replay from.
            return BasisChain(slug=slug,
                              run_ids=tuple(r.get("run_id", "") for r in rows[i:]),
                              seed=None, anchor="flat",
                              anchor_run_id=row.get("run_id", ""),
                              anchor_iso=row.get("started_iso", ""),
                              since_ts=_row_ts(row),
                              declarations=_declarations_from(slug, rows[i:]))
        # Neither carried nor quoted: this run's world does not contain the book, so nothing
        # older can explain it either. The run ABOVE (which must have carried it) is where the
        # position enters our records — by declaration, not by any fill we hold.
        break
    else:
        i = -1
    first = rows[i + 1] if i + 1 < len(rows) else None
    if first is None:
        return BasisChain(slug=slug, run_ids=(), seed=None, anchor="none")
    seed = parse_carries(first.get("carries") or "").get(slug)
    if seed is None:
        return BasisChain(slug=slug, run_ids=(), seed=None, anchor="none")
    return BasisChain(slug=slug,
                      run_ids=tuple(r.get("run_id", "") for r in rows[i + 1:]),
                      seed=seed, anchor="inherited",
                      anchor_run_id=first.get("run_id", ""),
                      anchor_iso=first.get("started_iso", ""),
                      since_ts=_row_ts(first),
                      # The anchor's OWN declaration is the seed, not a checkpoint — checking it
                      # against a replay seeded from itself is circular and always agrees.
                      declarations=_declarations_from(slug, rows[i + 2:]))


def chain_rows(slug: str, chain: BasisChain, rows: Sequence[Mapping[str, str]],
               unparseable: list[str] | None = None) -> list[dict]:
    """This book's tape rows inside the chain's TIME WINDOW.

    ⛔ `ts >= since_ts`, never `run_id in chain.run_ids` — see `BasisChain`. A run missing from the
    manifest still has fills on the tape, and dropping them is how a wrong basis gets the DERIVED
    label.

    ⛔ AND AN UNPARSEABLE `ts` IS REPORTED, NOT SKIPPED [mm-review confirm pass]: a row we cannot
    place in time is a row we cannot decide about. Zero such rows exist today, which is precisely
    when the branch is cheap to close.
    """
    out = []
    for r in rows:
        if r.get("slug") != slug:
            continue
        ts = dec_or_none(r.get("ts"))
        if ts is None:
            if unparseable is not None:
                unparseable.append(f"{slug}/{r.get('order_id') or '?'}: ts={r.get('ts')!r}")
            continue
        if float(ts) >= chain.since_ts:
            out.append(dict(r))
    out.sort(key=lambda r: float(dec_or_none(r.get("ts")) or 0))
    return out


@dataclass(frozen=True)
class DerivedCarry:
    """One book's start state, derived rather than transcribed.

    ⛔ THE ROLE ASSIGNMENT IS THE WHOLE DESIGN (§3):
      · the VENUE read is authoritative for QUANTITY, absolutely — it is the only thing that asks
        the venue;
      · OUR OWN FILLS TAPE, replayed through `bot/poly_us/maker.fill_accounting`, is authoritative
        for BASIS — it is literally the function that produces the maker's `avg_entry`, and it is
        sign-symmetric, yes-space throughout, and correct through a zero crossing by construction;
      · `logs/maker_state_poly.json` is the CROSS-CHECK and the `unknown_position` tripwire.

    ⚠️ SAID OUT LOUD, because it is a genuine narrowing: the derived basis is *our* `avg_entry`,
    GROSS of fees, built from `order.price`. It is NOT the venue's fee-net `avgPx` — so
    `--adopt-existing`'s short refusal and 's "which basis family does the venue report"
    question are SIDESTEPPED, not answered.
    """
    slug: str
    venue_qty: Decimal
    replayed_qty: Decimal | None
    basis: Decimal | None
    n_fills: int
    contracts: Decimal
    round_trips: int
    chain: "BasisChain"
    #: Every intermediate manifest declaration, checked against the replay. A single
    #: disagreement means the replay window is missing fills (or a past declaration was wrong),
    #: so the number is NOT established and may not be labelled DERIVED.
    checkpoints: tuple["Checkpoint", ...]
    durable_qty: Decimal | None
    status: str             # ok | qty_mismatch | no_history | unknown_position | basis_unusable
    detail: str
    fix: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "ok"

    @property
    def broken_checkpoints(self) -> tuple["Checkpoint", ...]:
        return tuple(c for c in self.checkpoints if not c.agrees)

    @property
    def derived(self) -> bool:
        """May this basis be called DERIVED?

        ⛔ BOTH CONDITIONS. A flat anchor alone is not enough — that was the B1 hazard: a genuine
        flat anchor with a corrupt window in between still printed DERIVED. The checkpoints are
        what make the walk self-checking.
        """
        return self.chain.anchored_flat and not self.broken_checkpoints

    @property
    def quantized_basis(self) -> Decimal:
        """4dp, matching the tape's own price precision and every `--carry` this repo has
        declared. The UNROUNDED value prints in the verify table beside it, so the rounding is
        visible rather than silent."""
        if self.basis is None:
            raise ValueError(f"{self.slug}: no derived basis ({self.status})")
        return self.basis.quantize(_BASIS_QUANTUM)

    @property
    def spec(self) -> str:
        """The `--carry` string. Raises unless the derivation is `ok` — a refusing book must
        never produce a spec, because a spec is a number real money is armed with."""
        if not self.ok:
            raise ValueError(f"{self.slug}: cannot emit a carry ({self.status}: {self.detail})")
        return f"{self.slug}:{_qty_str(self.venue_qty)}@{self.quantized_basis}"

    @property
    def long_cost(self) -> Decimal:
        """⚠️ PRINTS AS A PAIR WITH `short_collateral`, NEVER SUMMED — netting them reported
        <n> against a real ~<n>."""
        if self.venue_qty <= _ZERO or self.basis is None:
            return _ZERO
        return self.venue_qty * self.basis

    @property
    def short_collateral(self) -> Decimal:
        if self.venue_qty >= _ZERO or self.basis is None:
            return _ZERO
        return self.venue_qty.copy_abs() * (_ONE - self.basis)

    def as_dict(self) -> dict[str, Any]:
        return {"slug": self.slug, "venue_qty": str(self.venue_qty),
                "replayed_qty": None if self.replayed_qty is None else str(self.replayed_qty),
                "basis": None if self.basis is None else str(self.basis),
                "n_fills": self.n_fills, "round_trips": self.round_trips,
                "status": self.status,
                # ⛔ PROVENANCE IS JOURNALED, not just printed — "was this basis derived or
                # inherited?" is the question a bad night asks of these rows.
                # ⛔ A MISMATCH GETS ITS OWN TOKEN [mm-review confirm pass]: falling through to
                # `chain.anchor` recorded `"flat"` for a checkpoint-mismatch book, the exact
                # reassuring word for the case the printout calls "Do NOT trust this basis".
                "basis_provenance": ("derived" if self.derived
                                     else "checkpoint_mismatch" if self.broken_checkpoints
                                     else self.chain.anchor),
                "chain_anchor": self.chain.anchor,
                "checkpoints": [c.as_dict() for c in self.checkpoints],
                "checkpoints_broken": len(self.broken_checkpoints),
                "anchor_run_id": self.chain.anchor_run_id,
                "anchor_iso": self.chain.anchor_iso,
                "chain_runs": len(self.chain.run_ids)}
