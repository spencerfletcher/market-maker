"""
bot/core/tape_paths.py
──────────────────────
PER-LANE maker tape paths, and the ONE fold rule every consumer of those tapes must use.

⛔ WHY PER-LANE — the twice-bitten rotation trap [2026-08-27]. Two real maker processes
(`--lane main` and a probe lane) shared ONE `logs/poly_live_mm_fills.csv`. The probe's start
found a header mismatch and freeze-rotated the tape (`_open_writer`'s `os.replace` →
`logs/rotated/<stem>.pre-<tag>.csv`) — but the ALREADY RUNNING main maker holds an open file
handle, and a handle follows the INODE, not the name. So:

  · the main maker kept appending into the file now sitting in `logs/rotated/`, which grew
    until 15:59Z while the "live" path held only the probe's rows;
  · the tier-1 backup, which treats the fills tape as `append_only`, FATALed on the apparent
    SHRINK of `logs/poly_live_mm_fills.csv`;
  · and any consumer reading the live file alone saw one lane's run and called it the tape.

The class is **two writers, one tape, across a rotation boundary**. Renaming the archive, or
re-opening the handle, only narrows the window. A per-lane path removes the class outright:
one lane's rotation can never touch another lane's writer, because they never name the same
file. This is the same fix, one axis over, as the LANE-STAMPED HEARTBEAT
(`bot.core.heartbeat.lane_name`, I-OPS-1 2026-08-20) and the `.dry`-stamped tapes
(`PolyMaker.__init__`'s `_default`, 2026-08-01) — displacement by naming, not by locking.

⚠️ ONE DELIBERATE DIVERGENCE FROM `heartbeat.lane_name`: that helper keeps the BARE name for
the default lane, because units and runbooks name `poly_live_mm.json` by hand. The tapes go the
other way — EVERY lane, `main` included, gets its own stamped file — because here the bare name
already holds ~2,400 rows of pre-lane history that must stay readable and must stop growing.
Freezing the legacy path makes the history unambiguous: `logs/poly_live_mm_fills.csv` is the
pre-2026-08-27 era and nothing appends to it again.

⛔ THE COST, AND WHY `fold_patterns` IS NOT OPTIONAL. Splitting the writer splits the tape, and
this repo's single most expensive log defect is a live-file-only read (`poly_live_mm_fills.csv`
held 1,418 of its own 2,420 rows — 59%). Per-lane files make that worse, not better, unless
every consumer folds. So the read side gets ONE rule, here, and consumers call it rather than
hand-rolling a glob — `the private design notes` § rotation carries the same list for humans.
"""
from __future__ import annotations

import glob
import os
import re
from typing import Sequence
from datetime import datetime, timezone

# ONE copy of the lane-name rule, not a second one. `heartbeat` owns it because the heartbeat
# filename is where an unsafe lane does the most damage (an invisible deadman); the reason
# transfers verbatim to a tape path, so importing it beats restating it. Restating a rule in a
# second place is precisely how this repo's corrections fail to propagate.
from bot.core.heartbeat import _LANE_RE as LANE_RE

__all__ = ["LANE_RE", "KNOWN_LANES", "lane_tape_path", "fold_patterns", "lane_fold_patterns",
           "fold_paths", "is_dry_tape", "is_bare_stem_family", "bare_stem_archives"]

#: Every lane name this repo has ever stamped into a tape path.
#:
#: ⛔ **A DECLARED LIST, AND IT HAS TO BE — THE GRAMMAR CANNOT TELL A LANE FROM AN ARCHIVE TAG**
#: [round 3]. `poly_live_mm_quotes.probe.csv` and `poly_live_mm_quotes.<run-id>.csv` are the same
#: shape: `<stem>.<something><ext>`. One is another lane's live tape and must never fold into
#: `--lane main`; the other is 6.3% of the main lane's own history and must always fold. `LANE_RE`
#: does not separate them (it matches `<run-id>`, `pre-qsize` and `dry-<run-id>` too) — only knowing
#: which names are LANES does.
#:
#: ⛔ **ADD A LANE HERE IN THE SAME COMMIT THAT FIRST LAUNCHES IT.** A lane missing from this list
#: does not fail loudly: its archives quietly fold into `--lane main`, pooling two regimes into one
#: row of a confound-annotated table. `test_tape_paths` pins the list against the lane tapes
#: actually on disk, so a new lane's first real run turns that test RED rather than skewing a
#: report. `main` is included so `<stem>.main.…` is left to the lane half rather than double-folded.
KNOWN_LANES = ("main", "probe", "wsprobe")


def lane_tape_path(path: str, lane: str | None) -> str:
    """`logs/poly_live_mm_fills.csv` + lane `probe` → `logs/poly_live_mm_fills.probe.csv`.

    `lane=None` is the DEFAULT lane and still stamps (`…fills.main.csv`) — see the module
    docstring for why the tapes diverge from `heartbeat.lane_name` here.

    Raises ValueError on a lane that is not a safe filename component: the lane is concatenated
    into a path, and `../../x` would write a real-money tape outside `logs/` where neither the
    fold patterns below nor the tier-1 backup globs can see it.
    """
    from bot.core.maker_state import DEFAULT_LANE      # deferred: ledger owns "main"
    lane = str(lane or DEFAULT_LANE)
    if not LANE_RE.match(lane):
        raise ValueError(
            f"lane {lane!r} is not a safe filename component (allowed: {LANE_RE.pattern}) — the "
            f"lane is concatenated into the maker's TAPE path, and one containing a path "
            f"separator would write real-money fills outside logs/, where neither the fold "
            f"patterns nor the tier-1 backup globs would ever find them")
    stem, ext = os.path.splitext(path)
    return f"{stem}.{lane}{ext}"


def fold_patterns(live_path: str, *, include_gz: bool = False) -> tuple[str, ...]:
    """Every glob a consumer must read to see the WHOLE family behind `live_path`.

    Three patterns, and each covers a failure that has actually happened:
      1. the LEGACY shared file — frozen pre-lane history, still 59% of the fills tape;
      2. `<stem>.*<ext>` — every PER-LANE tape (`.main`, `.probe`, `.wsprobe`) AND every manual
         archive (`poly_live_mm_fills.<run-id>.csv` matched no `pre-*` pattern and cost every
         fill-hazard number before 2026-08-09 42% of its population);
      3. `rotated/<stem>.*<ext>` — every frozen schema era, of every lane.

    ⚠️ `include_gz` is OFF by default: `.csv.gz` needs a `gzip.open` reader, and handing a
    plain-`open` consumer a compressed path turns a fold into a crash. Only a caller that
    actually decompresses (`poly_seat_actuals`) asks for them.

    ⚠️ These are PATTERNS, so they also match `.dry` tapes. Prefer `fold_paths`, which resolves
    them AND drops dry/shadow files — a dry tape folded into a real-money figure is the
    2026-08-01 incident, and this split is the only place that guard can live for all callers.

    ⛔ GLOB, NEVER A HANDWRITTEN LIST: a list omits the rotation that happens after it is
    written, which is exactly how a three-file `FILL_TAPES` constant dropped 58% of the tape.

    ⚠️ Passing an already-lane-stamped path (`…fills.probe.csv`) narrows the fold to THAT lane
    plus its rotated eras — deliberate: an operator who names a lane meant that lane.
    """
    directory = os.path.dirname(live_path) or "."
    stem, ext = os.path.splitext(os.path.basename(live_path))
    pats = [
        os.path.join(directory, f"{stem}{ext}"),
        os.path.join(directory, f"{stem}.*{ext}"),
        os.path.join(directory, "rotated", f"{stem}.*{ext}"),
    ]
    if include_gz:
        pats.append(os.path.join(directory, "rotated", f"{stem}.*{ext}.gz"))
    return tuple(pats)


def lane_fold_patterns(live_path: str, lane: str | None, *,
                       include_gz: bool = False) -> tuple[str, ...]:
    """Every glob for ONE lane's tape family. `lane=None` = every lane (the plain fold).

    ⛔ **THE `main` LANE ALSO OWNS THE FROZEN LEGACY FILE, AND THAT IS THE BUG THIS FIXES**
    [2026-09-01]. `lane_tape_path` stamps every lane, `main` included, so `--lane main` resolved to
    `logs/poly_live_mm_quotes.main.csv` — a file that does not exist, because no real main-lane run
    has happened since the 2026-08-27 per-lane split. The literal is a non-glob pattern, so
    `expand_deduped` passes it through and the reader raises `TapeVanished` on a tape that never
    existed. Meanwhile EVERY pre-split run WAS a main-lane run, and all of that history is in the
    bare file — so the main lane's family is the lane-stamped fold PLUS the legacy bare fold, and
    a pattern matching no file is simply dropped.

    ⛔ **THE LEGACY HALF IS `<stem>.*<ext>` PLUS A PREDICATE, RESOLVED TO PATHS — AND BOTH HALVES
    OF THAT SENTENCE ARE LOAD-BEARING** [round 3, BLOCKING A]. The constraint is "the bare stem's
    own family, and NO named lane's", and **a glob cannot express it**: `<stem>.*<ext>` also matches
    `…quotes.probe.pre-latchactive.csv`, while narrowing to `<stem>.pre-*` / `<stem>.upto-*` drops
    `logs/rotated/poly_live_mm_quotes.<run-id>.csv` — a MANUAL archive holding 39,879 real main-lane
    rows (run `legacy-<run-id>-real-backfill`), **6.3% of the main-lane quote population**, and the
    exact file `fold_patterns`' own docstring names as the 42% incident. Round 2 traded the probe
    leak for that silent 6.3% truncation. So the wide glob comes back and `is_bare_stem_family`
    decides, which means this half returns EXISTING PATHS rather than patterns.
      · a `.<lane>.…` component (`KNOWN_LANES`) ⇒ NOT ours — it is that lane's family, already
        folded by the lane half above;
      · `is_dry_tape` ⇒ NOT ours. ⚠️ **AN ASYMMETRY WITH `fold_patterns`, STATED ON PURPOSE**: that
        function returns patterns which DO match `.dry` tapes and documents `fold_paths` as the
        guard. Here the paths are concrete, so a dry tape would reach a real-money read with no
        second chance to drop it — `logs/rotated/poly_live_mm_quotes.dry.pre-qsize.csv` is on disk
        today. Dropping it here is the 2026-08-01 incident's own rule, not a new policy.
      · a non-archive suffix (`…<run-id>.csv.prebackfill-20260815`, `.csv.tmp`) ⇒ NOT ours.
    ⚠️ RESOLUTION AT CALL TIME: this half is a snapshot of the directory, not a standing glob. The
    final line already resolves literals, so the caller's contract is unchanged.

    ⚠️ **THE EXISTENCE FILTER ON THE FINAL LINE TRADES A REFUSAL FOR A SILENCE — KNOW WHICH YOU
    WANT** [round 2, CONCERN 6]. Literal (non-glob) patterns that do not exist are DROPPED, because
    a `main` lane whose legacy bare file is absent must not raise `TapeVanished` on a tape it never
    needed. The cost: for a lane that has NEVER RUN, every pattern is either an empty glob or a
    dropped literal, so the reader sees an empty fold and reports "no run in scope" — a
    CANNOT-VERIFY rendered as a confirmed absence. Documented rather than fixed: the callers here
    (`poly_presence_capture`, `poly_channel_join`) already print an explicit refusal naming the lane
    when no run is selected, so the distinction survives at the seam that reports it. ⛔ A caller
    that does NOT refuse on an empty selection must not read "no rows" as "no run".
    """
    if lane is None:
        return fold_patterns(live_path, include_gz=include_gz)
    from bot.core.maker_state import DEFAULT_LANE      # deferred: ledger owns "main"
    pats = list(fold_patterns(lane_tape_path(live_path, lane), include_gz=include_gz))
    if lane == DEFAULT_LANE:
        directory = os.path.dirname(live_path) or "."
        stem, ext = os.path.splitext(os.path.basename(live_path))
        # ⛔ The bare stem's LIVE file, plus every archive of it that is not another lane's.
        legacy = [os.path.join(directory, f"{stem}{ext}")]
        legacy += bare_stem_archives(live_path, include_gz=include_gz)
        pats += [p for p in legacy if p not in pats]
    return tuple(p for p in pats if "*" in p or os.path.exists(p))


def is_bare_stem_family(path: str, live_path: str) -> bool:
    """Does `path` belong to the BARE stem's own tape family — the legacy/manual archives?

    The `--lane main` question, stated once [round 3, BLOCKING A]. True for the live bare file and
    for every archive of it (`…quotes.pre-qsize.csv`, `…quotes.upto-<id>.csv.gz`, and the MANUAL
    `…quotes.<run-id>.csv`, which is 6.3% of the main-lane quote population); False for another lane's
    file, for a dry tape and for a non-archive leftover.

    ⛔ The lane test is on the FIRST post-stem component only. `…quotes.main.dry.pre-latchactive.csv`
    is the main LANE's file — the lane half of `lane_fold_patterns` folds it, so admitting it here
    too would double-fold; `…quotes.<run-id>.csv.prebackfill-20260815` is not a CSV at all.
    """
    base = os.path.basename(path)
    stem, ext = os.path.splitext(os.path.basename(live_path))
    if base == f"{stem}{ext}":
        return True
    if not base.startswith(f"{stem}."):
        return False
    if not (base.endswith(ext) or base.endswith(f"{ext}.gz")):
        return False                    # `.csv.prebackfill-*`, `.csv.tmp`, an editor's `.csv~`
    if is_dry_tape(base):
        return False                    # see `lane_fold_patterns` — concrete paths get no second guard
    head = base[len(stem) + 1:].split(".", 1)[0]
    return head not in KNOWN_LANES


def bare_stem_archives(live_path: str, *, include_gz: bool = False) -> list[str]:
    """The EXISTING archives of `live_path`'s bare stem — every era and manual copy, no other lane's.

    Both directories, because a manual archive has landed in each: `logs/rotated/` holds
    `poly_live_mm_quotes.<run-id>.csv`, and `fold_patterns`' own second pattern exists for the live
    directory's copies. The wide glob is what makes a `.gz` visible; `is_bare_stem_family` is the
    narrow filter, and it is the only thing separating this family from another lane's.
    """
    directory = os.path.dirname(live_path) or "."
    stem, ext = os.path.splitext(os.path.basename(live_path))
    found: set[str] = set()
    for folder in (directory, os.path.join(directory, "rotated")):
        found.update(glob.glob(os.path.join(folder, f"{stem}.*{ext}")))
        if include_gz:
            found.update(glob.glob(os.path.join(folder, f"{stem}.*{ext}.gz")))
    return sorted(p for p in found if is_bare_stem_family(p, live_path))


def is_dry_tape(path: str) -> bool:
    """A dry/shadow tape, BY FILENAME — never fold one into a real-money figure. Matches the
    rule in `scripts/metrics_tracker.is_dry_tape`; per-lane dry tapes stamp lane THEN mode
    (`…fills.probe.dry.csv`), so the substring test still catches them."""
    base = os.path.basename(path)
    return ".dry" in base or "dry-" in base


def fold_paths(live_path: str, *, include_dry: bool = False,
               include_gz: bool = False) -> list[str]:
    """The EXISTING files behind `fold_patterns(live_path)`, deduped, oldest first by mtime.

    Ordering is by mtime so a rotation sequence reads oldest → newest; the basename tie-break
    keeps the result deterministic when an rsync mirror copies several files in one second.
    """
    found: set[str] = set()
    for pattern in fold_patterns(live_path, include_gz=include_gz):
        found.update(glob.glob(pattern))
    keep = [p for p in sorted(found) if include_dry or not is_dry_tape(p)]
    return sorted(keep, key=lambda p: (os.path.getmtime(p), os.path.basename(p)))


_UPTO_STAMP_RE = re.compile(r"\.upto-(\d{8}T\d{6})\.")


def archives_in_window(paths: Sequence[str], *, lo: float, hi: float) -> list[str]:
    """The members of a rotation family that CAN hold a row with `lo <= ts <= hi`, by NAME.

    A size-rotated archive `<stem>.upto-<UTC stamp>.csv[.gz]` ends at its stamp and starts at the
    previous archive's stamp, so the two stamps bound its rows without opening it. ⛔ NEVER by
    mtime: a `.gz` carries the GZIP time (the nightly compress job), not the rotation — the mtime
    floor in `poly_probe_night._presence_paths` kept every archive compressed after the floor.
    Unstamped members (the live tape, `pre-<schema>` freezes, manual archives) are always kept;
    the caller's row filter still decides. Order is preserved.
    """
    stamped: list[tuple[float, str]] = []
    for p in paths:
        m = _UPTO_STAMP_RE.search(os.path.basename(p))
        if m:
            stamped.append((datetime.strptime(m.group(1), "%Y%m%dT%H%M%S")
                            .replace(tzinfo=timezone.utc).timestamp(), p))
    stamped.sort()
    keep: set[str] = set()
    prev_end = float("-inf")
    for end, p in stamped:
        if end >= lo and prev_end <= hi:
            keep.add(p)
        prev_end = end
    return [p for p in paths if p in keep or not _UPTO_STAMP_RE.search(os.path.basename(p))]
