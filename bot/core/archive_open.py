"""
bot/core/archive_open.py
────────────────────────
THE ONE `.csv` / `.csv.gz` READ SEAM, dependency-free.

⛔ WHY IT MOVED HERE [2026-08-27]. `scripts/poly_spread_watch.open_csv` has been this repo's one
gz-transparent read seam since I-TAPE-1 (2026-08-20), and every reader in the spread-watch family
inherits gz support from it. But `poly_spread_watch` imports `bot.poly_us.client`, the feed cache
and three sibling scripts — so the deliberately-standalone screen readers (`poly_room_watch`,
`poly_room_hunt`, `metrics_tracker`) could not reach it without pulling a network client into a
pure tape reader. They therefore globbed `*.csv` and called plain `open()`, and when
`logs/flow_map`'s screen passes were compressed on 2026-08-27 they silently lost the compressed
half of their population: `poly_room_watch`'s era-break backtest went from 140 passes on 08-07 to
108, and its `improvable_frac` from 0.7% to 0.0%.

⛔ THE FAILURE MODE IS SILENT, WHICH IS WHY THE SEAM IS SHARED RATHER THAN COPIED. A plain
`open()` on a gzipped tape does not raise — it hands `csv.DictReader` binary noise, whose first
"header" line carries none of the required columns, so the reader reports the archive as MISSING
EVERY COLUMN and skips it. A truncated population wearing a schema-mismatch message. Measured
twice before this move: the 2026-08-19 weather analysis lost 13,858 arrivals (36%), and the box's
manual emergency gzip made rotated archives invisible to box-side folds the same week.

`poly_spread_watch` now RE-EXPORTS these names, so there is exactly one implementation and one
`TapeVanished` class identity; every existing `poly_spread_watch.open_csv` / `.TapeVanished`
reference keeps working unchanged.

Stdlib only, and it must stay that way — the whole point is that a standalone reader can import
it without inheriting a dependency graph.
"""
from __future__ import annotations

import glob
import gzip
import os
from typing import Sequence

__all__ = ["ARCHIVE_SUFFIXES", "PREFER_GZIPPED_ARCHIVE", "TapeVanished", "UnreadableArchive",
           "archive_stem", "csv_paths", "drop_page_cache", "one_representation", "open_csv",
           "uncompressed_size"]

#: Conservative stand-in when the gzip trailer cannot be read. These tapes measure ~10:1; a
#: smaller number would make a RAM estimate optimistic, which is the wrong direction for a refusal.
_NOMINAL_RATIO = 10

#: Below this compressed size the wrap correction is not applied. ⛔ THE FLOOR IS ABOUT THE TEST,
#: NOT JUST THE ARITHMETIC. The correction fires on `isize < size`, and gzip **EXPANDS**
#: incompressible data slightly — so `isize < size` is satisfied with NO wrap at all, and a 4 MiB
#: floor let a 5 MB near-incompressible archive report 4.3 GB (measured: **820x**), which inside
#: `poly_room_hunt.ram_refusal` is a spurious refusal of a legitimate backtest. Set at
#: `(1 << 32) // 20`: a wrap is inferred only where a ≥4 GiB plaintext is plausible at these
#: tapes’ real ~10:1 ratio, with 2x of headroom.
_WRAP_POSSIBLE_BYTES = (1 << 32) // 20          # ~215 MB compressed

#: ⛔ GLOB WIDE, FILTER NARROW. Patterns end `.csv*` so a `.csv.gz` is seen at all; this tuple
#: narrows the matches back so a half-written `.csv.tmp`, a `.gz.tmp`, a `.bak` or an editor's
#: `.csv~` is never folded into a population read.
ARCHIVE_SUFFIXES: tuple[str, ...] = (".csv", ".csv.gz")


class TapeVanished(FileNotFoundError):
    """A listed tape is gone in BOTH representations by the time the reader reached it.

    ⛔ A `FileNotFoundError` subclass on purpose: the readers' existing `except FileNotFoundError`
    handlers must keep catching it unchanged, while the read seams can catch THIS and skip exactly
    one file. A file that is GONE mid-run is the known, named rotation race (see `open_csv`), and
    killing a nine-minute pass over it is a worse answer than finishing with one named hole.

    ⚠️ ITS `str()` IS THE PLAIN `FileNotFoundError` TEXT, deliberately. `open_csv` cannot tell a
    raced archive from a tape that never existed, and `poly_prelaunch` renders this exception
    straight into an operator's launch gate — so the message states the FACT (this path is not
    there) and this docstring, not the message, carries the race. Blaming a rotation race for a
    missing live tape would be a wrong diagnosis in the one place an operator reads it.
    """

    def __init__(self, path: str, cause: FileNotFoundError) -> None:
        super().__init__(cause.errno, cause.strerror, path)
        self.path = path


def open_csv(path: str):
    """Open a tape for reading, TRANSPARENTLY decompressing `.csv.gz`. Text mode, `newline=""`.

    `newline=""` on both branches: `csv` requires it, and `gzip.open(..., "rt")` accepts it.

    ⛔ **A VANISHED `.csv` FALLS BACK TO ITS `.gz` SIBLING — THE ROTATION RACE** [2026-08-26].
    Readers LIST a directory at start and OPEN the files minutes later (`poly_room_reopen` walks
    ~9 min over the 14k-slug universe), and a compression pass — the box's daily `<unit>`
    timer, or `scripts/mirror_groom` on the laptop — turns `X.csv` into `X.csv.gz` inside that
    window. The listed path then does not exist and the whole pass dies. The compressor is
    content-identical, so the sibling is the SAME ROWS in another representation: following it
    changes the population by nothing.

    Neither representation left ⇒ `TapeVanished`.
    """
    if path.endswith(".gz"):
        return gzip.open(path, "rt", newline="")
    try:
        return open(path, newline="")
    except FileNotFoundError as absent:
        original = absent
    try:
        return gzip.open(f"{path}.gz", "rt", newline="")
    except FileNotFoundError:
        # ⛔ Re-raised against the path the CALLER asked for, carrying the ORIGINAL errno text: an
        # operator hunting a hole in the population needs the name they enumerated, not the `.gz`
        # we also tried.
        raise TapeVanished(path, original) from None


def drop_page_cache(fh) -> None:
    """Tell the kernel we will not re-read this file — releases its page-cache charge.

    ⛔ ONE IMPLEMENTATION [2026-09-05]. It was copied in `poly_market_screen` and `poly_room_watch`
    (that copy's note said the original could not be imported without pulling a network client
    into a standalone reader); a third caller made the copy a defect, and this module is the
    dependency-free seam both already import. [RAM audit 2026-08-12 F3: the pass scan streams
    ~957 MB through the cgroup 4x/hr and the cache charge was ~500 MB of the measured peak.]

    Linux-only (`posix_fadvise` is absent on macOS — the laptop just skips); best-effort,
    byte-identical output either way. ⚠️ `AttributeError`/`ValueError` are caught alongside
    `OSError` because the handle may be a gzip text wrapper (`open_csv` on a `.csv.gz`), whose
    `fileno()` chain is not guaranteed.
    """
    try:
        if hasattr(os, "posix_fadvise"):
            os.posix_fadvise(fh.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    except (OSError, AttributeError, ValueError):
        pass


def archive_stem(path: str) -> str:
    """An archive's identity INDEPENDENT OF COMPRESSION: `X.upto-T.csv.gz` → `.../X.upto-T`.

    Keyed on the directory + stem rather than the bare name, so two same-named archives under
    different tapes' `rotated/` dirs stay distinct.
    """
    name = os.path.basename(path)
    for suffix in ARCHIVE_SUFFIXES:
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    return os.path.join(os.path.dirname(path), name)


def _gzip_isize(path: str) -> "int | None":
    """The gzip trailer's ISIZE — uncompressed length mod 2**32 — WITHOUT decompressing.

    The `gzip -l` read: last four bytes, little-endian. Returns None when the file is too short
    to carry a trailer at all (a truncation so severe there is nothing to compare).

    ⚠️ TWO documented limits, both load-bearing: ISIZE is mod 2**32, so it cannot distinguish
    sizes 4 GiB apart; and on a MULTI-MEMBER gzip it describes only the LAST member.
    """
    try:
        if os.path.getsize(path) < 18:      # 10-byte header + 8-byte trailer minimum
            return None
        with open(path, "rb") as fh:
            fh.seek(-4, os.SEEK_END)
            return int.from_bytes(fh.read(4), "little")
    except OSError:
        return None


class UnreadableArchive(OSError):
    """An archive exists but may not be READ — unopenable, or a pair whose members disagree.

    ⛔ RAISED, NEVER SKIPPED. "I cannot read part of your population" is not a fact a reader may
    absorb; absorbing it produces a truncated population under a message blaming something else.
    """


def _assert_pair_agrees(plain: str, gz: str) -> None:
    """Both members of one archive exist — do they hold the SAME NUMBER OF BYTES?

    ⛔ THE PAIR-INTEGRITY GUARD. A preference for either member closes ONE direction only: a pair
    can disagree because the `.gz` is truncated OR because the PLAINTEXT is the stale short thing
    (an interrupted `rsync --append`, a partially restored backup, a hand-`zcat` out of disk).
    Neither member can be trusted when they disagree, so the archive is a HOLE.

    ⚠️ A GUARD, NOT A REPAIR — expected never to fire. One `seek` and one `stat` per pair, no
    decompression, so being wrong about that costs ~nothing.
    """
    isize = _gzip_isize(gz)
    try:
        plain_size = os.path.getsize(plain)
    except OSError as exc:
        raise UnreadableArchive(f"{plain}: archive exists but cannot be stat'd ({exc})") from exc
    if isize is None:
        raise UnreadableArchive(
            f"{gz}: gzip member is too short to carry a trailer — a truncated archive beside "
            f"{plain}. Refusing to read a population that may silently omit part of it; delete "
            f"or re-create the bad member.")
    if isize != plain_size % (1 << 32):
        raise UnreadableArchive(
            f"{plain} and {gz} are two representations of ONE archive and DISAGREE "
            f"({plain_size} bytes plaintext vs {isize} in the gzip trailer). Either member could "
            f"be the short one, so neither may be read; delete the bad one and re-derive.")


#: Which member of a coexisting `X.csv` / `X.csv.gz` pair a reader takes. ⛔ **THE GZ** — on the
#: laptop mirror 190 of 248 moat stems exist as both, and the plaintext side of those is 56 MB
#: against the gz's 9 MB, so a plaintext preference walked ~6x the bytes on 77% of the family's
#: archives. Both members are byte-identical populations (`_assert_pair_agrees` runs first
#: regardless), and both compressors publish the `.gz` ATOMICALLY at its final name
#: (`<deploy unit>`, `scripts/mirror_groom`), so the pair is never half-written.
#: ⚠️ THE TRADE IS I/O FOR CPU; flip this one constant back if a measurement ever says otherwise.
PREFER_GZIPPED_ARCHIVE = True


def one_representation(paths: Sequence[str]) -> list[str]:
    """ONE file per archive stem — the `PREFER_GZIPPED_ARCHIVE` member when both exist.

    ⛔ ONE RULE, ONE OBJECT [moved here 2026-08-27]. `poly_spread_watch` re-exports this. A SECOND
    copy briefly existed here with the OPPOSITE preference and NO integrity check, which would
    have let the screen readers silently serve the possibly-short plaintext of a pair
    `mirror_groom` had just flagged as divergent and kept for an operator — the identical silent
    truncation this whole change closes, one layer down.

    A directory holding both representations of one archive is a NORMAL, EXPECTED state in three
    ways: the window between a compressor publishing `X.csv.gz` and unlinking `X.csv`, a failed
    unlink, and the laptop mirror, where a one-way rsync accumulates both sides and never
    deletes. Folding both doubles every row on that archive, and realpath dedup cannot catch it:
    the pair is two different files with different bytes.

    ⛔ **AN EXPECTED PAIR IS ALWAYS AN *AGREEING* PAIR — a disagreement is a HOLE, not a case this
    preference resolves.** `_assert_pair_agrees` runs FIRST and is independent of the preference;
    it is what makes either member safe to choose.

    ⛔ THE PREFERENCE IS RESOLVED ACROSS THE WHOLE INPUT, NOT PER PATTERN: an earlier cut resolved
    each pattern separately, so a caller whose FIRST pattern matched only the `.gz` kept it even
    though a later pattern offered the plaintext.

    ORDER IS FIRST-APPEARANCE of the stem, so a caller's pattern order survives.
    """
    chosen: dict[str, str] = {}
    order: list[str] = []
    for path in paths:
        stem = archive_stem(path)
        incumbent = chosen.get(stem)
        if incumbent is None:
            chosen[stem] = path
            order.append(stem)
        elif os.path.realpath(incumbent) == os.path.realpath(path):
            continue                                        # same file, two spellings
        elif incumbent.endswith(".gz") != path.endswith(".gz"):
            plain, gz = (path, incumbent) if incumbent.endswith(".gz") else (incumbent, path)
            # ⛔ THE INTEGRITY CHECK RUNS FIRST AND IS INDEPENDENT OF THE PREFERENCE — it is what
            # makes either member safe to choose; the preference is never asked to paper over a
            # disagreement (a disagreeing pair is an `UnreadableArchive`, i.e. a HOLE).
            _assert_pair_agrees(plain, gz)
            chosen[stem] = gz if PREFER_GZIPPED_ARCHIVE else plain
    return [chosen[stem] for stem in order]


def csv_paths(pattern: str) -> list[str]:
    """Every `.csv` AND `.csv.gz` matching a `.csv`-spelled glob, one representation per archive.

    ⛔ THE REPLACEMENT FOR `sorted(glob.glob(dir + "/*.csv"))` in a HISTORICAL reader. A bare
    `*.csv` glob does not fail on a compressed archive, it simply stops matching it — which is
    how `poly_room_watch`'s backtest silently lost 32 of 08-07's 140 passes.

    `pattern` is globbed as spelled AND with `.gz` appended, then narrowed to `ARCHIVE_SUFFIXES`
    (so `.csv.tmp` / `.gz.tmp` / `.bak` can never enter) and pair-collapsed. Returns paths sorted
    by archive stem, so a caller that relied on `sorted(glob.glob(...))` keeps its ordering.

    ⛔ RAISES `UnreadableArchive` on a pair whose two members DISAGREE — it does not pick one.
    Either member could be the short one, and silently serving the short one is the failure this
    function exists to prevent. `mirror_groom` keeps both members of such a pair and exits 1; the
    readers refuse until an operator resolves it.
    """
    found = set(glob.glob(pattern))
    found.update(glob.glob(pattern + ".gz"))
    kept = sorted((p for p in found if p.endswith(ARCHIVE_SUFFIXES)), key=archive_stem)
    return one_representation(kept)


def uncompressed_size(path: str) -> int:
    """Bytes this archive occupies ONCE READ — the plaintext length, not the on-disk length.

    ⛔ EXISTS FOR THE RAM REFUSALS. `poly_room_hunt.ram_refusal` sizes a backtest's load from
    `os.path.getsize`, and these tapes compress ~10:1 — so summing on-disk `.gz` sizes would
    understate the load by an order of magnitude and wave through exactly the full-archive read
    the refusal exists to block (1.00 GB peak RSS beside a live maker on a 1.9 GB box).

    The gzip trailer's ISIZE field is read directly — a `gzip -l` class read, NO decompression —
    and is modulo 2**32, so it is corrected upward against the compressed size using a
    conservative floor on the achievable ratio. On a short/unreadable file it falls back to the
    on-disk size scaled by a nominal ratio rather than raising: a sizing estimate must never be
    the thing that kills a run.
    """
    try:
        size = os.path.getsize(path)
    except OSError:
        return 0
    if not path.endswith(".gz"):
        return size
    isize = _gzip_isize(path)
    if isize is None:
        return size * _NOMINAL_RATIO
    if size >= _WRAP_POSSIBLE_BYTES:
        while isize < size:
            isize += 1 << 32            # ISIZE is mod 2**32; lift by whole wraps
    return isize
