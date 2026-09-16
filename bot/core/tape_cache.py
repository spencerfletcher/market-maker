"""
bot/core/tape_cache.py
──────────────────────
A process-lifetime cache for PARSED TAPE ROWS, sitting under the family's existing read seams.

    from bot.core import tape_cache
    rows = tape_cache.file_rows(path, required=REQUIRED_COLUMNS, label="spread tape",
                                slugs=frozenset(cands), open_fn=open_csv)

⛔ **IT CACHES BYTES→DICTS AND NOTHING ELSE.** What is stored is exactly what
`csv.DictReader` yielded for one file, optionally narrowed to a slug set. No parsing, no
segmentation, no aggregation, no interpretation — every owner keeps doing all of that on its own
rows, so the never-re-derive law is untouched. If this module ever grows a `Decimal`, a timestamp
bound or a per-slug grouping, it has become a second implementation of somebody else's reader and
must be reverted.

⛔ **WHY IT EXISTS.** The composer reads the touch family 2×, the moat family 2× and the trade
family 5× in a single pass, because each owner owns its own read — which is the correct design and
is also why a 24 h pass costs ~13 min and a 240 h pass was killed at 1 h 35 m. The duplicate work
is at the BYTE level only, so that is the only level worth sharing.

⛔ **THE ROWS ARE SHARED AND READ-ONLY — NEVER MUTATE A ROW THIS MODULE RETURNS.** A hit (and a
subset reuse) serves the SAME dict objects the miss built; an owner that normalises in place
(`row["ts"] = Decimal(...)`, a status rewrite) silently corrupts every later reader of that
file, and only on the second read — the exact "two readers of one file disagree" shape this
cache exists to make impossible. Copying would defeat the perf win, so the contract is the
rule: owners parse INTO THEIR OWN structures, never into the row. [Verified 2026-08-22: no
current consumer mutates; pinned by test_returned_rows_are_SHARED_and_the_contract_is_read_only.]

⛔ **`read_fn` MUST BE A PURE FUNCTION OF `(path, required, slugs)`.** The key captures nothing
else — a future caller injecting a read_fn with a time bound or an extra filter would collide
on the key and be served another caller's rows. Extra semantics belong in the OWNER, after the
cache, like every other interpretation.

⛔ **INVALIDATION IS `(realpath, mtime_ns, size)` — ALL THREE.** The collector rotates these tapes
every ~10 minutes UNDER a running reader, and a same-second append is routine, so `mtime` alone is
not enough resolution and `size` alone cannot see a rewrite that preserves length. Dropping any
component serves stale bytes to a reader whose whole contract is a bounded, right-edge-fixed
window — the exact failure the composer's "capture the right edge once" discipline exists to
prevent, re-introduced one layer down.

⛔ **MEMORY-BOUNDED, PER FAMILY, AND IT REFUSES RATHER THAN GROWS.** An uncapped reader has already
memory-halted a live maker run on the box, so:
  · a read with NO slug set is never cached (that is the whole-universe read — `tap_cell_flows`
    touches all ~9,600 books — and caching it is precisely the unbounded case);
  · each FAMILY (`label`) has its own row budget, so a huge trade fold cannot evict the touch
    family's entries and re-create the very re-read this exists to remove;
  · a single file whose rows exceed the family budget is STREAMED and not cached, rather than
    evicting everything else for one file;
  · a family that REACHES its budget is dropped whole and never cached again — see below.

✅ **A SUBSET QUERY REUSES A WIDER ENTRY.** Rows are already narrowed by slug, so a request for a
SUBSET of a cached entry's slug set is answered by filtering it in memory. That is what makes the
composer's second trade read (`poly_size_rec.build_books`, scoped to books with a timeline) free
after the first (scoped to every candidate) — without either caller changing which rows it asks
for.

⛔ **A FAMILY THAT EVICTS IS A FAMILY THAT CANNOT PAY — IT IS DROPPED AND NEVER CACHED AGAIN**
[PERF-3, 2026-08-26]. Every reader here walks its file list FORWARD, once per pass. Under LRU that
is the textbook sequential-scan thrash: if the pass does not fit in the budget, the entry evicted
to make room for file N is exactly the entry the NEXT pass asks for first, so the second pass
misses on every file it reads — the cache pays full price in RAM and returns nothing. MEASURED on
the live mirror at `--since-h 24`: **hits 0 · misses 768 · evictions 636 · rows retained 1,552,243**
(~1.1 GB) inside a 4.52 GB peak. So the FIRST eviction in a family is the proof that the family's
working set does not fit, and from that moment the family is marked and served straight from
`read_fn` — which is what `PMB_TAPE_CACHE_ROWS=0` was doing by hand, per family instead of
per process, and without the operator having to know. A family that DOES fit is untouched and
still hits. ⚠️ Direction of the residual: a pass that re-read its files in REVERSE order would hit
under LRU and now will not. No reader in this repo does that, and the memory bound is the one that
has halted a live maker.

⚠️ **PROCESS-LIFETIME, NOT PERSISTENT.** Nothing is written to disk. A long-lived process that
reads a rotating tape for hours will hold whatever it last read; the cap is what bounds that, and
`clear()` is available to a caller that knows a phase is over.
"""
from __future__ import annotations

import os
from typing import Callable, Iterable, Iterator, Optional, Sequence

#: Rows a single FAMILY may hold. ⛔ **MEASURED, not estimated** [PERF-2, 2026-08-22]: a cached row
#: is a `DictReader` dict costing **697 B (moat, 10 col) / 644 B (touch, 8 col) / 1,011 B (trade,
#: 12 col)** by deep `sys.getsizeof` over 2,000 real-header rows with sharing accounted. So 400k
#: rows is **0.26–0.40 GB** for one family and ~0.9 GB across four — not the 600–800 MB per family
#: this line used to claim from an unmeasured "~1.5–2 KB/row". ⛔ That wrong number is exactly the
#: failure PERF-2 §3 names: an estimate of a structure's size is not a measurement, and this one
#: was 2–3x high in the direction that made the cache look like the memory problem when it was
#: ~4% of a 22 GB resident set. Tune with `PMB_TAPE_CACHE_ROWS`; `0` disables the cache entirely,
#: which is the honest switch for anything that must run on the 1.9 GB box.
#: ⚠️ **THE BUDGET IS NO LONGER THE ONLY BOUND** [PERF-3, 2026-08-26]: a family that reaches it is
#: dropped and stops being cached, so on a tape too big for the budget the cache costs ~nothing
#: instead of the full 400k rows per family. See `_evict` and the module header.
DEFAULT_MAX_ROWS_PER_FAMILY = 400_000


def _budget() -> int:
    raw = os.environ.get("PMB_TAPE_CACHE_ROWS", "").strip()
    if not raw:
        return DEFAULT_MAX_ROWS_PER_FAMILY
    try:
        return max(0, int(raw))
    except ValueError:
        return DEFAULT_MAX_ROWS_PER_FAMILY


class _Entry:
    #: ⛔ NO `seq`. The recency stamp existed ONLY to pick an LRU victim, and there is no victim to
    #: pick any more — a family that reaches its budget is dropped whole [PERF-3, 2026-08-26].
    #: Keeping it would leave a field nothing reads and a rule nothing enforces.
    __slots__ = ("rows", "slugs")

    def __init__(self, rows: list, slugs: frozenset) -> None:
        self.rows = rows
        self.slugs = slugs


#: (label) -> {key -> _Entry}. Keyed per FAMILY so the budget is per family.
_families: dict[str, dict] = {}
#: Families that have reached their budget and are therefore never cached again — module header.
_thrashing: set[str] = set()
_stats = {"hits": 0, "misses": 0, "uncacheable": 0, "evictions": 0, "rows": 0, "dropped": 0}


def stats() -> dict:
    """A copy of the counters, for a provenance line. Reporting only — nothing branches on it."""
    return dict(_stats)


def clear() -> None:
    """Drop everything. For tests and for a caller that knows a phase is over."""
    _families.clear()
    _thrashing.clear()
    _stats.update(hits=0, misses=0, uncacheable=0, evictions=0, rows=0, dropped=0)


def _fingerprint(path: str) -> Optional[tuple]:
    """`(realpath, mtime_ns, size)` — the identity of these BYTES, or None if unstat-able.

    ⛔ All three, and `realpath` first: a glob, the explicit tape and a symlinked laptop mirror all
    name the same file, and the family's own expansion rule already dedups on `realpath` for the
    same reason (a double read doubles every summed quantity).
    """
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (os.path.realpath(path), st.st_mtime_ns, st.st_size)


def file_rows(path: str, *, required: Sequence[str], label: str,
              slugs: Optional[frozenset], open_fn: Callable,
              read_fn: Callable[[str], Iterable[dict]],
              slug_column: str = "slug") -> Iterator[dict]:
    """Rows of ONE file, from cache when possible. `read_fn(path)` is the owner's own reader.

    `slugs=None` means "every row" → never cached (see the module header) and simply delegated.
    With a slug set, rows are narrowed to it BEFORE caching; every consumer in this family already
    filters by the same column, so the narrowing cannot change any owner's numbers — it removes
    rows the owner would have dropped one layer up.

    ⛔ `read_fn` is injected rather than implemented here. The owner decides what a row IS (its
    required-column check, its gz handling, its schema-archive rules); this module decides only
    whether it has to run again.
    """
    if slugs is None or _budget() <= 0 or label in _thrashing:
        _stats["uncacheable"] += 1
        yield from read_fn(path)
        return

    fp = _fingerprint(path)
    if fp is None:                     # unreadable → let the owner raise its own error
        _stats["uncacheable"] += 1
        yield from read_fn(path)
        return

    key = (fp, tuple(required))
    family = _families.setdefault(label, {})
    entry = family.get(key)
    if entry is not None and slugs <= entry.slugs:
        _stats["hits"] += 1
        if slugs == entry.slugs:
            yield from entry.rows
        else:
            # ✅ SUBSET REUSE — the rows are already slug-narrowed, so a narrower question is a
            # filter, never a re-read.
            yield from (r for r in entry.rows
                        if str(r.get(slug_column, "")).strip() in slugs)
        return

    _stats["misses"] += 1
    rows: list = []
    budget = _budget()
    stream = iter(read_fn(path))
    for row in stream:
        if str(row.get(slug_column, "")).strip() not in slugs:
            continue
        rows.append(row)
        if len(rows) > budget:
            # ⛔ ONE FILE MAY NOT EVICT A FAMILY, AND MAY NOT GROW PAST IT EITHER. Past the budget
            # this file is simply not cached — the buffered head is handed over and the SAME open
            # iterator continues, so nothing is re-read and nothing further is retained.
            _stats["uncacheable"] += 1
            yield from rows
            for tail in stream:
                if str(tail.get(slug_column, "")).strip() in slugs:
                    yield tail
            return
    _store(label, key, rows, slugs)
    yield from rows


def _store(label: str, key: tuple, rows: list, slugs: frozenset) -> None:
    family = _families.setdefault(label, {})
    family[key] = _Entry(rows, slugs)
    _evict(label)
    _stats["rows"] = sum(len(e.rows) for fam in _families.values() for e in fam.values())


def _evict(label: str) -> None:
    """The family's ceiling — and on the FIRST breach of it, GIVE UP on caching that family.

    ⛔ **AN EVICTION IS A VERDICT, NOT A ROUTINE HOUSEKEEPING STEP** [PERF-3, 2026-08-26]. It says
    this family's pass does not fit in the budget, and every reader here scans its file list
    forward, so under LRU the entry thrown out for file N is the one the next pass wants first:
    hits go to ZERO while the retained rows keep costing RAM (measured 0/768 with 636 evictions
    holding 1.55 M rows). Once that is proven, the honest move is to stop paying — the family is
    dropped whole and marked, and every later read is delegated straight to its owner.
    """
    family = _families.get(label, {})
    budget = _budget()
    total = sum(len(e.rows) for e in family.values())
    if total <= budget or not family:
        return
    # ⛔ Dropped WHOLE, not trimmed to fit: the entries that would survive an LRU trim are the ones
    # this pass just read and the next pass will not ask for again before they are evicted too.
    _stats["evictions"] += len(family)
    _stats["dropped"] += 1
    family.clear()
    _thrashing.add(label)
