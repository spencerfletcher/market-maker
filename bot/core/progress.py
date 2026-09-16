"""
bot/core/progress.py
────────────────────
LIVE progress for long tape reads — two functions, stderr only, rate-limited.

    from bot.core import progress
    progress.phase("trade_fold")
    for i, path in enumerate(paths, 1):
        progress.tick(i, len(paths), rows=n_rows)

⛔ **WHY THIS EXISTS.** On 2026-08-22 the operator killed a 240 h composer run at **1 h 35 m**
because there was no way to tell a working process from a dead one: the log's last line was a fold
banner written an hour earlier. Post-hoc phase timings do not solve that — they are printed by a
run that FINISHES. So the rule here is: **silence means dead, never working.** A phase with no
natural unit still emits a heartbeat, from a daemon thread, so the log advances even inside a
single un-chunked call.

⛔ **STDERR ONLY, ALWAYS — NEVER STDOUT, AND NEVER CONDITIONALLY.** Stdout is a PARSED surface in
this repo (`poly_night` parses the prelaunch battery's stdout; the composer's stdout IS the
artifact), so a progress line on stdout is a data-corruption bug, not a cosmetic one. A tty check
never GATES emission — these runs go through `nohup`/systemd logs, which is exactly where the
lines are needed, and a tty-gated logger is silent in the only situation that motivated it. It
only sets the RATE (`NONTTY_INTERVAL_S`) and drops the `(alive)` token.

⛔ **RATE-LIMITED, NOT PER-ROW, AND THE FLOOR COVERS PHASE STARTS TOO.** `MIN_INTERVAL_S` is a
floor between LINES, shared by ticks, phase starts and heartbeats, so a 9,630-book loop that calls
`tick` 9,630 times writes ~1 line per interval and not 9,630.

⛔ **A RUN SHORTER THAN ONE INTERVAL PRINTS NOTHING.** Nothing is emitted until the floor has
elapsed — the clock starts at import. That is deliberate and was found by a test rather than
reasoned: emitting on every `phase()` put a progress line into the stderr of tools whose contract
is that stderr carries WARNINGS ONLY (`poly_arrivals`' "a clean tape warns about nothing"), which
turns an observability aid into a false alarm. A tool that finishes in 12 s never needed watching;
the one that was killed at 1 h 35 m did.

⛔ **NO ETA.** The reason this run needed diagnosing at all is that the cost is SUPER-LINEAR in
kept rows, so "done/total × elapsed" would be a confident lie precisely when it matters. Only
`done/total` and `elapsed` are printed — facts, both of them.

⚠️ **NOT A LOGGER AND NOT A METRIC.** Nothing here is read back, aggregated, or asserted on by
another tool. If you need a number for a report, measure it and print it in the report; this
module's output is for a human watching a log tail decide whether to kill a run.
"""
from __future__ import annotations

import os
import resource
import sys
import threading
import time
import tracemalloc
from typing import Callable, Optional

#: Minimum seconds between emitted lines — the floor a burst cannot breach. 15 s is short enough
#: that a watcher can tell live from hung within one screen of `tail -f`, long enough that an
#: hour-long phase costs ~240 lines rather than one per unit of work.
MIN_INTERVAL_S = 15.0

#: Floor between lines when stdout is NOT a tty — a log file or a systemd journal, where the
#: operator reads the run after the fact and a 15 s cadence is 240 lines an hour of noise. The
#: heartbeat's `(alive)` token is dropped there too: a line that arrives at all already says the
#: process is alive, and the token only distinguishes the emitter for someone watching live.
#: ⚠️ This RATE-LIMITS, it never silences — silence still means dead (see the header).
NONTTY_INTERVAL_S = 60.0

#: How often the daemon thread wakes to consider a heartbeat. Strictly finer than
#: `MIN_INTERVAL_S` so the heartbeat lands close to the deadline rather than a full interval late.
_HEARTBEAT_POLL_S = 1.0


def _stdout_is_tty() -> bool:
    """A seam, not a policy: the emitter still writes to STDERR always (see the header). Stdout is
    what says whether a HUMAN is watching — a redirected stdout is a run being logged, not read."""
    try:
        return bool(sys.stdout.isatty())
    except (AttributeError, ValueError):    # closed / replaced stdout ⇒ treat as a log
        return False


def _interval() -> float:
    """Floor between lines, right now. Never BELOW `MIN_INTERVAL_S`, so a caller that widens the
    tty floor widens both."""
    if _stdout_is_tty():
        return MIN_INTERVAL_S
    return max(MIN_INTERVAL_S, NONTTY_INTERVAL_S)

# ─────────────────────────────────────────────────────────────────────────────
# PEAK RSS — measured, platform-corrected
# ─────────────────────────────────────────────────────────────────────────────
def _rss_scale(platform: str) -> int:
    """Multiplier turning `ru_maxrss` into BYTES on `platform`.

    ⛔⛔ **`ru_maxrss` IS BYTES ON macOS AND KiB ON LINUX.** Verified on this laptop (Darwin, bare
    interpreter = 15,532,032 — obviously bytes, not 15 GB). A guard that hardcodes one unit is
    wrong by 1024x on the other box, and on Linux it is wrong in the **PERMISSIVE** direction: a
    10 GB ceiling would read as 10 TB and never fire, which is precisely the failure mode that
    lets a run swap-thrash. The live box is Linux and this laptop is macOS, so BOTH branches are
    real and neither is hypothetical.

    ⛔ It takes the platform as an ARGUMENT so both branches are testable on one machine. A module
    constant computed from `sys.platform` at import can only ever be tested on the platform the
    suite happens to run on — i.e. the branch that matters for the box would have shipped
    unexercised [review 2026-08-22].
    """
    return 1 if platform == "darwin" else 1024


_RSS_SCALE = _rss_scale(sys.platform)


def peak_rss_bytes() -> Optional[int]:
    """This process's PEAK resident set, in BYTES, or None if unavailable.

    ⚠️ It is a HIGH-WATER MARK and never decreases, so it can arm a degrade path but can never
    disarm one. That is the correct direction: a run that has already touched 10 GB has already
    fragmented the arenas that made it 10 GB.
    """
    try:
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * _rss_scale(sys.platform)
    except (OSError, ValueError):        # platform edge
        return None


#: ⛔ **SOFT CEILING RAISED 5 → 7 GB [operator decision, review 2026-08-22]**, and the reason is
#: that a ceiling under the measured steady state is not a guard, it is a permanent alarm: the
#: 240 h pass measures **5.55 GB** and tripped the 5 GB soft ceiling on every rung from 96 h up, so
#: "degraded" became the normal state and the line stopped carrying information. 7 GB sits ABOVE
#: the steady state and well below the hard 10, which restores the signal: **a soft trip is an
#: ANOMALY again.** ⚠️ 5.55 GB still MISSES the ≤5 GB target — PERF-3 (the private design notes) owns
#: closing that, and raising this default is explicitly NOT closing it.
DEFAULT_SOFT_RSS_BYTES = 7 * 10 ** 9
DEFAULT_HARD_RSS_BYTES = 10 * 10 ** 9

_lock = threading.Lock()
_state: dict = {
    "phase": None,          # current phase name, or None
    "phase_started": None,  # monotonic
    "run_started": None,    # monotonic, first phase() of the process
    "done": None,
    "total": None,
    "rows": None,
    #: ⛔ Seeded at IMPORT, not 0: the floor must be measured from process start, or the very
    #: first call would always print and a 12-second tool would emit a progress line.
    "last_emit": time.monotonic(),
    "thread": None,
    #: F6 — (soft, hard, on_soft) once armed; the soft path fires at most ONCE.
    "rss_guard": None,
    "rss_degraded": False,
    #: §4 instrument — EXACTLY ONE previous snapshot, ever. A list of them is itself an unbounded
    #: holder, which is the failure this instrument exists to find.
    "tm_prev": None,
}


def phase(name: str) -> None:
    """Start (or switch to) a named phase. Emits a line if the rate floor has elapsed — a phase
    boundary inside a long run is the most useful line in the log, but a whole RUN inside one
    interval must stay silent (see the header).

    ⛔ Starting a phase also arms the heartbeat thread. A DAEMON thread, so it can never keep a
    finished process alive; it is started once per process and re-used across phases.
    """
    now = time.monotonic()
    with _lock:
        if _state["run_started"] is None:
            _state["run_started"] = now
        # ⛔ THE PHASE THAT JUST ENDED, CAPTURED BEFORE THE SWITCH. The instrument's whole contract
        # is that growth is attributed to the phase it happened IN; reading `_state["phase"]` after
        # the update named the phase that had not started yet, so every boundary line was labelled
        # one phase LATE — the `expand` boundary printed `(end of phase 'heartbeat')`. A mislabelled
        # holder sends the next reader to the wrong function [PERF-3, 2026-08-26].
        ended = _state["phase"]
        _state.update(phase=name, phase_started=now, done=None, total=None, rows=None)
        # ⛔ SUBJECT TO THE SAME FLOOR as a tick — see the header. A phase boundary inside a long
        # run does print (the floor has long since elapsed); a whole run inside one interval does
        # not print at all.
        if now - _state["last_emit"] >= _interval():
            _emit_locked(now, kind="start")
        _arm_heartbeat_locked()
        # ⛔ BOUNDARY-ONLY WORK, both of these, and NEITHER is subject to `MIN_INTERVAL_S`: there are
        # ~15 boundaries in a run and each one is the point of the instrument, while the floor exists
        # to stop a 9,630-iteration tick loop. Gating them would drop exactly the boundary that
        # follows a fast phase. ⛔ Never on the heartbeat thread — `take_snapshot()` under this lock
        # on a 1 s poll would serialise the whole run.
        _rss_guard_locked(name)
        _tracemalloc_locked(ended)


def arm_rss_guard(*, soft_bytes: int = DEFAULT_SOFT_RSS_BYTES,
                  hard_bytes: int = DEFAULT_HARD_RSS_BYTES,
                  on_soft: Optional[Callable[[], None]] = None) -> None:
    """Arm the peak-RSS guard, checked at every phase boundary. F6 — it DEGRADES, never dies quietly.

    ⛔ **THE ORDER MATTERS AND IT IS: soft ⇒ degrade + say so, hard ⇒ REFUSE THE NEXT PHASE.** On the
    soft breach `on_soft` runs (the composer sets `PMB_TAPE_CACHE_ROWS=0` and calls
    `tape_cache.clear()` — the cache module's own documented honest switch) and a loud line is
    printed; the callback fires at most ONCE, because `ru_maxrss` never decreases and re-running it
    every boundary would be noise.

    ⚠️ **THE DEGRADE IS NOW CLOSE TO A NO-OP, AND THAT IS DELIBERATE** [PERF-3, 2026-08-26]. It used
    to hand back ~1.55 M retained rows (~1.1 GB) mid-run; since `tape_cache` gives up on a family
    the first time it reaches its budget, a breaching run has ~2 k rows left to release. Not
    allocating beats freeing — but read the consequence honestly: between the soft ceiling and the
    hard refusal there is now a WARNING, not a recovery. Do not re-tune the soft ceiling believing a
    gigabyte comes back when it trips. What the soft line still buys is the ANOMALY signal. On the hard breach the next `phase()` raises `MemoryError`.

    ⚠️ **Refusing is the point.** A run that would have completed now stops — and that is strictly
    better than the alternative it replaces: the 240 h pass that reached ~22 GB swap-thrashed for
    two hours with its own heartbeat starved, i.e. it did not fail, it became undiagnosable. The
    cliff is binary, so the guard is too.

    ⚠️ **CHECKED AT PHASE BOUNDARIES ONLY — a phase that balloons INTERNALLY is caught at its end,
    which is too late to degrade that phase.** The peak of a composer run is inside `size_rec`, so
    a breach there is observed on the way out of it: the guard's value is the loud line and the
    refusal of the NEXT phase, not in-run savings for the phase that breached. The in-phase
    diagnostic is the tracemalloc instrument (`PMB_PROGRESS_TRACEMALLOC=1`), which reports the
    allocating file:line rather than a single number after the fact.
    """
    with _lock:
        _state["rss_guard"] = (soft_bytes, hard_bytes, on_soft)
        _state["rss_degraded"] = False


def _rss_guard_locked(name: str) -> None:
    guard = _state["rss_guard"]
    rss = peak_rss_bytes()
    if guard is None or rss is None:
        return
    soft, hard, on_soft = guard
    if rss >= hard:
        raise MemoryError(
            f"REFUSING to start phase {name!r}: peak RSS {rss / 1e9:.2f} GB is at or over the hard "
            f"ceiling {hard / 1e9:.2f} GB. Re-run with a narrower --from/--to window, or with "
            f"PMB_TAPE_CACHE_ROWS=0. ⛔ This is a deliberate refusal, not a crash: past this point "
            f"the process swaps, and a swapping run is slower AND undiagnosable (its own progress "
            f"heartbeat starves).")
    if rss >= soft and not _state["rss_degraded"]:
        _state["rss_degraded"] = True
        print(f"[{time.strftime('%H:%M:%S')}] ⛔ RSS {rss / 1e9:.2f} GB over the soft ceiling "
              f"{soft / 1e9:.2f} GB at phase={name} — DEGRADING to uncached streaming "
              f"(hard ceiling {hard / 1e9:.2f} GB refuses the next phase)",
              file=sys.stderr, flush=True)
        if on_soft is not None:
            try:
                on_soft()
            except Exception as exc:      # the guard never kills the run
                print(f"[{time.strftime('%H:%M:%S')}] ⚠️ RSS degrade callback failed: {exc!r}",
                      file=sys.stderr, flush=True)


#: §4 instrument. Off by default: tracemalloc costs ~2x time and ~1.3x memory, so a run with it on
#: is NOT a valid wall-time measurement — which is why the composer names it in provenance.
TRACEMALLOC_ENV = "PMB_PROGRESS_TRACEMALLOC"
TRACEMALLOC_TOP_ENV = "PMB_PROGRESS_TRACEMALLOC_TOP"


def tracemalloc_enabled() -> bool:
    return os.environ.get(TRACEMALLOC_ENV, "").strip().lower() in ("1", "true", "yes")


def _tracemalloc_locked(ended: Optional[str]) -> None:
    """Per-boundary allocation diff — attributing growth to the phase that just ENDED.

    ⛔ `ended` is the phase NAME THAT IS ENDING, passed in by `phase()` before it switches. It used
    to be read back out of `_state["phase"]` here, which `phase()` had already overwritten — so the
    label named the phase about to START and every line was off by one.

    ⛔ Wrapped whole in `except Exception`: `progress.py`'s standing contract is that a library
    added to OBSERVE a run must never be the thing that kills it."""
    if not tracemalloc_enabled():
        return
    try:
        if not tracemalloc.is_tracing():
            # ⛔ DEPTH 1. A deeper trace multiplies the trace table, which is itself a memory
            # holder — the instrument becoming the disease it diagnoses. file:line names a holder.
            tracemalloc.start(1)
            return
        try:
            top_n = int(os.environ.get(TRACEMALLOC_TOP_ENV, "10"))
        except ValueError:
            top_n = 10
        snap = tracemalloc.take_snapshot()
        current, peak = tracemalloc.get_traced_memory()
        rss = peak_rss_bytes()
        stamp = time.strftime("%H:%M:%S")
        print(f"[{stamp}]   rss={_bytes(rss)} traced={_bytes(current)} peak={_bytes(peak)}  "
              f"(end of phase {ended!r})", file=sys.stderr, flush=True)
        prev = _state["tm_prev"]
        if prev is not None:
            for stat in snap.compare_to(prev, "lineno")[:top_n]:
                if stat.size_diff <= 0:
                    break
                where = stat.traceback[0] if stat.traceback else None
                site = "?" if where is None else f"{os.path.basename(where.filename)}:{where.lineno}"
                print(f"[{stamp}]   +{_bytes(stat.size_diff)}  {site}  "
                      f"({stat.count} blocks, {stat.count_diff:+d})", file=sys.stderr, flush=True)
        # ⛔ EXACTLY ONE previous snapshot is retained — see `_state["tm_prev"]`.
        _state["tm_prev"] = snap
    except Exception as exc:              # never kills the run
        print(f"[{time.strftime('%H:%M:%S')}] ⚠️ tracemalloc instrument failed: {exc!r}",
              file=sys.stderr, flush=True)


def _bytes(n: Optional[int]) -> str:
    if n is None:
        return "—"
    for unit, scale in (("G", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(n) >= scale:
            return f"{n / scale:.2f}{unit}"
    return f"{n}B"


def tick(done: Optional[int] = None, total: Optional[int] = None,
         rows: Optional[int] = None) -> None:
    """Report progress inside the current phase. Rate-limited to `MIN_INTERVAL_S` between lines.

    `done`/`total` are UNITS THE CALLER ALREADY KNOWS — files of a fold, books of a loop. `rows` is
    included only when it is free to compute; a counter that costs a pass over the data does not
    belong in a progress line. Calling this without a phase is a no-op rather than an error: a
    library that crashes the run it was added to observe is worse than one that says nothing.
    """
    now = time.monotonic()
    with _lock:
        if _state["phase"] is None:
            return
        if done is not None:
            _state["done"] = done
        if total is not None:
            _state["total"] = total
        if rows is not None:
            _state["rows"] = rows
        if now - _state["last_emit"] >= _interval():
            _emit_locked(now, kind="tick")


# ─────────────────────────────────────────────────────────────────────────────
# internals — everything below runs under `_lock`
# ─────────────────────────────────────────────────────────────────────────────
def _arm_heartbeat_locked() -> None:
    if _state["thread"] is not None:
        return
    thread = threading.Thread(target=_heartbeat_loop, name="progress", daemon=True)
    _state["thread"] = thread
    thread.start()


def _heartbeat_loop() -> None:
    """Emit a line whenever `MIN_INTERVAL_S` has passed with nothing else printed.

    ⛔ This is the half that makes silence mean DEAD. A phase whose work happens inside one
    un-chunked call (a `sort`, a single owner's whole-tape parse) has no tick to hang progress on,
    and that is exactly the shape the killed run died in.
    """
    while True:
        # ⛔ THE POLL MUST BE FINER THAN THE INTERVAL, ENFORCED HERE rather than assumed by the
        # constants. A caller (or a test) that shortens `MIN_INTERVAL_S` below the poll would
        # otherwise get heartbeats a whole poll late — the docstring claimed "strictly finer" while
        # the code only hoped for it, and a flaky test is how that was found.
        time.sleep(min(_HEARTBEAT_POLL_S, max(_interval() / 4, 0.01)))
        now = time.monotonic()
        with _lock:
            if _state["phase"] is None:
                continue
            if now - _state["last_emit"] >= _interval():
                _emit_locked(now, kind="alive")


def _emit_locked(now: float, *, kind: str) -> None:
    _state["last_emit"] = now
    if _silent():
        return
    parts = [f"[{time.strftime('%H:%M:%S')}]", f"phase={_state['phase']}"]
    done, total = _state["done"], _state["total"]
    if done is not None:
        parts.append(f"unit={done}/{total}" if total is not None else f"unit={done}")
    if _state["rows"] is not None:
        parts.append(f"rows={_human(_state['rows'])}")
    started = _state["phase_started"] or now
    parts.append(f"elapsed={_hms(now - started)}")
    run_started = _state["run_started"]
    if run_started is not None and started != run_started:
        parts.append(f"run={_hms(now - run_started)}")
    # ⛔ TTY ONLY. In a log the token is noise: the line's own arrival is the aliveness fact, and
    # the emitter that wrote it is not something a post-hoc reader can act on.
    if kind == "alive" and _stdout_is_tty():
        parts.append("(alive)")
    # ⛔ STDERR. Never `print()` — that defaults to stdout, which is a parsed surface.
    print(" ".join(parts), file=sys.stderr, flush=True)


def _hms(seconds: float) -> str:
    seconds = int(max(0.0, seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


def _human(n: int) -> str:
    if n < 1_000:
        return str(n)
    if n < 1_000_000:
        return f"{n / 1_000:.1f}k"
    return f"{n / 1_000_000:.1f}M"


def _reset_for_tests() -> None:
    """Drop all state EXCEPT the daemon thread (a thread per test would leak).

    Named for what it is. There is no production caller and there must not be one: a run that
    resets its own progress state is lying about its elapsed time.
    """
    with _lock:
        _state.update(phase=None, phase_started=None, run_started=None,
                      done=None, total=None, rows=None, last_emit=time.monotonic(),
                      rss_guard=None, rss_degraded=False, tm_prev=None)


def _silent() -> bool:
    """⛔ Environment escape hatch for a caller that must not write to stderr either (a test
    harness capturing it, a cron that treats any stderr as failure). Read AT EMIT TIME, not at
    import — the whole test suite silences via `tests/conftest.py` while the progress module's
    own tests re-enable per-test; an import-time snapshot made those two needs mutually
    exclusive (measured: 4 tests red the day the conftest guard landed). A production run never
    flips the variable mid-run, so log-gap semantics are unchanged in practice."""
    return os.environ.get("PMB_PROGRESS_SILENT", "").strip().lower() in ("1", "true", "yes")
