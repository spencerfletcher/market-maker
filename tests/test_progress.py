"""`bot/core/progress.py` — live progress lines for long tape reads.

Two properties are load-bearing and both are pinned here: the lines go to STDERR ONLY (stdout is a
parsed surface in this repo), and a burst cannot flood the log (a 9,630-book loop must not write
9,630 lines).
"""
from __future__ import annotations

import sys
import time
import tracemalloc

import pytest

from bot.core import progress


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    # conftest silences progress for the whole suite (PMB_PROGRESS_SILENT=1, warnings-only
    # stderr); THIS module's tests are the one place emission itself is under test, so
    # re-enable per-test. _silent() reads the env at emit time precisely so this works.
    monkeypatch.delenv("PMB_PROGRESS_SILENT", raising=False)
    # Under pytest stdout is never a tty, which would put every test on the 60 s non-tty floor.
    # The tty branch is the DEFAULT here so each test below keeps pinning `MIN_INTERVAL_S`; the
    # non-tty floor has its own pins, which flip this back.
    monkeypatch.setattr(progress, "_stdout_is_tty", lambda: True)
    progress._reset_for_tests()
    yield
    progress._reset_for_tests()


def test_a_BURST_of_ticks_cannot_exceed_the_rate_ceiling(capsys, monkeypatch):
    """⛔⛔ MUTANT: emit per call (drop the `MIN_INTERVAL_S` check) → RED. The composer's loops run
    to ~9,600 units; a line each would bury the fold banners that make the log readable."""
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 3600.0)
    progress.phase("read:trade tape")
    for i in range(5000):
        progress.tick(i, 5000)
    err = capsys.readouterr().err
    assert err.count("phase=") <= 1, f"a 5,000-tick burst wrote {err.count('phase=')} lines"


def test_a_run_INSIDE_one_interval_prints_NOTHING(capsys):
    """⛔ MUTANT: emit unconditionally on `phase()` → RED. It put a progress line into the stderr
    of tools whose contract is that stderr carries WARNINGS ONLY (`poly_arrivals`' "a clean tape
    warns about nothing" went red on exactly this)."""
    progress.phase("read:touch tape")
    progress.tick(1, 3)
    progress.tick(2, 3)
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


def test_STDOUT_IS_NEVER_TOUCHED(capsys, monkeypatch):
    """⛔⛔ MUTANT: `print(...)` without `file=sys.stderr` → RED. `poly_night` parses the prelaunch
    battery's stdout and the composer's stdout IS its artifact — a progress line there is a
    data-corruption bug, not a cosmetic one."""
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    progress.phase("read:moat tape")
    progress.tick(1, 2, rows=1_234_567)
    captured = capsys.readouterr()
    assert captured.out == "", "stdout is a parsed surface"
    assert "phase=read:moat tape" in captured.err
    assert "rows=1.2M" in captured.err
    assert "unit=1/2" in captured.err


def test_the_line_carries_ONLY_FACTS_and_no_ETA(capsys, monkeypatch):
    """⛔ No ETA, deliberately: the cost is SUPER-LINEAR in kept rows, so `done/total × elapsed`
    would be a confident lie exactly when someone is deciding whether to kill the run."""
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    progress.phase("read:trade tape")
    progress.tick(3, 13)
    err = capsys.readouterr().err
    assert "elapsed=" in err
    for forbidden in ("eta", "ETA", "remaining", "%"):
        assert forbidden not in err


def test_a_phase_BOUNDARY_prints_once_the_floor_has_elapsed(capsys, monkeypatch):
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    progress.phase("read:touch tape")
    progress.phase("read:moat tape")
    err = capsys.readouterr().err
    assert "phase=read:touch tape" in err and "phase=read:moat tape" in err


def test_a_phase_with_NO_ticks_still_heartbeats_so_silence_means_DEAD(capsys, monkeypatch):
    """⛔ The half that makes silence diagnostic. A phase whose work is one un-chunked call — a
    sort, an owner's whole-tape parse — has nothing to tick, and that is the shape the killed run
    died in: the last log line was a fold banner from an hour earlier."""
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.05)
    monkeypatch.setattr(progress, "_HEARTBEAT_POLL_S", 0.01)
    progress.phase("read:touch tape")
    capsys.readouterr()                     # drop the boundary line
    # ⚠️ POLLED, not a fixed sleep: the emitter is a daemon THREAD, and a fixed sleep makes this
    # test a bet on the scheduler under a loaded parallel suite (it flaked exactly that way).
    seen = ""
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline and "(alive)" not in seen:
        time.sleep(0.05)                    # …do nothing at all, like a long single call
        seen += capsys.readouterr().err
    assert "(alive)" in seen, "a working phase must keep saying so"


def test_tick_without_a_phase_is_a_NO_OP_never_an_error(capsys):
    """A library added to observe a run must never be the thing that kills it."""
    progress.tick(1, 2)
    assert capsys.readouterr().err == ""


# ─────────────────────────────────────────────────────────────────────────────
# F6 — the peak-RSS guard: degrade at the soft ceiling, REFUSE at the hard one
# ─────────────────────────────────────────────────────────────────────────────
def test_ru_maxrss_is_scaled_PER_PLATFORM(monkeypatch):
    """⛔⛔ THE UNIT TRAP. `ru_maxrss` is BYTES on macOS and KiB on Linux. A guard that hardcodes
    one is wrong by 1024x on the other box — and on Linux it is wrong in the PERMISSIVE direction,
    so a 10 GB ceiling reads as 10 TB and never fires. MUTANT: drop the scale (or invert it) → RED."""
    assert progress._RSS_SCALE == (1 if sys.platform == "darwin" else 1024)
    rss = progress.peak_rss_bytes()
    assert rss is not None and 5e6 < rss < 5e11, (
        f"{rss} is not a plausible byte count for a test process — the scale is wrong by ~1024x")


def test_the_SOFT_ceiling_degrades_ONCE_and_says_so(capsys, monkeypatch):
    """⛔ MUTANT: fire the callback every boundary → RED. `ru_maxrss` never DEcreases, so a
    re-check would degrade on every phase for the rest of the run and bury the one line that
    matters."""
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    calls: list = []
    progress.arm_rss_guard(soft_bytes=1, hard_bytes=10 ** 15, on_soft=lambda: calls.append(1))
    progress.phase("read:moat tape")
    progress.phase("size_rec")
    err = capsys.readouterr().err
    assert calls == [1], "the degrade path fires exactly once"
    assert "over the soft ceiling" in err and "DEGRADING" in err


def test_the_HARD_ceiling_REFUSES_the_next_phase(monkeypatch):
    """⛔ Refusing beats swapping: the 240 h run that reached ~22 GB did not fail, it became
    undiagnosable — two hours of thrash with its own heartbeat starved. MUTANT: warn instead of
    raising → RED."""
    progress.arm_rss_guard(soft_bytes=1, hard_bytes=1)
    with pytest.raises(MemoryError, match="hard ceiling"):
        progress.phase("size_rec")


def test_a_FAILING_degrade_callback_never_kills_the_run(capsys, monkeypatch):
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)

    def boom():
        raise RuntimeError("nope")

    progress.arm_rss_guard(soft_bytes=1, hard_bytes=10 ** 15, on_soft=boom)
    progress.phase("read:moat tape")            # must not raise
    assert "degrade callback failed" in capsys.readouterr().err


def test_an_UNARMED_guard_is_inert(capsys):
    progress.phase("read:moat tape")
    progress.phase("size_rec")
    assert "soft ceiling" not in capsys.readouterr().err


# ─────────────────────────────────────────────────────────────────────────────
# §4 — the tracemalloc phase-boundary instrument
# ─────────────────────────────────────────────────────────────────────────────
def test_the_instrument_is_OFF_by_default_and_leaves_no_trace(capsys, monkeypatch):
    """⚠️ A run with tracemalloc active is NOT a valid wall-time measurement (~2x time, ~1.3x
    memory), so it must be opt-in and byte-identical when off. MUTANT: start tracing regardless
    → RED."""
    monkeypatch.delenv(progress.TRACEMALLOC_ENV, raising=False)
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    assert not progress.tracemalloc_enabled()
    progress.phase("a")
    progress.phase("b")
    assert not tracemalloc.is_tracing()
    err = capsys.readouterr().err
    assert "traced=" not in err and "rss=" not in err


def test_the_instrument_retains_EXACTLY_ONE_snapshot_across_N_phases(capsys, monkeypatch):
    """⛔⛔ THE PROPERTY THAT KEEPS THE INSTRUMENT FROM BEING THE HOLDER IT HUNTS. A list of
    snapshots is itself unbounded growth — the exact failure this was added to find. MUTANT: append
    snapshots to a list → RED."""
    monkeypatch.setenv(progress.TRACEMALLOC_ENV, "1")
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    try:
        for name in ("a", "b", "c", "d"):
            progress.phase(name)
        prev = progress._state["tm_prev"]
        assert prev is not None
        assert isinstance(prev, tracemalloc.Snapshot), "one snapshot, not a collection"
        assert tracemalloc.is_tracing()
        err = capsys.readouterr().err
        assert err.count("rss=") >= 2 and "traced=" in err
        assert "peak=" in err
    finally:
        tracemalloc.stop()


def test_the_instrument_attributes_growth_to_the_phase_that_JUST_ENDED(capsys, monkeypatch):
    """The diff is `compare_to(prev, "lineno")`, so an allocation made during phase A is reported
    at the A→B boundary — not cumulatively at every later one. MUTANT: diff against the FIRST
    snapshot → the same allocation is re-reported at B→C → RED."""
    monkeypatch.setenv(progress.TRACEMALLOC_ENV, "1")
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    try:
        progress.phase("start")                 # starts tracing
        progress.phase("allocating")            # first snapshot
        capsys.readouterr()
        held = [bytearray(200_000) for _ in range(20)]      # ~4 MB during THIS phase
        progress.phase("after")                 # ← the growth must appear HERE
        first = capsys.readouterr().err
        progress.phase("later")                 # ← and NOT again here
        second = capsys.readouterr().err
        assert "test_progress.py" in first, "the allocating line must be named at its own boundary"
        assert "test_progress.py" not in second, "growth must not be re-reported at the next one"
        assert len(held) == 20
    finally:
        tracemalloc.stop()


def test_the_boundary_line_NAMES_the_phase_that_ENDED_not_the_one_starting(capsys, monkeypatch):
    """⛔ MUTANT [PERF-3, 2026-08-26 — this was live]: read the name back out of `_state["phase"]`
    inside `_tracemalloc_locked` → RED. `phase()` overwrites that key BEFORE the boundary work, so
    the label named the phase about to START and every line was off by one: the real composer's
    `expand` boundary printed `(end of phase 'heartbeat')`, sending anyone reading the instrument
    to the wrong function. The diff itself was always right — only its label was wrong, which is
    the worst version, because the numbers look trustworthy."""
    monkeypatch.setenv(progress.TRACEMALLOC_ENV, "1")
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    try:
        progress.phase("expand")                # starts tracing, prints nothing
        progress.phase("heartbeat")             # first snapshot — ends 'expand'
        first = capsys.readouterr().err
        progress.phase("screen")                # ends 'heartbeat'
        second = capsys.readouterr().err
        assert "(end of phase 'expand')" in first, first
        assert "(end of phase 'heartbeat')" in second, second
        assert "'screen'" not in second, "a boundary must never name the phase that is starting"
    finally:
        tracemalloc.stop()


def test_the_instrument_NEVER_kills_the_run(capsys, monkeypatch):
    """`progress.py`'s standing contract: a library added to OBSERVE a run must never be the thing
    that kills it. MUTANT: drop the try/except → RED."""
    monkeypatch.setenv(progress.TRACEMALLOC_ENV, "1")
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    monkeypatch.setattr(tracemalloc, "take_snapshot",
                        lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    try:
        progress.phase("a")
        progress.phase("b")                     # must not raise
        assert "tracemalloc instrument failed" in capsys.readouterr().err
    finally:
        if tracemalloc.is_tracing():
            tracemalloc.stop()


def test_the_rss_scale_is_correct_for_BOTH_platforms_not_just_this_one():
    """⛔⛔ MUTANT: `return 1` (hardcode macOS) → RED on the linux case; `return 1024` → RED on
    darwin. The live box is LINUX and this laptop is macOS, so both branches are real — and a
    module constant computed from `sys.platform` at import can only ever be tested on the platform
    the suite happens to run on, which would ship the box's branch unexercised [review 2026-08-22].

    On Linux the error direction is PERMISSIVE: KiB read as bytes makes a 10 GB ceiling look like
    10 TB, so the guard never fires and the run swaps — the exact failure it exists to prevent."""
    assert progress._rss_scale("darwin") == 1, "macOS ru_maxrss is already BYTES"
    assert progress._rss_scale("linux") == 1024, "Linux ru_maxrss is KiB — 1024x, permissively wrong"
    assert progress._rss_scale("freebsd") == 1024, "anything not-darwin takes the KiB branch"


def test_the_soft_default_sits_ABOVE_the_measured_steady_state_and_below_the_hard():
    """⛔ A ceiling under the measured steady state is not a guard, it is a permanent alarm: at
    5 GB the 240 h pass (5.55 GB measured) tripped on every rung from 96 h up and 'degraded' became
    the normal state, so the line stopped carrying information. MUTANT: put it back under 5.55 →
    RED. ⚠️ This does NOT close the ≤5 GB target — PERF-3 owns that."""
    assert progress.DEFAULT_SOFT_RSS_BYTES > 5.55e9, (
        "the soft ceiling must sit above the MEASURED 240 h steady state or it fires on every run")
    assert progress.DEFAULT_SOFT_RSS_BYTES < progress.DEFAULT_HARD_RSS_BYTES


def test_the_guard_documents_that_it_only_sees_phase_BOUNDARIES():
    """⚠️ The peak is INSIDE `size_rec`, so a breach there is observed on the way OUT — too late to
    degrade the phase that breached. The honest scope has to be in the docstring, because the
    number it prints looks like a live reading."""
    doc = progress.arm_rss_guard.__doc__ or ""
    assert "BOUNDARIES ONLY" in doc and "too late to degrade that phase" in doc
    assert "tracemalloc" in doc, "…and it must name the in-phase diagnostic instead"


def test_a_NON_TTY_stdout_puts_the_lines_on_the_SIXTY_second_floor(capsys, monkeypatch):
    """⛔⛔ MUTANT: return `MIN_INTERVAL_S` unconditionally from `_interval` (drop the non-tty
    branch) → RED. The operator's complaint is a LOG, not a screen: the verdict child ran 5
    minutes and wrote a progress line every 15 s into a file nobody watched live."""
    monkeypatch.setattr(progress, "_stdout_is_tty", lambda: False)
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    monkeypatch.setattr(progress, "NONTTY_INTERVAL_S", 3600.0)
    progress.phase("fold")
    capsys.readouterr()                     # a phase start may or may not have made the floor
    for i in range(200):
        progress.tick(i, 200)
    assert capsys.readouterr().err == "", "a non-tty run is on the 60 s floor, not the 15 s one"


def test_a_TTY_keeps_the_fifteen_second_floor_when_the_non_tty_one_is_wider(capsys, monkeypatch):
    """The other side of the branch — `_stdout_is_tty` must be what selects it, not the wider
    constant winning everywhere."""
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.0)
    monkeypatch.setattr(progress, "NONTTY_INTERVAL_S", 3600.0)
    progress.phase("fold")
    progress.tick(1, 2)
    assert "phase=fold" in capsys.readouterr().err


def test_the_ALIVE_token_is_dropped_in_a_LOG(capsys, monkeypatch):
    """The line arriving IS the aliveness fact once nobody is watching live."""
    monkeypatch.setattr(progress, "_stdout_is_tty", lambda: False)
    monkeypatch.setattr(progress, "MIN_INTERVAL_S", 0.05)
    monkeypatch.setattr(progress, "NONTTY_INTERVAL_S", 0.05)
    monkeypatch.setattr(progress, "_HEARTBEAT_POLL_S", 0.01)
    progress.phase("read:touch tape")
    capsys.readouterr()
    seen = ""
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline and "phase=read:touch tape" not in seen:
        time.sleep(0.05)
        seen += capsys.readouterr().err
    assert "phase=read:touch tape" in seen, "silence still means dead, tty or not"
    assert "(alive)" not in seen
