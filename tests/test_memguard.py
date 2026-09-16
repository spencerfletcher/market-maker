"""Memory headroom guard — catch the next OOM before the kernel does.

WHY. This box has 1.9 GB of RAM and has OOM-killed roughly every other day (six on record). Two
hazards were fixed on 2026-07-27 alone, at 894 MB and 1.64 GB peak RSS. `bot/kalshi/maker.py`
names SIGKILL as the one death that always strands live orders, and SIGKILL is exactly what the
OOM killer sends — no handler, no cancel-all, no flatten.

So the guard's job is narrow and specific: notice the approach and halt CLEANLY, through the
process's own teardown, while there is still enough memory left to run it. A clean halt with
orders cancelled beats a SIGKILL with orders resting, every time.

/tmp IS RAM ON THIS BOX — a 953 MB tmpfs against 1.9 GB total — so a process staging files there
consumes exactly the resource this guard protects. It is reported, but does not halt: the writer
is usually somebody else, and halting the maker for another process's scratch files would be an
observability fault making a money decision.

FAIL DIRECTION IS DELIBERATELY *OPEN* HERE, and that is the opposite of `reconcile.py`'s rule.
The asymmetry is the point: an unreadable POSITION is missing evidence about money, so it must
read as danger; an unreadable /proc/meminfo is missing evidence about a *proxy*, and carries no
information about memory pressure at all. Halting a live maker because procfs hiccuped would be
the guard causing the outage. It is loud instead.
"""
from __future__ import annotations

import logging
import sys

import pytest

from bot.core import memguard

# The guard only ever runs on the Linux box; these tests read the box's real /proc and tmpfs
# mount, which do not exist off-Linux. The pure-decision tests below run everywhere.
_linux_only = pytest.mark.skipif(sys.platform != "linux",
                                 reason="reads /proc and the tmpfs /tmp — Linux-box only")

LIM = memguard.MemLimits(warn_rss_mb=400.0, halt_rss_mb=700.0,
                         min_avail_mb=120.0, tmpfs_warn_mb=400.0)


# ── assess(): the pure decision ──────────────────────────────────────────────────────────────

def test_a_small_process_on_a_healthy_box_is_ok():
    st = memguard.assess(rss_mb=60.0, avail_mb=800.0, tmpfs_used_mb=10.0, limits=LIM)
    assert st.level == "ok"
    assert st.should_halt is False


def test_crossing_the_warn_threshold_warns_but_does_not_halt():
    st = memguard.assess(rss_mb=450.0, avail_mb=800.0, tmpfs_used_mb=10.0, limits=LIM)
    assert st.level == "warn"
    assert st.should_halt is False


def test_crossing_the_HALT_rss_threshold_halts():
    """894 MB and 1.64 GB were both real, today."""
    st = memguard.assess(rss_mb=900.0, avail_mb=500.0, tmpfs_used_mb=10.0, limits=LIM)
    assert st.level == "halt"
    assert st.should_halt is True
    assert "900" in st.detail or "rss" in st.detail.lower()


def test_exactly_at_the_halt_threshold_halts():
    """A guard written `>` instead of `>=` is a guard that is one byte from not firing."""
    assert memguard.assess(rss_mb=700.0, avail_mb=800.0, tmpfs_used_mb=0.0,
                           limits=LIM).should_halt is True


def test_low_SYSTEM_available_memory_halts_even_when_our_own_rss_is_small():
    """The OOM killer does not care whose fault it is — it picks the biggest RSS, which on this
    box is us. A clean halt beats being the victim of someone else's leak."""
    st = memguard.assess(rss_mb=50.0, avail_mb=80.0, tmpfs_used_mb=10.0, limits=LIM)
    assert st.should_halt is True
    assert "avail" in st.detail.lower()


def test_tmpfs_pressure_warns_but_never_halts():
    st = memguard.assess(rss_mb=50.0, avail_mb=800.0, tmpfs_used_mb=900.0, limits=LIM)
    assert st.level == "warn"
    assert st.should_halt is False
    assert "tmp" in st.detail.lower()


def test_unreadable_rss_does_NOT_halt_but_is_reported_as_unknown():
    """Deliberate fail-OPEN — see the module docstring. Missing evidence about a proxy is not
    evidence of danger, and a guard must not become the outage."""
    st = memguard.assess(rss_mb=None, avail_mb=None, tmpfs_used_mb=None, limits=LIM)
    assert st.should_halt is False
    assert st.level == "unknown"


def test_a_disabled_limit_of_zero_turns_that_check_off():
    off = memguard.MemLimits(warn_rss_mb=0.0, halt_rss_mb=0.0, min_avail_mb=0.0,
                             tmpfs_warn_mb=0.0)
    st = memguard.assess(rss_mb=5000.0, avail_mb=1.0, tmpfs_used_mb=5000.0, limits=off)
    assert st.should_halt is False
    assert st.level == "ok"


# ── swap-aware floor (2026-08-12) ────────────────────────────────────────────────────────────
# The OOM killer fires when RAM *and* swap are exhausted, not when RAM alone is low. Attempt 4
# (shadow, 258 books) was halted at avail 116 MB while 1.7 GB of swap sat free — a false halt
# that cost the 27 h tape. The floor is therefore COMBINED avail+swap headroom; RAM-only low is
# a paging WARN (latency, opswatch-visible), not a halt.

_SWAP_LIM = memguard.MemLimits(warn_rss_mb=400.0, halt_rss_mb=700.0, min_avail_mb=256.0,
                               tmpfs_warn_mb=400.0, warn_avail_mb=120.0)


def test_low_ram_with_ample_swap_does_not_halt_but_warns_paging():
    """The attempt-4 shape: avail 116 MB, swap free 1.7 GB. Combined headroom ~1.9 GB is nowhere
    near the kill zone — halting here is the guard causing the outage."""
    st = memguard.assess(rss_mb=80.0, avail_mb=116.0, tmpfs_used_mb=10.0, limits=_SWAP_LIM,
                         swap_free_mb=1769.0)
    assert st.should_halt is False
    assert st.level == "warn"
    assert "paging" in st.detail.lower()


def test_combined_headroom_at_or_below_the_floor_halts():
    """avail 100 + swap 156 = 256 = floor exactly — `<=`, one byte from not firing."""
    st = memguard.assess(rss_mb=80.0, avail_mb=100.0, tmpfs_used_mb=10.0, limits=_SWAP_LIM,
                         swap_free_mb=156.0)
    assert st.should_halt is True
    assert "swap" in st.detail.lower()


def test_unreadable_swap_falls_back_to_ram_only_headroom():
    """Swap unreadable is CONSERVATIVE (halts earlier), unlike the module's fail-open rule for a
    fully unreadable /proc — avail alone is still real evidence, so it is used alone."""
    st = memguard.assess(rss_mb=80.0, avail_mb=100.0, tmpfs_used_mb=10.0, limits=_SWAP_LIM,
                         swap_free_mb=None)
    assert st.should_halt is True
    ok = memguard.assess(rss_mb=80.0, avail_mb=300.0, tmpfs_used_mb=10.0, limits=_SWAP_LIM,
                         swap_free_mb=None)
    assert ok.should_halt is False


def test_warn_avail_of_zero_disables_the_paging_warn():
    lim = memguard.MemLimits(warn_rss_mb=400.0, halt_rss_mb=700.0, min_avail_mb=256.0,
                             tmpfs_warn_mb=400.0, warn_avail_mb=0.0)
    st = memguard.assess(rss_mb=80.0, avail_mb=116.0, tmpfs_used_mb=10.0, limits=lim,
                         swap_free_mb=1769.0)
    assert st.level == "ok"


def test_exactly_at_the_paging_warn_boundary_warns():
    st = memguard.assess(rss_mb=80.0, avail_mb=120.0, tmpfs_used_mb=10.0, limits=_SWAP_LIM,
                         swap_free_mb=1769.0)
    assert st.level == "warn"
    assert st.should_halt is False


# ── the hard RAM floor (mm-review 2026-08-12 BLOCKING-1) ────────────────────────────────────
# The combined floor alone would let the halt fire at ~0 MB RAM + 256 MB swap — the teardown
# (cancel-all → flatten → sweep, venue HTTP across possibly hundreds of books) would run
# entirely from a swapfile. The halt is a DISJUNCTION: combined floor OR hard RAM floor.

_HARD_LIM = memguard.MemLimits(warn_rss_mb=400.0, halt_rss_mb=700.0, min_avail_mb=256.0,
                               tmpfs_warn_mb=400.0, warn_avail_mb=120.0,
                               hard_ram_floor_mb=80.0)


def test_ram_below_the_hard_floor_halts_even_with_ample_swap():
    st = memguard.assess(rss_mb=80.0, avail_mb=70.0, tmpfs_used_mb=10.0, limits=_HARD_LIM,
                         swap_free_mb=1900.0)
    assert st.should_halt is True
    assert "hard ram floor" in st.detail.lower()


def test_exactly_at_the_hard_ram_floor_halts():
    assert memguard.assess(rss_mb=80.0, avail_mb=80.0, tmpfs_used_mb=10.0, limits=_HARD_LIM,
                           swap_free_mb=1900.0).should_halt is True


def test_every_recorded_false_halt_clears_the_hard_floor():
    """The five recorded false halts — 116 (attempt 4), 111, 108, 105 (real run, left dem −88
    open), 104 — all clear the 80 MB HARD floor. ⚠️ This establishes the HARD-FLOOR leg only:
    the swap_free=1400 below is INVENTED (the old halt string carried no swap term, so the real
    values were never recorded — only attempt 4's ~1.7 GB is session-observed). If any of those
    halts had swap already drained, the combined floor halts it again and this test would not
    notice [mm-review round-3]. The cycle-tape avail/swap columns exist to close this."""
    for avail in (104.0, 105.0, 108.0, 111.0, 116.0):
        st = memguard.assess(rss_mb=80.0, avail_mb=avail, tmpfs_used_mb=10.0,
                             limits=_HARD_LIM, swap_free_mb=1400.0)
        assert st.should_halt is False, f"avail={avail} false-halted again"


def test_deployed_hard_ram_floor_is_wired_and_80():
    """[mm-review round-2 BLOCKING-1] The dataclass default for hard_ram_floor_mb is 0.0 — the
    DISABLED state — so losing the limits_from_config() wire or the config default silently
    turns the round-1 BLOCKING fix off. Two-layer pin, same shape as the 256 pin: a revert of
    either layer to 0 (or the RAM-era absence) goes RED here."""
    from bot.core import config
    assert config.MEMGUARD_HARD_RAM_FLOOR_MB == 80.0
    assert memguard.limits_from_config().hard_ram_floor_mb == 80.0


def test_both_floor_breaches_are_attributed_in_the_halt_reason():
    """[mm-review round-2 concern 5] avail 50 / swap 100 breaches BOTH floors. The residency
    breach means 'expect a degraded flatten — check the venue for resting orders'; recording
    only the milder combined reason hides that. Both attributions must appear."""
    st = memguard.assess(rss_mb=80.0, avail_mb=50.0, tmpfs_used_mb=10.0, limits=_HARD_LIM,
                         swap_free_mb=100.0)
    assert st.should_halt is True
    assert "combined" in st.detail
    assert "hard ram floor" in st.detail.lower()


def test_unreadable_swap_in_a_halt_reason_renders_question_mark_not_zero():
    """A procfs hiccup must not be recorded in the runs ledger as 'swap free 0MB' — that
    asserts a memory state that was never measured [mm-review CONCERN-5]."""
    st = memguard.assess(rss_mb=80.0, avail_mb=100.0, tmpfs_used_mb=10.0, limits=_SWAP_LIM,
                         swap_free_mb=None)
    assert st.should_halt is True
    assert "swap free ?" in st.detail
    assert "0MB" not in st.detail.split("swap free")[1].split("=")[0]


def test_check_wires_the_swap_reader_into_the_verdict(monkeypatch):
    """Wiring pin: identical RAM picture, only swap differs — the verdict must differ. Guards
    against check() never passing swap_free_mb (the default-argument trap)."""
    monkeypatch.setattr(memguard, "read_rss_mb", lambda pid=None: 80.0)
    monkeypatch.setattr(memguard, "read_avail_mb", lambda: 116.0)
    monkeypatch.setattr(memguard, "read_tmpfs_used_mb", lambda p="/tmp": 10.0)
    monkeypatch.setattr(memguard, "read_swap_free_mb", lambda: 1769.0)
    assert memguard.check(limits=_SWAP_LIM, label="t").should_halt is False
    monkeypatch.setattr(memguard, "read_swap_free_mb", lambda: 50.0)
    assert memguard.check(limits=_SWAP_LIM, label="t").should_halt is True


def test_limits_from_config_reads_the_warn_avail_knob(monkeypatch):
    from bot.core import config
    monkeypatch.setattr(config, "MEMGUARD_WARN_AVAIL_MB", 77.0, raising=False)
    assert memguard.limits_from_config().warn_avail_mb == 77.0


def test_deployed_default_floor_is_the_combined_256():
    """The 2026-08-12 fix RAISED the floor 120 → 256 because it now measures RAM+swap COMBINED —
    a revert of config.py's default to the RAM-era 120 must go RED here (mutant 3 survived the
    getattr-fallback pin: config.py always defines the attr, so the fallback is unreachable and
    the config constant is the only real site). A deliberate env override legitimately changes
    this — then update the pin with the reason."""
    from bot.core import config
    assert config.MEMGUARD_MIN_AVAIL_MB == 256.0
    assert memguard.limits_from_config().min_avail_mb == 256.0


@_linux_only
def test_read_swap_free_mb_returns_a_plausible_value():
    swap = memguard.read_swap_free_mb()
    assert swap is not None
    assert swap >= 0.0


# ── the readers ──────────────────────────────────────────────────────────────────────────────

@_linux_only
def test_read_rss_mb_returns_a_plausible_value_for_this_process():
    rss = memguard.read_rss_mb()
    assert rss is not None
    assert 1.0 < rss < 8000.0, rss


@_linux_only
def test_read_avail_mb_returns_a_plausible_value():
    avail = memguard.read_avail_mb()
    assert avail is not None
    assert avail > 0.0


@_linux_only
def test_read_tmpfs_used_mb_reads_the_ram_backed_mount():
    used = memguard.read_tmpfs_used_mb("/tmp")
    assert used is not None
    assert used >= 0.0


def test_read_tmpfs_used_mb_is_None_off_linux(monkeypatch):
    """"/tmp is RAM" is a Linux/tmpfs fact. Off-Linux, statvfs reads whatever filesystem holds
    /tmp — on macOS that is the whole APFS volume (442 GB observed), which is disk, not memory —
    so the reader must report nothing rather than a fiction that trips the warn threshold."""
    monkeypatch.setattr(sys, "platform", "darwin")
    assert memguard.read_tmpfs_used_mb("/tmp") is None


def test_the_readers_return_None_rather_than_raising_on_a_bad_path():
    assert memguard.read_rss_mb(pid=4_000_000) is None
    assert memguard.read_tmpfs_used_mb("/no/such/mount/point") is None


# ── check(): reads + logs + verdict ──────────────────────────────────────────────────────────

def test_check_logs_CRITICAL_when_it_halts(caplog, monkeypatch):
    monkeypatch.setattr(memguard, "read_rss_mb", lambda pid=None: 900.0)
    monkeypatch.setattr(memguard, "read_avail_mb", lambda: 500.0)
    monkeypatch.setattr(memguard, "read_tmpfs_used_mb", lambda p="/tmp": 10.0)
    with caplog.at_level(logging.INFO, logger="bot.core.memguard"):
        st = memguard.check(limits=LIM, label="maker")
    assert st.should_halt is True
    assert any(r.levelno >= logging.CRITICAL for r in caplog.records), \
        "an imminent OOM was not logged loudly"


def test_check_logs_a_WARNING_at_the_warn_level(caplog, monkeypatch):
    monkeypatch.setattr(memguard, "read_rss_mb", lambda pid=None: 450.0)
    monkeypatch.setattr(memguard, "read_avail_mb", lambda: 800.0)
    monkeypatch.setattr(memguard, "read_tmpfs_used_mb", lambda p="/tmp": 10.0)
    with caplog.at_level(logging.INFO, logger="bot.core.memguard"):
        memguard.check(limits=LIM, label="maker")
    assert any(r.levelno == logging.WARNING for r in caplog.records)


def test_check_is_silent_when_healthy(caplog, monkeypatch):
    monkeypatch.setattr(memguard, "read_rss_mb", lambda pid=None: 60.0)
    monkeypatch.setattr(memguard, "read_avail_mb", lambda: 800.0)
    monkeypatch.setattr(memguard, "read_tmpfs_used_mb", lambda p="/tmp": 10.0)
    with caplog.at_level(logging.INFO, logger="bot.core.memguard"):
        memguard.check(limits=LIM, label="maker")
    assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []


def test_check_never_raises_even_if_every_reader_explodes(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("procfs gone")
    monkeypatch.setattr(memguard, "read_rss_mb", boom)
    monkeypatch.setattr(memguard, "read_avail_mb", boom)
    monkeypatch.setattr(memguard, "read_tmpfs_used_mb", boom)
    monkeypatch.setattr(memguard, "read_swap_free_mb", boom)
    monkeypatch.setattr(memguard, "read_swap_total_mb", boom)
    st = memguard.check(limits=LIM, label="maker")
    assert st.should_halt is False


def test_limits_from_config_reads_the_deployed_knobs(monkeypatch):
    from bot.core import config
    monkeypatch.setattr(config, "MEMGUARD_HALT_RSS_MB", 123.0, raising=False)
    assert memguard.limits_from_config().halt_rss_mb == 123.0


def test_disabling_memguard_in_config_produces_an_all_zero_limit_set(monkeypatch):
    from bot.core import config
    monkeypatch.setattr(config, "MEMGUARD_ENABLED", False, raising=False)
    lim = memguard.limits_from_config()
    assert lim.halt_rss_mb == 0.0 and lim.min_avail_mb == 0.0
