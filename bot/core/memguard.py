"""
bot/core/memguard.py
────────────────────
RSS / system-memory headroom guard for the long-running money-path processes.

WHY. A small box, repeated recorded OOM kills, and `bot/kalshi/maker.py` naming SIGKILL as the
one death that ALWAYS strands live orders. The OOM killer sends SIGKILL — no handler runs, no
cancel-all, no flatten, no durable write. Hazards were caught by hand before this existed; it
exists so the next one is caught by the process instead of the kernel.

The job is narrow: notice the approach and halt through the process's OWN teardown while there is
still enough memory to run it. A clean halt with orders cancelled beats a SIGKILL with orders
resting.

TWO SIGNALS, DELIBERATELY:
  · our own RSS — the leak we can attribute; and
  · system headroom = `MemAvailable` + `SwapFree` — our model of kill distance (see the
    verification note below). The OOM killer does not care whose fault the pressure is; it picks
    the largest RSS, which on this box is us. So low system-wide headroom halts us even when our
    own footprint is small.

SWAP COUNTS TOWARD THE FLOOR. Under normal reclaim the kernel pushes anonymous pages to swap
before OOM-killing, so RAM-avail alone low usually just means paging. A RAM-only floor produced
a false halt with plenty of swap free, triggered by the hourly producer stack, and was replaced
rather than the RAM upgraded [operator decision]. The halt is therefore a DISJUNCTION [mm-review
BLOCKING-1 — a combined-only floor would let the halt fire at ~0 RAM with only swap left, where
the teardown itself would thrash]:
  · COMBINED headroom (avail + swap free) <= MIN_AVAIL — the OOM-distance floor; and
  · RAM avail alone <= HARD_RAM_FLOOR — a residency guarantee for the teardown
    (cancel-all → flatten → sweep is venue HTTP across possibly hundreds of books; it must not
    run entirely from a swapfile). The floor clears every recorded false halt.
RAM-avail low but above the hard floor is a WARN ("paging territory" — quote-cycle latency),
and swap unreadable falls back to RAM-only headroom — conservative, halts earlier.
✅ VERIFIED on this box's FULL kill history [2026-08-12, /var/log/kern.log incl. all rotated
.gz — 14 recorded OOM kills, 2026-07-16 → 2026-08-08]: ALL 14 fired with swap EXHAUSTED
(`Free swap` 40–244 kB at the kill moment, never more; swappiness=60) — the kernel really does
drain swap before killing here, every time it has ever killed. Still a model at the margins: a
fast allocator can outrun swap-out and OOM with swap free, and /proc/meminfo is host-wide
(cgroup limits invisible) — which is what the hard RAM floor and the RSS ceiling backstop. ⚠️ The 80 MB hard floor was chosen to clear the recorded false halts (104–116 MB),
NOT derived from a measured teardown cost — the calibrating read is peak-RSS delta across a
full-slate shadow teardown. ⚠️ The five false halts' swap-free values were never recorded (the
old halt string had no swap term); only attempt 4's ~1.7 GB is session-observed. A box with NO
swap configured reads SwapFree 0 (not None) and the combined floor degenerates to RAM-only at
256 — stricter than the old 120, safe direction, but loud: check() warns when SwapTotal is 0.

/tmp IS RAM HERE (a 953 MB tmpfs against 1.9 GB total), so anything staged there spends the very
resource this guard protects — CLAUDE.md's scratch-file rule is a memory rule, not a tidiness one.
It is REPORTED but never halts: the writer is usually another process, and halting a live maker
over someone else's scratch files would be the guard causing the outage.

⚠️ FAIL DIRECTION IS *OPEN*, AND THAT IS THE OPPOSITE OF `reconcile.py`'S RULE — on purpose.
An unreadable POSITION is missing evidence about money, so it must read as danger. An unreadable
`/proc/meminfo` is missing evidence about a PROXY and carries no information about memory pressure
at all; treating it as danger would let a procfs hiccup stop a live maker. Unknown is therefore
loud, surfaced by `scripts/opswatch.py`, and non-halting.
"""
from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass

from bot.core import config

log = logging.getLogger(__name__)

_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else 4096
_MB = 1024.0 * 1024.0


@dataclass(frozen=True)
class MemLimits:
    """All in MB. **0 disables that individual check** — same convention as the loss caps.

    `min_avail_mb` is the HALT floor on COMBINED avail+swap headroom; `hard_ram_floor_mb` is
    the HALT floor on RAM avail alone (teardown residency guarantee); `warn_avail_mb` is the
    non-halting paging warn on RAM avail alone."""
    warn_rss_mb: float
    halt_rss_mb: float
    min_avail_mb: float
    tmpfs_warn_mb: float
    warn_avail_mb: float = 0.0
    hard_ram_floor_mb: float = 0.0
    tmpfs_path: str = "/tmp"


@dataclass(frozen=True)
class MemStatus:
    level: str                     # ok | warn | halt | unknown
    detail: str
    rss_mb: float | None = None
    avail_mb: float | None = None
    tmpfs_used_mb: float | None = None
    swap_free_mb: float | None = None

    @property
    def should_halt(self) -> bool:
        return self.level == "halt"


def limits_from_config() -> MemLimits:
    """Deployed thresholds. `MEMGUARD_ENABLED=false` zeroes every check rather than adding a
    second `if enabled:` branch at each call site — one disable path, not four."""
    if not getattr(config, "MEMGUARD_ENABLED", True):
        return MemLimits(0.0, 0.0, 0.0, 0.0)
    return MemLimits(
        warn_rss_mb=float(getattr(config, "MEMGUARD_WARN_RSS_MB", 400.0)),
        halt_rss_mb=float(getattr(config, "MEMGUARD_HALT_RSS_MB", 700.0)),
        min_avail_mb=float(getattr(config, "MEMGUARD_MIN_AVAIL_MB", 256.0)),
        tmpfs_warn_mb=float(getattr(config, "MEMGUARD_TMPFS_WARN_MB", 400.0)),
        warn_avail_mb=float(getattr(config, "MEMGUARD_WARN_AVAIL_MB", 120.0)),
        hard_ram_floor_mb=float(getattr(config, "MEMGUARD_HARD_RAM_FLOOR_MB", 80.0)),
    )


# ── readers (None = could not read; never raise) ─────────────────────────────────────────────

def read_rss_mb(pid: int | None = None) -> float | None:
    """Resident set size in MB from `/proc/<pid>/statm` (field 2 = resident pages).

    `statm` rather than `status`: it is one short line with no parsing, and this runs every
    quote cycle."""
    target = "self" if pid is None else str(pid)
    try:
        with open(f"/proc/{target}/statm", encoding="ascii") as f:
            resident_pages = int(f.read().split()[1])
    except (OSError, IndexError, ValueError):
        return None
    return resident_pages * _PAGE_SIZE / _MB


def read_avail_mb() -> float | None:
    """System `MemAvailable` in MB — the kernel's own estimate of allocatable-without-swapping
    memory, which is a far better OOM predictor than `MemFree`."""
    try:
        with open("/proc/meminfo", encoding="ascii") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return float(line.split()[1]) / 1024.0
    except (OSError, IndexError, ValueError):
        return None
    return None


def read_swap_free_mb() -> float | None:
    """System `SwapFree` in MB. Together with `MemAvailable` this is the headroom the OOM killer
    actually exhausts before firing — RAM-avail alone low just means paging."""
    try:
        with open("/proc/meminfo", encoding="ascii") as f:
            for line in f:
                if line.startswith("SwapFree:"):
                    return float(line.split()[1]) / 1024.0
    except (OSError, IndexError, ValueError):
        return None
    return None


def read_swap_total_mb() -> float | None:
    """System `SwapTotal` in MB. Only used to make "no swap configured" LOUD: SwapFree reads 0
    (not None) on a swapless box, silently degenerating the combined floor to RAM-only at 256 —
    stricter than the old 120, but a false-halt class if the swapfile ever fails to mount."""
    try:
        with open("/proc/meminfo", encoding="ascii") as f:
            for line in f:
                if line.startswith("SwapTotal:"):
                    return float(line.split()[1]) / 1024.0
    except (OSError, IndexError, ValueError):
        return None
    return None


def read_tmpfs_used_mb(path: str = "/tmp") -> float | None:
    """Bytes used on the filesystem holding `path`, in MB. On this box /tmp is tmpfs, so this is
    RAM consumed by files. Linux-only: elsewhere /tmp is ordinary disk (macOS statvfs reports the
    whole APFS volume), so this returns None rather than a number that is not memory."""
    if sys.platform != "linux":
        return None
    try:
        st = os.statvfs(path)
    except OSError:
        return None
    return (st.f_blocks - st.f_bfree) * st.f_frsize / _MB


# ── the decision ─────────────────────────────────────────────────────────────────────────────

def assess(rss_mb: float | None, avail_mb: float | None, tmpfs_used_mb: float | None,
           limits: MemLimits, swap_free_mb: float | None = None) -> MemStatus:
    """Pure verdict. Halting conditions are checked with `>=` / `<=`: a guard written with a
    strict inequality is a guard that is one byte away from not firing.

    The availability floor is on COMBINED avail+swap headroom (see module docstring); swap
    unreadable falls back to avail alone, which halts EARLIER — the conservative direction."""
    reasons: list[str] = []

    halt = False
    if limits.halt_rss_mb > 0 and rss_mb is not None and rss_mb >= limits.halt_rss_mb:
        halt = True
        reasons.append(f"rss {rss_mb:.0f}MB >= halt {limits.halt_rss_mb:.0f}MB")
    if avail_mb is not None:
        # Two floors, deliberately (see module docstring): the combined floor is OOM distance,
        # the hard RAM floor is a residency guarantee so the teardown never runs from swap.
        # Swap None falls back to RAM-only headroom (conservative), and the halt string renders
        # it "?" — a procfs hiccup must not be recorded as "swap free 0MB" in the runs ledger.
        swap_for_sum = swap_free_mb if swap_free_mb is not None else 0.0
        headroom = avail_mb + swap_for_sum
        if limits.min_avail_mb > 0 and headroom <= limits.min_avail_mb:
            halt = True
            reasons.append(f"system avail {avail_mb:.0f}MB + swap free {_fmt(swap_free_mb)} = "
                           f"{headroom:.0f}MB combined <= floor {limits.min_avail_mb:.0f}MB "
                           f"(the OOM killer picks the largest RSS — that is us)")
        if limits.hard_ram_floor_mb > 0 and avail_mb <= limits.hard_ram_floor_mb:
            # No `and not halt`: if BOTH floors are breached the record must say so — a halt
            # below the residency floor means "expect a degraded flatten, go check the venue",
            # and suppressing that attribution under the milder combined reason hides it
            # [mm-review round-2 concern 5].
            halt = True
            reasons.append(f"system avail {avail_mb:.0f}MB <= hard RAM floor "
                           f"{limits.hard_ram_floor_mb:.0f}MB — teardown needs resident memory "
                           f"regardless of swap (free {_fmt(swap_free_mb)})")
    if halt:
        return MemStatus("halt", "; ".join(reasons), rss_mb, avail_mb, tmpfs_used_mb,
                         swap_free_mb)

    if limits.warn_rss_mb > 0 and rss_mb is not None and rss_mb >= limits.warn_rss_mb:
        reasons.append(f"rss {rss_mb:.0f}MB >= warn {limits.warn_rss_mb:.0f}MB")
    if limits.warn_avail_mb > 0 and avail_mb is not None and avail_mb <= limits.warn_avail_mb:
        # RAM-avail low with swap headroom left = the box is paging: quote-cycle latency, not an
        # imminent kill. Never halts — the combined floor above owns the halt.
        reasons.append(f"system avail {avail_mb:.0f}MB <= {limits.warn_avail_mb:.0f}MB — paging "
                       f"territory (swap free {_fmt(swap_free_mb)}); expect latency jitter")
    if (limits.tmpfs_warn_mb > 0 and tmpfs_used_mb is not None
            and tmpfs_used_mb >= limits.tmpfs_warn_mb):
        # Never halts — see the module docstring.
        reasons.append(f"{limits.tmpfs_path} holds {tmpfs_used_mb:.0f}MB and is RAM-backed on "
                       f"this box (>= {limits.tmpfs_warn_mb:.0f}MB)")
    if reasons:
        return MemStatus("warn", "; ".join(reasons), rss_mb, avail_mb, tmpfs_used_mb,
                         swap_free_mb)

    if rss_mb is None and avail_mb is None:
        # Nothing measurable. Loud, and NOT a halt — see the fail-direction note above.
        return MemStatus("unknown", "memory could not be read (/proc unavailable) — headroom is "
                                    "UNMONITORED for this process",
                         rss_mb, avail_mb, tmpfs_used_mb, swap_free_mb)

    return MemStatus("ok", f"rss={_fmt(rss_mb)} avail={_fmt(avail_mb)} "
                           f"swap_free={_fmt(swap_free_mb)} "
                           f"{limits.tmpfs_path}={_fmt(tmpfs_used_mb)}",
                     rss_mb, avail_mb, tmpfs_used_mb, swap_free_mb)


def _fmt(v: float | None) -> str:
    return "?" if v is None else f"{v:.0f}MB"


_swap_total_checked = False


def check(limits: MemLimits | None = None, *, label: str = "") -> MemStatus:
    """Read, assess, log at the matching level, return the verdict.

    Never raises: this is called from inside a trading loop, and every reader is wrapped because a
    guard that can throw is a new way to die.
    """
    lim = limits if limits is not None else limits_from_config()
    try:
        rss = read_rss_mb()
    except Exception:
        rss = None
    try:
        avail = read_avail_mb()
    except Exception:
        avail = None
    try:
        tmpfs = read_tmpfs_used_mb(lim.tmpfs_path)
    except Exception:
        tmpfs = None
    try:
        swap_free = read_swap_free_mb()
    except Exception:
        swap_free = None
    global _swap_total_checked
    if not _swap_total_checked:
        # Latches on the FIRST read either way — swap presence doesn't change mid-process, and
        # re-reading /proc/meminfo every quote cycle for a constant is waste [round-3 nit].
        try:
            total = read_swap_total_mb()
            if total is not None:
                _swap_total_checked = True
            if total == 0.0:
                log.warning("🧠 no swap configured (SwapTotal=0) — the combined avail+swap halt "
                            "floor degenerates to a RAM-only floor at the combined threshold; "
                            "if a swapfile should be mounted, it is not")
        except Exception:
            pass

    st = assess(rss, avail, tmpfs, lim, swap_free_mb=swap_free)
    tag = f"[{label}] " if label else ""
    if st.level == "halt":
        log.critical(
            f"🧠 {tag}MEMORY HALT: {st.detail}. Stopping CLEANLY now — a SIGKILL from the OOM "
            f"killer runs no teardown and would leave live orders resting on the venue."
        )
    elif st.level == "warn":
        log.warning(f"🧠 {tag}memory pressure: {st.detail}")
    elif st.level == "unknown":
        log.warning(f"🧠 {tag}{st.detail}")
    return st
