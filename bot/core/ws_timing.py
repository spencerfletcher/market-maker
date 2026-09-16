"""
bot/core/ws_timing.py
─────────────────────
Lightweight, OPT-IN receive-loop timing for the Poly US + Kalshi WS feeds.

Answers one question: is the WS receive loop keeping up with the feed, or is it
falling behind during high-volume game moments? Both feeds process INLINE on the
receive coroutine (recv → parse → cache update → callback, no queue between them;
the callback only *schedules* detection via create_task, so detection runs
decoupled and is NOT part of the inline span measured here). With no queue, the
keeping-up signal is: per-message inline handler time vs inter-arrival gap — when
handler time approaches the gap under load, the loop is saturated. The independent
event-loop-lag probe is the closest analogue to "queue depth" for an asyncio system.

Design constraints (see the diagnostic request + CLAUDE.md):
  • Default OFF. When `WS_LOOP_TIMING` is false this module costs one bool check per
    message (`ws_timer.enabled`) and nothing else — no file handle, no allocation.
  • Negligible overhead when ON: perf_counter only, no per-message disk I/O (rows
    are buffered and flushed periodically / 1-in-N sampled), so the measurement does
    not slow the loop it measures.
  • Writes ONLY logs/ws_loop_timing.csv — a SEPARATE diagnostic file, hard-barred
    (mirroring scripts/settlement_capture.assert_safe_output) from clobbering any
    existing log, backtest, or loss-cap file.
  • Zero behaviour change: this records what was true; it never feeds a gate, sizer,
    fire decision, staleness check, or DRY_RUN. Detection/cache logic is byte-for-byte
    identical whether timing is on or off.

CSV rows (one `record` kind per row, so the file is greppable):
  record="msg"  — venue=poly_us|kalshi: gap_ms, handler_ms, rate_1s (loop_lag_ms blank)
  record="loop" — venue=event_loop: loop_lag_ms (the others blank)
"""
from __future__ import annotations

import asyncio
import csv
import os
import time
from typing import Optional, TextIO

from bot.core import config
from bot.core.logger import get_logger

log = get_logger(__name__)

_OUT = "logs/ws_loop_timing.csv"
# Reserved names this diagnostic must NEVER write — clobbering them would corrupt the
# settlement-backtest join or feed a fake loss into the live loss cap.
_FORBIDDEN = {
    "execution_pnl.csv", "would_fire.csv", "would_fire_samples.csv",
    "rejected_edges.csv", "positive_edges.csv", "edge_freshness.csv",
    "settlement_capture.csv", "fat_spike_samples.csv",
}

_HEADER = [
    "wall_ts", "venue", "record", "msg_seq",
    "gap_ms", "handler_ms", "rate_1s", "loop_lag_ms",
]


def assert_safe_output(path: str) -> None:
    """Refuse to open a reserved log/backtest/loss-cap file as the timing CSV. A hard
    assertion, not a convention: this file is pure observability and must never stand in
    for a file something live reads. Basename match catches the reserved names anywhere."""
    if os.path.basename(os.path.normpath(path)) in _FORBIDDEN:
        raise ValueError(
            f"ws_timing refuses to write '{path}': that is a reserved log/backtest/loss-cap "
            f"file. Use {_OUT}."
        )


class WSLoopTimer:
    """Process-wide singleton (`ws_timer`). All methods run on the single asyncio event
    loop thread — record/flush never interleave mid-update — so no lock is needed."""

    def __init__(self) -> None:
        self.enabled: bool = False          # set by configure(); plain bool → 1 load/msg when off
        self.sample_n: int = 1              # buffer 1 row per N messages (per venue)
        self.flush_n: int = 200             # flush the buffer to disk every N buffered rows
        self._buf: list[list] = []
        self._writer: Optional[csv.writer] = None
        self._file: Optional[TextIO] = None
        # Per-venue receive-loop state.
        self._seq: dict[str, int] = {}
        self._last_start: dict[str, float] = {}     # perf_counter of the previous message
        self._rate_start: dict[str, float] = {}     # rolling 1s rate window start (perf_counter)
        self._rate_count: dict[str, int] = {}
        self._last_rate: dict[str, float] = {}      # last computed msgs/s, emitted on sampled rows

    def configure(self) -> None:
        """Read the config flags once (called at runner start). Safe to call repeatedly."""
        self.enabled = bool(config.WS_LOOP_TIMING)
        self.sample_n = max(1, int(config.WS_LOOP_TIMING_SAMPLE_N))
        self.flush_n = max(1, int(config.WS_LOOP_TIMING_FLUSH_N))
        if self.enabled:
            log.info(
                f"ws_timing: ENABLED → {_OUT} (sample 1-in-{self.sample_n}, "
                f"flush every {self.flush_n} rows)"
            )

    def record_message(self, venue: str, t_start: float) -> None:
        """Record one inline receive-loop pass. `t_start` is perf_counter() captured
        immediately BEFORE parse+cache+callback ran; handler time is measured to now.

        Inter-arrival gap and rolling rate are tracked on EVERY call (cheap); a CSV row is
        buffered only 1-in-`sample_n`, so the gap stays true even when sampling thins rows.
        Callers gate this behind `ws_timer.enabled` for speed; the internal guard below makes
        the module self-safe if a future caller forgets — an off timer records nothing."""
        if not self.enabled:
            return
        now = time.perf_counter()
        handler_ms = (now - t_start) * 1000.0

        prev = self._last_start.get(venue)
        gap_ms = (t_start - prev) * 1000.0 if prev is not None else None
        self._last_start[venue] = t_start

        # Rolling msgs/sec over a ~1s window, decoupled from sampling so the rate is
        # always available on whichever message happens to be sampled.
        self._rate_count[venue] = self._rate_count.get(venue, 0) + 1
        rstart = self._rate_start.get(venue)
        if rstart is None:
            self._rate_start[venue] = now
        else:
            win = now - rstart
            if win >= 1.0:
                self._last_rate[venue] = self._rate_count[venue] / win
                self._rate_start[venue] = now
                self._rate_count[venue] = 0

        seq = self._seq.get(venue, 0) + 1
        self._seq[venue] = seq
        if seq % self.sample_n != 0:
            return

        self._buf.append([
            f"{time.time():.3f}", venue, "msg", seq,
            f"{gap_ms:.3f}" if gap_ms is not None else "",
            f"{handler_ms:.4f}",
            f"{self._last_rate.get(venue, 0.0):.1f}",
            "",
        ])
        if len(self._buf) >= self.flush_n:
            self.flush()

    def record_loop_lag(self, lag_s: float) -> None:
        """Record one event-loop-lag probe sample (sleep overshoot, seconds). High lag =
        the loop is saturated (handler hogging the coroutine, or scheduled detection tasks
        piling up) — the asyncio analogue of a growing queue. Not sampled (1 row/probe tick)."""
        self._buf.append([
            f"{time.time():.3f}", "event_loop", "loop", "",
            "", "", "", f"{lag_s * 1000.0:.4f}",
        ])

    def _ensure_writer(self) -> bool:
        if self._writer is not None:
            return True
        assert_safe_output(_OUT)
        os.makedirs("logs", exist_ok=True)
        is_new = not os.path.exists(_OUT) or os.path.getsize(_OUT) == 0
        self._file = open(_OUT, "a", newline="", buffering=1)
        self._writer = csv.writer(self._file)
        if is_new:
            self._writer.writerow(_HEADER)
        return True

    def flush(self) -> None:
        """Append buffered rows to the CSV and clear the buffer. No-op if nothing buffered;
        opens the file lazily on first write so an OFF run never creates the file."""
        if not self._buf:
            return
        try:
            self._ensure_writer()
            self._writer.writerows(self._buf)  # type: ignore[union-attr]
            self._file.flush()                  # type: ignore[union-attr]
        except Exception as e:
            log.warning(f"ws_timing: flush failed: {e}")
        finally:
            self._buf.clear()


# Process-wide singleton imported by the feeds and the runner.
ws_timer = WSLoopTimer()


async def run_loop_lag_probe(interval: float = 1.0) -> None:
    """Background task: every `interval`s, measure how much asyncio.sleep overshoots
    (event-loop lag) and record it, then flush buffered rows so timing data lands within
    ~1s even at low message volume. Returns immediately (does nothing) when timing is OFF,
    so it is safe to add unconditionally to the runner's task gather."""
    if not ws_timer.enabled:
        return
    while True:
        t0 = time.perf_counter()
        await asyncio.sleep(interval)
        lag = time.perf_counter() - t0 - interval
        ws_timer.record_loop_lag(max(lag, 0.0))
        ws_timer.flush()
