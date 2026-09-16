"""Tests for the opt-in WS receive-loop timer (bot/core/ws_timing.py).

Diagnostic-only module: default OFF records nothing and creates no file; ON buffers
per-message timing and flushes to a SEPARATE CSV that is hard-barred from clobbering any
reserved log/backtest/loss-cap file. All pure (no real WS, no event loop needed except the
probe early-return test).
"""
import os

import pytest

from bot.core import ws_timing
from bot.core.ws_timing import WSLoopTimer, assert_safe_output, run_loop_lag_probe


def _timer(tmp_path, monkeypatch, *, enabled, sample_n=1, flush_n=100):
    """Fresh timer (not the process singleton) writing to a temp CSV."""
    monkeypatch.setattr(ws_timing, "_OUT", str(tmp_path / "ws_loop_timing.csv"))
    t = WSLoopTimer()
    t.enabled, t.sample_n, t.flush_n = enabled, sample_n, flush_n
    return t


# ── default OFF is a no-op ────────────────────────────────────────────────────────────

def test_off_records_nothing_and_creates_no_file(tmp_path, monkeypatch):
    t = _timer(tmp_path, monkeypatch, enabled=False)
    t.record_message("kalshi", 0.0)      # self-guard: off → buffers nothing
    t.record_message("poly_us", 0.0)
    t.flush()
    assert t._buf == []
    assert not os.path.exists(ws_timing._OUT)   # lazy open → off run never creates the file


def test_configure_defaults_to_disabled(monkeypatch):
    # config.WS_LOOP_TIMING defaults False → configure() leaves the timer off.
    monkeypatch.setattr(ws_timing.config, "WS_LOOP_TIMING", False)
    t = WSLoopTimer()
    t.configure()
    assert t.enabled is False


def test_configure_reads_flags_when_enabled(monkeypatch):
    monkeypatch.setattr(ws_timing.config, "WS_LOOP_TIMING", True)
    monkeypatch.setattr(ws_timing.config, "WS_LOOP_TIMING_SAMPLE_N", 5)
    monkeypatch.setattr(ws_timing.config, "WS_LOOP_TIMING_FLUSH_N", 50)
    t = WSLoopTimer()
    t.configure()
    assert t.enabled is True and t.sample_n == 5 and t.flush_n == 50


# ── hard bar on reserved files ────────────────────────────────────────────────────────

def test_assert_safe_output_rejects_reserved_files():
    for bad in ("logs/would_fire.csv", "logs/execution_pnl.csv", "logs/settlement_capture.csv",
                "some/dir/fat_spike_samples.csv"):
        with pytest.raises(ValueError):
            assert_safe_output(bad)
    assert_safe_output("logs/ws_loop_timing.csv")   # the legitimate target → no raise


def test_ensure_writer_refuses_reserved_out(tmp_path, monkeypatch):
    # If _OUT were ever pointed at a reserved file, opening the writer must fail loudly.
    monkeypatch.setattr(ws_timing, "_OUT", "logs/would_fire.csv")
    t = WSLoopTimer()
    t.enabled = True
    t._buf.append(["x"])
    t.flush()                                  # flush swallows + logs; writer stays unopened
    assert t._writer is None


# ── ON: buffering, flushing, row shapes ───────────────────────────────────────────────

def test_on_buffers_and_flush_writes_header_and_rows(tmp_path, monkeypatch):
    t = _timer(tmp_path, monkeypatch, enabled=True, sample_n=1)
    import time
    t.record_message("poly_us", time.perf_counter() - 0.002)   # ~2ms handler
    t.record_message("poly_us", time.perf_counter() - 0.001)
    t.record_loop_lag(0.005)                                   # 5ms loop lag
    t.flush()
    rows = open(ws_timing._OUT).read().strip().splitlines()
    assert rows[0] == "wall_ts,venue,record,msg_seq,gap_ms,handler_ms,rate_1s,loop_lag_ms"
    assert len(rows) == 4                                       # header + 2 msg + 1 loop
    first_msg = rows[1].split(",")
    assert first_msg[1] == "poly_us" and first_msg[2] == "msg" and first_msg[4] == ""  # gap blank #1
    assert rows[2].split(",")[4] != ""                         # 2nd msg has a real gap
    loop = rows[3].split(",")
    assert loop[1] == "event_loop" and loop[2] == "loop"
    assert loop[7] == "5.0000" and loop[4] == "" and loop[5] == ""  # lag set, msg cols blank


def test_sampling_one_in_n_thins_rows_but_tracks_gap(tmp_path, monkeypatch):
    t = _timer(tmp_path, monkeypatch, enabled=True, sample_n=3)
    for _ in range(6):
        t.record_message("kalshi", 0.0)
    assert len(t._buf) == 2                                     # only seq 3 and 6 buffered
    # gap is tracked on EVERY call (not just sampled), so the sampled rows carry a real gap.
    assert t._buf[0][4] != "" and t._buf[1][4] != ""


def test_flush_clears_buffer_and_is_idempotent(tmp_path, monkeypatch):
    t = _timer(tmp_path, monkeypatch, enabled=True, sample_n=1)
    t.record_message("kalshi", 0.0)
    t.flush()
    assert t._buf == []
    t.flush()                                                  # no rows → no-op, no error
    assert open(ws_timing._OUT).read().strip().count("\n") == 1  # header + 1 row only


# ── probe is safe-by-default ──────────────────────────────────────────────────────────

async def test_loop_lag_probe_returns_immediately_when_disabled(monkeypatch):
    # The probe is added to the runner gather unconditionally; off → it must return at once
    # (not spin). Uses the module singleton, which defaults disabled.
    monkeypatch.setattr(ws_timing.ws_timer, "enabled", False)
    await run_loop_lag_probe(interval=0.01)                     # returns, doesn't loop
