"""Heartbeat + deadman: a 24/7 process must be observable from OUTSIDE itself.

WHY. The maker prints to stdout and nothing else. If it is SIGKILLed by the OOM killer — six times
on record for this box — the process simply stops existing. There is no `WatchdogSec`, no
`OnFailure=`, and the last thing in the log is an ordinary cycle. "It stopped" and "it is quietly
running with nothing to quote" produce identical evidence.

A deadman fixes that by INVERTING the signal: instead of the dying process reporting its death
(which SIGKILL makes impossible), the living process continuously reports its life, and a third
party notices the absence. So the tests below care about exactly two things — that a beat lands on
disk durably as it happens, and that the ABSENCE of beats is classified as a problem rather than
as silence.

The states are deliberately five, not two. `missing`, `corrupt` and `stale` are all "not ok" but
they mean different things to an operator, and a `clean exit` must NOT read as a deadman trip or
every planned shutdown pages someone.
"""
from __future__ import annotations

import json
import os
import stat

import pytest

from bot.core import heartbeat


def test_beat_writes_the_operator_facing_fields(tmp_path):
    """'still alive, N markets quoted, inventory X, P&L Y' — the actual ask."""
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat(markets_quoted=3, inventory=-2.5, pnl=-0.17)
    d = json.loads(open(hb.path).read())
    assert d["name"] == "maker"
    assert d["markets_quoted"] == 3
    assert d["inventory"] == -2.5
    assert d["pnl"] == -0.17
    assert d["pid"] == os.getpid()
    assert d["ts"] > 0


def test_beat_is_written_DURABLY(tmp_path, monkeypatch):
    """A heartbeat that is only in the page cache when the box resets tells the recovery path
    nothing. It must go through the fsync'd writer, not `open().write()`."""
    calls = []
    real = heartbeat.durable.write_json_durable
    monkeypatch.setattr(heartbeat.durable, "write_json_durable",
                        lambda p, o, **k: (calls.append(p), real(p, o, **k))[1])
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat()
    assert calls == [hb.path]


def test_beat_is_READABLE_BY_ANOTHER_USER(tmp_path):
    """⛔ The whole point of a heartbeat is that a DIFFERENT process reads it, and on the box that
    process runs as a different user: `<unit>` beats as ROOT, `opswatch` reads as
    `ubuntu`. The durable writer stages through `tempfile.mkstemp` (0600 regardless of umask, by
    design — the `UMask=` drop-in could not fix it), so on 2026-08-28 opswatch got Errno 13 and
    paged "liveness UNKNOWN" every sweep against a perfectly healthy process.

    A strict umask is set here on purpose: the mode must come from an explicit chmod, not from
    inheritance. Mutation: drop `mode=` in `Heartbeat._write` → RED.
    """
    old = os.umask(0o077)
    try:
        hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
        hb.beat()
        mode = stat.S_IMODE(os.stat(hb.path).st_mode)
        assert mode & stat.S_IROTH, f"heartbeat is not world-readable: {mode:o}"
        assert mode == 0o644, f"{mode:o}"
    finally:
        os.umask(old)


def test_mark_exit_is_also_world_readable(tmp_path):
    """The exit stamp is the one an operator most needs to read from outside the process."""
    old = os.umask(0o077)
    try:
        hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
        hb.mark_exit("halted:memory")
        assert stat.S_IMODE(os.stat(hb.path).st_mode) == 0o644
    finally:
        os.umask(old)


def test_the_file_carries_its_own_staleness_budget(tmp_path):
    """The watchdog must not need separate configuration to know how late is too late — otherwise
    the budget drifts out of sync with the process's real cadence and the deadman either
    false-trips or never trips."""
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat()
    d = json.loads(open(hb.path).read())
    assert d["interval_s"] == 10.0
    assert d["stale_after_s"] > 10.0


def test_seq_increments_so_a_frozen_writer_is_distinguishable_from_a_slow_one(tmp_path):
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat()
    hb.beat()
    assert json.loads(open(hb.path).read())["seq"] == 2


def test_beat_never_raises_into_the_caller(tmp_path, monkeypatch):
    """It is called from the quote loop. Instrumentation must not be able to stop trading."""
    def explode(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(heartbeat.durable, "write_json_durable", explode)
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat()


def _capture_chown(monkeypatch):
    calls: list[tuple[str, int, int]] = []
    monkeypatch.setattr(heartbeat.os, "chown", lambda path, uid, gid: calls.append((path, uid, gid)))
    return calls


def test_a_ROOT_writer_hands_the_published_file_to_the_directory_owner(tmp_path, monkeypatch):
    """`<unit>` runs as root into a ubuntu-owned `logs/heartbeat/`, and mkstemp+replace
    publishes a root:root file the directory's owner cannot rewrite. After a successful write the
    ONE published file is chowned to the directory's uid/gid — never the directory, never
    recursive. Mutation: drop the `geteuid() == 0` gate → the non-root pin goes RED."""
    monkeypatch.setattr(heartbeat.os, "geteuid", lambda: 0)
    calls = _capture_chown(monkeypatch)
    hb = heartbeat.Heartbeat("shed", interval_s=10.0, directory=str(tmp_path))
    hb.beat()
    st = os.stat(tmp_path)
    assert calls == [(hb.path, st.st_uid, st.st_gid)]


def test_a_NON_ROOT_writer_never_chowns(tmp_path, monkeypatch):
    monkeypatch.setattr(heartbeat.os, "geteuid", lambda: 1000)
    calls = _capture_chown(monkeypatch)
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat()
    assert calls == []


def test_a_FAILED_chown_never_raises_into_the_caller(tmp_path, monkeypatch):
    def explode(*a, **k):
        raise PermissionError("nope")
    monkeypatch.setattr(heartbeat.os, "geteuid", lambda: 0)
    monkeypatch.setattr(heartbeat.os, "chown", explode)
    hb = heartbeat.Heartbeat("shed", interval_s=10.0, directory=str(tmp_path))
    hb.beat()
    assert json.loads(open(hb.path).read())["seq"] == 1


# ── the deadman ──────────────────────────────────────────────────────────────────────────────

def test_a_fresh_heartbeat_is_OK(tmp_path):
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat(now=1000.0)
    st = heartbeat.deadman_check(hb.path, now=1001.0)
    assert st.state == "ok"
    assert st.ok is True


def test_a_STOPPED_heartbeat_TRIPS_THE_DEADMAN(tmp_path):
    """The OOM-kill signature: beats, then nothing, forever."""
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat(now=1000.0)
    st = heartbeat.deadman_check(hb.path, now=1000.0 + 10_000)
    assert st.state == "stale"
    assert st.ok is False
    assert st.age_s == pytest.approx(10_000)


def test_the_deadman_does_not_trip_on_one_late_cycle(tmp_path):
    """A single slow cycle (a 429 retry ladder is 1.5+3+6s) must not page anyone."""
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat(now=1000.0)
    assert heartbeat.deadman_check(hb.path, now=1012.0).state == "ok"


def test_a_MISSING_heartbeat_is_not_OK(tmp_path):
    """Absence of a heartbeat file is absence of evidence of life — never evidence of health.
    Same rule as reconcile's cannot-verify-is-not-flat."""
    st = heartbeat.deadman_check(str(tmp_path / "nope.json"), now=1.0)
    assert st.state == "missing"
    assert st.ok is False


def test_a_CORRUPT_heartbeat_is_not_OK_and_not_MISSING(tmp_path):
    """A truncated heartbeat is what a SIGKILL mid-write looks like — it must not be read as
    'never started', which is a benign story."""
    p = tmp_path / "maker.json"
    p.write_text("{trunca")
    st = heartbeat.deadman_check(str(p), now=1.0)
    assert st.state == "corrupt"
    assert st.ok is False


def test_a_CLEAN_EXIT_is_reported_as_exited_not_as_a_deadman_trip(tmp_path):
    """Otherwise every planned shutdown looks exactly like a crash and the alarm gets ignored."""
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat(now=1000.0)
    hb.mark_exit("clean", now=1001.0)
    st = heartbeat.deadman_check(hb.path, now=1000.0 + 10_000)
    assert st.state == "exited"
    assert st.exit_status == "clean"
    assert st.ok is True


def test_mark_exit_PRESERVES_the_last_beat_fields(tmp_path):
    """The state at the moment of the halt is the single most useful thing in the file, and a bare
    exit stamp would overwrite it. An operator arriving after a loss-cap halt needs to see what
    inventory was on and what the P&L was — not just that it stopped."""
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat(now=1000.0, markets_quoted=3, inventory=-2.0, marked_pnl=-0.17)
    hb.mark_exit("halted:loss_cap", now=1001.0)
    d = json.loads(open(hb.path).read())
    assert d["inventory"] == -2.0
    assert d["marked_pnl"] == -0.17
    assert d["exit_status"] == "halted:loss_cap"


def test_mark_exit_fields_override_the_carried_ones(tmp_path):
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat(now=1000.0, fills=2)
    hb.mark_exit("clean", now=1001.0, fills=5)
    assert json.loads(open(hb.path).read())["fills"] == 5


def test_an_UNCLEAN_marked_exit_is_reported_as_not_ok(tmp_path):
    """A halt (loss cap, kill switch, memory guard) is an exit an operator must see."""
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path))
    hb.beat(now=1000.0)
    hb.mark_exit("halted:loss_cap", now=1001.0)
    st = heartbeat.deadman_check(hb.path, now=1002.0)
    assert st.state == "exited"
    assert st.ok is False


def test_a_stale_heartbeat_reports_whether_the_pid_is_still_running(tmp_path):
    """Stale + pid gone = killed (SIGKILL/OOM). Stale + pid alive = hung (the black-hole-socket
    freeze this repo has already hit). Different diagnoses, different fixes."""
    hb = heartbeat.Heartbeat("maker", interval_s=10.0, directory=str(tmp_path), pid=os.getpid())
    hb.beat(now=1000.0)
    st = heartbeat.deadman_check(hb.path, now=1000.0 + 10_000)
    assert st.pid_running is True

    hb2 = heartbeat.Heartbeat("ghost", interval_s=10.0, directory=str(tmp_path), pid=4_000_000)
    hb2.beat(now=1000.0)
    st2 = heartbeat.deadman_check(hb2.path, now=1000.0 + 10_000)
    assert st2.pid_running is False
    assert "gone" in st2.detail.lower() or "kill" in st2.detail.lower()


def test_scan_reports_every_heartbeat_in_the_directory(tmp_path):
    heartbeat.Heartbeat("a", interval_s=10.0, directory=str(tmp_path)).beat(now=1000.0)
    heartbeat.Heartbeat("b", interval_s=10.0, directory=str(tmp_path)).beat(now=1000.0)
    names = sorted(s.name for s in heartbeat.scan(str(tmp_path), now=1001.0))
    assert names == ["a", "b"]


def test_scan_of_an_empty_directory_is_empty_not_an_error(tmp_path):
    assert heartbeat.scan(str(tmp_path), now=1.0) == []


def test_deadman_problems_lists_only_the_unhealthy(tmp_path):
    heartbeat.Heartbeat("live", interval_s=10.0, directory=str(tmp_path)).beat(now=1000.0)
    heartbeat.Heartbeat("dead", interval_s=10.0, directory=str(tmp_path)).beat(now=1.0)
    probs = heartbeat.problems(str(tmp_path), now=1001.0)
    assert len(probs) == 1
    assert "dead" in probs[0]


class TestHaltAcknowledgement:
    """⛔ A halt must be SEEN — and "seen" needs a way to be recorded, or the alarm is permanent.
    The Poly maker halted on memory at 02:29Z 2026-08-08; the operator saw it and dealt with it;
    opswatch reported the same halt every 5 minutes for 20 hours as its ONLY problem. A rail that
    cannot be answered trains the operator to ignore it."""

    def _halted(self, tmp_path, name="poly_live_mm", exit_status="halted:memory: avail 86MB"):
        import json
        import time
        p = tmp_path / f"{name}.json"
        p.write_text(json.dumps({"name": name, "pid": 999999, "ts": time.time() - 72000,
                                 "interval_s": 10.0, "exit_status": exit_status}))
        return p

    def test_an_unacknowledged_halt_is_NOT_ok(self, tmp_path):
        from bot.core.heartbeat import deadman_check
        d = deadman_check(str(self._halted(tmp_path)))
        assert d.state == "exited" and not d.ok

    def test_acknowledging_clears_the_PROBLEM_but_keeps_the_RECORD(self, tmp_path):
        """Mutation: make `ok` ignore acknowledged_ts → RED. Mutation: delete the file instead
        of stamping it → RED (exit_status must survive)."""
        import json
        from bot.core.heartbeat import acknowledge, deadman_check
        self._halted(tmp_path)
        acknowledge("poly_live_mm", by="spencer", directory=str(tmp_path))
        d = deadman_check(str(tmp_path / "poly_live_mm.json"))
        assert d.ok, "an acknowledged halt must stop being a problem"
        assert d.state == "exited", "…but it is still an exit, not an 'ok' process"
        assert "ACKNOWLEDGED by spencer" in d.detail
        raw = json.loads((tmp_path / "poly_live_mm.json").read_text())
        assert raw["exit_status"].startswith("halted:memory"), "the halt reason must survive"

    def test_a_MALFORMED_ack_is_no_ack(self, tmp_path):
        """Never silence on junk. Mutation: coerce a bad value to now() → RED."""
        import json
        from bot.core.heartbeat import deadman_check
        p = self._halted(tmp_path)
        raw = json.loads(p.read_text())
        raw["acknowledged_ts"] = "sometime"
        p.write_text(json.dumps(raw))
        assert not deadman_check(str(p)).ok

    def test_acknowledge_REFUSES_an_unclean_death(self, tmp_path):
        """⛔ THE SAFETY EDGE. No exit_status means the PID vanished without saying why — live
        orders may be resting on the venue. That cannot be waved through from here.
        Mutation: drop the exit_status guard → RED."""
        import json
        import time

        import pytest
        from bot.core.heartbeat import acknowledge
        (tmp_path / "ghost.json").write_text(
            json.dumps({"name": "ghost", "pid": 999999, "ts": time.time() - 9000,
                        "interval_s": 10.0}))
        with pytest.raises(SystemExit, match="unclean death"):
            acknowledge("ghost", by="spencer", directory=str(tmp_path))

    def test_acknowledge_LEAVES_THE_FILE_READABLE(self, tmp_path):
        """Acknowledging republishes the file, so it must republish it world-readable — otherwise
        answering the alarm swaps a halt the watcher can read for a 0600 file it cannot, and
        opswatch pages "liveness UNKNOWN" instead (incident 2026-08-28). Strict umask on purpose:
        `open()` inherits it, so the mode must come from the explicit chmod.
        Mutation: drop the chmod in `acknowledge` → RED."""
        import os
        import stat
        from bot.core.heartbeat import acknowledge
        old = os.umask(0o077)
        try:
            self._halted(tmp_path)
            path = acknowledge("poly_live_mm", by="spencer", directory=str(tmp_path))
            assert stat.S_IMODE(os.stat(path).st_mode) == 0o644
        finally:
            os.umask(old)

    def test_acknowledge_REFUSES_a_missing_heartbeat(self, tmp_path):
        import pytest
        from bot.core.heartbeat import acknowledge
        with pytest.raises(SystemExit, match="no readable heartbeat"):
            acknowledge("nope", by="spencer", directory=str(tmp_path))


class TestAcknowledgeIsAnAllowlist:
    """⛔ The first guard was `exit_status` truthiness, reasoning
    "no status means the venue was never asked". But `record_open:<status>` IS a written status,
    and its whole meaning is that the teardown sweep's LISTING FAILED — the venue could not
    confirm nothing is resting. So the one status that most needs a human to open the venue was
    the one that got waved through, by an operator this very tool trains to answer the rail."""

    def _write(self, tmp_path, status):
        import json
        import time
        (tmp_path / "poly_live_mm.json").write_text(json.dumps(
            {"name": "poly_live_mm", "pid": 999999, "ts": time.time() - 7200,
             "interval_s": 10.0, "exit_status": status}))

    def test_record_open_is_REFUSED(self, tmp_path):
        """Mutation: revert to the truthiness check → RED."""
        import pytest
        from bot.core.heartbeat import acknowledge
        self._write(tmp_path, "record_open:clean")
        with pytest.raises(SystemExit, match="not acknowledgeable"):
            acknowledge("poly_live_mm", by="spencer", directory=str(tmp_path))

    def test_prior_run_unresolved_is_REFUSED(self, tmp_path):
        import pytest
        from bot.core.heartbeat import acknowledge
        self._write(tmp_path, "halted:prior_run_unresolved")
        with pytest.raises(SystemExit, match="not acknowledgeable"):
            acknowledge("poly_live_mm", by="spencer", directory=str(tmp_path))

    def test_an_ordinary_halt_is_still_acknowledgeable(self, tmp_path):
        from bot.core.heartbeat import acknowledge, deadman_check
        self._write(tmp_path, "halted:memory: system avail 86MB <= floor 120MB")
        acknowledge("poly_live_mm", by="spencer", directory=str(tmp_path))
        assert deadman_check(str(tmp_path / "poly_live_mm.json")).ok

    def test_an_UNKNOWN_status_family_refuses_by_default(self, tmp_path):
        """The point of an allowlist: a status added later inherits the SAFE default."""
        import pytest
        from bot.core.heartbeat import acknowledge
        self._write(tmp_path, "wedged:something_new")
        with pytest.raises(SystemExit, match="not acknowledgeable"):
            acknowledge("poly_live_mm", by="spencer", directory=str(tmp_path))

    def test_a_NON_FINITE_ack_does_not_silence(self, tmp_path):
        """⛔ `json` accepts NaN/Infinity by default and `float("NaN")` does not raise, so the
        malformed-ack guard let a non-finite value through. This repo learned the same lesson in
        the orderbook (`feed._levels_to_dict` drops non-finite wire values).
        Mutation: drop the `math.isfinite` check → RED."""
        import json
        from bot.core.heartbeat import deadman_check
        p = tmp_path / "poly_live_mm.json"
        self._write(tmp_path, "halted:memory")
        for bad in ("NaN", "Infinity", "1e999", True):
            raw = json.loads(p.read_text())
            raw["acknowledged_ts"] = bad
            p.write_text(json.dumps(raw))
            assert not deadman_check(str(p)).ok, f"{bad!r} must not silence the alarm"


def test_scan_skips_the_inplay_ramp_marker(tmp_path):
    # The sampler's ramp marker shares the heartbeat directory (poly_night reads it there) but
    # it is a phase stamp, not a beat: 2026-09-06 opswatch paged it as a 30 s deadman trip.
    heartbeat.Heartbeat("a", interval_s=10.0, directory=str(tmp_path)).beat(now=1000.0)
    (tmp_path / heartbeat.RAMP_MARKER_NAME).write_text('{"phase": "done", "ts": 1.0, "pid": 1}')
    assert [s.name for s in heartbeat.scan(str(tmp_path), now=1001.0)] == ["a"]
