"""The durable-write primitive every operational rail depends on.

WHY THESE TESTS ARE STRICT ABOUT fsync. `PositionTracker._save` already wrote tmp → `os.replace`,
which is ATOMIC (a reader sees the old file or the new one, never a splice) but NOT DURABLE: after
`os.replace` returns, both the new file's bytes and the rename itself can still be sitting in the
page cache. A power loss or a hard reset then leaves either the OLD contents or a zero-length /
garbage file. the private design notes 1.3 names that gap, and it is exactly the gap that
matters for a SIGKILL-oriented design: state has to be on the platter BEFORE it is needed, not at
the writer's convenience.

So the assertions below are about CALL ORDER, not just the round-trip. A round-trip test passes
against a plain `open().write()` — which is the whole reason the gap survived.
"""
from __future__ import annotations

import json
import os
import stat

import pytest

from bot.core import durable


def _record_syscalls(monkeypatch) -> list[tuple[str, str]]:
    """Log (syscall, kind) in order, where kind is 'file' or 'dir', still doing the real thing."""
    seen: list[tuple[str, str]] = []
    real_fsync, real_replace = os.fsync, os.replace

    def fake_fsync(fd: int) -> None:
        kind = "dir" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file"
        seen.append(("fsync", kind))
        real_fsync(fd)

    def fake_replace(src, dst) -> None:
        seen.append(("replace", "file"))
        real_replace(src, dst)

    monkeypatch.setattr(durable.os, "fsync", fake_fsync)
    monkeypatch.setattr(durable.os, "replace", fake_replace)
    return seen


def test_write_json_durable_roundtrips(tmp_path):
    p = tmp_path / "state.json"
    durable.write_json_durable(str(p), {"a": 1, "b": ["x"]})
    assert json.loads(p.read_text()) == {"a": 1, "b": ["x"]}


def test_write_json_durable_fsyncs_the_file_BEFORE_the_rename(tmp_path, monkeypatch):
    """The bytes must be on disk before the rename publishes them.

    fsync AFTER the replace is a different (and weaker) guarantee: the rename can be durable while
    the data it points at is not, which is how you get a zero-length state file after a reset.
    """
    seen = _record_syscalls(monkeypatch)
    durable.write_json_durable(str(tmp_path / "state.json"), {"a": 1})
    assert ("fsync", "file") in seen, f"no file fsync at all: {seen}"
    assert seen.index(("fsync", "file")) < seen.index(("replace", "file")), seen


def test_write_json_durable_fsyncs_the_DIRECTORY_after_the_rename(tmp_path, monkeypatch):
    """A rename is a directory mutation, and it is not durable until the directory is fsync'd.

    Without this the file's contents survive a crash but the *name* may not — the state file can
    revert to its previous version, or vanish, while every byte we wrote is safely on disk under
    the tmp name. That is worse than no write, because it is invisible.
    """
    seen = _record_syscalls(monkeypatch)
    durable.write_json_durable(str(tmp_path / "state.json"), {"a": 1})
    assert ("fsync", "dir") in seen, f"no directory fsync: {seen}"
    assert seen.index(("replace", "file")) < seen.index(("fsync", "dir")), seen


def test_write_json_durable_replaces_atomically_not_in_place(tmp_path, monkeypatch):
    """It must go through a rename. Truncate-and-write leaves a window where the file is empty."""
    seen = _record_syscalls(monkeypatch)
    p = tmp_path / "state.json"
    p.write_text('{"old": true}')
    durable.write_json_durable(str(p), {"new": True})
    assert ("replace", "file") in seen, f"wrote in place, not via rename: {seen}"


def test_write_json_durable_leaves_the_old_file_intact_when_serialisation_fails(tmp_path):
    """An unserialisable value must not destroy the last good state."""
    p = tmp_path / "state.json"
    p.write_text('{"old": true}')
    with pytest.raises(TypeError):
        durable.write_json_durable(str(p), {"bad": object()})
    assert json.loads(p.read_text()) == {"old": True}


def test_write_json_durable_leaves_no_tmp_litter_when_serialisation_fails(tmp_path):
    p = tmp_path / "state.json"
    with pytest.raises(TypeError):
        durable.write_json_durable(str(p), {"bad": object()})
    assert list(tmp_path.iterdir()) == [], f"litter left behind: {list(tmp_path.iterdir())}"


def test_write_json_durable_creates_the_parent_directory(tmp_path):
    p = tmp_path / "nested" / "deeper" / "state.json"
    durable.write_json_durable(str(p), {"a": 1})
    assert json.loads(p.read_text()) == {"a": 1}


def test_write_json_durable_defaults_to_a_PRIVATE_file(tmp_path):
    """No `mode` ⇒ the mkstemp default, 0600. The private state this module was built for — the
    maker's crash record, the loss-cap ledger — must not become world-readable as a side effect of
    the heartbeat's needs. Mutation: default `mode` to 0o644 → RED."""
    p = tmp_path / "state.json"
    durable.write_json_durable(str(p), {"a": 1})
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o600


def test_write_json_durable_publishes_at_the_REQUESTED_mode(tmp_path):
    """`mode=0o644` must land on the PUBLISHED path, not just the tmp file. mkstemp creates 0600
    regardless of umask, so without an explicit chmod a root-written heartbeat is unreadable to
    the watcher (incident 2026-08-28). Mutation: drop the chmod → RED."""
    p = tmp_path / "state.json"
    durable.write_json_durable(str(p), {"a": 1}, mode=0o644)
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o644
    assert json.loads(p.read_text()) == {"a": 1}


def test_write_json_durable_chmods_BEFORE_the_rename(tmp_path, monkeypatch):
    """The published name must never be observable at the wrong mode — a reader sees the old file
    or the final one, never a 0600 window on the new one."""
    p = tmp_path / "state.json"
    order: list[str] = []
    real_chmod, real_replace = os.chmod, os.replace

    def fake_chmod(path, mode, **kw):
        order.append("chmod")
        real_chmod(path, mode, **kw)

    def fake_replace(src, dst):
        order.append("replace")
        real_replace(src, dst)

    monkeypatch.setattr(durable.os, "chmod", fake_chmod)
    monkeypatch.setattr(durable.os, "replace", fake_replace)
    durable.write_json_durable(str(p), {"a": 1}, mode=0o644)
    assert order == ["chmod", "replace"], order


def test_write_bytes_durable_mode_survives_a_strict_umask(tmp_path):
    """The mode is set by chmod, not inherited — `tempfile.mkstemp` ignores the umask by design,
    which is exactly why the `UMask=` systemd drop-in could not fix the incident."""
    old = os.umask(0o077)
    try:
        p = tmp_path / "beat.json"
        durable.write_bytes_durable(str(p), b"{}", mode=0o644)
        assert stat.S_IMODE(os.stat(p).st_mode) == 0o644
    finally:
        os.umask(old)


def test_read_json_or_none_returns_None_for_a_missing_file(tmp_path):
    assert durable.read_json_or_none(str(tmp_path / "nope.json")) is None


def test_read_json_or_none_returns_None_for_corrupt_json(tmp_path):
    p = tmp_path / "state.json"
    p.write_text("{not json")
    assert durable.read_json_or_none(str(p)) is None


def test_read_json_or_none_distinguishes_missing_from_corrupt_via_read_json_strict(tmp_path):
    """CANNOT-VERIFY is not ABSENT. A caller that must not treat a corrupt file as 'no state yet'
    needs the two cases separated — the reconciler's discipline, applied to files."""
    p = tmp_path / "state.json"
    p.write_text("{not json")
    with pytest.raises(durable.StateCorrupt):
        durable.read_json_strict(str(p))
    assert durable.read_json_strict(str(tmp_path / "nope.json")) is None


def test_repo_path_is_absolute_and_anchored_at_the_repo_root():
    """Every operational file path resolves absolute. A RELATIVE default is how
    `_EXEC_PNL_PATH` made both loss caps permanently inert under any other cwd
    (the private design notes 1.4)."""
    p = durable.repo_path("logs", "x.json")
    assert os.path.isabs(p)
    assert os.path.isdir(os.path.join(durable.REPO_ROOT, "bot", "core"))
    assert p.endswith(os.path.join("logs", "x.json"))
