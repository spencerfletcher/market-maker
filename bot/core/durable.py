"""
bot/core/durable.py
───────────────────
Crash-durable file writes, and the one place a repo-relative operational path is resolved.

WHY THIS EXISTS
───────────────
Every rail in this package — the alert-delivery ledger, the heartbeat, the maker's crash state —
has the same requirement: *the record must already be on disk when the process dies*, because the
death this repo actually suffers is SIGKILL (six OOM kills on record; `bot/kalshi/maker.py` names
SIGKILL as the one death that always strands live orders). SIGKILL runs no `finally`, no `atexit`,
no signal handler. A rail that writes its state during cleanup protects nothing against it.

`PositionTracker._save` had the atomic half right and the durable half missing: tmp file →
`os.replace` is ATOMIC (a concurrent reader sees the old bytes or the new ones, never a splice) but
after `os.replace` returns, both the data and the rename may still be only in the page cache. A
power loss or a hard reset can therefore leave the OLD contents, a zero-length file, or garbage —
which is what makes "refuse to boot on a corrupt state file" a routine event rather than a rare
one. the private design notes 1.3 names the gap; this module closes it, and
`PositionTracker._save` now routes through it.

Durability needs THREE steps, and skipping any one of them still passes a round-trip test:
  1. write to a sibling tmp file, then `fsync` THE FILE       → the bytes are on the platter
  2. `os.replace`                                             → atomic publish
  3. `fsync` THE DIRECTORY                                    → the *rename* is on the platter
Step 3 is the one people drop. Without it the file's contents survive a crash but its NAME may
not: the state file silently reverts to its previous version while every byte you wrote sits
safely on disk under a tmp name nobody will ever look at.

This module deliberately has NO dependency on `bot.core.config` — config imports `repo_path` from
here, and a rail that cannot be imported during config load is a rail that cannot guard startup.
"""
from __future__ import annotations

import errno
import json
import os
import tempfile
from typing import Any

# The repo root, resolved from this file. `bot/core/durable.py` → up two → the checkout.
#
# ⛔ ABSOLUTE ON PURPOSE. A relative operational path is not a cosmetic issue in this repo: the
# loss caps read `logs/execution_pnl.csv` relative to the cwd, so any launch from another directory
# gives FileNotFoundError → 0.0 loss → both caps permanently and SILENTLY inert
# (the private design notes 1.4). Same class as the `bot_state.json` root-vs-`bot/` split
# that printed "clean" over three live strand records. One resolver, one answer.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def repo_path(*parts: str) -> str:
    """An absolute path under the repo root. Use for every default operational file location."""
    return os.path.join(REPO_ROOT, *parts)


class StateCorrupt(Exception):
    """The file exists but could not be parsed.

    A DISTINCT condition from "the file is absent", and the distinction is load-bearing: absent
    means "no state yet, a fresh start is fine", corrupt means "there was state and we cannot read
    it" — i.e. CANNOT-VERIFY, which `bot/runner/reconcile.py` establishes must never resolve to
    "we're flat". Collapsing the two is how a crashed writer becomes a clean-looking startup.
    """


def _fsync_dir(path: str) -> None:
    """fsync the directory containing `path`, so the rename into it is durable.

    Best-effort by design: some filesystems refuse `O_RDONLY` on a directory or reject fsync on
    one. A refusal there must not turn a successful write into an exception — the data fsync in
    `write_bytes_durable` has already happened and is the larger half of the guarantee.
    """
    d = os.path.dirname(os.path.abspath(path)) or "."
    try:
        fd = os.open(d, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def write_bytes_durable(path: str, data: bytes, *, mode: int | None = None) -> None:
    """Atomically AND durably replace `path` with `data`. Creates the parent directory.

    The tmp file is a SIBLING (same directory), not `/tmp`: `os.replace` is only atomic within one
    filesystem, and on this box `/tmp` is a separate tmpfs — a cross-device rename raises, and
    "fall back to a copy" would reintroduce the torn-write window. It also matters for a second
    reason here: `/tmp` is RAM-backed on a 1.9 GB box, so staging through it spends the very
    resource the OOM guard exists to protect.

    `mode` (octal, e.g. 0o644) sets the PUBLISHED file's permissions. `tempfile.mkstemp` creates
    0600 REGARDLESS OF UMASK — by design, and a `UMask=` systemd drop-in therefore cannot change
    it. That default is right for private state (the maker's crash record, the loss-cap ledger) and
    is what `mode=None` keeps, byte-for-byte. It is WRONG for a file whose whole purpose is to be
    read by another process under another user — see `bot/core/heartbeat.py` and incident
    2026-08-28. The chmod happens BEFORE `os.replace`, so the published name never transitions
    through the wrong mode: a reader either sees the old file or the new one at its final mode.
    """
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", suffix=os.path.basename(path), dir=directory)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())        # (1) bytes durable BEFORE the rename publishes them
        if mode is not None:
            os.chmod(tmp, mode)         # (1b) final mode set on the TMP, before it is published
        os.replace(tmp, path)           # (2) atomic publish
        tmp = ""                        # renamed away; nothing left to clean up
        _fsync_dir(path)                # (3) the RENAME durable — the step everyone drops
    finally:
        if tmp:
            try:
                os.unlink(tmp)
            except OSError as exc:      # unlink of our own fresh tmp
                if exc.errno != errno.ENOENT:
                    raise


def write_json_durable(path: str, obj: Any, *, indent: int | None = 2,
                       mode: int | None = None) -> None:
    """Atomically + durably write `obj` as JSON. `mode` is passed through to
    `write_bytes_durable` — see there for why a caller would set it.

    Serialises FULLY IN MEMORY first, deliberately. Streaming `json.dump` into the tmp file would
    leave a half-written tmp behind on an unserialisable value — and, worse, has already replaced
    nothing, so the caller sees an exception with litter on disk. Here a serialisation failure
    happens before any file is touched, so the previous good state is untouched by construction.
    """
    payload = json.dumps(obj, indent=indent, sort_keys=False).encode("utf-8")
    write_bytes_durable(path, payload, mode=mode)


def read_json_or_none(path: str) -> Any | None:
    """Parsed JSON, or None if the file is missing OR unreadable OR corrupt.

    For callers where "no usable state" is genuinely all they need to know (a status display).
    Anything that makes a SAFETY decision on the result must use `read_json_strict` instead, so a
    corrupt file cannot masquerade as a clean absence.
    """
    try:
        return read_json_strict(path)
    except StateCorrupt:
        return None


def read_json_strict(path: str) -> Any | None:
    """Parsed JSON, or None if the file does not exist. Raises `StateCorrupt` if it exists but
    cannot be read or parsed — absent and unreadable are different answers."""
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise StateCorrupt(f"{path}: unreadable ({exc})") from exc
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StateCorrupt(f"{path}: not parseable as JSON ({exc})") from exc
