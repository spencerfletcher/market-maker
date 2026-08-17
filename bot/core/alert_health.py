"""
bot/core/alert_health.py
────────────────────────
A durable, LOCAL record of whether alerts are actually being delivered.

THE PROBLEM. An alert channel cannot be its own witness. `bot/core/alerts.py` dispatched every
post and swallowed the outcome at `log.debug`, with `LOG_LEVEL=INFO` — so a dead webhook was
undetectable, and every guard that "alerts the operator" (strand pause, reconcile divergence,
schema drift) was silently reporting into a void.

The log level was the smaller half. `DiscordWebhook.execute()` posts with `wait=true` and RETURNS
a `requests.Response`; a deleted or revoked webhook comes back as **401/404 with no exception**.
So `except Exception: log.debug(...)` never ran at all — there was nothing to raise the level of.
The outcome has to be read off the STATUS, which is what `classify_delivery` does.

WHERE THE RECORD GOES. To a plain JSON file on local disk (`logs/alert_health.json`) and to the
process log at ERROR/CRITICAL. Deliberately not to Discord: a notifier reporting its own death
through itself is the same fallacy as the tracker being the only witness to our own positions
(see `bot/runner/reconcile.py`). The file is written through `bot.core.durable`, so a SIGKILL
between two alerts cannot leave a half-written ledger — the record of a delivery failure has to
outlive the process that observed it, since the whole point is that nobody was watching.

NEVER RAISES. `record_delivery` is called from the alert path, which is called from inside the
fire window with `_execution_lock` held. The bookkeeping about a best-effort alert must be even
less capable of failing than the alert.

Read it with `python -m scripts.opswatch`, or just `cat logs/alert_health.json`.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from bot.core import durable

log = logging.getLogger(__name__)

DEFAULT_PATH = durable.repo_path("logs", "alert_health.json")

# Consecutive failures before a channel is called DEAD rather than flaky. One 500 or one dropped
# connection is noise; a streak means every alert since the streak began was lost — including,
# potentially, a strand pause.
ALARM_STREAK = 3

# 2xx only. 3xx is a redirect we did not follow (so nothing was posted); 429 is an explicit
# "your message was NOT accepted".
_OK_STATUSES = frozenset({200, 201, 202, 204})


@dataclass(frozen=True)
class ChannelHealth:
    channel: str
    last_ok_ts: float | None
    last_fail_ts: float | None
    last_error: str
    consecutive_failures: int
    total_ok: int
    total_failed: int

    @property
    def healthy(self) -> bool:
        """A channel is healthy only while its current failure streak is empty.

        NOT "has ever succeeded" — a channel that worked yesterday and 404s today is dead now, and
        the number that must drive the answer is the streak, not the lifetime total.
        """
        return self.consecutive_failures == 0


def classify_delivery(status_code: int | None, exc: BaseException | None) -> tuple[bool, str]:
    """(delivered?, human detail) for one send attempt.

    Fails CLOSED on ambiguity: no status and no exception means we never observed a result, and an
    unobserved delivery must not be recorded as a delivery. That is the same rule
    `bot/runner/reconcile.py` applies to positions — cannot-verify is not "all clear" — applied to
    the notifier.
    """
    if exc is not None:
        return False, f"{type(exc).__name__}: {exc}"
    if status_code is None:
        return False, "no response observed (transport returned nothing)"
    if status_code in _OK_STATUSES:
        return True, f"HTTP {status_code}"
    if status_code == 429:
        return False, "HTTP 429 rate limited — the message was NOT delivered"
    return False, f"HTTP {status_code}"


def record_delivery(
    channel: str,
    ok: bool,
    detail: str = "",
    *,
    path: str | None = None,
    now: float | None = None,
) -> None:
    """Fold one send outcome into the durable ledger. Loud on failure, silent on success.

    Read-modify-write rather than append: the ledger is a small fixed-size summary, so it can never
    become the 88 MB log file nobody rotates. The cost is that two threads posting at the same
    instant can lose one increment — acceptable, because the field that matters (`consecutive
    failures > 0` on a dead channel) is monotone under any interleaving where a failure is written.
    """
    if now is None:
        now = time.time()
    target = path or DEFAULT_PATH
    try:
        raw = durable.read_json_or_none(target) or {}
        channels: dict[str, Any] = dict(raw.get("channels") or {})
        prev = channels.get(channel) or {}
        streak = 0 if ok else int(prev.get("consecutive_failures") or 0) + 1
        channels[channel] = {
            "last_ok_ts": now if ok else prev.get("last_ok_ts"),
            "last_fail_ts": prev.get("last_fail_ts") if ok else now,
            "last_error": "" if ok else (detail or "unspecified failure"),
            "consecutive_failures": streak,
            "total_ok": int(prev.get("total_ok") or 0) + (1 if ok else 0),
            "total_failed": int(prev.get("total_failed") or 0) + (0 if ok else 1),
        }
        durable.write_json_durable(target, {"updated_ts": now, "channels": channels})
    except Exception as exc:                   # never raise into an alert caller
        log.error(f"alert_health: could not record {channel} delivery ({exc!r}) — "
                  f"alert delivery is now UNOBSERVED")
        return

    if ok:
        return
    # ⛔ ERROR, not DEBUG. This line is the entire fix for "a dead webhook is undetectable": the
    # old code's only report was below the deployed LOG_LEVEL, and (because a 404 does not raise)
    # was not even reached.
    log.error(f"🔕 ALERT DELIVERY FAILED on '{channel}': {detail}. "
              f"Streak={streak}. Recorded in {target}")
    if streak >= ALARM_STREAK:
        log.critical(
            f"🔕 ALERT CHANNEL '{channel}' IS DEAD — {streak} consecutive delivery failures "
            f"({detail}). Every alert since the streak began was LOST, including any strand or "
            f"reconcile alert. Check the webhook/token; see {target}."
        )


def load_health(path: str | None = None) -> dict[str, ChannelHealth]:
    """channel → ChannelHealth. Missing or corrupt ledger → {} (use `problems` for the verdict)."""
    raw = durable.read_json_or_none(path or DEFAULT_PATH)
    if not isinstance(raw, dict):
        return {}
    out: dict[str, ChannelHealth] = {}
    for name, d in (raw.get("channels") or {}).items():
        if not isinstance(d, dict):
            continue
        out[str(name)] = ChannelHealth(
            channel=str(name),
            last_ok_ts=d.get("last_ok_ts"),
            last_fail_ts=d.get("last_fail_ts"),
            last_error=str(d.get("last_error") or ""),
            consecutive_failures=int(d.get("consecutive_failures") or 0),
            total_ok=int(d.get("total_ok") or 0),
            total_failed=int(d.get("total_failed") or 0),
        )
    return out


def problems(path: str | None = None) -> list[str]:
    """Human-readable reasons alerting cannot currently be trusted. Empty ⇒ every channel is
    delivering. A CORRUPT ledger is a problem, not a clean slate — that file is exactly what a
    SIGKILL mid-write produces, and "no failures recorded" is the wrong reading of it."""
    target = path or DEFAULT_PATH
    try:
        raw = durable.read_json_strict(target)
    except durable.StateCorrupt as exc:
        return [f"alert-delivery ledger is CORRUPT ({exc}) — delivery status is UNKNOWN"]
    if raw is None:
        return []                    # never written: no alert has been attempted yet
    out: list[str] = []
    for h in load_health(target).values():
        if h.consecutive_failures >= ALARM_STREAK:
            out.append(f"alert channel '{h.channel}' DEAD: {h.consecutive_failures} consecutive "
                       f"failures ({h.last_error})")
        elif h.consecutive_failures:
            out.append(f"alert channel '{h.channel}' failing: {h.consecutive_failures}x "
                       f"({h.last_error})")
    return out
