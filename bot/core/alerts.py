"""
bot/core/alerts.py
──────────────────
Runtime push notification DELIVERY: Discord embeds + Pushover phone alerts.

This module owns two things, and deliberately not a third: how a notification leaves the process
without stalling the event loop (`_dispatch`, `_send_pushover`), and how its delivery outcome gets
recorded somewhere other than the channel that may have just died (`bot/core/alert_health.py`).
It does not own message content — callers build their own embeds and hand them here.
"""
from __future__ import annotations

import asyncio
import logging

import requests as _requests
from discord_webhook import DiscordWebhook

from bot.core import alert_health, config

log = logging.getLogger(__name__)

# ⚠️ DiscordWebhook defaults its timeout to None, and requests reads None as "wait forever" — no
# connect timeout, no read timeout. Every webhook here MUST pass this. A single un-timed-out post
# is enough to hang the process indefinitely: proven against a black-hole socket (a task ticking
# many times a second emitted one warning and never ticked again, still blocked when killed). A
# service unit with Restart=always but no watchdog cannot save you either — a blocked-but-alive
# process reports `active (running)` forever. 10s is generous for a real slow post and bounded
# for a hung one.
_DISCORD_TIMEOUT_S = 10


def _dispatch(webhook: DiscordWebhook) -> None:
    """Send `webhook` WITHOUT blocking the event loop.

    `webhook.execute()` is synchronous `requests`. Called straight from a coroutine it stalls the
    single event loop — both WS feeds stop reading, the `websockets` library cannot answer server
    pings (same loop) so the sockets drop, and every timer freezes with them. That is bad enough
    while idle; four of these fire from the exec helpers while `_execution_lock` is held, i.e.
    potentially between a Poly fill and the Kalshi hedge, on a path whose whole budget is ~100ms.
    The alarm becoming the outage is not hypothetical here: the loudest callers are the strand and
    reconcile alerts, which fire exactly when a leg is stranded or unknown exposure exists.

    Off the loop, a hung post costs an idle worker for `_DISCORD_TIMEOUT_S` and nothing else. The
    result is deliberately not awaited — an alert is best-effort and must never be able to fail,
    delay, or raise into a decision path. With no loop running (a `to_thread` worker, a script, a
    test) we are already off the loop, so post inline.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Off-loop already — post inline, but through the SAME helper so the delivery outcome is
        # recorded on this path too. It used to have its own private `try/execute/log.debug`,
        # which is how a second, unobserved send path grows next to an observed one.
        _execute_quietly(webhook)
        return
    loop.run_in_executor(None, _execute_quietly, webhook)


def _execute_quietly(webhook: DiscordWebhook) -> None:
    """Post and swallow — but RECORD the outcome. Runs on an executor thread; a raise there would
    be an unretrieved-future warning at best, and must never reach a caller that is mid-hedge.

    ⛔ THE OUTCOME IS READ OFF THE STATUS CODE, NOT OFF THE ABSENCE OF AN EXCEPTION. `execute()`
    posts with `wait=true` and RETURNS a `requests.Response`: a deleted or revoked webhook comes
    back 401/404 having raised nothing. The previous body's `except Exception: log.debug(...)`
    therefore never ran, at any log level, for the failure mode that actually happens — a dead
    webhook posted "successfully" forever. Raising the log level alone would not have fixed it.

    Swallowing stays (an alert must never fail a decision path); what changes is that the failure
    now lands in a durable local file and a non-DEBUG log line, neither of which depends on the
    channel that just failed. See bot/core/alert_health.py.
    """
    status, exc = None, None
    try:
        resp = webhook.execute()
        status = getattr(resp, "status_code", None)
    except Exception as e:
        exc = e
    ok, detail = alert_health.classify_delivery(status, exc)
    alert_health.record_delivery("discord", ok, detail)


def _send_pushover(title: str, message: str, priority: int = 0) -> None:
    """Send a push notification via Pushover WITHOUT blocking the event loop. Silent no-op if not
    configured. Same rationale as `_dispatch`: this fires from the exec helpers
    (send_kalshi_arb_alert / send_strand_pause_alert) during the ~100ms fire window, so a
    synchronous `requests.post` would stall the single event loop mid-execution — the alarm
    becoming the outage. Off the loop, a hung post costs only an idle worker; not awaited (an alert
    is best-effort). No loop running (script/test/`to_thread` worker) → already off it, post inline."""
    if not config.PUSHOVER_USER_KEY or not config.PUSHOVER_API_TOKEN:
        return
    data = {
        "token": config.PUSHOVER_API_TOKEN,
        "user": config.PUSHOVER_USER_KEY,
        "title": title,
        "message": message,
        "priority": priority,
        "sound": "cashregister" if priority >= 1 else "pushover",
    }
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _post_pushover_quietly(data)
        return
    loop.run_in_executor(None, _post_pushover_quietly, data)


def _post_pushover_quietly(data: dict) -> None:
    """Same contract as `_execute_quietly`: swallow, but record. Pushover answers a bad
    token/user with HTTP 400 and a JSON body — again, no exception — so the status is the only
    honest signal that the phone alert did not arrive."""
    status, exc = None, None
    try:
        resp = _requests.post("https://api.pushover.net/1/messages.json", data=data, timeout=5)
        status = getattr(resp, "status_code", None)
    except Exception as e:
        exc = e
    ok, detail = alert_health.classify_delivery(status, exc)
    alert_health.record_delivery("pushover", ok, detail)


