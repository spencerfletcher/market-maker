"""
bot/alerts.py
─────────────
Runtime push notifications: Discord embeds + Pushover phone alerts.

  - send_reconcile_alert — unknown venue exposure, or a failure to VERIFY it

The arb-bot senders (kalshi_arb / proper_edge / strand_pause / schema_drift) were deleted with the
arb bot on 2026-09-04; the webhook classes below stay for it and for the logger/trade-logger posts.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import requests as _requests

from bot.core import alert_health, config

log = logging.getLogger(__name__)

# ⚠️ Every webhook here MUST pass a timeout. `requests` reads timeout=None as "wait forever" — no
# connect timeout, no read timeout. A single un-timed-out post is enough to hang the process
# indefinitely: proven 2026-07-16 against a black-hole socket (a task ticking 19x/0.2s emitted one
# warning and never ticked again, still blocked at 25s), and the systemd unit has Restart=always
# but no WatchdogSec, so a blocked-but-alive bot reports `active (running)` forever. 10s is
# generous for a real slow post and bounded for a hung one.
_DISCORD_TIMEOUT_S = 10

# Discord's hard limits. Over any of them the API answers 400 and the message is LOST — which is
# the signature of the 2026-09-15 19:56Z halt, where every send 400'd while the log lines carried
# `VenueBanned(...)` strings holding a whole Cloudflare HTML page.
CONTENT_LIMIT = 2_000
EMBED_DESCRIPTION_LIMIT = 4_096

_CUT_MARKER = "…[cut {n} chars; full text in the maker log]"


def _truncate(text: str, limit: int) -> str:
    """`text` capped to EXACTLY `limit` chars, cut from the TAIL so the first line — the
    fingerprint/id every operator line leads with — survives. Never returns an empty string for a
    non-empty input: a truncated alert beats a dropped one.

    The marker prints the number of cut chars, and its own length depends on that number, so the
    keep-length is solved by iterating (two passes converge; a third is free insurance).
    """
    if len(text) <= limit:
        return text
    keep, cut = limit, len(text) - limit
    for _ in range(3):
        marker = _CUT_MARKER.format(n=cut)
        keep = max(limit - len(marker), 0)
        if len(text) - keep == cut:
            break
        cut = len(text) - keep
    return text[:keep] + _CUT_MARKER.format(n=cut)


class DiscordEmbed:
    """One Discord embed. `payload()` is the dict posted inside `embeds`.

    ⛔ Replaces `discord_webhook.DiscordEmbed` (dependency dropped 2026-09-04). The key set and
    ORDER reproduce that class's `__dict__` verbatim — `DiscordWebhook.add_embed` posted
    `embed.__dict__`, so the null `url`/`image`/`thumbnail`/`video`/`provider`/`author` keys were
    always on the wire, `color` was always the int from `int(color, 16)`, and `timestamp` was
    present only when `set_timestamp()` had been called. Callers are unchanged.
    """

    def __init__(self, title: str | None = None, description: str | None = None,
                 color: str | int | None = None) -> None:
        self._payload: dict[str, Any] = {
            "title": title,
            "description": description,
            "url": None,
            "footer": None,
            "image": None,
            "thumbnail": None,
            "video": None,
            "provider": None,
            "author": None,
            "fields": [],
            "color": int(color, 16) if isinstance(color, str) else color,
        }

    def set_footer(self, text: str) -> None:
        self._payload["footer"] = {"text": text, "icon_url": None, "proxy_icon_url": None}

    def add_embed_field(self, name: str, value: str, inline: bool = True) -> None:
        self._payload["fields"].append({"name": name, "value": value, "inline": inline})

    def set_timestamp(self) -> None:
        self._payload["timestamp"] = datetime.now(timezone.utc).isoformat()

    def payload(self) -> dict[str, Any]:
        return self._payload


class DiscordWebhook:
    """One webhook post. `execute()` is synchronous `requests` — go through `_dispatch`.

    ⛔ Replaces `discord_webhook.DiscordWebhook` (dependency dropped 2026-09-04). `json()`
    reproduces that class's `json` property, whose filter was
    `if value and key not in ["url", "files"] or key in ["embeds", "attachments"]` — so an empty
    `attachments` and an empty `embeds` were ALWAYS sent, and `timeout` and `wait` (its own
    constructor kwargs, not Discord fields) were sent in the BODY as well as, for `wait`, the
    query string. The body extras were DROPPED 2026-09-15 (they are not Discord fields and were on
    the wire for all 13,560 successful sends, so they were never the 400); `?wait=true` stays as the
    query param, which is the spec form.
    """

    def __init__(self, url: str | None = None, content: str | None = None,
                 timeout: int | None = None, caller: str = "unknown") -> None:
        self.url = url
        self.content = content
        self.timeout = timeout
        self.caller = caller
        self.embeds: list[dict[str, Any]] = []

    def add_embed(self, embed: DiscordEmbed) -> None:
        self.embeds.append(embed.payload())

    def message_text(self) -> str:
        """The FULL, un-truncated human text of this post — what the failure record keeps."""
        parts = [self.content or ""] + [str(e.get("description") or "") for e in self.embeds]
        return "\n".join(p for p in parts if p)

    def json(self) -> dict[str, Any]:
        """The posted body, capped to Discord's limits. Under the limits it is unchanged."""
        payload: dict[str, Any] = {"attachments": []}
        if self.content:
            payload["content"] = _truncate(self.content, CONTENT_LIMIT)
        payload["embeds"] = [self._capped(e) for e in self.embeds]
        return payload

    @staticmethod
    def _capped(embed: dict[str, Any]) -> dict[str, Any]:
        desc = embed.get("description")
        if not isinstance(desc, str) or len(desc) <= EMBED_DESCRIPTION_LIMIT:
            return embed
        return {**embed, "description": _truncate(desc, EMBED_DESCRIPTION_LIMIT)}

    def execute(self) -> Any:
        """POST and return the `requests.Response` — the caller reads `.status_code`."""
        return _requests.post(self.url, json=self.json(), params={"wait": True},
                              timeout=self.timeout)


def _dispatch(webhook: DiscordWebhook) -> None:
    """Send `webhook` WITHOUT blocking the event loop.

    `webhook.execute()` is synchronous `requests`. Called straight from a coroutine it stalls the
    single event loop — both WS feeds stop reading, the `websockets` library cannot answer server
    pings (same loop) so the sockets drop, and every timer freezes with them. That is bad enough
    while idle; the trade-logger post fires while `_execution_lock` is held, i.e. potentially
    between a Poly fill and the Kalshi hedge, on a path whose whole budget is ~100ms.
    The alarm becoming the outage is not hypothetical here: the loudest caller is the reconcile
    alert, which fires exactly when unknown exposure exists.

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
    status, exc, body = None, None, ""
    try:
        resp = webhook.execute()
        status = getattr(resp, "status_code", None)
        body = str(getattr(resp, "text", "") or "")
    except Exception as e:
        exc = e
    ok, detail = alert_health.classify_delivery(status, exc)
    alert_health.record_delivery("discord", ok, detail)
    if not ok:
        # ⛔ The BODY, not just the status. A 400 without its body is what made the 2026-09-15
        # 19:56Z outage undiagnosable: Discord names the rejected field only in the response.
        alert_health.record_failure("discord", status, body, webhook.message_text(),
                                    webhook.caller)


def _send_pushover(title: str, message: str, priority: int = 0) -> None:
    """Send a push notification via Pushover WITHOUT blocking the event loop. Silent no-op if not
    configured. Same rationale as `_dispatch`: this can fire during the ~100ms fire window, so a
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
    status, exc, body = None, None, ""
    try:
        resp = _requests.post("https://api.pushover.net/1/messages.json", data=data, timeout=5)
        status = getattr(resp, "status_code", None)
        body = str(getattr(resp, "text", "") or "")
    except Exception as e:
        exc = e
    ok, detail = alert_health.classify_delivery(status, exc)
    alert_health.record_delivery("pushover", ok, detail)
    if not ok:
        alert_health.record_failure("pushover", status, body, str(data.get("message") or ""),
                                    "pushover")


def send_reconcile_alert(msg: str) -> None:
    """Venue-vs-tracker reconciliation alert: unknown exposure, or a failure to VERIFY it.

    It carries the operator's ACKNOWLEDGE option, and it is the ONLY alert for CANNOT-VERIFY —
    which strands nothing, so nothing else would speak.

    Fires even in DRY: a real venue position while we believe we are flat is exactly as wrong in
    DRY as in live (in DRY the bot places no orders at all, so it can only be unrecorded).
    """
    if config.DISCORD_WEBHOOK_URL and (not config.DRY_RUN or config.DISCORD_NOTIFY_DRY_RUN):
        try:
            webhook = DiscordWebhook(
                url=config.DISCORD_WEBHOOK_URL,
                content="@everyone" if not config.DRY_RUN else "",
                timeout=_DISCORD_TIMEOUT_S,
                caller="send_reconcile_alert",
            )
            embed = DiscordEmbed(title="🚨 POSITION RECONCILIATION",
                                 description=msg, color="ff0000")
            embed.set_timestamp()
            webhook.add_embed(embed)
            _dispatch(webhook)
        except Exception:
            pass
    _send_pushover(title="🚨 Position reconciliation", message=msg,
                   priority=1 if not config.DRY_RUN else -1)
