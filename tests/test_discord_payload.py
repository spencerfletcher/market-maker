"""The Discord post is byte-identical to what `discord-webhook` built before it was dropped.

Provenance for every expected value below — read off discord_webhook 1.4.1 `webhook.py` before the
dependency was removed (2026-09-04), and confirmed by capturing `requests.post` under the real
library:

  - `DiscordWebhook.json` (`webhook.py:375-393`) filters `self.__dict__` with
    `if value and key not in ["url", "files"] or key in ["embeds", "attachments"]`. Python binds
    that as `(value and ...) or (key in ...)`, so `attachments` and `embeds` are ALWAYS emitted
    even when empty, while the library's OWN constructor kwargs `timeout` and `wait` are emitted
    in the BODY because they are truthy `__dict__` entries. Key order is `__init__` assignment
    order: attachments, content (only when truthy), embeds, timeout, wait.
  - `api_post_request` (`webhook.py:395-416`) posts `json=self.json, params=self._query_params,
    proxies=self.proxies, timeout=self.timeout`.
  - `_query_params` (`webhook.py:439-449`) is `{"wait": True}` (no thread_id anywhere in this repo).
  - `DiscordWebhook.add_embed` (`webhook.py:285-290`) appends `embed.__dict__`, so the embed's
    null `url`/`image`/`thumbnail`/`video`/`provider`/`author` keys go on the wire, `color` is the
    int from `DiscordEmbed.set_color`'s `int(color, 16)` (`webhook.py:109-116`), `footer` is the
    three-key dict from `set_footer` (`webhook.py:118-129`), and `timestamp` exists only once
    `set_timestamp` has been called (`webhook.py:90-107`).
"""
import json
from typing import Any

import pytest

import bot.core.alerts as alerts


class _Resp:
    status_code = 200
    text = ""


@pytest.fixture
def captured(monkeypatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def _fake_post(url: str, **kwargs: Any) -> _Resp:
        calls.append({"url": url, **kwargs})
        return _Resp()

    monkeypatch.setattr(alerts._requests, "post", _fake_post)
    return calls


def test_text_alert_payload_matches_the_wrapper_byte_for_byte(captured):
    """One content-only alert (the `@everyone` mention form used by the strand/reconcile alerts)."""
    alerts.DiscordWebhook(url="https://discord/hook", content="@everyone",
                          timeout=alerts._DISCORD_TIMEOUT_S).execute()

    assert len(captured) == 1
    call = captured[0]
    assert call["url"] == "https://discord/hook"
    assert call["params"] == {"wait": True}
    assert call["timeout"] == 10
    # Serialised form, not just the mapping: key ORDER is part of "byte-identical".
    assert json.dumps(call["json"]) == (
        '{"attachments": [], "content": "@everyone", "embeds": []}'
    )


def test_embed_alert_payload_matches_the_wrapper_byte_for_byte(captured):
    """One embed alert, built exactly as `send_schema_drift_alert` builds it (plus a footer and a
    field, so every mutator the repo uses is covered)."""
    webhook = alerts.DiscordWebhook(url="https://discord/hook",
                                    timeout=alerts._DISCORD_TIMEOUT_S)
    embed = alerts.DiscordEmbed(title="T", description="D", color="ff8800")
    embed.set_footer(text="F")
    embed.add_embed_field(name="N", value="V", inline=False)
    embed.set_timestamp()
    webhook.add_embed(embed)
    webhook.execute()

    call = captured[0]
    assert call["params"] == {"wait": True}
    assert call["timeout"] == 10
    payload = call["json"]
    # `set_timestamp()` reads the clock; check it is present and ISO-UTC, then compare the rest.
    stamp = payload["embeds"][0].pop("timestamp")
    assert stamp.endswith("+00:00")
    assert payload == {
        "attachments": [],
        "embeds": [{
            "title": "T",
            "description": "D",
            "url": None,
            "footer": {"text": "F", "icon_url": None, "proxy_icon_url": None},
            "image": None,
            "thumbnail": None,
            "video": None,
            "provider": None,
            "author": None,
            "fields": [{"name": "N", "value": "V", "inline": False}],
            "color": 16746496,          # int("ff8800", 16)
        }],
    }
    assert list(payload) == ["attachments", "embeds"]


def test_the_body_no_longer_carries_the_non_spec_timeout_and_wait_fields(captured):
    """They were never Discord fields — only the dropped library's own constructor kwargs leaking
    through its `__dict__` filter. `?wait=true` is the spec form and stays in the query string."""
    alerts.DiscordWebhook(url="https://discord/hook", content="x",
                          timeout=alerts._DISCORD_TIMEOUT_S).execute()
    body = captured[0]["json"]
    assert "timeout" not in body and "wait" not in body
    assert captured[0]["params"] == {"wait": True}      # still on the wire, as the query param
    assert captured[0]["timeout"] == 10                 # still passed to requests


def test_an_over_limit_description_is_truncated_to_the_limit_keeping_the_first_line(captured):
    """The 2026-09-15 19:56Z failing shape: a `VenueBanned(...)` string carrying a whole Cloudflare
    HTML page inside the embed description. Over Discord's limit the API answers 400 and the
    message is LOST, so it is cut — never dropped — and the fingerprint line survives."""
    huge = "FP-abc123 halt reason\n" + "<html>" * 4_000
    webhook = alerts.DiscordWebhook(url="https://discord/hook", timeout=10)
    webhook.add_embed(alerts.DiscordEmbed(title="T", description=huge))
    webhook.execute()

    desc = captured[0]["json"]["embeds"][0]["description"]
    assert len(desc) == alerts.EMBED_DESCRIPTION_LIMIT
    assert desc.startswith("FP-abc123 halt reason\n")
    kept, marker = desc.split("…[cut ")[0], "…[cut " + desc.split("…[cut ")[-1]
    assert marker == alerts._CUT_MARKER.format(n=len(huge) - len(kept))   # the count is honest
    # The content path is capped the same way, at its own (smaller) limit.
    webhook2 = alerts.DiscordWebhook(url="https://discord/hook", content="ID\n" + "y" * 9_000,
                                     timeout=10)
    webhook2.execute()
    content = captured[1]["json"]["content"]
    assert len(content) == alerts.CONTENT_LIMIT and content.startswith("ID\n")


def test_a_400_with_a_body_writes_one_failure_line_carrying_that_body(monkeypatch, tmp_path):
    """The whole point of the change: at the 19:56Z halt every send returned 400 and the response
    BODY — the only place Discord names the rejected field — was never captured."""
    failures = tmp_path / "alert_failures.jsonl"
    monkeypatch.setattr(alerts.alert_health, "DEFAULT_PATH", str(tmp_path / "alert_health.json"))

    class _Rejected:
        status_code = 400
        text = '{"code": 50035, "errors": {"embeds": {"0": {"description": "too long"}}}}'

    monkeypatch.setattr(alerts._requests, "post", lambda url, **kw: _Rejected())
    webhook = alerts.DiscordWebhook(url="https://discord/hook", content="@everyone",
                                    timeout=10, caller="send_reconcile_alert")
    webhook.add_embed(alerts.DiscordEmbed(title="T", description="FP-abc123 halted"))
    alerts._execute_quietly(webhook)

    lines = failures.read_text().splitlines()
    assert len(lines) == 1
    row = json.loads(lines[0])
    assert row["status"] == 400
    assert row["body"] == _Rejected.text
    assert row["channel"] == "discord" and row["caller"] == "send_reconcile_alert"
    assert "FP-abc123 halted" in row["message"]
    assert row["message_len"] == len(webhook.message_text())


def test_a_2xx_writes_no_failure_line_and_counts_as_one_delivery(monkeypatch, tmp_path, captured):
    """The invariant: a message that delivers today still delivers, and the health counters move
    exactly as before."""
    failures = tmp_path / "alert_failures.jsonl"
    health = tmp_path / "alert_health.json"
    monkeypatch.setattr(alerts.alert_health, "DEFAULT_PATH", str(health))

    alerts._execute_quietly(alerts.DiscordWebhook(url="https://discord/hook", content="ok",
                                                  timeout=10))
    assert not failures.exists()
    channel = json.loads(health.read_text())["channels"]["discord"]
    assert (channel["total_ok"], channel["total_failed"], channel["consecutive_failures"]) == (1, 0, 0)


def test_dead_webhook_status_is_recorded_as_a_failure(monkeypatch):
    """The alert-health classification is read off the STATUS CODE, unchanged by the swap: a 404
    from a revoked webhook raises nothing and must still record a FAILED delivery."""
    recorded: list[tuple[str, bool, str]] = []

    class _Dead:
        status_code = 404
        text = "Unknown Webhook"

    monkeypatch.setattr(alerts._requests, "post", lambda url, **kw: _Dead())
    monkeypatch.setattr(alerts.alert_health, "record_delivery",
                        lambda channel, ok, detail: recorded.append((channel, ok, detail)))
    monkeypatch.setattr(alerts.alert_health, "record_failure", lambda *a, **kw: None)

    alerts._execute_quietly(alerts.DiscordWebhook(url="https://discord/dead", timeout=10))
    assert recorded == [("discord", False, "HTTP 404")]
