"""Alert DELIVERY must be verifiable — the alert channel cannot be its own witness.

THE DEFECT THIS PINS. `bot/core/alerts.py` swallowed every send outcome at `log.debug` while
`LOG_LEVEL=INFO`, so a dead webhook was undetectable. Worse than the log level: a dead Discord
webhook does **not raise** — `DiscordWebhook.execute()` posts with `wait=true` and RETURNS a
`Response` carrying 401/404 (verified against the installed library). The `except Exception` around
it therefore never fired, so even at DEBUG there was nothing to see. The bot would report "alert
sent" forever against a webhook deleted months ago.

Two requirements follow, and both are tested here:
  • the OUTCOME is read from the response status, not from the absence of an exception;
  • the record lands somewhere that does NOT depend on the channel that just failed — a local
    file, plus a non-DEBUG log line.
"""
from __future__ import annotations

import json
import logging

import pytest

from bot.core import alert_health


# ── classify_delivery: the pure decision, where the real bug lived ────────────────────────────

def test_a_204_is_a_successful_delivery():
    ok, detail = alert_health.classify_delivery(204, None)
    assert ok is True, detail


def test_a_200_is_a_successful_delivery():
    assert alert_health.classify_delivery(200, None)[0] is True


def test_a_404_is_a_FAILED_delivery_even_though_nothing_raised():
    """The whole defect in one assertion: a deleted webhook returns 404 and raises nothing."""
    ok, detail = alert_health.classify_delivery(404, None)
    assert ok is False
    assert "404" in detail


def test_a_401_is_a_FAILED_delivery():
    assert alert_health.classify_delivery(401, None)[0] is False


def test_a_429_is_a_FAILED_delivery_because_the_message_did_not_go():
    """Rate-limited is not delivered. Counting it as success is how a throttled alert storm
    reports perfect health while every alert in it was dropped."""
    assert alert_health.classify_delivery(429, None)[0] is False


def test_an_exception_is_a_FAILED_delivery():
    ok, detail = alert_health.classify_delivery(None, ValueError("boom"))
    assert ok is False
    assert "boom" in detail


def test_no_status_and_no_exception_is_a_FAILED_delivery_not_a_success():
    """CANNOT-VERIFY is not delivery. Same discipline as reconcile.py's cannot-verify-is-not-flat:
    if we did not observe a success we must not record one."""
    ok, detail = alert_health.classify_delivery(None, None)
    assert ok is False


# ── the durable ledger ───────────────────────────────────────────────────────────────────────

def test_a_failure_is_recorded_to_a_local_file_the_operator_can_read(tmp_path):
    p = tmp_path / "alert_health.json"
    alert_health.record_delivery("discord", False, "HTTP 404", path=str(p), now=100.0)
    data = json.loads(p.read_text())
    assert "discord" in data["channels"]
    assert data["channels"]["discord"]["consecutive_failures"] == 1
    assert "404" in data["channels"]["discord"]["last_error"]


def test_a_failure_logs_at_ERROR_not_DEBUG(tmp_path, caplog):
    """`log.debug` under `LOG_LEVEL=INFO` is indistinguishable from silence."""
    p = tmp_path / "alert_health.json"
    with caplog.at_level(logging.INFO, logger="bot.core.alert_health"):
        alert_health.record_delivery("discord", False, "HTTP 404", path=str(p), now=100.0)
    levels = [r.levelno for r in caplog.records]
    assert levels, "nothing logged at INFO or above — the failure is silent"
    assert max(levels) >= logging.ERROR, f"loudest record was {levels}"


def test_a_success_does_not_log_at_error(tmp_path, caplog):
    p = tmp_path / "alert_health.json"
    with caplog.at_level(logging.INFO, logger="bot.core.alert_health"):
        alert_health.record_delivery("discord", True, "", path=str(p), now=100.0)
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []


def test_consecutive_failures_accumulate_across_calls(tmp_path):
    p = tmp_path / "alert_health.json"
    for i in range(3):
        alert_health.record_delivery("discord", False, "HTTP 404", path=str(p), now=100.0 + i)
    assert alert_health.load_health(str(p))["discord"].consecutive_failures == 3


def test_a_success_resets_the_failure_streak(tmp_path):
    p = tmp_path / "alert_health.json"
    alert_health.record_delivery("discord", False, "HTTP 404", path=str(p), now=1.0)
    alert_health.record_delivery("discord", True, "", path=str(p), now=2.0)
    h = alert_health.load_health(str(p))["discord"]
    assert h.consecutive_failures == 0
    assert h.last_ok_ts == 2.0


def test_the_streak_alarm_escalates_to_CRITICAL(tmp_path, caplog):
    """One transient 500 is noise; a streak means the channel is gone and every alert since the
    streak started was lost."""
    p = tmp_path / "alert_health.json"
    with caplog.at_level(logging.INFO, logger="bot.core.alert_health"):
        for i in range(alert_health.ALARM_STREAK):
            alert_health.record_delivery("discord", False, "HTTP 404", path=str(p), now=float(i))
    assert any(r.levelno >= logging.CRITICAL for r in caplog.records), \
        "a dead channel never escalates past ERROR"


def test_channels_are_tracked_independently(tmp_path):
    p = tmp_path / "alert_health.json"
    alert_health.record_delivery("discord", False, "HTTP 404", path=str(p), now=1.0)
    alert_health.record_delivery("pushover", True, "", path=str(p), now=2.0)
    health = alert_health.load_health(str(p))
    assert health["discord"].healthy is False
    assert health["pushover"].healthy is True


def test_recording_never_raises_into_the_caller(tmp_path, monkeypatch):
    """An alert is best-effort and fires from inside the fire window — the bookkeeping about it
    must be even MORE incapable of raising than the alert itself."""
    def explode(*a, **k):
        raise OSError("disk on fire")
    monkeypatch.setattr(alert_health.durable, "write_json_durable", explode)
    alert_health.record_delivery("discord", False, "x", path=str(tmp_path / "h.json"), now=1.0)


def test_load_health_of_a_missing_file_is_empty_not_an_error(tmp_path):
    assert alert_health.load_health(str(tmp_path / "nope.json")) == {}


def test_a_corrupt_ledger_is_reported_as_unhealthy_not_as_clean(tmp_path):
    """A truncated ledger (the classic SIGKILL-mid-write artefact) must not read as 'no failures
    recorded'. Unknown is not healthy."""
    p = tmp_path / "alert_health.json"
    p.write_text("{trunca")
    problems = alert_health.problems(str(p))
    assert problems, "a corrupt delivery ledger reported no problem"


def test_problems_lists_a_dead_channel(tmp_path):
    p = tmp_path / "alert_health.json"
    for i in range(alert_health.ALARM_STREAK):
        alert_health.record_delivery("discord", False, "HTTP 404", path=str(p), now=float(i))
    assert any("discord" in s for s in alert_health.problems(str(p)))


def test_problems_is_empty_when_every_channel_is_delivering(tmp_path):
    p = tmp_path / "alert_health.json"
    alert_health.record_delivery("discord", True, "", path=str(p), now=1.0)
    assert alert_health.problems(str(p)) == []


# ── the wiring: alerts.py must actually feed this ────────────────────────────────────────────

def test_alerts_execute_quietly_records_a_404_as_a_failure(tmp_path, monkeypatch):
    """End-to-end through the real `_execute_quietly`: a webhook that returns 404 and raises
    nothing must land in the ledger."""
    from bot.core import alerts

    p = tmp_path / "alert_health.json"
    monkeypatch.setattr(alert_health, "DEFAULT_PATH", str(p))

    class DeadWebhook(alerts.DiscordWebhook):    # a real webhook, only the transport is stubbed
        def execute(self):
            class R:
                status_code = 404
            return R()

    alerts._execute_quietly(DeadWebhook())
    # `in` first, DELIBERATELY. `load_health(...)["discord"]` alone raises KeyError when the
    # recording is removed, and the mutation probe classifies a crash as INVALID — evidence of
    # nothing — so the assertion below would not actually pin the wiring.
    health = alert_health.load_health(str(p))
    assert "discord" in health, "the send outcome was not recorded at all"
    assert health["discord"].consecutive_failures == 1


def test_alerts_execute_quietly_records_a_204_as_a_success(tmp_path, monkeypatch):
    from bot.core import alerts

    p = tmp_path / "alert_health.json"
    monkeypatch.setattr(alert_health, "DEFAULT_PATH", str(p))

    class LiveWebhook:
        def execute(self):
            class R:
                status_code = 204
            return R()

    alerts._execute_quietly(LiveWebhook())
    health = alert_health.load_health(str(p))
    assert "discord" in health, "the send outcome was not recorded at all"
    assert health["discord"].healthy is True


def test_alerts_execute_quietly_records_a_raise_as_a_failure(tmp_path, monkeypatch):
    from bot.core import alerts

    p = tmp_path / "alert_health.json"
    monkeypatch.setattr(alert_health, "DEFAULT_PATH", str(p))

    class BrokenWebhook(alerts.DiscordWebhook):
        def execute(self):
            raise ConnectionError("no route to host")

    alerts._execute_quietly(BrokenWebhook())
    health = alert_health.load_health(str(p))
    assert "discord" in health, "the send outcome was not recorded at all"
    assert health["discord"].consecutive_failures == 1
    assert "no route" in health["discord"].last_error


def test_alerts_pushover_post_records_its_outcome(tmp_path, monkeypatch):
    from bot.core import alerts

    p = tmp_path / "alert_health.json"
    monkeypatch.setattr(alert_health, "DEFAULT_PATH", str(p))

    class R:
        status_code = 400

    monkeypatch.setattr(alerts._requests, "post", lambda *a, **k: R())
    alerts._post_pushover_quietly({"token": "t", "user": "u"})
    health = alert_health.load_health(str(p))
    assert "pushover" in health, "the send outcome was not recorded at all"
    assert health["pushover"].healthy is False
