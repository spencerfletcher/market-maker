"""
tests/test_safety.py
────────────────────
Unit tests for the kill-switch + daily loss cap helpers. Uses tmp_path
fixture to isolate trades.log and pause.json file paths from the real bot.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from bot.core import config, safety


# ── daily loss cap → tests/test_daily_loss_cap.py ────────────────────────────
# compute_daily_pnl and its tests were REMOVED 2026-07-15: the function could not fire.
# It summed `guaranteed_profit` from trades.log — `shares × edge`, > 0 BY CONSTRUCTION for anything
# that fires — so the sum was always >= 0 and `pnl < -limit` was unreachable for any limit > 0.
#
# The tests hid that, and the way they hid it is worth remembering: they INJECTED
# `{"guaranteed_profit": -0.75}` and stubbed compute_daily_pnl negative — values production never
# produces. The fixture defined a world where the cap worked. Same shape as the V1 FOK-kill mock
# that "proved" the flatten against a venue that no longer existed.
#
# The cap is now re-based on execution_pnl.csv (real realized costs) and pinned in
# tests/test_daily_loss_cap.py against data the bot actually writes.

# ── is_paused ───────────────────────────────────────────────────────────────

def test_is_paused_file_exists(tmp_path, monkeypatch):
    pause_path = tmp_path / "pause.json"
    pause_path.write_text("{}")
    monkeypatch.setattr(config, "KILL_SWITCH_FILE", str(pause_path))
    assert safety.is_paused() is True


def test_is_paused_file_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "KILL_SWITCH_FILE", str(tmp_path / "does-not-exist.json"))
    assert safety.is_paused() is False


def test_is_paused_empty_path(monkeypatch):
    """Empty KILL_SWITCH_FILE means feature disabled."""
    monkeypatch.setattr(config, "KILL_SWITCH_FILE", "")
    assert safety.is_paused() is False


# ── is_paused: LANE SCOPING (2026-08-31) ─────────────────────────────────────
# pause.json may narrow to `{"lanes": ["probe"]}` so halting one lane's run does not tear down a
# concurrent maker in another lane. EVERY other shape is a GLOBAL pause: ambiguity halts MORE.

def _pause(tmp_path, monkeypatch, body: str):
    p = tmp_path / "pause.json"
    p.write_text(body)
    monkeypatch.setattr(config, "KILL_SWITCH_FILE", str(p))
    return p


def test_is_paused_empty_file_pauses_every_lane(tmp_path, monkeypatch):
    """(a) THE TOUCH PATH. `touch pause.json` writes ZERO BYTES — the documented operator control,
    and the one shape that must never be read as "narrows to no lanes". Pinned hard: every lane,
    plus the lane-less callers."""
    _pause(tmp_path, monkeypatch, "")
    for lane in ("main", "probe", "wsprobe", "anything-else"):
        assert safety.is_paused(lane) is True, lane
    assert safety.is_paused() is True
    assert safety.is_paused(None) is True


def test_is_paused_scoped_pauses_only_the_listed_lane(tmp_path, monkeypatch):
    """(b) The whole point: `{"lanes": ["probe"]}` halts probe and leaves main quoting."""
    _pause(tmp_path, monkeypatch, json.dumps({"lanes": ["probe"]}))
    assert safety.is_paused("probe") is True
    assert safety.is_paused("main") is False
    assert safety.is_paused("wsprobe") is False


def test_is_paused_scoped_multi_lane(tmp_path, monkeypatch):
    _pause(tmp_path, monkeypatch, json.dumps({"lanes": ["probe", "wsprobe"]}))
    assert safety.is_paused("probe") is True
    assert safety.is_paused("wsprobe") is True
    assert safety.is_paused("main") is False


@pytest.mark.parametrize("body", [
    "not json at all",
    "{lanes: [probe]}",          # JSON-ish typo — the realistic operator error
    '{"lanes": ["probe"',        # truncated write
    '["probe"]',                 # right idea, wrong shape (list, not object)
    '"probe"',                   # bare string
    "null",
    "{}",                        # no `lanes` key
    '{"lane": "probe"}',         # singular key typo
    '{"lanes": "probe"}',        # string, not list
    '{"lanes": [1, 2]}',         # non-string members
    '{"lanes": ["probe", 7]}',   # ONE bad member poisons the whole list
])
def test_is_paused_malformed_is_global(tmp_path, monkeypatch, body):
    """(c) Garbage — in every flavour that has plausibly come off an operator's keyboard — pauses
    ALL lanes. A typo must never silently un-halt a live maker."""
    _pause(tmp_path, monkeypatch, body)
    assert safety.is_paused("main") is True
    assert safety.is_paused("probe") is True
    assert safety.is_paused() is True


def test_is_paused_empty_lanes_list_is_global(tmp_path, monkeypatch):
    """(d) `{"lanes": []}` means "I could not name a lane", NOT "halt nothing"."""
    _pause(tmp_path, monkeypatch, json.dumps({"lanes": []}))
    assert safety.is_paused("main") is True
    assert safety.is_paused("probe") is True


def test_is_paused_scoped_file_pauses_lane_less_callers(tmp_path, monkeypatch):
    """(e) The arb bot / Kalshi maker / probe scripts call `is_paused()` with NO lane — they are not
    in the Poly lane namespace, so a scoped file halts them too (fail-closed)."""
    _pause(tmp_path, monkeypatch, json.dumps({"lanes": ["probe"]}))
    assert safety.is_paused() is True
    assert safety.is_paused(None) is True


def test_is_paused_scoped_file_absent_no_pause(tmp_path, monkeypatch):
    """No file is still no pause, lane or not — scoping must not invent a halt."""
    monkeypatch.setattr(config, "KILL_SWITCH_FILE", str(tmp_path / "nope.json"))
    assert safety.is_paused("probe") is False
    assert safety.is_paused() is False


@pytest.fixture(autouse=True)
def _clear_scoped_miss_throttle():
    """The throttle's memo is module state — a leaked key would make a later test's first
    sighting arrive as INFO and silently weaken the pin."""
    safety._scoped_miss_last_warn.clear()
    yield
    safety._scoped_miss_last_warn.clear()


def test_the_scoped_MISS_warning_is_THROTTLED_off_the_discord_path(tmp_path, monkeypatch, caplog):
    """⛔ [review r3, CONCERN A] `_DiscordWebhookHandler` posts every WARNING with only a 30 s
    dedup, and this line fires once per quote cycle in the feature's INTENDED steady state — a
    scoped pause up while the other lane runs all evening. At `--requote-s 10` that is ~960 posts
    onto the same webhook that carries strand alerts.

    The TAIL stays loud (every check emits a record); only the level is throttled.
    ⛔ MUTANT: return WARNING unconditionally → RED."""
    _pause(tmp_path, monkeypatch, json.dumps({"lanes": ["probe"]}))
    with caplog.at_level("INFO"):
        for _ in range(6):
            assert safety.is_paused("main") is False
    levels = [r.levelname for r in caplog.records]
    assert len(levels) == 6, "every check must still emit a line for the tail"
    assert levels[0] == "WARNING", "the first sighting must be loud"
    assert set(levels[1:]) == {"INFO"}, f"the repeats must not reach Discord; {levels}"


def test_the_throttle_REOPENS_after_the_window(tmp_path, monkeypatch, caplog):
    """A spell that outlives the window re-announces — an operator who walked away and came back
    must still find a live WARNING, not only INFO."""
    _pause(tmp_path, monkeypatch, json.dumps({"lanes": ["probe"]}))
    clock = {"t": 1_000_000.0}
    monkeypatch.setattr(safety.time, "time", lambda: clock["t"])
    with caplog.at_level("INFO"):
        safety.is_paused("main")
        clock["t"] += safety._SCOPED_MISS_WARN_EVERY_S - 1.0
        safety.is_paused("main")
        clock["t"] += 2.0
        safety.is_paused("main")
    assert [r.levelname for r in caplog.records] == ["WARNING", "INFO", "WARNING"]


def test_an_EDIT_to_the_pause_file_re_fires_the_warning_immediately(tmp_path, monkeypatch,
                                                                    caplog):
    """The key carries the file's mtime AND its parsed scope. An operator correcting the lane
    name is watching for a response right then; making them wait out the window would read as
    "my edit did nothing". ⛔ MUTANT: key on the path alone → RED."""
    p = _pause(tmp_path, monkeypatch, json.dumps({"lanes": ["probe"]}))
    with caplog.at_level("INFO"):
        safety.is_paused("main")
        safety.is_paused("main")
        p.write_text(json.dumps({"lanes": ["probe", "wsprobe"]}))
        os.utime(p, (2_000_000.0, 2_000_000.0))
        safety.is_paused("main")
    assert [r.levelname for r in caplog.records] == ["WARNING", "INFO", "WARNING"]
    assert "wsprobe" in caplog.records[-1].message


def test_the_COVERED_halt_is_never_throttled(tmp_path, monkeypatch, caplog):
    """Only the not-paused line is throttled. An actual halt stays WARNING every time — it is a
    once-per-run event (the maker stops), so it costs Discord nothing and must never be quiet."""
    _pause(tmp_path, monkeypatch, json.dumps({"lanes": ["probe"]}))
    with caplog.at_level("INFO"):
        for _ in range(4):
            assert safety.is_paused("probe") is True
    assert [r.levelname for r in caplog.records] == ["WARNING"] * 4


def test_a_scoped_MISS_is_LOUD_every_check(tmp_path, monkeypatch, caplog):
    """⛔ THE FAIL-OPEN AN OPERATOR CANNOT SEE. A pause file that
    exists but does not name our lane returns False — and the first cut did it in SILENCE, so an
    operator who typo'd the lane name (`"Probe"` for `probe`) watched a live maker quote straight
    through what they believed was a halt with nothing in the tail to say so.

    ⛔ MUTANT: delete the log call on the not-covered branch → RED. Twice, because "loud once at
    the top of a spell that lasts hours" is not loud: EVERY check must emit a LINE. (Only the
    LEVEL is throttled, to keep it off Discord — see the throttle tests above.)"""
    _pause(tmp_path, monkeypatch, json.dumps({"lanes": ["Probe"]}))
    with caplog.at_level("INFO"):
        assert safety.is_paused("probe") is False
        assert safety.is_paused("probe") is False
    lines = [r.message for r in caplog.records]
    assert len(lines) == 2, f"one line per check, got {lines}"
    assert "scoped" in lines[0] and "'Probe'" in lines[0] and "'probe'" in lines[0]
    assert "CONTINUES" in lines[0]


def test_a_scoped_HIT_halts_without_the_continues_warning(tmp_path, monkeypatch, caplog):
    """The other side of the same pin: a lane that IS covered must halt, and must not also print
    the reassuring "continues quoting" line — two contradictory lines in one tail is worse than
    either alone."""
    _pause(tmp_path, monkeypatch, json.dumps({"lanes": ["probe"]}))
    with caplog.at_level("WARNING"):
        assert safety.is_paused("probe") is True
    lines = [r.message for r in caplog.records]
    assert len(lines) == 1 and "Kill switch active" in lines[0] and "lane=probe" in lines[0]
    assert "CONTINUES" not in lines[0]


def test_is_paused_non_utf8_file_pauses_and_does_NOT_raise(tmp_path, monkeypatch):
    """⛔ `f.read()` on non-UTF-8 raises UnicodeDecodeError — a ValueError SUBCLASS, but raised by
    the READ, outside the json.loads guard. Escaping from the first statement of the quote cycle,
    it reaches teardown as an anonymous crash with no halt_reason. A corrupt pause file must
    PAUSE, not explode."""
    p = tmp_path / "pause.json"
    p.write_bytes(b'{"lanes": ["\xff\xfe\x00probe"]}')
    monkeypatch.setattr(config, "KILL_SWITCH_FILE", str(p))
    assert safety.is_paused("main") is True
    assert safety.is_paused("probe") is True
    assert safety.is_paused() is True


def test_an_oversized_pause_file_is_global(tmp_path, monkeypatch):
    """The read is BOUNDED (64 KB): this is polled per quote cycle against an operator-writable
    path on a 1.9 GB box, so an unbounded read is an OOM path into the live maker. Over the bound
    the content is unjudgeable ⇒ global pause."""
    body = json.dumps({"lanes": ["probe"], "pad": "x" * (64 * 1024)})
    assert len(body) > safety._PAUSE_READ_MAX_BYTES
    _pause(tmp_path, monkeypatch, body)
    assert safety.is_paused("main") is True
    assert safety.is_paused("probe") is True


def test_is_paused_unreadable_file_is_global(tmp_path, monkeypatch):
    """An existing-but-unreadable pause file is maximum ambiguity → global halt."""
    p = _pause(tmp_path, monkeypatch, json.dumps({"lanes": ["probe"]}))
    monkeypatch.setattr(
        safety, "open",
        lambda *a, **k: (_ for _ in ()).throw(OSError("boom")), raising=False)
    assert safety.is_paused("main") is True
    assert p.exists()
