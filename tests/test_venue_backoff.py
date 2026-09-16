"""The shared venue-health latch + the producer backoff it drives.

The load-bearing assertions here, in order of what they cost when they break:

  1. **A producer inside a ban window spends ZERO venue requests.** Pinned at the TRANSPORT seam
     (a fake `_sdk` that raises if it is touched at all), not by asserting a log line — every
     request during a CF-1015 ban extends the ban, so "we raised the right exception" is not the
     claim; "no packet left the box" is.
  2. **The maker's client NEVER consults the latch.** The default `PolyUSClient()` is what the
     live maker, every order tool and every operator read construct. A producer's observation of
     a sick bulk route must not be able to starve a real-money run's own book read.
  3. A corrupt latch reads as ABSENT and LOUD — never as a permanent hold. `poly_event_risk`
     gate 7 fails closed on a stale flow map, so a latch that cannot lapse blocks launches.

Every test drives an explicit `tmp_path` latch. `bot/core/venue_backoff.LATCH_PATH` resolves under
the repo's real `logs/`, and `tests/_logs_write_guard.py` blocks that write — deliberately.
"""
from __future__ import annotations

import asyncio
import json
import os

import pytest

from bot.core import venue_backoff
from bot.core.venue_backoff import BAN, DEGRADED, ErrorWatcher, VenueBackoff
from bot.poly_us.client import PolyUSClient


@pytest.fixture(autouse=True)
def _clear_parse_memo():
    """The module memoises the parsed latch on (path, mtime_ns, size). tmp_path files created in
    the same nanosecond with the same size across two tests could otherwise collide."""
    venue_backoff._memo = (None, None)
    yield
    venue_backoff._memo = (None, None)


@pytest.fixture
def latch(tmp_path):
    return str(tmp_path / "venue_backoff.json")


class _Boom(Exception):
    """A venue error with an SDK-shaped `status_code` / `body`."""

    def __init__(self, message: str, *, status_code=None, body=None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


# ── the latch: write / read / expiry / extension ─────────────────────────────────────────────

def test_absent_latch_reads_as_no_backoff(latch):
    assert venue_backoff.read_latch(latch, now=1000.0) is None
    venue_backoff.raise_if_active(latch, now=1000.0)     # must not raise


def test_arm_ban_writes_a_latch_that_reads_back(latch):
    venue_backoff.arm(BAN, "error code: 1015", path=latch, now=1000.0)
    got = venue_backoff.read_latch(latch, now=1000.0)
    assert got is not None
    assert got["kind"] == BAN
    assert got["until"] == pytest.approx(1000.0 + venue_backoff.BAN_S)
    assert "1015" in got["reason"]


def test_ban_window_is_25_minutes_and_degraded_is_10(latch):
    """Not a style constant. 25 min is the TOP of the recorded 15–25 min CF ban overhang: coming
    back early means resuming INTO the ban, and every request during it extends the ban."""
    assert venue_backoff.BAN_S == 25 * 60
    assert venue_backoff.DEGRADED_S == 10 * 60


def test_latch_expires_by_wall_clock_with_nobody_clearing_it(latch):
    venue_backoff.arm(BAN, "1015", path=latch, now=1000.0)
    assert venue_backoff.read_latch(latch, now=1000.0 + venue_backoff.BAN_S - 1) is not None
    # The releasing process may be long dead; expiry is the whole release mechanism.
    assert venue_backoff.read_latch(latch, now=1000.0 + venue_backoff.BAN_S) is None
    assert os.path.exists(latch)                         # expired, not deleted


def test_same_kind_rearm_extends_exponentially(latch):
    a = venue_backoff.arm(DEGRADED, "500", path=latch, now=1000.0)
    assert a["duration_s"] == venue_backoff.DEGRADED_S
    b = venue_backoff.arm(DEGRADED, "500 again", path=latch, now=1100.0)
    assert b["duration_s"] == venue_backoff.DEGRADED_S * 2
    assert b["extends"] == 1
    assert b["since"] == 1000.0                          # the hold's origin, not the re-arm


def test_extension_caps_at_max_s(latch):
    now = 1000.0
    for _ in range(12):
        got = venue_backoff.arm(DEGRADED, "500", path=latch, now=now)
        now += 1.0
    assert got["duration_s"] == venue_backoff.MAX_S


def test_degraded_never_downgrades_an_active_ban(latch):
    venue_backoff.arm(BAN, "1015", path=latch, now=1000.0)
    got = venue_backoff.arm(DEGRADED, "a stray 500", path=latch, now=1100.0)
    assert got["kind"] == BAN
    assert got["until"] == pytest.approx(1000.0 + venue_backoff.BAN_S)


def test_ban_overrides_an_active_degraded(latch):
    venue_backoff.arm(DEGRADED, "500", path=latch, now=1000.0)
    got = venue_backoff.arm(BAN, "1015", path=latch, now=1100.0)
    assert got["kind"] == BAN
    assert got["extends"] == 0                            # a fresh hold, not an extension
    assert got["until"] == pytest.approx(1100.0 + venue_backoff.BAN_S)


def test_until_never_moves_backwards(latch):
    """Two producers racing can only ever agree on a LONGER hold."""
    venue_backoff.arm(BAN, "1015", path=latch, now=1000.0)
    far = venue_backoff.read_latch(latch, now=1000.0)["until"]
    # A degraded arm 1s later would naturally expire sooner; it must not shorten the ban.
    venue_backoff.arm(DEGRADED, "500", path=latch, now=1001.0)
    assert venue_backoff.read_latch(latch, now=1001.0)["until"] == pytest.approx(far)


def test_clear_removes_the_latch_and_is_idempotent(latch):
    venue_backoff.arm(BAN, "1015", path=latch, now=1000.0)
    venue_backoff.clear(latch)
    assert venue_backoff.read_latch(latch, now=1000.0) is None
    venue_backoff.clear(latch)                            # absent → no raise


# ── corrupt / unrecognised → absent + LOUD ───────────────────────────────────────────────────

def test_corrupt_latch_is_treated_as_absent_and_logged_loudly(latch, caplog):
    with open(latch, "w") as fh:
        fh.write("{not json at all")
    with caplog.at_level("ERROR"):
        assert venue_backoff.read_latch(latch, now=1000.0) is None
        venue_backoff.raise_if_active(latch, now=1000.0)   # fail OPEN
    assert any("CORRUPT" in r.message for r in caplog.records), caplog.text


def test_unrecognised_latch_shape_is_absent_and_loud(latch, caplog):
    with open(latch, "w") as fh:
        json.dump({"kind": "sideways", "until": 9e9}, fh)
    with caplog.at_level("ERROR"):
        assert venue_backoff.read_latch(latch, now=1000.0) is None
    assert any("UNRECOGNISED" in r.message for r in caplog.records), caplog.text


def test_latch_without_a_usable_until_is_absent_and_loud(latch, caplog):
    with open(latch, "w") as fh:
        json.dump({"kind": "ban", "until": "soon"}, fh)
    with caplog.at_level("ERROR"):
        assert venue_backoff.read_latch(latch, now=1000.0) is None
    assert any("until" in r.message for r in caplog.records), caplog.text


# ── classification ───────────────────────────────────────────────────────────────────────────

def test_cf_1015_in_the_body_is_a_ban_whatever_the_status():
    """CF serves its own page IN FRONT of the origin — the recorded 1015s came back on assorted
    statuses with the marker only in the body. Missing one is the expensive direction."""
    exc = _Boom("Forbidden", status_code=403, body="<html>error code: 1015</html>")
    assert venue_backoff.classify(exc) is BAN


def test_http_429_is_a_ban_and_5xx_is_degraded():
    assert venue_backoff.classify(_Boom("slow down", status_code=429)) is BAN
    assert venue_backoff.classify(_Boom("bad gateway", status_code=502)) is DEGRADED
    assert venue_backoff.classify(_Boom("boom", status_code=500)) is DEGRADED


def test_a_404_is_not_a_venue_health_signal():
    assert venue_backoff.classify(_Boom("no such market", status_code=404)) is None


def test_a_venue_backoff_is_never_itself_a_venue_health_signal(latch):
    """⛔ Found by mutation. `VenueBackoff`'s message quotes the 1015 that armed the latch, so a
    hold swallowed by a broad `except Exception` and fed back to the watcher would RE-ARM off a
    request that was never issued — the hold extending itself forever."""
    venue_backoff.arm(BAN, "error code: 1015", path=latch, now=1000.0)
    held = venue_backoff.read_latch(latch, now=1000.0)
    exc = VenueBackoff(BAN, held["until"], 900.0, "error code: 1015")
    assert venue_backoff.classify(exc) is None
    assert ErrorWatcher(path=latch).record(exc, now=1001.0) is None
    assert venue_backoff.read_latch(latch, now=1001.0)["until"] == held["until"]


def test_1015_must_be_word_bounded_not_a_substring():
    """A quantity or market id containing 1015 must not arm a 25-minute fleet-wide hold."""
    assert venue_backoff.classify(_Boom("filled 21015 of 30000", status_code=404)) is None


# ── the watcher: threshold, window, extension ────────────────────────────────────────────────

def test_watcher_arms_a_ban_on_the_first_1015_with_no_threshold(latch):
    w = ErrorWatcher(path=latch)
    got = w.record(_Boom("error code: 1015", status_code=429), now=1000.0)
    assert got is not None and got["kind"] == BAN


def test_watcher_needs_threshold_5xx_within_the_window(latch):
    w = ErrorWatcher(path=latch, threshold=5, window_s=300.0)
    for i in range(4):
        assert w.record(_Boom("500", status_code=500), now=1000.0 + i) is None
    got = w.record(_Boom("500", status_code=500), now=1004.0)
    assert got is not None and got["kind"] == DEGRADED


def test_watcher_prunes_5xx_older_than_the_window(latch):
    w = ErrorWatcher(path=latch, threshold=5, window_s=300.0)
    for i in range(4):
        w.record(_Boom("500", status_code=500), now=1000.0 + i)
    # The 5th arrives an hour later — the earlier four are no longer evidence of anything.
    assert w.record(_Boom("500", status_code=500), now=5000.0) is None
    assert venue_backoff.read_latch(latch, now=5000.0) is None


def test_watcher_extends_an_active_degraded_on_every_further_5xx(latch):
    w = ErrorWatcher(path=latch, threshold=2, window_s=300.0)
    w.record(_Boom("500", status_code=500), now=1000.0)
    first = w.record(_Boom("500", status_code=500), now=1001.0)
    assert first["duration_s"] == venue_backoff.DEGRADED_S
    # Already held and STILL erroring → the hold was too short. No fresh threshold required.
    second = w.record(_Boom("500", status_code=500), now=1002.0)
    assert second["duration_s"] == venue_backoff.DEGRADED_S * 2


def test_watcher_5xx_does_not_disturb_an_active_ban(latch):
    w = ErrorWatcher(path=latch, threshold=1)
    venue_backoff.arm(BAN, "1015", path=latch, now=1000.0)
    assert w.record(_Boom("500", status_code=500), now=1001.0) is None
    assert venue_backoff.read_latch(latch, now=1001.0)["kind"] == BAN


def test_raise_if_active_carries_kind_and_remaining(latch):
    venue_backoff.arm(BAN, "error code: 1015", path=latch, now=1000.0)
    with pytest.raises(VenueBackoff) as ei:
        venue_backoff.raise_if_active(latch, now=1300.0)
    assert ei.value.kind == BAN
    assert ei.value.remaining_s == pytest.approx(venue_backoff.BAN_S - 300.0)
    assert "1015" in ei.value.reason


# ── opswatch rail ────────────────────────────────────────────────────────────────────────────

def test_status_is_clear_when_no_latch(latch):
    row, problems, notes = venue_backoff.venue_backoff_status(path=latch, now=1000.0)
    assert row["kind"] is None and problems == [] and notes == []


def test_status_reports_an_active_latch_as_a_NOTE_not_a_problem(latch):
    """A hold is the fleet working. Paging on it trains the operator to ignore the rail."""
    venue_backoff.arm(BAN, "1015", path=latch, now=1000.0)
    row, problems, notes = venue_backoff.venue_backoff_status(path=latch, now=1100.0)
    assert row["kind"] == BAN
    assert problems == []
    assert notes and "BAN" in notes[0]


def test_status_pages_when_a_latch_has_been_held_longer_than_max_s(latch):
    # `since` survives only across a CONTINUOUS hold — a latch that lapses and is re-armed is a
    # fresh hold, which is the design: the rail pages for a stuck re-armer, not for a venue that
    # went down twice. So re-arm inside the window, the way a producer hitting 1015 every pass would.
    now = 1000.0
    venue_backoff.arm(BAN, "1015", path=latch, now=now)
    while now - 1000.0 <= venue_backoff.MAX_S:
        now += 60.0
        venue_backoff.arm(BAN, "1015 again", path=latch, now=now)
    _row, problems, _notes = venue_backoff.venue_backoff_status(path=latch, now=now)
    assert problems and "continuously in force" in problems[0]


def test_a_latch_that_lapsed_and_rearmed_is_a_FRESH_hold_not_a_stuck_one(latch):
    """The complement of the test above, and the reason it is written that way: a venue that went
    down at 04:00 and again at 09:00 must not page as "held for 5 hours"."""
    venue_backoff.arm(BAN, "1015", path=latch, now=1000.0)
    later = 1000.0 + venue_backoff.BAN_S + 1.0            # the first hold has lapsed
    got = venue_backoff.arm(BAN, "1015 again", path=latch, now=later)
    assert got["since"] == later and got["extends"] == 0
    _row, problems, _notes = venue_backoff.venue_backoff_status(path=latch, now=later)
    assert problems == []


# ── the client seam: suppression, and the maker-path pin ─────────────────────────────────────

class _ExplodingSDK:
    """Any attribute touch is a venue request that must not have happened."""

    def __init__(self):
        self.calls = 0

    def __getattr__(self, name):
        self.calls += 1
        raise AssertionError(
            f"a venue request ({name}) was issued during a ban window — every request during a "
            f"CF-1015 hold EXTENDS it")


class _CountingSDK:
    """A minimal book transport that records that it was reached."""

    def __init__(self):
        self.calls = 0

    async def get(self, path, query=None):
        self.calls += 1
        return {"marketData": {}}

    class _Markets:
        def __init__(self, outer):
            self._outer = outer

        async def book(self, slug):
            self._outer.calls += 1
            return {"marketData": {}}

    @property
    def markets(self):
        return _CountingSDK._Markets(self)


def _client(*, obey: bool, sdk, latch_path: str, monkeypatch) -> PolyUSClient:
    monkeypatch.setattr(venue_backoff, "LATCH_PATH", latch_path)
    c = PolyUSClient.__new__(PolyUSClient)
    c._dry_run = True
    c._obey_venue_backoff = obey
    c._sdk = sdk
    return c


def test_producer_client_in_a_ban_window_spends_zero_venue_calls(latch, monkeypatch):
    """THE test. Transport seam, not a log assertion: the claim is that no packet left the box."""
    venue_backoff.arm(BAN, "error code: 1015", path=latch, now=None)
    sdk = _ExplodingSDK()
    c = _client(obey=True, sdk=sdk, latch_path=latch, monkeypatch=monkeypatch)
    with pytest.raises(VenueBackoff):
        asyncio.run(c._fetch_book("some-slug", fresh=True))
    assert sdk.calls == 0


def test_maker_path_client_never_consults_the_latch(latch, monkeypatch):
    """⛔ PINNED. The live maker constructs `PolyUSClient()` with no flag. A producer's latch must
    never starve a real-money run's own book read — the maker has its own budget discipline."""
    venue_backoff.arm(BAN, "error code: 1015", path=latch, now=None)
    sdk = _CountingSDK()
    c = _client(obey=False, sdk=sdk, latch_path=latch, monkeypatch=monkeypatch)
    assert asyncio.run(c._fetch_book("some-slug", fresh=True)) == {"marketData": {}}
    assert sdk.calls == 1


def test_default_client_flag_is_false():
    """The default IS the money path — a producer must opt IN, never a maker opt OUT."""
    import inspect
    sig = inspect.signature(PolyUSClient.__init__)
    assert sig.parameters["obey_venue_backoff"].default is False


def test_get_fill_quote_propagates_the_hold_rather_than_failing_to_none(latch, monkeypatch):
    """`get_fill_quote` fails every error to a closed `(None, "?", …)`. If the check sat inside
    that try, a hold would read as "no quote" and the caller would walk to the next book."""
    venue_backoff.arm(BAN, "1015", path=latch, now=None)
    sdk = _ExplodingSDK()
    c = _client(obey=True, sdk=sdk, latch_path=latch, monkeypatch=monkeypatch)
    with pytest.raises(VenueBackoff):
        asyncio.run(c.get_fill_quote("some-slug"))
    assert sdk.calls == 0


def test_get_book_and_get_book_depth_are_suppressed_for_a_producer(latch, monkeypatch):
    venue_backoff.arm(BAN, "1015", path=latch, now=None)
    sdk = _ExplodingSDK()
    c = _client(obey=True, sdk=sdk, latch_path=latch, monkeypatch=monkeypatch)
    with pytest.raises(VenueBackoff):
        asyncio.run(c.get_book("some-slug"))
    with pytest.raises(VenueBackoff):
        asyncio.run(c.get_book_depth("some-slug"))
    assert sdk.calls == 0


def test_an_expired_latch_lets_a_producer_read_again(latch, monkeypatch):
    """Self-healing: nothing restarts the producer, and nothing clears the latch."""
    venue_backoff.arm(BAN, "1015", path=latch, now=1000.0)
    with open(latch) as fh:                      # rewrite it into the past, no clock mocking
        obj = json.load(fh)
    obj["until"] = 1.0
    with open(latch, "w") as fh:
        json.dump(obj, fh)
    venue_backoff._memo = (None, None)
    sdk = _CountingSDK()
    c = _client(obey=True, sdk=sdk, latch_path=latch, monkeypatch=monkeypatch)
    assert asyncio.run(c._fetch_book("s", fresh=True)) == {"marketData": {}}
    assert sdk.calls == 1
