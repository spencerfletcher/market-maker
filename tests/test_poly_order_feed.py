"""The private-WS fill accelerator: envelope parsing, overflow policy, echo-watchdog liveness.

Liveness is designed around what a long observer recording actually showed: the venue sends NO
snapshot, NO heartbeat and NO sequence number on this feed, and a subscription dies silently
after a few hours while the socket keeps answering pings. So the socket being open proves
nothing. The only liveness signal that exists is the echo of our own order actions — every
placement came back on the feed, sub-second — so the watchdog is: every successful PLACEMENT
expects its order id back within ECHO_DEADLINE_S, and an expectation that expires means the
subscription is dead. Cancels deliberately assert nothing (their echoes are observed, but the
rate was never validated — no per-cancel timestamps, so no denominator). A cycle that placed
nothing asserts nothing: a healthy feed can go quiet for an hour, which is exactly the case
every wall-clock silence threshold gets wrong.

The maker-side seam (expect_echo at placement, check_liveness per drain) is pinned in
test_poly_maker.py.
"""

from __future__ import annotations

import asyncio
import time
from unittest import mock

import bot.poly_us.order_feed as order_feed_module
from bot.poly_us.order_feed import (
    DATA_AGE_BACKSTOP_S,
    ECHO_DEADLINE_S,
    QUEUE_MAX,
    OrderFeed,
    _SEEN_TTL_S,
    order_bodies,
)

# Verbatim shape from a recorded private-WS capture — the field names match
# the REST get_order body, which is what lets the maker book from it unchanged.
_EVENT = {
    "requestId": "probe-orders-1",
    "subscriptionType": "SUBSCRIPTION_TYPE_ORDER",
    "orderSubscriptionUpdate": {
        "execution": {
            "id": "EX1",
            "lastPx": {"value": "0.847", "currency": "USD"},
            "lastShares": 22,
            "order": {
                "id": "BJNDGF2GAB9E",
                "marketSlug": "mkt-a",
                "side": "ORDER_SIDE_BUY",
                "price": {"value": "0.847", "currency": "USD"},
                "quantity": 22,
                "cumQuantity": 22,
                "leavesQuantity": 0,
                "state": "ORDER_STATE_FILLED",
                "intent": "ORDER_INTENT_BUY_LONG",
                "commissionNotionalTotalCollected": {"value": "-0.04", "currency": "USD"},
            },
        }
    },
}


def test_order_bodies_extracts_the_recorded_envelope():
    bodies = order_bodies(_EVENT)
    assert len(bodies) == 1
    assert bodies[0]["id"] == "BJNDGF2GAB9E"
    assert bodies[0]["cumQuantity"] == 22


def test_order_bodies_returns_empty_for_everything_else():
    assert order_bodies({}) == []
    assert order_bodies({"subscriptionType": "SUBSCRIPTION_TYPE_POSITION"}) == []
    assert order_bodies({"orderSubscriptionUpdate": "not-a-dict"}) == []
    assert order_bodies({"orderSubscriptionUpdate": {"execution": {"order": {}}}}) == [], (
        "an order body without an id is unattributable and must not be surfaced")


def test_overflow_drops_the_OLDEST_and_counts_it():
    """The newest body carries the highest cumQuantity and booking is cum-based, so the newest
    alone is sufficient — and the socket callback must never block. Driven through
    _handle_message — an earlier version performed the drop-oldest policy in its own body, so
    deleting the branch under test kept it green."""
    feed = OrderFeed()
    for i in range(QUEUE_MAX + 3):
        feed._handle_message(_msg(f"O{i}"))
    assert feed.dropped == 3
    assert feed.queue.qsize() == QUEUE_MAX
    newest_kept = None
    while not feed.queue.empty():
        newest_kept = feed.queue.get_nowait()
    assert newest_kept["id"] == f"O{QUEUE_MAX + 2}", "the newest survives"


def test_data_age_still_reads_for_the_teardown_summary():
    feed = OrderFeed()
    assert feed.data_age_s() == float("inf")
    feed.last_data_ts = time.time()
    assert feed.data_age_s() < 1.0


# ── the echo watchdog ───────────────────────────────────────────────────────────────────────

def _live_feed() -> OrderFeed:
    """A feed in the connected-and-subscribed state, whose last inbound message was 20s ago —
    long enough ago that an expectation aged past the deadline post-dates it (the recorded
    death mode: total silence after the action)."""
    feed = OrderFeed()
    feed._subscribed = True
    feed._conn_established_mono = time.monotonic()
    feed.last_data_ts = time.time() - 20
    feed._last_msg_mono = time.monotonic() - 20
    return feed


def _msg(order_id: str) -> dict:
    ex = dict(_EVENT["orderSubscriptionUpdate"]["execution"])
    ex["order"] = dict(ex["order"], id=order_id)
    return {"orderSubscriptionUpdate": {"execution": ex}}


def test_the_snapshot_watchdog_is_gone():
    """The recording refuted the premise this watchdog was built on: the venue never sends a
    snapshot at all. Worse, the watchdog false-fired on most subscribes to a HEALTHY feed and
    reconnect-stormed a dead one. Its constants coming back means the design regressed."""
    assert not hasattr(order_feed_module, "RESUBSCRIBE_EVERY_S")
    assert not hasattr(order_feed_module, "SNAPSHOT_TIMEOUT_S")
    assert not hasattr(OrderFeed, "healthy"), (
        "healthy() was a wall-clock mis-specification — its silence threshold was far shorter "
        "than the longest legitimate quiet gap on a working feed; liveness is check_liveness() now")


def test_an_echo_clears_the_expectation_and_the_feed_stays_alive():
    feed = _live_feed()
    feed.expect_echo("B-PLACED")
    feed._handle_message(_msg("B-PLACED"))
    # The first cut of this test never aged the expectation, so it was green
    # with the clearing line DELETED — the exact mutation that turns the watchdog into a
    # once-per-cycle reconnect storm. Assert the clear directly, then age whatever survived
    # so the liveness call would go red too.
    assert feed._pending_echo == {}, "the echo must CLEAR the expectation, not merely coexist"
    for oid in list(feed._pending_echo):
        feed._pending_echo[oid] -= ECHO_DEADLINE_S + 1
    feed._last_msg_mono = time.monotonic() - 20   # silence since — a survivor means death
    assert feed.check_liveness() is None
    assert feed.deaths == 0


def test_an_unechoed_placement_past_the_deadline_is_a_death():
    """The single cleanest datum in the recording: the WS announced one order's birth and then
    never mentioned its death. With the echo watchdog, the next cycle's unechoed placement
    declares the subscription dead — instead of never declaring it at all."""
    feed = _live_feed()
    feed.expect_echo("B-NEVER-ECHOED")
    feed._pending_echo["B-NEVER-ECHOED"] -= ECHO_DEADLINE_S + 1
    reason = feed.check_liveness()
    assert reason is not None and "echo_timeout" in reason
    assert feed.deaths == 1
    assert not feed._pending_echo, "a declared death must not re-fire on the same entries"
    assert feed._force_reconnect.is_set(), "death must tear the connection down for the ladder"


def test_a_quiet_book_asserts_nothing_even_across_the_3213s_real_gap():
    """The longest legitimate quiet gap observed on a WORKING feed was most of an hour, and the
    old wall-clock healthy() called that dead. No placements => no assertion."""
    feed = _live_feed()
    feed.last_data_ts = time.time() - 3213
    feed._conn_established_mono = time.monotonic() - 3213
    assert feed.check_liveness() is None
    assert feed.deaths == 0


def test_an_echo_that_beats_the_rest_response_is_not_a_false_death():
    """The WS delivers in tens of milliseconds; the REST place response can lose that race. An id seen
    on the feed BEFORE expect_echo() must not pend an expectation that already arrived."""
    feed = _live_feed()
    feed._handle_message(_msg("B-FAST"))
    feed.expect_echo("B-FAST")
    assert not feed._pending_echo
    assert feed.check_liveness() is None


def test_a_stale_seen_id_does_not_absorb_a_new_expectation():
    """The TTL is applied AT THE POINT OF USE — a seen id older than _SEEN_TTL_S proves
    nothing about the subscription NOW. (Historically, a bare-membership check
    absorbed expectations registered by the since-deleted cancel leg; under placements-only
    this branch is defense-in-depth, not a live path — expect_echo runs once per id,
    immediately after the create.)"""
    feed = _live_feed()
    feed._seen_ids["B-OLD"] = time.monotonic() - (_SEEN_TTL_S + 1)
    feed.expect_echo("B-OLD")
    assert "B-OLD" in feed._pending_echo


def test_the_seen_set_is_pruned_each_check():
    """Unpruned, _seen_ids grows for the run's lifetime and every retained id is
    a permanent absorber — a blind spot in exactly the direction that hides a death."""
    feed = _live_feed()
    feed._seen_ids["B-ANCIENT"] = time.monotonic() - (_SEEN_TTL_S + 1)
    feed._seen_ids["B-RECENT"] = time.monotonic()
    feed.check_liveness()
    assert "B-ANCIENT" not in feed._seen_ids
    assert "B-RECENT" in feed._seen_ids


def test_an_unechoed_action_on_a_DELIVERING_feed_is_an_anomaly_not_a_death():
    """The recorded death mode is TOTAL silence. If messages keep arriving
    after the action but our id never shows, the likely cause is an envelope change the parser
    misses — tearing down would reconnect-storm a WORKING feed (the 1015 direction). Log,
    count, keep the connection; the REST poll books everything regardless."""
    feed = _live_feed()
    feed._pending_echo["B-UNSEEN"] = time.monotonic() - (ECHO_DEADLINE_S + 1)
    # The feed delivered AFTER the action — through _handle_message, not a hand-set
    # timestamp, so the guard's INPUT wiring is pinned too — hand-setting _last_msg_mono here
    # would leave its production write a mutation survivor.
    feed._handle_message(_msg("SOME-OTHER-ORDER"))
    assert feed.check_liveness() is None
    assert feed.deaths == 0
    assert feed.echo_anomalies == 1
    assert not feed._pending_echo, "anomalous entries must clear, not re-warn every cycle"
    assert not feed._force_reconnect.is_set()


def test_expect_echo_while_disconnected_asserts_nothing():
    """A placement made while the feed is down (reconnect ladder mid-delay) can never echo —
    its NEW event was emitted before the next subscribe, and the stream has no backfill.
    Pending it would false-kill every fresh connection."""
    feed = OrderFeed()
    assert feed._subscribed is False
    feed.expect_echo("B-WHILE-DOWN")
    assert not feed._pending_echo


def test_connection_teardown_clears_pending_so_lost_echoes_cannot_false_fire():
    feed = _live_feed()
    feed.expect_echo("B-IN-FLIGHT")
    feed._seen_ids["B-SEEN-ON-DEAD-CONN"] = time.monotonic()
    feed._on_connection_down()
    assert feed._subscribed is False
    assert not feed._pending_echo
    assert not feed._seen_ids, (
        "ids seen on the dead connection must not absorb expectations on the fresh one")
    assert feed.check_liveness() is None


def test_the_data_age_backstop_fires_after_hours_of_total_silence():
    """Belt-and-braces: observed alive durations were 1.9–4.3h, so >4h with zero messages on a
    subscribed connection is presumed dead even with no placements to echo."""
    feed = _live_feed()
    feed.last_data_ts = time.time() - (DATA_AGE_BACKSTOP_S + 60)
    feed._conn_established_mono = time.monotonic() - (DATA_AGE_BACKSTOP_S + 60)
    reason = feed.check_liveness()
    assert reason is not None and "data_age" in reason
    assert feed.deaths == 1
    assert feed._force_reconnect.is_set()


def test_a_fresh_connection_is_not_killed_by_the_backstop():
    """last_data_ts starts 0.0 (age=inf): the backstop must read the CONNECTION's age too, or
    every subscribe would die at the first check before any message arrived."""
    feed = _live_feed()
    feed.last_data_ts = 0.0
    assert feed.check_liveness() is None


class TestReconnectLadder:
    """The storm guard had no test at all. A venue rejecting or instantly dropping every fresh
    subscription must see escalating delays, not a fixed 1s — one reconnect per second with
    errors=0 forever, on an account that has already been rate-limited once, is the failure both
    guards exist for."""

    def _run_ladder(self, script: list[str]) -> list[float]:
        """script steps: 'death' (echo-death teardown), 'clean' (long-lived ordinary close),
        'instant' (subscribe accepted, closed immediately). Returns the ladder delays taken."""
        delays: list[float] = []

        async def main():
            feed = OrderFeed()
            steps = iter(script)

            async def scripted_run_once():
                try:
                    step = next(steps)
                except StopIteration:
                    feed._stopping.set()
                    return
                feed._conn_established_mono = (
                    time.monotonic() - 120 if step == "clean" else time.monotonic())
                if step == "death":
                    feed.deaths += 1

            async def fake_sleep(delay):
                delays.append(delay)

            feed._run_once = scripted_run_once
            with mock.patch.object(order_feed_module.asyncio, "sleep", fake_sleep):
                await feed._run()

        asyncio.run(main())
        return delays

    def test_consecutive_echo_deaths_walk_the_ladder(self):
        assert self._run_ladder(["death", "death", "death"]) == [1.0, 2.0, 5.0]

    def test_an_instantly_closing_venue_walks_the_ladder_too(self):
        assert self._run_ladder(["instant", "instant", "instant"]) == [1.0, 2.0, 5.0]

    def test_a_long_lived_ordinary_close_resets_the_ladder(self):
        assert self._run_ladder(["death", "clean", "death"]) == [1.0, 1.0, 2.0], (
            "the clean long-lived close must reset (second delay 1.0), and the next death "
            "must start the ladder fresh from there (third delay 2.0)")
