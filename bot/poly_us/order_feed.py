"""Private order WebSocket feed — the fill-detection ACCELERATOR.

Why this exists, measured twice: at ~95 fills/hour (2026-07-30 <run-id>) the REST read-back loop
fell 36 contracts behind the venue over 2.5h; at ~2.8 fills/MINUTE (the size-22 evening run) it
booked 15 of ~16 fills but missed ONE 22-lot ask, leaving belief +22 against a venue-certified
flat — and one missed 22-lot is a full cap of invisible exposure. (⛔ An earlier version of this
line said "booked ZERO of six fills"; the tape refutes that — 15 rows, all late_booked=N. The
defect is the unbooked tail at speed, not wholesale failure.) Polling mostly keeps up and
sometimes doesn't, and "sometimes" scales with fill rate. The venue pushes order updates over the private WS — `orderSubscriptionUpdate`
carries the full order body (`cumQuantity`, `avgPx`, the order-cumulative commission), which is
exactly the payload the maker's existing `_book_fill` books from, idempotently (cum-deltas), so
this feed can only ever make booking FASTER, never different.

⛔ ACCELERATOR, NEVER REPLACEMENT. The REST poll and the delayed verify stay exactly as they
are; a dead feed degrades to yesterday's behaviour, never to silence.

LIVENESS — the echo watchdog [redesigned 2026-07-31 against 41h of recorded evidence,
the private design notes]. Three venue facts drive the design:

  1. The stream has NO snapshot, NO heartbeat and NO sequence number — 0 of 1,419 recorded
     messages across 4 subscribes, including a reconnect with orders resting. It is strictly
     incremental-from-subscribe-time. (The SDK's `orderSubscriptionSnapshot` branches are dead
     code against this venue; the previous design waited for a snapshot that does not exist.)
  2. A subscription DIES SILENTLY every 1.9–4.3h while the socket keeps answering protocol
     pings (~32% uptime across 3 probe runs, `errors=0 connects=1` printing healthy for hours).
     No transport-level keepalive can ever see this.
  3. Our own order actions echo back reliably and fast: 60/60 recorded placements produced an
     `EXECUTION_TYPE_NEW` within 1.36s (p50 0.55s).

So the ONLY liveness proof available is the echo of actions we ourselves take: the maker calls
`expect_echo(order_id)` after each successful real PLACEMENT (placements only — cancel echoes
are observed on the tape but their RATE is unvalidated, see `expect_echo`), `_handle_message` clears the
expectation when any event for that id arrives, and `check_liveness()` (called once per quote
cycle) declares the subscription dead when an expectation outlives ECHO_DEADLINE_S — then tears
the connection down for the reconnect ladder. A cycle that placed nothing asserts nothing: the
longest legitimate quiet gap recorded on a WORKING feed was 3,213s, which defeats every
wall-clock threshold (the old `healthy()`'s 360s included). Applied to the recorded 02:27:17
death, this detects it 1–2 requote cycles after the first post-death placement (the deadline
spans a cycle boundary) — instead of never.

Because the venue kills subscriptions on a clock, the reconnect ladder is load-bearing, not
exceptional: expect ~1 reconnect every 2–4h in steady state.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from polymarket_us.websocket import PrivateWebSocket

from bot.core import config

log = logging.getLogger(__name__)

# 7× the worst observed echo latency (1.36s max over 60/60 recorded placements). An expectation
# older than this on a subscribed connection means the venue is no longer delivering our own
# events back to us — the one failure mode the socket itself can never reveal.
ECHO_DEADLINE_S = 10.0
# Belt-and-braces only: observed alive durations were 1.9–4.3h, so a subscribed connection is
# presumed dead when BOTH the last inbound message AND the connection itself are older than
# this — after a reconnect the clock restarts, so worst-case detection is 4h from establish.
DATA_AGE_BACKSTOP_S = 4 * 3600.0
# How long a seen order id can absorb a LATER expect_echo call — the WS delivers at ~81ms median
# (max 1.175s over 1,166 messages), so the echo can beat the REST place response that carries the
# id. Under PLACEMENTS-ONLY expectations, absorption is always correct: ids are unique and
# expect_echo runs once, immediately after the create, so a seen id can only mean the echo
# already arrived — a larger TTL is therefore strictly safer, and 30s covers a create round-trip
# out to client-timeout scale (observed whole-cycle wall max 3.25s). The bound exists for memory
# hygiene, applied at the point of use as defense-in-depth.
# justified by the since-deleted cancel leg and left only ~1.5× headroom over the observed max]
_SEEN_TTL_S = 30.0
# A connection must live this long for an ordinary close to reset the reconnect ladder — a venue
# accepting connect+subscribe and instantly closing would otherwise reconnect at 1s forever
# (errors=0, deaths=0) on an account with a Cloudflare 1015 already on record [mm-review r1].
# The shortest observed ALIVE duration is 1.9h ≈ 114× this, so no real connection can be
# misclassified; the failure direction is only a slower reconnect, never a missed death.
_MIN_HEALTHY_CONN_S = 60.0
RECONNECT_DELAYS_S = (1.0, 2.0, 5.0, 10.0, 30.0, 60.0)
QUEUE_MAX = 512


def order_bodies(message: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract venue order bodies from one WS message. Pure; returns [] for anything else.

    Envelope (recorded, logs/poly_private_ws.jsonl): `orderSubscriptionUpdate.execution.order`
    with the same field names as the REST `get_order` body — which is what lets the maker book
    from it unchanged."""
    upd = message.get("orderSubscriptionUpdate")
    if not isinstance(upd, dict):
        return []
    out: list[dict[str, Any]] = []
    ex = upd.get("execution")
    if isinstance(ex, dict):
        o = ex.get("order")
        if isinstance(o, dict) and o.get("id"):
            out.append(o)
    return out


class OrderFeed:
    """Owns the private WS connection; surfaces order bodies on a bounded queue.

    The consumer (the maker) drains `queue` each cycle. On overflow the OLDEST event is dropped
    with a counter — never block the socket callback, and a dropped event is only a lost
    acceleration (the REST poll still books it)."""

    def __init__(self) -> None:
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=QUEUE_MAX)
        self.events = 0
        self.dropped = 0
        self.connects = 0
        self.errors = 0
        self.deaths = 0                  # echo-watchdog / backstop kills, distinct from errors
        self.echo_anomalies = 0          # unechoed actions on a DELIVERING feed — see check_liveness
        self.last_data_ts = 0.0          # wall clock of the last inbound message, any type
        self._last_msg_mono = 0.0        # monotonic twin of last_data_ts, for ordering vs actions
        self._pending_echo: dict[str, float] = {}    # order id → monotonic ts of the action
        self._seen_ids: dict[str, float] = {}        # order id → monotonic ts last seen on WS
        self._subscribed = False
        self._conn_established_mono = 0.0
        self._force_reconnect = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()

    # ── public surface ───────────────────────────────────────────────────────────────────────
    def start(self) -> None:
        self._task = asyncio.get_running_loop().create_task(self._run(), name="order-feed")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    def data_age_s(self) -> float:
        return time.time() - self.last_data_ts if self.last_data_ts else float("inf")

    @property
    def subscribed(self) -> bool:
        return self._subscribed

    def expect_echo(self, order_id: str) -> None:
        """Register that `order_id` — a PLACEMENT we just made against the venue — must echo on
        this feed within ECHO_DEADLINE_S. The expectation is on the id, not the event type
        (a FILL clears it as well as a NEW).

        Placements only [mm-review r1+r2]: the 60/60-echoed evidence is placements → NEW.
        Cancel echoes ARE observed — r2 found 30 CANCELED echoes inside the night22b WS-alive
        window, all on ids born in the same window on the run's own slugs — but the maker logs
        no per-order cancel timestamp, so neither the echo RATE nor the echo LATENCY can be
        computed, and arming the watchdog on an unvalidated behaviour risks exactly the
        false-death reconnect storm it exists to prevent. Cancels were 0.6% of quote actions
        on the measured slate, so the detection loss is negligible; revisit once a run logs
        cancel times."""
        if not self._subscribed:
            return          # an action while the feed is down can never echo — asserts nothing
        seen_mono = self._seen_ids.get(order_id)
        if seen_mono is not None and time.monotonic() - seen_mono <= _SEEN_TTL_S:
            return          # the echo beat the REST response that carried the id (~81ms median)
        self._pending_echo[order_id] = time.monotonic()

    def check_liveness(self) -> Optional[str]:
        """Called once per maker quote cycle. Returns a death reason (having torn the
        connection down for the reconnect ladder) or None. Only ever asserts on evidence we
        ourselves created — an expected echo that never came, or hours of total silence — so a
        quiet book can never false-fire it."""
        now_mono = time.monotonic()
        for oid in [o for o, ts in self._seen_ids.items() if now_mono - ts > _SEEN_TTL_S]:
            del self._seen_ids[oid]
        if not self._subscribed:
            return None
        expired = [oid for oid, ts in self._pending_echo.items()
                   if now_mono - ts > ECHO_DEADLINE_S]
        if expired:
            oldest_action_mono = min(self._pending_echo[oid] for oid in expired)
            if self._last_msg_mono > oldest_action_mono:
                # The feed HAS delivered since the action — this is not the recorded death mode
                # (which goes totally silent), it is our parser failing to attribute an
                # envelope the venue changed. Tearing down would storm on a working feed
                # [mm-review r1]; the REST poll books everything regardless, so log loudly and
                # keep the connection.
                for oid in expired:
                    del self._pending_echo[oid]
                self.echo_anomalies += len(expired)
                log.warning(f"order feed ANOMALY: {len(expired)} action(s) unechoed past "
                            f"{ECHO_DEADLINE_S:.0f}s while the feed is delivering (first "
                            f"{expired[0]}) — envelope change? Booking still verified by the "
                            f"REST poll.")
                return None
            return self._declare_dead(
                f"echo_timeout: {len(expired)} action(s) unechoed past "
                f"{ECHO_DEADLINE_S:.0f}s (first {expired[0]})")
        conn_age_s = now_mono - self._conn_established_mono
        # `min`: BOTH the last message and the connection itself must be older than the backstop
        # — a fresh reconnect is not dead, and neither is a connection whose data is flowing.
        if min(self.data_age_s(), conn_age_s) > DATA_AGE_BACKSTOP_S:
            return self._declare_dead(
                f"data_age: nothing inbound for {self.data_age_s() / 3600:.1f}h on a "
                f"subscribed connection (backstop {DATA_AGE_BACKSTOP_S / 3600:.0f}h)")
        return None

    def _declare_dead(self, reason: str) -> str:
        self.deaths += 1
        self._pending_echo.clear()       # a declared death must not re-fire on the same entries
        self._force_reconnect.set()
        log.warning(f"order feed presumed DEAD — {reason}; reconnecting")
        return reason

    # ── inbound path ─────────────────────────────────────────────────────────────────────────
    def _handle_message(self, message: dict[str, Any]) -> None:
        self.last_data_ts = time.time()
        self._last_msg_mono = time.monotonic()
        for body in order_bodies(message):
            oid = str(body.get("id"))
            self._seen_ids[oid] = time.monotonic()
            self._pending_echo.pop(oid, None)
            self.events += 1
            try:
                self.queue.put_nowait(body)
            except asyncio.QueueFull:
                # Drop the OLDEST — the newest body carries the highest cum, and booking is
                # cum-based so the newest alone is sufficient; the poll verifies the rest.
                try:
                    self.queue.get_nowait()
                    self.dropped += 1
                    self.queue.put_nowait(body)
                except asyncio.QueueEmpty:
                    pass

    def _handle_error(self, exc: Exception) -> None:
        self.errors += 1
        log.warning(f"order feed ws error: {type(exc).__name__}: {str(exc)[:120]}")

    def _on_connection_down(self) -> None:
        """Echoes in flight died with the connection (the stream has no backfill), so pending
        expectations must not survive into the next connection as instant false deaths — and
        ids SEEN on the dead connection must not absorb expectations on the fresh one, or a
        stale seen-entry hides a real death for up to the TTL [mm-review r1].

        `_last_msg_mono` is deliberately NOT cleared: a stale value necessarily predates any
        action taken on the next connection, so it can never wrongly rescue an echo-death —
        monotonic ordering makes the carry-over safe [mm-review r2]."""
        self._subscribed = False
        self._pending_echo.clear()
        self._seen_ids.clear()
        self._force_reconnect.clear()

    # ── connection lifetime ──────────────────────────────────────────────────────────────────
    async def _run(self) -> None:
        delay_i = 0
        while not self._stopping.is_set():
            deaths_before = self.deaths
            try:
                await self._run_once()
                lived_s = time.monotonic() - self._conn_established_mono
                if self.deaths == deaths_before and lived_s >= _MIN_HEALTHY_CONN_S:
                    delay_i = 0          # an ordinary close of a LONG-LIVED connection resets
                # Neither an echo-death nor an instant close resets the ladder: if the venue is
                # rejecting or immediately dropping every fresh subscription, resetting would
                # turn the requote cadence into a reconnect storm on an account that has
                # already been Cloudflare-1015'd.
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.errors += 1
                log.warning(f"order feed connection died ({type(exc).__name__}: "
                            f"{str(exc)[:120]}) — reconnecting")
            if self._stopping.is_set():
                return
            delay = RECONNECT_DELAYS_S[min(delay_i, len(RECONNECT_DELAYS_S) - 1)]
            delay_i += 1
            await asyncio.sleep(delay)

    async def _run_once(self) -> None:
        ws = PrivateWebSocket(key_id=config.POLYMARKET_US_KEY_ID,
                              secret_key=config.POLYMARKET_US_SECRET_KEY)
        closed = asyncio.Event()
        ws.on("message", self._handle_message)
        ws.on("error", self._handle_error)
        ws.on("close", lambda: closed.set())

        await ws.connect()
        self.connects += 1
        try:
            # ONE subscribe per connection, and nothing to wait for after it: the venue answers
            # with no snapshot, no heartbeat, no sequence number (0 of 1,419 recorded messages),
            # so there is no in-band liveness to poll. Liveness is the echo watchdog above,
            # driven by the maker's own order actions via check_liveness().
            await ws.subscribe_orders(f"maker-orders-{self.connects}")
            self._conn_established_mono = time.monotonic()
            self._subscribed = True
            while not (closed.is_set() or self._stopping.is_set()
                       or self._force_reconnect.is_set()):
                await asyncio.sleep(0.25)
        finally:
            self._on_connection_down()
            try:
                await ws.close()
            except Exception:
                pass
