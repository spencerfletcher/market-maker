"""
bot/poly_us_feed.py
───────────────────
Real-time Polymarket US order-book cache via the SDK WebSocket (ws.markets).
Interface: get_best_ask / get_age / prime / run_forever. Prices are keyed by
market-side slug (the identifier MarketPair carries).

Book frames arrive as `market_data` (payload `marketData`) with `offers`/`bids`
levels of `{"px": {"value": str, ...}, "qty": str}`; the lite channel carries
best bid/ask and no depth.
"""
from __future__ import annotations

import asyncio
import base64
import json
import time
import uuid
from typing import Any, Callable, Optional

from bot.core import config
from bot.core.feed_health import _stale_book_reconnect
from bot.core.logger import get_logger
from bot.core.ws_timing import ws_timer
from bot.poly_us.sides import parse_token

log = get_logger(__name__)

# Raw markets WebSocket (used when config.POLY_US_FEED_SOURCE == "raw").
_WS_BASE = "wss://api.polymarket.us"
_WS_PATH = "/v1/ws/markets"
_WS_URL = _WS_BASE + _WS_PATH
# Full order book carries best ask + per-level share depth (lite carries no depth).
_SUB_TYPE_MARKET_DATA = "SUBSCRIPTION_TYPE_MARKET_DATA"
# Poly DOES publish a per-print trade channel, but the bot prices off the book, so it stays OPT-IN
# via `subscribe_trades_too` and OFF by default. It exists for the MEASUREMENT path: the markout
# probe otherwise infers trades from `stats.lastTradeSetTime`, which records only the LAST print of
# a burst and so undercounts prints AND selectively drops the adverse ones.
_SUB_TYPE_TRADE = "SUBSCRIPTION_TYPE_TRADE"

#: Minimum seconds between two reject-driven reconnects. A subscribe reject means the socket's
#: per-connection request budget is spent (2026-09-12: 10 requests accepted, the 11th
#: rejected), and only a fresh connection refills it — the reconnect resubscribes the whole
#: tracked set in ONE request. Rate-limited so a venue rejecting for any OTHER reason cannot
#: turn the feed into a reconnect loop: a second reject inside the window is left as-is
#: (`subscribe_rejected` names the slugs and the consumer refuses the add).
RECONNECT_ON_REJECT_MIN_S = 300.0


def _level_px_qty(lvl: dict) -> tuple[float, float] | None:
    """Parse ONE order-book level → (price, qty), or None if it doesn't parse.

    A level is {"px": {"value": str, "currency": "USD"}, "qty": str}. A garbage frame can put the
    currency code where the number belongs, so parse DEFENSIVELY per level and let the caller SKIP a
    bad level rather than discard the whole frame (which would freeze that market's cache)."""
    try:
        px = lvl["px"]
        price = float(px["value"] if isinstance(px, dict) else px)
        return price, float(lvl["qty"])
    except (KeyError, TypeError, ValueError):
        return None


class PolyUSOrderBookCache:
    # ── Two SEPARATE liveness concerns — do not conflate ────────────────────────────────────
    # 1) CONNECTION liveness → the websockets library's ping/pong: a dead socket closes in ~40s →
    #    ConnectionClosed → reconnect. The Poly MARKET channel accepts the 20s library ping.
    # 2) DATA freshness / ZOMBIE → a socket-wide watchdog on REAL book changes, NOT on frames: a
    #    frozen feed keeps ping/pong healthy while the tradeable book never moves. 180s is calibrated
    #    for the QUIET-SLATE false-positive floor, so it is slow-for-games BY DESIGN — tolerable only
    #    because the fire path re-checks vs fresh REST before firing.
    _FRESHNESS_RECONNECT_S = 180        # no real book change across all slugs for this long → reconnect
    _FRESHNESS_RECONNECT_COOLDOWN_S = 60  # min gap between forced reconnects (anti-storm)
    _FRESHNESS_POLL_S = 30              # recv() wakes this often to run the watchdog on a quiet socket
    _PRUNE_AFTER_ABSENT_CYCLES = 2     # prune a slug only after it's been absent from discovery this long
                                       # (debounce vs a transient scanner error cold-pruning live markets)

    def __init__(self, sdk: Any, *, source_ip: Optional[str] = None) -> None:
        self._sdk = sdk
        #: The LOCAL address the WebSocket binds to; `None` takes `config.POLY_SOURCE_IP`, blank
        #: is the default route. The socket is not metered (a subscription is one connection, not
        #: a request stream) — it binds so that a collector moved to the secondary IP keeps ALL of
        #: its venue traffic off the maker's address (the private design notes § Second IP for collectors).
        self._source_ip = config.POLY_SOURCE_IP if source_ip is None else str(source_ip).strip()
        # ⛔ ONLY THE RAW PATH CAN BIND. The SDK WebSocket (`POLY_US_FEED_SOURCE=sdk`) opens its own
        # socket with no local-address argument, so it would silently leave from the primary IP
        # while the operator believed this process was off it. Refuse instead of half-binding.
        if self._source_ip and config.POLY_US_FEED_SOURCE == "sdk":
            raise ValueError(
                f"REFUSING: POLY_SOURCE_IP={self._source_ip} with POLY_US_FEED_SOURCE=sdk — the "
                f"SDK WebSocket cannot bind a source address; use the raw feed (unset "
                f"POLY_US_FEED_SOURCE) or clear POLY_SOURCE_IP for this unit")
        # slug -> (ask, ask_depth, bid, bid_depth, ts). Bid fields drive the
        # moneyline SHORT side (short ask = 1 − bid; see get_best_ask / sides.py).
        self._prices: dict[str, tuple[float, float, float, float, float]] = {}
        # slug -> last server-side book transactTime (ISO-8601 ns string). CONTENT freshness, which
        # `_prices`'s `ts` (local receipt, refreshed on every frozen re-send) is NOT: a frozen origin
        # re-sends the SAME stale transactTime, so this ages while `ts`/get_age stay fresh.
        # [VERIFIED 2026-07-14, poly_book_freshness_probe — LAST-MUTATION time, not a serve clock:
        # 0/78 advance on a byte-identical quiet book, 2/53 on a hot one at ratios 0.59/3.27 vs the
        # read gap. SCOPE: steady state only.] ✅ Present on a live WS `marketData` frame 2026-08-12,
        # so count_stale_books is LIVE on WS, not inert. Last-known kept when a frame omits it.
        self._transact_times: dict[str, str] = {}
        # slug -> (raw stats dict, market state str) from the full market_data frame. RAW, not parsed:
        # `parse_book_stats` computes *SetTime ages against read-time `now`, so parsing is at read.
        self._stats: dict[str, dict] = {}
        self._states: dict[str, str] = {}
        # slug -> {"offers": [...], "bids": [...]}: the RAW `{"px": {"value": str}, "qty": str}`
        # levels, EXACT wire strings — NOT the floats `_prices` stores — so the maker's crossing guard
        # reads `Decimal(str(wire))` identically to a REST book (a float→str reconstruction would
        # launder binary error into the tick-tie cases the guard catches). Bounded to the TOUCH price.
        self._touch_raw: dict[str, dict] = {}
        self.subscriptions_ready = False
        self._on_update_callback: Optional[Callable[[], None]] = None
        self._ws: Any = None  # live MarketsWebSocket (SDK) while connected, else None
        # OPT-IN (raw transport only): also subscribe the per-print TRADE channel on this socket. OFF
        # by default so the arb feed is byte-for-byte unchanged; set it BEFORE run_forever().
        self.subscribe_trades_too: bool = False
        # OPT-IN (raw transport only): on a subscribe REJECT, bounce the socket so the fresh
        # connection's request budget is refilled (see RECONNECT_ON_REJECT_MIN_S). OFF by default:
        # a COLLECTOR asking for more slugs than one connection holds is rejected on every socket,
        # and a flapping socket every 300 s is worse for it than a short subscription list. The
        # MAKER sets it True (scripts/poly_live_mm.py) — its whole set fits, the rejects are budget.
        self.reconnect_on_reject: bool = False
        # OPT-IN (raw transport only): restrict the market_data (book) subscription to this subset;
        # the trade channel still covers everything. The venue caps subs PER CONNECTION (~30-36k).
        self.book_slugs: Optional[set[str]] = None
        # Count of venue error frames mentioning subscriptions ('max subscriptions per connection
        # reached'). Nonzero = some slugs we asked for are NOT watched and nothing else will say so.
        self.subscribe_reject_count: int = 0
        # raw transport: the slugs the venue REJECTED on the current socket, by name. A reject
        # frame carries only our requestId, so `_sub_requests` maps each book subscribe we sent
        # to its slugs until the venue answers. Both cleared with `_subscribed` on reconnect.
        # A consumer (the maker's staged add) reads this to refuse a book the socket will never
        # frame, instead of graduating it to REST as if the feed were merely slow [2026-09-11].
        self.subscribe_rejected: set[str] = set()
        self._sub_requests: dict[str, list[str]] = {}
        # OPT-IN trade sink: called with the raw Trade frame for each print, ONLY when set. Defaults
        # None so the book path — and every live maker — is byte-for-byte unchanged.
        self.on_trade: Optional[Callable[[dict[str, Any]], None]] = None
        # Raw marketData frame tap (same contract as on_trade: verbatim parsed frame, fired
        # from _dispatch AFTER cache processing). Measurement-only — see _dispatch comment.
        self.on_book: Optional[Callable[[dict[str, Any]], None]] = None
        # PRE-routing tap: every frame kind, fired FIRST in _dispatch before any cache work. Both
        # stamps sit after json.loads, so ~tens of µs of parse differential remains — not zero.
        self.on_frame: Optional[Callable[[dict[str, Any]], None]] = None
        # Per-tap escaped-exception counters (taps are wrapped; see _safe_tap). A measurement
        # consumer MUST health-gate on this — errors here are silent frame loss [prereg r5 B5].
        self.tap_errors: dict[str, int] = {}
        # Reconnect gap records [(disconnect_mono, resubscribe_complete_mono)] — OPT-IN. Frames
        # between the two stamps were LOST, so a sampler excludes any window overlapping a gap.
        self.gap_log: Optional[list[tuple[float, float]]] = None
        self._gap_started_mono: Optional[float] = None
        self._raw_ws: Any = None  # live raw websocket connection, else None
        # raw transport: slugs already subscribed on the CURRENT socket (the server rejects a
        # re-subscribe); only the delta is sent. MUST be cleared on every reconnect, else after a
        # drop we would never re-subscribe and the feed would silently go dark.
        self._subscribed: set[str] = set()
        # Discovery-membership pruning: cycles a tracked slug has been ABSENT from discovery. Absent
        # for _PRUNE_AFTER_ABSENT_CYCLES → pruned; reset on return, so a 1-cycle flicker never prunes.
        self._absent_cycles: dict[str, int] = {}
        # Active-game-window gate for the freshness watchdog (set each discovery by the runner).
        # None = not computed yet (startup) → ARMED. See _books_should_move.
        self._game_windows: list[tuple[float, float]] | None = None
        self._windows_unknown: bool = False
        self._last_msg_ts: float = 0.0  # wall-clock of last WS message (any slug)
        # ⛔ SEPARATE from `_last_msg_ts` ON PURPOSE. Both transports SEED `_last_msg_ts = now` at
        # connect for the watchdog's warm-up, and a seed is a CLOCK RESET, not a frame.
        # `last_frame_age` is consumed as EVIDENCE, so reading that seed would report a
        # freshly-reconnected, still-silent socket as "delivering". Bumped ONLY by real frames.
        self._last_frame_ts: float = 0.0
        # CONTENT freshness (distinct from socket liveness). A frozen feed delivers heartbeats —
        # _last_msg_ts stays current — while the book never changes: the phantom-edge disguise.
        self._book_changes: int = 0          # book changes since the last health log
        self._last_book_change_ts: float = 0.0
        self._last_forced_reconnect: float = 0.0  # last freshness-watchdog reconnect (anti-storm)
        # Reject-driven reconnect (raw transport): set by _dispatch, consumed by the recv loop —
        # the SAME `break` a stale-book reconnect takes, never a second reconnect mechanism.
        self._reject_reconnect_due: bool = False
        self._last_reject_reconnect: float = 0.0
        self._last_health_log: float = 0.0

    def set_callback(self, callback: Callable[[], None]) -> None:
        """Register a callable fired on every meaningful price update (so US mode gets
        WS-speed arb detection)."""
        self._on_update_callback = callback

    def prime(self, token: str, ask: float) -> None:
        # Accept a bare slug or a "<slug>::short" token — both key off the bare slug.
        slug, _is_short = parse_token(token)
        if ask and ask > 0:
            # depth/bid unknown until the WS book arrives.
            self._prices[slug] = (ask, 0.0, 0.0, 0.0, time.time())

    def forget(self, slug: str) -> None:
        """Drop ONE slug from every per-slug map and the subscribed/rejected sets — the maker's
        slate DROP calls this so a later `resubscribe()` never re-sends a book nobody quotes
        (each re-send re-earns a reject on a socket at the per-IP ceiling). No WS unsubscribe."""
        self._prices.pop(slug, None)
        self._transact_times.pop(slug, None)
        self._stats.pop(slug, None)
        self._states.pop(slug, None)
        self._touch_raw.pop(slug, None)
        self._absent_cycles.pop(slug, None)
        self._subscribed.discard(slug)
        self.subscribe_rejected.discard(slug)

    def retain_slugs(self, live: set[str]) -> None:
        """Prune per-slug state for markets NO LONGER discovered (closed/delisted), so the tracked set
        follows the live slate — closed markets never update and would serve stale prices forever.

        Prune on DISCOVERY MEMBERSHIP, NEVER on staleness: a still-discovered market is kept even if
        momentarily quiet, and only a slug ABSENT for `_PRUNE_AFTER_ABSENT_CYCLES` CONSECUTIVE cycles
        is pruned. `live` MUST be keyed identically to `_prices` (BARE slugs); otherwise no key
        matches and EVERYTHING is pruned. Sends no WS unsubscribe."""
        for slug in list(self._prices):
            if slug in live:
                self._absent_cycles.pop(slug, None)            # discovered → reset the absence count
            else:
                n = self._absent_cycles.get(slug, 0) + 1
                if n >= self._PRUNE_AFTER_ABSENT_CYCLES:
                    self._prices.pop(slug, None)
                    self._transact_times.pop(slug, None)
                    self._stats.pop(slug, None)
                    self._states.pop(slug, None)
                    self._touch_raw.pop(slug, None)
                    self._subscribed.discard(slug)
                    self._absent_cycles.pop(slug, None)
                else:
                    self._absent_cycles[slug] = n              # absent but under the debounce → keep

    def set_game_windows(self, windows: list[tuple[float, float]], unknown: bool) -> None:
        """Push this discovery's active-game windows (epoch start/end) + the `unknown`-timing flag for
        the watchdog gate. Refreshed each discovery (~5min), evaluated LIVE at each poll (~30s)."""
        self._game_windows = windows
        self._windows_unknown = unknown

    def _books_should_move(self, now: float) -> bool:
        """Active-game-window gate for the freshness watchdog. True (ARMED) UNLESS we can CONFIDENTLY
        say no game is in-window — suppressing a real freeze DURING a game is the dangerous direction:
          • windows never computed (startup, None) → True;
          • any game's timing unreadable (`_windows_unknown`) → True;
          • else armed iff some game's window contains `now` (inclusive both ends)."""
        if self._game_windows is None:
            return True
        if self._windows_unknown:
            return True
        return any(start <= now <= end for start, end in self._game_windows)

    @property
    def connected(self) -> bool:
        """Whole-connection liveness for the WS-maker's fallback→halt guard: True while a socket (raw
        or SDK) is open. Both transports None their handle on disconnect, so this is read-only truth."""
        return self._raw_ws is not None or self._ws is not None

    def last_frame_age(self, now: Optional[float] = None) -> Optional[float]:
        """Seconds since ANY frame arrived on this socket — book, lite, or heartbeat.
        `None` = no frame has ever arrived; the caller MUST read None as "unknown", never "fresh".

        ⛔ WHAT THIS PROVES, EXACTLY: that the venue PUBLISHER is alive and delivering on THIS
        connection. It does NOT prove any particular book's stream is being served — there is no
        per-book sequence number and the delta channel does not re-send an unchanged book, so a book
        we have been silently unsubscribed from is indistinguishable from one nobody is trading; that
        is why the maker pairs this with a periodic fresh-REST re-verify per book.

        ⚠️ TRANSPORT-DEPENDENT: on the raw transport every dispatched message bumps this, heartbeats
        included; on the SDK transport only the `market_data*` callbacks do, so it degenerates to
        "age of the newest book frame ACROSS THE SLATE" and goes inert on an ALL-QUIET slate (safe
        direction, but silent). Protocol-level ping/pong never reaches `_dispatch`.

        ⛔ Reads `_last_frame_ts`, NOT `_last_msg_ts`, which is SEEDED at connect — see `__init__`."""
        if not self._last_frame_ts:
            return None
        return max(0.0, (time.time() if now is None else now) - self._last_frame_ts)

    def get_book_md(self, slug: str) -> Optional[dict]:
        """A `marketData`-shaped dict for `slug` from the retained book — the read the WS BookSource
        seam hands the maker so `touch_from_md` / `qty_at_price` / `parse_book_stats` work with ZERO
        maker-side change. None when there is no book yet (treated like a failed REST read).
        ⛔ EXACT WIRE STRINGS, not reconstructed from floats, so the crossing guard reads
        `Decimal(str(wire))` identically to a REST book. Bounded to the touch level per side."""
        raw = self._touch_raw.get(slug)
        if raw is None:
            return None
        md: dict[str, Any] = {"marketSlug": slug}
        if raw.get("offers"):
            md["offers"] = raw["offers"]
        if raw.get("bids"):
            md["bids"] = raw["bids"]
        tt = self._transact_times.get(slug)
        if tt:
            md["transactTime"] = tt
        st = self._stats.get(slug)
        if st is not None:
            md["stats"] = st
        state = self._states.get(slug)
        if state:
            md["state"] = state
        return md

    def get_best_ask(self, token: str) -> Optional[float]:
        """Best ask for a token. Long side → bestAsk; short side → 1 − bestBid
        (None if that side has no quote)."""
        slug, is_short = parse_token(token)
        rec = self._prices.get(slug)
        if not rec:
            return None
        if is_short:
            bid = rec[2]
            return (1.0 - bid) if bid > 0 else None
        return rec[0]

    def get_depth(self, token: str) -> Optional[float]:
        """Shares available at the current best price for this side, or None."""
        slug, is_short = parse_token(token)
        rec = self._prices.get(slug)
        if not rec:
            return None
        return rec[3] if is_short else rec[1]

    def get_best_bid(self, token: str) -> Optional[float]:
        """Best bid you'd SELL into when flattening this side. Long → bestBid; short → 1 − bestAsk
        (selling the short crosses the long offers). None if that side has no quote."""
        slug, is_short = parse_token(token)
        rec = self._prices.get(slug)
        if not rec:
            return None
        if is_short:
            ask = rec[0]
            return (1.0 - ask) if ask > 0 else None
        bid = rec[2]
        return bid if bid > 0 else None

    def get_age(self, token: str) -> Optional[float]:
        slug, _is_short = parse_token(token)
        rec = self._prices.get(slug)
        return (time.time() - rec[4]) if rec else None

    def count_stale_books(self, now: float, max_age: float, *,
                          exclude_token: Optional[str] = None,
                          in_window: Optional[set[str]] = None) -> int:
        """Number of OTHER IN-WINDOW markets whose last server transactTime is older than `max_age`
        — the CROSS-MARKET origin-freeze signal. An origin-side freeze stalls many LIVE markets'
        transactTime at the same instant; a single illiquid book stale while neighbors tick does NOT.
        Counts ONLY confirmable staleness (transactTime present AND aged past `max_age`).

        `in_window` (bare slugs, from common.in_window_slugs) scopes the peer set to markets whose
        game is LIVE now — load-bearing, since unscoped an UPCOMING or just-FINISHED game counts as a
        stale peer and falsely confirms a freeze. ⚠️ `in_window=None` = UNSCOPED exists ONLY for the
        raw-mechanic unit tests; NEVER pass None from a decision path. An EMPTY set → 0 peers → no
        reject (fail toward firing). `exclude_token` is the firing market itself."""
        from bot.poly_us.client import transact_age_s   # lazy: keep client import off feed load
        exclude_slug = parse_token(exclude_token)[0] if exclude_token else None
        n = 0
        for slug, tt in self._transact_times.items():
            if slug == exclude_slug:
                continue
            # in_window is None ONLY in the raw-mechanic unit tests (unscoped = pre-fix full slate).
            # The fire path always passes the scoped set, so this filter is live in production.
            if in_window is not None and slug not in in_window:
                continue   # out-of-window (pre-game / finished) → legitimately quiet, not freeze evidence
            age = transact_age_s(tt, now)
            if age is not None and age > max_age:
                n += 1
        return n

    def _note_book(self, slug: str, ask: float, depth: float,
                   bid: float, bid_depth: float) -> None:
        """Store a slug's book, counting it as a CHANGE only when ask/depth/bid actually move
        (identical re-sends are not changes). The ts still refreshes on every message."""
        old = self._prices.get(slug)
        if old is None or old[:4] != (ask, depth, bid, bid_depth):
            self._book_changes += 1
            self._last_book_change_ts = time.time()
        self._prices[slug] = (ask, depth, bid, bid_depth, time.time())

    def _maybe_log_health(self) -> None:
        """Every ~60s, log feed CONTENT health: real book changes and time since the last one. A
        heartbeat-only socket shows '0 changes', so the WARNING is gated on _books_should_move."""
        now = time.time()
        if now - self._last_health_log < 60.0:
            return
        self._last_health_log = now
        changes, self._book_changes = self._book_changes, 0
        if self._last_book_change_ts:
            last = f"{now - self._last_book_change_ts:.0f}s ago"
        else:
            last = "never"
        msg = (f"poly_us_feed: book health — {changes} change(s)/60s, last change {last}, "
               f"{len(self._prices)} slugs tracked")
        # Quiet when healthy (DEBUG), loud only on a real freeze: the book never sits at 0 changes/60s
        # while live (~95/min floor), so in-window 0 = content frozen. Off-hours quiet stays DEBUG.
        if changes == 0 and self._books_should_move(now):
            log.warning(msg + " ⚠️ no book changes — possible frozen feed")
        else:
            log.debug(msg)

    def _on_market_data_lite(self, message: dict[str, Any]) -> None:
        """Handle a marketDataLite event (no depth — kept for completeness; we
        subscribe to full market_data, but register both handlers)."""
        _now = time.time()
        self._last_msg_ts = _now         # any message proves the socket is alive
        self._last_frame_ts = _now       # a REAL frame — see last_frame_age
        try:
            payload = message.get("marketDataLite", {})
            slug: str | None = payload.get("marketSlug")
            best_ask_raw = payload.get("bestAsk")
            if not slug or not best_ask_raw:
                return
            ask = float(best_ask_raw["value"])
            # bestBid (if present) prices the moneyline short side; lite carries no depth.
            best_bid_raw = payload.get("bestBid")
            bid = float(best_bid_raw["value"]) if best_bid_raw else 0.0
            if ask > 0:
                self._note_book(slug, ask, 0.0, bid, 0.0)
                if self._on_update_callback:
                    self._on_update_callback()
        except Exception as e:
            log.warning(f"poly_us_feed: error parsing market_data_lite: {e}")

    def _on_market_data(self, message: dict[str, Any]) -> None:
        """Handle a full marketData order-book event (snapshot/update). Captures
        both the best ask and the share quantity available at it (depth)."""
        _now = time.time()
        self._last_msg_ts = _now         # any message proves the socket is alive
        self._last_frame_ts = _now       # a REAL frame — see last_frame_age
        try:
            payload = message.get("marketData", {})
            slug: str | None = payload.get("marketSlug")
            offers: list[dict[str, Any]] = payload.get("offers", [])
            if not slug:
                return
            if not offers:
                # An ask-EMPTY frame is a real book state, not a skippable one: a taker clearing the
                # whole ask side produces exactly this frame, and dropping it left get_book_md serving
                # the stale PRE-sweep touch — the worst adverse event, invisible. Retain the touch so
                # md readers see the sweep; PRICING state (_prices) keeps last-known, because its
                # readers gate on age and an ask=0.0 sentinel would poison every mid built on it.
                bid_levels = [pq for pq in (_level_px_qty(l) for l in payload.get("bids", []))
                              if pq is not None]
                bid = max(p for p, _ in bid_levels) if bid_levels else 0.0
                self._touch_raw[slug] = {
                    "offers": [],
                    "bids": [lv for lv in payload.get("bids", [])
                             if (pq := _level_px_qty(lv)) is not None and pq[0] == bid],
                    "offers_key_absent": "offers" not in payload,
                }
                tt = payload.get("transactTime")
                if tt:
                    self._transact_times[slug] = tt
                state = payload.get("state")
                if state:
                    self._states[slug] = state
                if self._on_update_callback:
                    self._on_update_callback()
                return
            # Offers are sorted lowest-to-highest; best ask = lowest level, depth = shares at it.
            # Parse DEFENSIVELY (see _level_px_qty): one malformed level is skipped, never allowed to
            # discard the frame; if NOTHING parses, keep the last book.
            ask_levels = [pq for pq in (_level_px_qty(l) for l in offers) if pq is not None]
            if not ask_levels:
                log.warning(f"poly_us_feed: market_data for {slug} had no parseable offer "
                            f"levels — sample={offers[:2]!r}")
                return
            ask = min(p for p, _ in ask_levels)
            depth = sum(q for p, q in ask_levels if p == ask)
            # Best bid + its depth price the moneyline SHORT side (short ask =
            # 1 − bid; short depth = shares bid at it). Absent for one-sided books.
            bid_levels = [pq for pq in (_level_px_qty(l) for l in payload.get("bids", []))
                          if pq is not None]
            if bid_levels:
                bid = max(p for p, _ in bid_levels)
                bid_depth = sum(q for p, q in bid_levels if p == bid)
            else:
                bid = bid_depth = 0.0
            if ask > 0:
                self._note_book(slug, ask, depth, bid, bid_depth)
                # Capture the server-side content-freshness stamp (last-known kept if absent on
                # this frame). Only the cross-market freeze gate reads it; never alters firing here.
                tt = payload.get("transactTime")
                if tt:
                    self._transact_times[slug] = tt
                # Retain stats (raw) + market state for the WS-maker book source [2026-08-12].
                st = payload.get("stats")
                if isinstance(st, dict):
                    self._stats[slug] = st
                state = payload.get("state")
                if state:
                    self._states[slug] = state
                # Retain the RAW touch levels (exact wire strings) — match on the parsed float
                # price to the touch we just derived; the raw dicts carry the unlaundered value.
                self._touch_raw[slug] = {
                    "offers": [lv for lv in offers
                               if (pq := _level_px_qty(lv)) is not None and pq[0] == ask],
                    "bids": [lv for lv in payload.get("bids", [])
                             if (pq := _level_px_qty(lv)) is not None and pq[0] == bid],
                }
                if self._on_update_callback:
                    self._on_update_callback()
        except Exception as e:
            log.warning(f"poly_us_feed: error parsing market_data: {e} — "
                        f"payload={str(message.get('marketData', {}))[:400]!r}")

    def _dispatch(self, message: dict[str, Any]) -> None:
        """Route a parsed raw-WS message by its top-level key. Every message (heartbeat included)
        bumps BOTH clocks: `_last_msg_ts` for the stale-reconnect watchdog, `_last_frame_ts` for
        `last_frame_age`'s consumers — only the latter is evidence, since it is never seeded."""
        _now = time.time()
        self._last_msg_ts = _now
        self._last_frame_ts = _now
        if self.on_frame is not None:
            # PRE-routing verbatim tap: fires BEFORE any cache processing, so a consumer's arrival
            # stamps carry no parse-cost skew between trade and book frames.
            self._safe_tap(self.on_frame, message, "on_frame")
        if "marketData" in message:
            self._on_market_data(message)
        elif "marketDataLite" in message:
            self._on_market_data_lite(message)
        elif "heartbeat" in message:
            pass  # liveness already recorded above
        elif "error" in message:
            # ACK observation: a subscribe reject means slugs we believe watched are NOT —
            # `_subscribed` records the ASK. This counter is what a measurement host health-gates on.
            err_txt = str(message.get("error") or "")
            if "subscription" in err_txt.lower():
                self.subscribe_reject_count += 1
                # Un-subscribe the ASK: `_subscribed` is the dedup set, so leaving the slugs in
                # it means no later resubscribe ever re-sends them.
                rejected = self._sub_requests.pop(str(message.get("requestId") or ""), [])
                self._subscribed.difference_update(rejected)
                self.subscribe_rejected.update(rejected)
                # SELF-HEAL: the budget is per CONNECTION, so ask the recv loop to bounce the
                # socket — the reconnect resubscribes every tracked slug in one request and the
                # reject set clears. Rate-limited to one per RECONNECT_ON_REJECT_MIN_S; a second
                # reject inside the window leaves the refusal to the consumer.
                if (self.reconnect_on_reject
                        and _now - self._last_reject_reconnect >= RECONNECT_ON_REJECT_MIN_S):
                    self._last_reject_reconnect = _now
                    self._reject_reconnect_due = True
                    log.warning(
                        f"poly_us_feed: subscribe rejected for {len(rejected)} slug(s) — the "
                        f"socket's request budget is spent; reconnecting to refill it "
                        f"(≤1 per {RECONNECT_ON_REJECT_MIN_S:.0f}s)")
            log.warning(
                f"poly_us_feed: WS error message: {message.get('error')!r} "
                f"(requestId={message.get('requestId')})"
            )
        elif "trade" in message:
            # Per-print trade frame. The book path has no use for it; a MEASUREMENT consumer
            # (the flow probe) taps it via on_trade. No-op unless a sink is registered.
            if self.on_trade is not None:
                self._safe_tap(self.on_trade, message, "on_trade")
        if "marketData" in message and self.on_book is not None:
            # Raw-frame book tap for MEASUREMENT consumers, mirroring on_trade: the VERBATIM frame
            # after cache processing, so the consumer sees wire truth — offers present vs [] vs
            # key-absent, which get_book_md cannot serve.
            self._safe_tap(self.on_book, message, "on_book")
        # other unknown message types: ignored (we only price off the book)

    def _safe_tap(self, sink: Callable[[dict[str, Any]], None], message: dict[str, Any],
                  name: str) -> None:
        """Run a measurement tap without letting its exception escape _dispatch — an escaped exception
        forces a reconnect and a frame GAP, the data loss the sampler's gap rule exists to avoid
        manufacturing. Errors are counted per tap; the feed never dies for a tap."""
        try:
            sink(message)
        except Exception as e:
            n = self.tap_errors[name] = self.tap_errors.get(name, 0) + 1
            # Rate-limited: a systematically-raising tap at 14k-slug frame rates would log per frame
            # ON the receive path. First 5, then every 1000th; the COUNTER is the health gate.
            if n <= 5 or n % 1000 == 0:
                log.warning(f"poly_us_feed: {name} tap raised (#{n}): {e!r}")

    def _auth_headers(self) -> dict[str, str]:
        """Ed25519-signed handshake headers for the markets WS. Signs `timestamp_ms + "GET" + path`
        with the base64 Ed25519 secret key; a 64-byte key is seed||pubkey, so use the first 32.
        """
        # lazy: only needed in raw mode. Same primitive the Kalshi client signs with
        # [bot/kalshi/client.py:_sign]; `from_private_bytes` takes the raw 32-byte seed.
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

        ts = str(int(time.time() * 1000))
        seed = base64.b64decode(config.POLYMARKET_US_SECRET_KEY)
        if len(seed) == 64:
            seed = seed[:32]
        sig = Ed25519PrivateKey.from_private_bytes(seed).sign(f"{ts}GET{_WS_PATH}".encode())
        return {
            "X-PM-Access-Key": config.POLYMARKET_US_KEY_ID,
            "X-PM-Timestamp": ts,
            "X-PM-Signature": base64.b64encode(sig).decode(),
        }

    def _subscribe_payload(self, slugs: list[str],
                           sub_type: str = _SUB_TYPE_MARKET_DATA) -> dict[str, Any]:
        """Build a subscribe request for the given slugs. Defaults to the full order book; pass
        `_SUB_TYPE_TRADE` for the per-print trade channel (opt-in, see `subscribe_trades_too`)."""
        return {
            "subscribe": {
                "requestId": str(uuid.uuid4()),
                "subscriptionType": sub_type,
                "marketSlugs": slugs,
            }
        }

    async def run_forever(self) -> None:
        """Maintain the live price feed, dispatching to the configured transport."""
        if not config.poly_creds_present():
            log.info(config.POLY_CREDS_MISSING_LOG)
            return
        if config.POLY_US_FEED_SOURCE == "raw":
            await self._run_forever_raw()
        else:
            await self._run_forever_sdk()

    async def _run_forever_raw(self) -> None:
        """Own raw-JSON markets WebSocket. Two separated liveness layers: the library's ping/pong
        closes a dead SOCKET (~40s → reconnect), and the DATA-freshness watchdog reconnects a ZOMBIE
        (socket alive, book frozen). recv() uses a short poll timeout only to run that watchdog."""
        import websockets  # lazy: keep import cost off non-US startups

        while True:
            self.subscriptions_ready = False
            self._raw_ws = None
            try:
                async with websockets.connect(
                    _WS_URL,
                    additional_headers=self._auth_headers(),
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=None,
                    # Forwarded to `loop.create_connection`; omitted entirely when blank so the
                    # default route stays the default route.
                    **({"local_addr": (self._source_ip, 0)} if self._source_ip else {}),
                ) as ws:
                    self._raw_ws = ws
                    self._subscribed.clear()   # fresh socket → nothing subscribed yet
                    self._sub_requests.clear()
                    self.subscribe_rejected.clear()
                    # …and a bounce this socket never served: a drop for ANY other reason already
                    # gave us the fresh budget, so it must not cost an extra reconnect.
                    self._reject_reconnect_due = False
                    log.info("poly_us_feed: raw WebSocket connected.")
                    await self._send_subscribe_raw(list(self._prices.keys()))
                    self.subscriptions_ready = True
                    if self.gap_log is not None and self._gap_started_mono is not None:
                        # GAP RECORD: (disconnect, resubscribe-complete), monotonic. A measurement
                        # consumer excludes any event window overlapping one — those frames were LOST.
                        self.gap_log.append((self._gap_started_mono, time.monotonic()))
                        self._gap_started_mono = None
                    now = time.time()
                    self._last_msg_ts = now
                    # Seed the freshness baseline at connect so the watchdog grants a full
                    # _FRESHNESS_RECONNECT_S for the first real book change (no warm-up trip), and
                    # start the health window at connect so the first _maybe_log_health() does not
                    # report the warm-up as a "0 changes/60s" frozen-feed false alarm.
                    self._last_book_change_ts = now
                    self._last_health_log = now

                    while True:
                        # recv() wakes every _FRESHNESS_POLL_S so the watchdog runs on a quiet-but-
                        # alive socket. A recv timeout is NOT a reconnect; only a stale BOOK is.
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=self._FRESHNESS_POLL_S)
                            # Diagnostic only (default off): time the inline parse+cache+callback
                            # span. t0 BEFORE json.loads so parse cost is included.
                            _t0 = time.perf_counter() if ws_timer.enabled else 0.0
                            try:
                                self._dispatch(json.loads(raw))
                            except json.JSONDecodeError:
                                log.warning(f"poly_us_feed: non-JSON WS frame: {raw[:120]!r}")
                            if ws_timer.enabled:
                                ws_timer.record_message("poly_us", _t0)
                        except asyncio.TimeoutError:
                            pass  # no frame this interval — fall through to the freshness watchdog
                        self._maybe_log_health()
                        if self._reject_reconnect_due:
                            # Reject-driven bounce: same exit as the zombie watchdog, so the
                            # maker rides the ordinary ~5 s reconnect gap it already handles.
                            self._reject_reconnect_due = False
                            break
                        # DATA-freshness / zombie watchdog (socket-wide, book-change-based, NOT frames):
                        now = time.time()
                        if _stale_book_reconnect(now, self._last_book_change_ts, len(self._prices),
                                                 self._last_forced_reconnect, self._FRESHNESS_RECONNECT_S,
                                                 self._FRESHNESS_RECONNECT_COOLDOWN_S,
                                                 self._books_should_move(now)):
                            log.warning(
                                f"poly_us_feed: no real book change for {self._FRESHNESS_RECONNECT_S}s "
                                f"across {len(self._prices)} slug(s) — frozen/zombie, forcing reconnect"
                            )
                            self._last_forced_reconnect = now
                            break
            except websockets.exceptions.ConnectionClosed as e:
                # 1001 going-away (server cycle/deploy) and 1006 no-close-frame are routine — the
                # server initiated them. Reconnect quietly at INFO; a dead feed surfaces elsewhere.
                log.info(f"poly_us_feed: raw WS closed by server ({e}); reconnecting in 5s…")
            except Exception as e:
                log.error(f"poly_us_feed: raw WS error: {e}. Reconnecting in 5s…")
            finally:
                if self.gap_log is not None and self._raw_ws is not None \
                        and self._gap_started_mono is None:
                    # Socket that HAD connected just died — open a gap record; closed at the
                    # next subscriptions_ready [prereg r5 C1].
                    self._gap_started_mono = time.monotonic()
                self.subscriptions_ready = False
                self._raw_ws = None
            await asyncio.sleep(5)

    async def _send_subscribe_raw(self, slugs: list[str]) -> None:
        """Subscribe to slugs NOT already subscribed on this socket (the server rejects duplicates);
        sends only the delta and marks them subscribed on success.

        ⚠️ The venue caps SUBSCRIPTIONS PER CONNECTION (observed 2026-08-13: 17,924 slugs × book+trade
        ≈ 35.8k → 'max subscriptions per connection reached'; 14,786 × 2 ≈ 29.6k worked). It also
        appears to cap subscribe REQUESTS per connection: 2026-09-12 the live maker's socket took
        10 requests (15 slugs total) and rejected the 11th with the same error — hypothesis, ONE
        night, one socket. Hence one request per slate apply, and the reject-driven reconnect that
        refills the budget. `book_slugs`
        restricts the market_data subscription to a subset while the trade channel still covers every
        slug; None (default) = both channels for everything, which every book reader keeps."""
        if self._raw_ws is None:
            return
        new = [s for s in slugs if s not in self._subscribed]
        if not new:
            return
        book_new = (new if self.book_slugs is None
                    else [s for s in new if s in self.book_slugs])
        if book_new:
            payload = self._subscribe_payload(book_new)
            self._sub_requests[payload["subscribe"]["requestId"]] = list(book_new)
            # ponytail: no success ack exists, so the map is bounded by age — a collector that
            # resubscribes every cycle would otherwise grow it for the socket's life.
            while len(self._sub_requests) > 256:
                del self._sub_requests[next(iter(self._sub_requests))]
            await self._raw_ws.send(json.dumps(payload))
        # OPT-IN second subscription on the SAME socket, measurement path only. Sent for the SAME
        # delta, so `_subscribed` stays the single source of truth; a failure is NOT swallowed.
        if self.subscribe_trades_too:
            await self._raw_ws.send(json.dumps(self._subscribe_payload(new, _SUB_TYPE_TRADE)))
        self._subscribed.update(new)
        log.info(f"poly_us_feed: subscribed to {len(new)} slugs (raw, "
                 f"book={len(book_new)}"
                 f"{', + trades' if self.subscribe_trades_too else ''}).")

    async def _run_forever_sdk(self) -> None:
        """Subscribe to the Poly US markets WS and keep _prices fresh. Uses
        SUBSCRIPTION_TYPE_MARKET_DATA_LITE (delivers bestAsk directly); 5s reconnect backoff."""
                # The Poly US WS requires API credentials; without them it can never connect, so do
                # not spin a 5s reconnect loop logging an ERROR forever. Log once at INFO and stop.
        if not config.poly_creds_present():
            log.info(config.POLY_CREDS_MISSING_LOG)
            return

        while True:
            self.subscriptions_ready = False
            ws = None
            try:
                ws = self._sdk.ws.markets()

                # Wire up event handlers before connecting so no messages are lost.
                ws.on("market_data_lite", self._on_market_data_lite)
                ws.on("market_data", self._on_market_data)

                await ws.connect()
                self._ws = ws
                log.info("poly_us_feed: WebSocket connected.")

                # Subscribe to lite price updates for all currently primed slugs.
                # New slugs discovered later are picked up via resubscribe().
                await self._subscribe(list(self._prices.keys()))
                self.subscriptions_ready = True
                _now = time.time()
                self._last_msg_ts = _now           # reset so the watchdog has a baseline
                self._last_book_change_ts = _now   # freshness baseline (no warm-up trip)
                self._last_health_log = _now       # start the health window at connect (no warm-up false alarm)

                # Spin until the connection drops. Don't trust ws.is_connected — SDK 0.1.2 leaves it
                # True on a zombie socket, which once froze prices 11 min; the freshness watchdog is
                # the zombie catch.
                while ws.is_connected:
                    await asyncio.sleep(1)
                    self._maybe_log_health()
                    now = time.time()
                    if _stale_book_reconnect(now, self._last_book_change_ts, len(self._prices),
                                             self._last_forced_reconnect, self._FRESHNESS_RECONNECT_S,
                                             self._FRESHNESS_RECONNECT_COOLDOWN_S,
                                             self._books_should_move(now)):
                        log.warning(
                            f"poly_us_feed: no real book change for {self._FRESHNESS_RECONNECT_S}s "
                            f"across {len(self._prices)} slug(s) — frozen/zombie, forcing reconnect"
                        )
                        self._last_forced_reconnect = now
                        break

                log.warning("poly_us_feed: WS connection lost, reconnecting in 5s…")

            except Exception as e:
                log.error(f"poly_us_feed: WS error: {e}. Reconnecting in 5s…")
            finally:
                self.subscriptions_ready = False
                self._ws = None
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass

            await asyncio.sleep(5)

    async def _subscribe(self, slugs: list[str]) -> None:
        """Send a full market_data subscription (carries best ask + share depth)."""
        if self._ws is None or not slugs:
            return
        request_id = str(uuid.uuid4())
        await self._ws.subscribe_market_data(request_id, slugs)
        log.info(f"poly_us_feed: subscribed to {len(slugs)} slugs (full book).")

    async def resubscribe(self) -> None:
        """Re-subscribe to the full current slug set. Call after new games are
        primed so they get live prices without waiting for a WS reconnect.
        No-op if the socket isn't connected (run_forever subscribes on connect)."""
        if config.POLY_US_FEED_SOURCE == "raw":
            if self._raw_ws is None:
                return
            try:
                await self._send_subscribe_raw(list(self._prices.keys()))
            except Exception as e:
                log.warning(f"poly_us_feed: raw resubscribe failed: {e}")
            return
        if self._ws is None or not getattr(self._ws, "is_connected", False):
            return
        try:
            await self._subscribe(list(self._prices.keys()))
        except Exception as e:
            log.warning(f"poly_us_feed: resubscribe failed: {e}")
