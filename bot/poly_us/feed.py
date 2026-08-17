"""
bot/poly_us/feed.py
───────────────────
Real-time Polymarket US order-book cache via the SDK WebSocket (ws.markets).
Interface: get_best_ask / get_age / prime / run_forever.
Prices are keyed by market-side slug (same identifier carried in MarketPair).

SDK WebSocket pattern (inspected from polymarket_us.websocket):
  ws = sdk.ws.markets()            # returns MarketsWebSocket (no args needed)
  await ws.connect()               # opens wss://api.polymarket.us/v1/ws/markets
  ws.on("market_data_lite", cb)    # event-emitter; cb receives the full parsed dict
  await ws.subscribe_market_data_lite(request_id, [slug1, slug2, ...])
  # messages arrive via _emit("market_data_lite", message) where message is:
  #   {"requestId": str, "subscriptionType": "SUBSCRIPTION_TYPE_MARKET_DATA_LITE",
  #    "marketDataLite": {"marketSlug": str, "bestAsk": {"value": str, "currency": "USD"},
  #                       "bestBid": {...}, "lastTradePx": {...}}}
  # For full order book, event is "market_data" and payload key is "marketData" with
  # "offers": [{"px": {"value": str, ...}, "qty": str}, ...]
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
# Poly DOES publish a per-print trade channel (SDK: `ws.subscribe_trades`; raw: this type). The bot does
# not want it — it prices off the book — so it stays OPT-IN via `subscribe_trades_too` and is OFF by
# default, purely to keep the arb feed's traffic unchanged. (An earlier version of this comment claimed
# the extra frames would perturb the data-freshness watchdog. That is FALSE: trade frames bump
# `_last_msg_ts`, but the zombie watchdog keys on `_last_book_change_ts`, set only in `_note_book`.
# Being wrong in the conservative direction is still being wrong.)
# It exists for the MEASUREMENT path: a markout probe otherwise has to infer trades from
# `stats.lastTradeSetTime` transitions, which records only the LAST print of a burst and therefore
# undercounts prints AND selectively drops the adverse ones.
_SUB_TYPE_TRADE = "SUBSCRIPTION_TYPE_TRADE"


def _level_px_qty(lvl: dict) -> tuple[float, float] | None:
    """Parse ONE order-book level → (price, qty), or None if it doesn't parse.

    A level is {"px": {"value": str, "currency": "USD"}, "qty": str} — the Poly "Amount"
    carries a currency sibling. A variant/garbage frame can put the currency code where the
    number belongs (seen live as float('USD')), so parse DEFENSIVELY per level
    and let the caller SKIP a bad level rather than let one throw discard the whole book frame
    (which would silently freeze that market's cache — the fire path re-reads fresh REST, so a
    skipped level costs detail, never a fire). Mirrors the REST client's _amount_to_float."""
    try:
        px = lvl["px"]
        price = float(px["value"] if isinstance(px, dict) else px)
        return price, float(lvl["qty"])
    except (KeyError, TypeError, ValueError):
        return None


class PolyUSOrderBookCache:
    # ── Two SEPARATE liveness concerns — do not conflate (the old single 45s timer did) ──────
    # 1) CONNECTION liveness → the websockets library's ping/pong (ping_interval/ping_timeout on
    #    connect): a genuinely dead socket closes in ~40s → ConnectionClosed → reconnect. (Verified
    #    live: the Poly MARKET channel accepts the 20s library ping — no client-side ping needed.)
    # 2) DATA freshness / ZOMBIE → a socket-wide watchdog on REAL book changes (_last_book_change_ts),
    #    NOT on frames. A frozen feed keeps ping/pong healthy while the tradeable book never moves —
    #    the disguise that manufactured phantom edges. See _stale_book_reconnect.
    #    180s is calibrated for the QUIET-SLATE false-positive floor (don't reconnect a legitimately
    #    slow market). During a live game (~95 changes/min) a real zombie shows in ~10-20s, so 180s is
    #    slow-for-games BY DESIGN — tolerable only because the fire path re-checks vs fresh REST
    #    (get_fill_quote fresh=True) before firing, so a stale WS book costs wasted DETECTIONS, not a
    #    stale fire. Lower this first if that REST backstop is ever in doubt (or add per-slug phase).
    _FRESHNESS_RECONNECT_S = 180        # no real book change across all slugs for this long → reconnect
    _FRESHNESS_RECONNECT_COOLDOWN_S = 60  # min gap between forced reconnects (anti-storm)
    _FRESHNESS_POLL_S = 30              # recv() wakes this often to run the watchdog on a quiet socket
    _PRUNE_AFTER_ABSENT_CYCLES = 2     # prune a slug only after it's been absent from discovery this long
                                       # (debounce vs a transient scanner error cold-pruning live markets)

    def __init__(self, sdk: Any) -> None:
        self._sdk = sdk
        # slug -> (ask, ask_depth, bid, bid_depth, ts). Bid fields drive the
        # moneyline SHORT side (short ask = 1 − bid; see get_best_ask / sides.py).
        self._prices: dict[str, tuple[float, float, float, float, float]] = {}
        # slug -> last server-side book transactTime (ISO-8601 ns string). This is the CONTENT-
        # freshness signal `_prices`'s `ts` (local receipt time, refreshed on every frozen re-send)
        # is NOT: a frozen origin re-sends the SAME stale transactTime, so this value ages while
        # `ts`/get_age stay fresh. ✅ VERIFIED by probing quiet and hot books: transactTime is
        # LAST-MUTATION time, not a serve clock — it does NOT advance on a byte-identical re-send,
        # it advances on essentially every genuine change, and the rare advance on an unchanged
        # book outran the read gap by more than a clock could, which only a past-mutation stamp
        # can do. So the staleness gate that rests on it is on solid ground. SCOPE: steady state
        # only — the freeze-RECOVERY transient is untested, and a republish could stamp a fresh
        # transactTime onto reconstructed state.
        # Powers count_stale_books (the cross-market origin-freeze gate).
        # Updated ONLY when a frame carries transactTime (last-known kept otherwise); pruned with
        # _prices. ✅ VERIFIED against a captured live WS frame: the `marketData` frame DOES carry
        # `transactTime` (ns-ISO, parses) alongside full `bids`/`offers` and a `stats` superset —
        # so count_stale_books is LIVE on the WS path, not inert, and the WS book feed rests on a
        # confirmed field rather than an assumed one. (Capture the frame before relying on a field
        # the docs do not promise; the earlier "still assumed, VERIFY" hedge is discharged.)
        self._transact_times: dict[str, str] = {}
        # slug -> (raw stats dict, market state str) from the full market_data frame, kept for the
        # WS book feed. The maker reads `parse_book_stats(md)` (the flow-tape columns) and the
        # market state per cycle; the arb never did, so these were dropped.
        # RAW stats (not parsed): `parse_book_stats` computes *SetTime ages relative to read-time
        # `now`, so parsing must happen at read, not at receive. Pruned with `_prices`.
        self._stats: dict[str, dict] = {}
        self._states: dict[str, str] = {}
        # slug -> {"offers": [raw wire dicts at best ask], "bids": [raw wire dicts at best bid]}.
        # The RAW `{"px": {"value": str}, "qty": str}` levels, EXACT wire strings — NOT the floats
        # `_prices` stores. `get_book_md` passes these through so the maker's crossing guard reads
        # `Decimal(str(wire))` identically to a REST book (a float→str reconstruction would launder
        # binary error into the tick-tie cases the guard exists to catch). Bounded to the TOUCH
        # price per side (the only depth the maker reads); pruned with `_prices`.
        self._touch_raw: dict[str, dict] = {}
        self.subscriptions_ready = False
        self._on_update_callback: Optional[Callable[[], None]] = None
        self._ws: Any = None  # live MarketsWebSocket (SDK) while connected, else None
        # OPT-IN (raw transport only): also subscribe the per-print TRADE channel on this socket.
        # OFF by default so the arb feed is byte-for-byte unchanged. Set it BEFORE run_forever().
        # Consumers read the trade frames by tapping `_dispatch` (which receives every raw message);
        # `_dispatch` itself still ignores them, since the book path has no use for a trade.
        self.subscribe_trades_too: bool = False
        # OPT-IN (raw transport only): restrict the market_data (book) subscription to this
        # subset of the slate; trade channel still covers everything. None (default) = book
        # subs for every slug — the original behavior; every maker/arb path keeps None.
        # Exists because the venue caps subscriptions PER CONNECTION, and on a large slate a
        # book+trade DOUBLE subscription blows that cap (see _send_subscribe_raw).
        self.book_slugs: Optional[set[str]] = None
        # Count of venue error frames mentioning subscriptions (e.g. 'max subscriptions
        # per connection reached'). Nonzero = some slugs we asked for are NOT watched and
        # nothing else will say so — measurement hosts surface this in their heartbeat.
        self.subscribe_reject_count: int = 0
        # OPT-IN trade sink: called with the raw Trade frame (top-level key "trade") for each
        # print, ONLY when set. Defaults None so the book path — and every live maker, which
        # never sets this — is byte-for-byte unchanged. Used by the per-print flow probe.
        self.on_trade: Optional[Callable[[dict[str, Any]], None]] = None
        # Raw marketData frame tap (same contract as on_trade: verbatim parsed frame, fired
        # from _dispatch AFTER cache processing). Measurement-only — see _dispatch comment.
        self.on_book: Optional[Callable[[dict[str, Any]], None]] = None
        # PRE-routing tap: every frame kind, fired FIRST in _dispatch before any cache work —
        # cache-processing-skew-free arrival stamps for the in-play sampler. ⚠️ Both stamps
        # still sit after json.loads, so a residual parse differential of ~tens of µs remains —
        # immaterial against the sampler's floor, but don't call it zero.
        self.on_frame: Optional[Callable[[dict[str, Any]], None]] = None
        # Per-tap escaped-exception counters (taps are wrapped; see _safe_tap). A measurement
        # consumer MUST health-gate on this — errors here are silent frame loss.
        self.tap_errors: dict[str, int] = {}
        # Reconnect gap records [(disconnect_mono, resubscribe_complete_mono)], monotonic —
        # OPT-IN: a measurement consumer assigns a list; None (default) records nothing.
        # Frames between the two stamps were LOST (Poly has no book sequence number), so the
        # in-play sampler excludes any event window overlapping a gap.
        self.gap_log: Optional[list[tuple[float, float]]] = None
        self._gap_started_mono: Optional[float] = None
        self._raw_ws: Any = None  # live raw websocket connection, else None
        # raw transport: slugs already subscribed on the CURRENT socket. Re-subscribing
        # a slug makes the server reject it ("slug already subscribed"); we send only the
        # delta. MUST be cleared on every reconnect (fresh socket → nothing subscribed),
        # else after a drop we'd never re-subscribe and the feed would silently go dark.
        self._subscribed: set[str] = set()
        # Discovery-membership pruning: consecutive cycles a tracked slug has been ABSENT from
        # discovery. A slug gone for _PRUNE_AFTER_ABSENT_CYCLES cycles is closed/delisted → pruned
        # from _prices + _subscribed; reset (popped) the moment discovery returns it, so a 1-cycle
        # flicker (transient scanner error) never prunes a live market. See retain_slugs.
        self._absent_cycles: dict[str, int] = {}
        # Active-game-window gate for the freshness watchdog (set each discovery by the runner from
        # active_pairs' start/end times). None = not computed yet (startup) → ARMED. See
        # _books_should_move + common.build_game_windows.
        self._game_windows: list[tuple[float, float]] | None = None
        self._windows_unknown: bool = False
        self._last_msg_ts: float = 0.0  # wall-clock of last WS message (any slug)
        # CONTENT freshness (distinct from socket liveness). A frozen feed delivers
        # heartbeats — _last_msg_ts stays current — while the book never changes; that
        # disguise is exactly what manufactured the phantom edges. Track real changes.
        self._book_changes: int = 0          # book changes since the last health log
        self._last_book_change_ts: float = 0.0
        self._last_forced_reconnect: float = 0.0  # last freshness-watchdog reconnect (anti-storm)
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

    def retain_slugs(self, live: set[str]) -> None:
        """Prune per-slug state for markets NO LONGER discovered (closed/delisted), so the tracked
        set follows the live slate instead of growing unbounded — closed markets never update, so
        they'd serve stale prices forever and inflate the freshness watchdog's `n_subscribed` count.

        Prune on DISCOVERY MEMBERSHIP, NEVER on staleness: a still-discovered market is keep-it even
        if momentarily quiet (pruning a live-but-quiet market would silently drop one we might detect
        an edge on). Only a slug ABSENT from discovery for `_PRUNE_AFTER_ABSENT_CYCLES` CONSECUTIVE
        cycles is closed-prune-it — the debounce protects against a transient discovery error (the
        scanner omits an errored series for a cycle, so a 1-cycle absence must not cold-prune live
        markets); seeing the slug again resets the count.

        `live` MUST be keyed identically to `_prices` (BARE slugs — the caller applies `parse_token`,
        exactly as `prime` does). If it weren't, no key would match → everything would look absent →
        prune EVERYTHING. Prunes both `_prices` and `_subscribed`; sends no WS unsubscribe (a closed
        market emits no frames, and the next reconnect re-subscribes only the current `_prices`)."""
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
        the freshness-watchdog gate. Refreshed each discovery cycle (~5min); evaluated LIVE against
        `now` at every watchdog poll (~30s), so a window opening/closing between discoveries is honored."""
        self._game_windows = windows
        self._windows_unknown = unknown

    def _books_should_move(self, now: float) -> bool:
        """Active-game-window gate for the freshness watchdog. True (ARMED) UNLESS we can CONFIDENTLY say
        no game is in-window — err toward watching, because suppressing a real freeze DURING a game is the
        dangerous direction (off-hours cry-wolf is the benign one):
          • windows never computed (startup, None) → True;
          • any game's timing was unreadable (`_windows_unknown`) → True;
          • else armed iff some game's window contains `now` (inclusive both ends).
        A genuinely empty window list with no unknowns → `any(...)` over [] is False → not armed (no games,
        the watchdog rests). The off-hours/stale-listing suppression that fixes the reconnect churn."""
        if self._game_windows is None:
            return True
        if self._windows_unknown:
            return True
        return any(start <= now <= end for start, end in self._game_windows)

    @property
    def connected(self) -> bool:
        """Whole-connection liveness for the WS-maker's fallback→halt guard: True while a
        socket (raw or SDK) is currently open. Distinct from per-book freshness (transactTime)
        and the slate-wide stale watchdog — this answers only "is there a connection at all".
        Both transports None their handle on disconnect, so this is read-only truth."""
        return self._raw_ws is not None or self._ws is not None

    def get_book_md(self, slug: str) -> Optional[dict]:
        """A `marketData`-shaped dict for `slug` from the retained book — the read the WS
        BookSource seam will hand the maker so `touch_from_md` / `qty_at_price` /
        `parse_book_stats` work with ZERO maker-side change. None when there is no book yet (the
        maker treats None like a failed REST read: skip this market this cycle).

        ⛔ EXACT WIRE STRINGS, not reconstructed from floats — `offers`/`bids` are the raw
        `_touch_raw` dicts, so the maker's crossing guard reads `Decimal(str(wire))` identically
        to a REST book. This getter does NOT put the maker on WS by itself; it is the source the
        seam (a separate, money-path-reviewed diff) will opt into behind `MAKER_BOOK_SOURCE`.
        Bounded to the touch level per side (all the maker reads). `state`/`stats`/`transactTime`
        ride along for the state check, the flow columns, and the staleness gate."""
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
        """Best bid you'd SELL into when flattening this side. Long → bestBid;
        short → 1 − bestAsk (selling the short crosses the long offers). None if
        that side has no quote. Used by the would-fire sampler to record the price
        an unwind would realize (flatten cost in the settlement backtest)."""
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
        transactTime at the same instant; a single illiquid book stale while neighbors keep
        ticking does NOT. Counts ONLY confirmable staleness (a parseable transactTime present AND
        aged past `max_age`); a market with no transactTime is NOT counted. Read-only and in-memory
        (a dict scan over the tracked slate, no I/O) — safe on the fire path.

        `in_window` (bare slugs, from common.in_window_slugs) scopes the peer set to markets whose
        game is LIVE right now. This is load-bearing: without it the scan spans the full discovery
        slate (active+not-closed), so an UPCOMING game (hours away, legitimately not trading → old
        transactTime) or a just-FINISHED one would be counted as a 'stale peer' and falsely confirm
        a freeze, making the gate reject a real illiquid-but-live edge during a quiet period — the
        stable-deep-book tail it must preserve. A market NOT in `in_window` is never counted, regardless of its
        transactTime.
        ⚠️ `in_window=None` = UNSCOPED = the PRE-FIX full-slate scan that re-introduces the
        quiet-period false-positive (upcoming/finished games counted as freeze peers → a real
        illiquid edge falsely rejected). It exists ONLY for the raw-mechanic unit tests. The fire
        path MUST pass the scoped set (common.in_window_slugs); NEVER call this with None from a
        decision path. An EMPTY set → 0 peers → no reject (fail toward firing) is the correct
        "no live peers right now" result — that, not None, is how a quiet slate reads.

        `exclude_token` is the firing market itself (its own staleness is the Stage-1 flag, judged
        from its fresh REST read, not from here); excluded so it can't count toward its own peers."""
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
        """Store a slug's book, counting it as a CHANGE only when ask/depth/bid actually
        move (identical re-sends — the frozen-cache signature — are not changes). The ts
        still refreshes every message so get_age reflects message recency as before."""
        old = self._prices.get(slug)
        if old is None or old[:4] != (ask, depth, bid, bid_depth):
            self._book_changes += 1
            self._last_book_change_ts = time.time()
        self._prices[slug] = (ask, depth, bid, bid_depth, time.time())

    def _maybe_log_health(self) -> None:
        """Every ~60s, log feed CONTENT health: how many real book changes landed and how
        long since the last one. A socket that is 'connected' but delivering only heartbeats
        (the freeze behind the phantom edges) shows here as '0 changes' — visible in real
        time and in hindsight. 0 changes is also a genuinely quiet market off-hours, so the
        WARNING is gated on _books_should_move (a game is in-window): in-window 0-changes is a
        real-freeze alarm; off-hours 0-changes drops to DEBUG. Healthy (>0 changes) is DEBUG."""
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
        # Quiet when healthy (DEBUG, off at INFO), loud only on a real freeze. The book
        # never sits at 0 changes/60s while live (empirical floor ~95/min), so 0 = the
        # socket is alive but content is frozen — the disguise we added this log to catch.
        # BUT 0 changes off-hours is a genuinely quiet market, not a freeze (the docstring's
        # "read it against whether a game is live"): gate the WARNING on the SAME active-game-
        # window signal the freshness watchdog uses (_books_should_move), so a quiet book only
        # alarms while a game is in-window. Off-hours / stale-listing → DEBUG, no per-minute
        # cry-wolf. Symmetric with the watchdog: when it rests (books_should_move False), so
        # does this alarm — a real freeze DURING a game still warns (books_should_move True).
        if changes == 0 and self._books_should_move(now):
            log.warning(msg + " ⚠️ no book changes — possible frozen feed")
        else:
            log.debug(msg)

    def _on_market_data_lite(self, message: dict[str, Any]) -> None:
        """Handle a marketDataLite event (no depth — kept for completeness; we
        subscribe to full market_data, but register both handlers)."""
        self._last_msg_ts = time.time()  # any message proves the socket is alive
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
        self._last_msg_ts = time.time()  # any message proves the socket is alive
        try:
            payload = message.get("marketData", {})
            slug: str | None = payload.get("marketSlug")
            offers: list[dict[str, Any]] = payload.get("offers", [])
            if not slug:
                return
            if not offers:
                # An ask-EMPTY frame is a real book state, not a skippable one:
                # a taker clearing the whole ask side produces exactly this frame, and dropping
                # it left get_book_md serving the stale PRE-sweep touch until a maker re-posted
                # — the worst adverse event, invisible. Retain the touch (empty offers side +
                # current bids + stamps) so md readers see the sweep. PRICING state (_prices)
                # deliberately keeps last-known: its readers gate on age, and an ask=0.0
                # sentinel would poison every mid built on it. `offers_key_absent` splits
                # "venue sent offers: []" from "key missing entirely" — the latter is assumed
                # a snapshot too, but a measurement consumer must confirm that on a raw
                # capture before treating key-absent frames as sweeps.
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
            # Offers are sorted lowest-to-highest; best ask = lowest price level, depth = total
            # shares at it. Parse each level DEFENSIVELY (see _level_px_qty): one malformed level
            # is skipped, not allowed to discard the whole frame and freeze this market's cache.
            # If NOTHING parses, log a sample and drop (keep the last-known book).
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
                # Retain stats (raw) + market state for the WS-maker book source.
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
        """Route a parsed raw-WS message by its top-level key.

        Every message (including heartbeat) bumps the liveness clock so the
        stale-reconnect watchdog only fires on a genuinely dead socket — the
        docs say to reconnect when heartbeats stop, so a heartbeat IS liveness.
        """
        self._last_msg_ts = time.time()
        if self.on_frame is not None:
            # PRE-routing verbatim tap: fires BEFORE any cache processing, so a consumer's
            # arrival stamps carry no parse-cost skew between trade and book frames — the same
            # instrument the book-vs-print offset measurements use. The in-play
            # sampler reads THIS tap for both frame kinds; on_trade/on_book stay for
            # consumers that want post-cache semantics (the flow probe).
            self._safe_tap(self.on_frame, message, "on_frame")
        if "marketData" in message:
            self._on_market_data(message)
        elif "marketDataLite" in message:
            self._on_market_data_lite(message)
        elif "heartbeat" in message:
            pass  # liveness already recorded above
        elif "error" in message:
            # A subscribe reject means slugs we believe watched are NOT — `_subscribed`
            # (and any .subs sidecar) records the ASK, not the venue's answer. A warning
            # line scrolls away unread; this counter is what a measurement host must
            # health-gate on, because a silently-capped subscription can run for hours
            # looking perfectly healthy.
            err_txt = str(message.get("error") or "")
            if "subscription" in err_txt.lower():
                self.subscribe_reject_count += 1
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
            # Raw-frame book tap for MEASUREMENT consumers, mirroring on_trade: the VERBATIM
            # frame, after cache processing, so the consumer sees the wire truth — offers
            # present vs [] vs key-absent — with no cache semantics (empty-side omission,
            # last-known retention, age clocks) in between. The cache getters CANNOT serve
            # this: get_book_md drops the empty/absent distinction and _prices deliberately
            # keeps last-known through a sweep.
            self._safe_tap(self.on_book, message, "on_book")
        # other unknown message types: ignored (we only price off the book)

    def _safe_tap(self, sink: Callable[[dict[str, Any]], None], message: dict[str, Any],
                  name: str) -> None:
        """Run a measurement tap without letting its exception escape _dispatch — an escaped
        exception forces a reconnect and a frame GAP, which is precisely the data loss the
        sampler's gap-exclusion rule exists to avoid manufacturing. Errors
        are counted per tap (tap_errors) and logged; the feed never dies for a consumer bug."""
        try:
            sink(message)
        except Exception as e:
            n = self.tap_errors[name] = self.tap_errors.get(name, 0) + 1
            # Rate-limited: a systematically-raising tap at full-slate frame rates
            # would log per frame ON the receive path, inflating the handler lag the sampler's
            # FLOOR is built from. First 5, then every 1000th; the COUNTER is the health gate.
            if n <= 5 or n % 1000 == 0:
                log.warning(f"poly_us_feed: {name} tap raised (#{n}): {e!r}")

    def _auth_headers(self) -> dict[str, str]:
        """Ed25519-signed handshake headers for the markets WS (per Poly US docs).

        Signs `timestamp_ms + "GET" + path` with the base64 Ed25519 secret key.
        A 64-byte key is the seed||pubkey form — use the first 32 bytes (seed).
        """
        from nacl.signing import SigningKey  # lazy: only needed in raw mode

        ts = str(int(time.time() * 1000))
        seed = base64.b64decode(config.POLYMARKET_US_SECRET_KEY)
        if len(seed) == 64:
            seed = seed[:32]
        sig = SigningKey(seed).sign(f"{ts}GET{_WS_PATH}".encode()).signature
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
        if not config.POLYMARKET_US_KEY_ID or not config.POLYMARKET_US_SECRET_KEY:
            log.info(
                "poly_us_feed: no API keys — live price WebSocket disabled. "
                "Set POLYMARKET_US_KEY_ID/SECRET_KEY to enable live prices."
            )
            return
        if config.POLY_US_FEED_SOURCE == "raw":
            await self._run_forever_raw()
        else:
            await self._run_forever_sdk()

    async def _run_forever_raw(self) -> None:
        """Own raw-JSON markets WebSocket. Two separated liveness layers: the library's ping/pong
        closes a dead SOCKET (~40s → ConnectionClosed → reconnect), and the DATA-freshness watchdog
        (_stale_book_reconnect on real book changes) reconnects a ZOMBIE — socket alive, book frozen
        (no frames OR identical re-sends). Replaces the SDK transport, whose is_connected lied on a
        zombie. recv() uses a short poll timeout only to run the watchdog on a quiet-but-alive socket."""
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
                ) as ws:
                    self._raw_ws = ws
                    self._subscribed.clear()   # fresh socket → nothing subscribed yet
                    log.info("poly_us_feed: raw WebSocket connected.")
                    await self._send_subscribe_raw(list(self._prices.keys()))
                    self.subscriptions_ready = True
                    if self.gap_log is not None and self._gap_started_mono is not None:
                        # GAP RECORD: (disconnect, resubscribe-complete), monotonic.
                        # A measurement consumer excludes any event window overlapping one — frames
                        # in the gap were LOST (no Poly book sequence number to detect it by).
                        self.gap_log.append((self._gap_started_mono, time.monotonic()))
                        self._gap_started_mono = None
                    now = time.time()
                    self._last_msg_ts = now
                    # Seed the freshness baseline at connect so the watchdog grants a full
                    # _FRESHNESS_RECONNECT_S for the first real book change (no warm-up trip).
                    self._last_book_change_ts = now
                    # Start the health window at connect, NOT epoch 0 — else the first
                    # _maybe_log_health() fires on the first frame (seconds in) and reports
                    # the warm-up as a full "0 changes/60s" frozen-feed false alarm.
                    self._last_health_log = now

                    while True:
                        # recv() wakes every _FRESHNESS_POLL_S so the freshness watchdog runs even on
                        # a quiet-but-alive socket. A recv timeout is NOT a reconnect (ping/pong owns
                        # connection liveness); only a stale BOOK forces one (checked below).
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
                # 1001 going-away (server cycle/deploy) and 1006 no-close-frame (network
                # drop) are routine, not bugs — the server initiated them. Reconnect
                # quietly at INFO; a genuinely dead feed surfaces via the 45s watchdog and
                # the book-health WARNING, not here. Keeps ERROR for real failures below.
                log.info(f"poly_us_feed: raw WS closed by server ({e}); reconnecting in 5s…")
            except Exception as e:
                log.error(f"poly_us_feed: raw WS error: {e}. Reconnecting in 5s…")
            finally:
                if self.gap_log is not None and self._raw_ws is not None \
                        and self._gap_started_mono is None:
                    # Socket that HAD connected just died — open a gap record; closed at the
                    # next subscriptions_ready.
                    self._gap_started_mono = time.monotonic()
                self.subscriptions_ready = False
                self._raw_ws = None
            await asyncio.sleep(5)

    async def _send_subscribe_raw(self, slugs: list[str]) -> None:
        """Subscribe to slugs NOT already subscribed on this socket (the server rejects
        duplicates with an error). Sends only the delta; marks them subscribed on success.

        ⚠️ The venue caps SUBSCRIPTIONS PER CONNECTION, and the cap counts ENTRIES, not
        slugs — so subscribing a big slate to book AND trade doubles your way into 'max
        subscriptions per connection reached'. `book_slugs` exists for exactly that
        arithmetic: when set (measurement hosts whose only book-frame consumer is a scoped
        sampler), the market_data subscription is restricted to that subset while the trade
        channel still covers every slug — halving the book-side entry count. None (default)
        = both channels for everything; the maker and every book-reading consumer keep it."""
        if self._raw_ws is None:
            return
        new = [s for s in slugs if s not in self._subscribed]
        if not new:
            return
        book_new = (new if self.book_slugs is None
                    else [s for s in new if s in self.book_slugs])
        if book_new:
            await self._raw_ws.send(json.dumps(self._subscribe_payload(book_new)))
        # OPT-IN second subscription on the SAME socket, for the measurement path only. Sent after the
        # book subscribe and for the SAME delta, so `_subscribed` stays the single source of truth for
        # what this socket has asked for and a reconnect re-requests both together. A failure here would
        # propagate like any other send failure — deliberately NOT swallowed, because a silently
        # trade-less socket is exactly the undercount this exists to remove.
        if self.subscribe_trades_too:
            await self._raw_ws.send(json.dumps(self._subscribe_payload(new, _SUB_TYPE_TRADE)))
        self._subscribed.update(new)
        log.info(f"poly_us_feed: subscribed to {len(new)} slugs (raw, "
                 f"book={len(book_new)}"
                 f"{', + trades' if self.subscribe_trades_too else ''}).")

    async def _run_forever_sdk(self) -> None:
        """Subscribe to the Poly US markets WS and keep _prices fresh.

        Uses SUBSCRIPTION_TYPE_MARKET_DATA_LITE for efficiency (delivers bestAsk
        directly).  Falls back gracefully on error with 5s reconnect backoff.
        """
        # The Poly US WS requires API credentials. Without them it can never
        # connect, so don't spin a 5s reconnect loop logging an ERROR forever
        # (which would also spam Discord). Log once at INFO and stop.
        if not config.POLYMARKET_US_KEY_ID or not config.POLYMARKET_US_SECRET_KEY:
            log.info(
                "poly_us_feed: no API keys — live price WebSocket disabled. "
                "Set POLYMARKET_US_KEY_ID/SECRET_KEY to enable live prices."
            )
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

                # Spin until the connection drops. Don't trust ws.is_connected alone — SDK 0.1.2
                # leaves it True on a zombie socket (TCP open, no data), which once froze prices
                # 11 min. The DATA-freshness watchdog (real book changes, NOT frames/heartbeats) is
                # the zombie catch; ConnectionClosed handles a truly dead socket.
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
