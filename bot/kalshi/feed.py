"""
bot/kalshi/feed.py
──────────────────
Real-time Kalshi price cache via WebSocket. Two sources (KALSHI_PRICE_SOURCE):

  "ticker"    — the `ticker` channel: top-of-book quote for ALL markets, filtered
                to the configured series prefixes. Can lag the executable book.
  "orderbook" — the `orderbook_delta` channel (per matched ticker): maintains the
                real book (snapshot + signed deltas) and derives executable best
                bid/ask + touch depth. Use this to avoid phantom edges.

Either way the cache exposes the same fields, so detection/unwind are agnostic:
  yes_bid, yes_ask, no_bid, no_ask — exact Decimal in [0, 1] INTERNALLY.
  no_bid = 1 - yes_ask, no_ask = 1 - yes_bid (complementary market identity).
  TWO SPELLINGS OF EACH GETTER (Phase D step 2): `*_d` returns the exact Decimal (read by
  `bot/kalshi/maker.py`); `get_best_bid`/`get_best_ask`/`get_depth`/`fillable_qty` DELEGATE
  through them and float the result. One derivation path, two return types.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional

import websockets

from bot.core import config
from bot.core.money import ONE as _ONE, ZERO as _ZERO, from_float, is_zero, parse_wire
from bot.core.feed_health import _stale_book_reconnect
from bot.core.logger import get_logger
from bot.core.ws_timing import ws_timer

log = get_logger(__name__)

# The book is maintained in EXACT Decimal since 2026-07-19 (Phase 2), so delta arithmetic no longer
# CREATES the ~1e-13 dust behind the KALSHI_CROSSED bug. `_QTY_STEP` is a resolution floor on VENUE
# data: Kalshi qty is 2-dp on the wire, so 4 dp is wire precision plus a buffer.
_QTY_STEP = Decimal("0.0001")   # 4 dp = wire precision (2 dp) + a two-decimal buffer


def _wire_or_none(msg: dict, keys: tuple[str, ...]) -> Optional[Decimal]:
    """The first of `keys` present on `msg`, parsed as an exact wire Decimal — else None.

    ⛔ **None IS "the venue did not send it", NEVER a zero and never a converted value.** Only the
    DOLLARS spellings are passed here; scaling a legacy cents integer would fabricate a price."""
    for key in keys:
        raw = msg.get(key)
        if raw is None or raw == "":
            continue
        try:
            value = parse_wire(raw)
        except (TypeError, ValueError, InvalidOperation):
            return None
        return value if value.is_finite() else None
    return None


@dataclass
class KalshiPriceData:
    """The cached top-of-book, in EXACT Decimal (migration Phase D). Both write paths already
    computed these exactly, so exactness reaches the cache and `float()` happens at the getters only.
    `last_updated` stays float: it is a `time.time()` clock, not money."""
    yes_bid: Decimal = _ZERO
    yes_ask: Decimal = _ONE
    no_bid:  Decimal = _ZERO
    no_ask:  Decimal = _ONE
    last_updated: float = field(default_factory=time.time)


class KalshiOrderBookCache:
    """Maintains real-time Kalshi prices from the WebSocket `ticker` channel (and, in orderbook mode,
    a full `orderbook_delta` book with the seq-gap resnapshot machinery)."""

    def __init__(self, client, series_prefixes: list[str] | None = None) -> None:
        """
        Args:
            client:           KalshiClient instance (provides ws_url and ws_headers())
            series_prefixes:  Ticker prefixes, e.g. ["KXNBAGAME"]; None → config.KALSHI_SERIES.
        """
        self._client = client
        self._prefixes = series_prefixes if series_prefixes is not None else config.KALSHI_SERIES
        self._prices: dict[str, KalshiPriceData] = {}
        # Liquidity/activity from the SAME ticker messages — LOGGING-ONLY, in its own dict so the
        # bid/ask path is never perturbed. Cleared on (re)subscribe: blank, never a frozen value.
        self._liquidity: dict[str, tuple[float | None, float | None, float | None, float]] = {}
        self._on_update_callback: Optional[Callable] = None
        self.subscriptions_ready = False

        # Orderbook mode (KALSHI_PRICE_SOURCE="orderbook"): maintain the real book per ticker and
        # derive executable prices/depth from it (kills the phantom edges the lagging ticker quote
        # produces). _books[ticker][side] = {px: qty}, Decimal keys AND values (Phase 2) — exact.
        self._orderbook_mode = config.KALSHI_PRICE_SOURCE == "orderbook"
        self._books: dict[str, dict[str, dict[Decimal, Decimal]]] = {}
        # `_depth` is exact touch depth (Decimal, Phase D), lifted verbatim off the exact book.
        self._depth: dict[str, dict[str, Decimal]] = {}  # ticker -> {yes,no} touch depth
        self._book_tickers: list[str] = []               # orderbook subscription set
        # DIAGNOSTIC: ticker -> ts it entered a CROSSED book (best_yes_bid + best_no_bid > 1, a no-arb
        # violation that makes _derive's yes_ask phantom-cheap). Observability only.
        self._crossed: dict[str, float] = {}
        # Kalshi's seq is monotonic PER SUBSCRIPTION (`sid`), not per-market. ⚠️ NOT verified to be
        # per-sid rather than socket-wide — no mixed-channel frame has ever been captured here.
        # `_seq_global` is the BOOK subscription's cursor (the collector's `venue_seq`), reset on
        # (re)subscribe; `_seq_by_sid` holds the cursors the gap gate actually reads.
        self._seq_global: int | None = None
        self._seq_by_sid: dict[int, int] = {}
        self._last_gap_action: float = 0.0               # throttle gap warn/resnapshot
        self._ws = None                                   # live socket (for resubscribe)
        self._last_msg_ts: float = 0.0                    # dead-stream watchdog (ANY message)
        # Data-freshness / frozen-book watchdog — a SEPARATE concern from _last_msg_ts above, advancing
        # ONLY on a real WS top-of-book change, so it catches the frozen-resend zombie. REST writes
        # deliberately do NOT touch it, else the 10s refresh would mask a frozen WS book.
        self._last_book_change_ts: float = 0.0
        self._last_forced_reconnect: float = 0.0          # anti-storm cooldown anchor for the above
        self._book_changes: int = 0                       # cumulative real WS top-of-book changes
        self._book_changes_anchor: int = 0                # book_health() reads the per-interval delta
        self._gap_count: int = 0                          # seq gaps detected (ORDERBOOK HEALTH log)
        self._resnap_count: int = 0                       # divergence-triggered resnapshots
        # Book-trust gate: a ticker (or all, on a global seq gap) is "suspect" until its book
        # restabilizes. Read by the execution gate via is_suspect().
        self._global_suspect_until: float = 0.0
        self._last_resnap_ts: float = 0.0   # throttle divergence-triggered resnapshots
        # Receive-loop profiling: is the handler keeping up? Reset when book_health() reads them.
        self._msg_count: int = 0
        self._handler_time: float = 0.0     # cumulative seconds inside _handle_message
        # Per-LEVEL observer (orderbook mode). None by default, so every existing caller pays nothing.
        self._level_observer: Optional[Callable] = None
        self._level_observer_errors: dict[str, int] = {}
        # Per-TRADE observer (the `trade` channel). None by default, and while it is None the channel
        # is NOT subscribed — so the arb path's socket and frames are byte-for-byte unchanged.
        self._trade_observer: Optional[Callable] = None
        self._trade_observer_errors: dict[str, int] = {}

    # ── Public price access ───────────────────────────────────────────────────

    def set_callback(self, callback: Callable) -> None:
        """Register a callable triggered on every meaningful price update."""
        self._on_update_callback = callback

    def set_level_observer(self, callback: Optional[Callable]) -> None:
        """Register a callable that sees every applied orderbook mutation, level by level.

        `set_callback` says only "something changed", which is all the arb path needs. The maker needs
        *which* price level moved and by how much, because "did that queue TRADE away or get CANCELLED
        away" is answered by pairing these deltas against the trade tape (the private design notes M23) — a caller
        polling the derived book cannot answer it, since an add and a remove cancel out.

        One dict per event:
          · `{"kind": "delta", "ticker", "side", "price", "delta", "qty_after"}` — `side` is the
            LADDER ("yes"/"no", both BID ladders per feed._derive), `delta` signed. All Decimal.
          · `{"kind": "snapshot", "ticker"}` — the whole book was replaced, so a consumer accumulating
            removals MUST treat its totals for that ticker as unreliable from here.

        Contract, in the order it matters:
        1. **The observer can never break the book.** It runs after the level is applied and any
           exception is caught and counted — an escape would cycle the WS connection.
        2. **Observation only.** Nothing here reads what the observer does.
        3. **Called inline on the socket.** Keep it cheap; a slow observer drops tape.
        """
        self._level_observer = callback

    @property
    def level_observer_errors(self) -> dict[str, int]:
        """Fault → count for the registered observer; empty when it has never raised. Exists so the
        swallow in `_notify_level` is REPORTABLE, and deliberately not folded into `book_health()`,
        which RESETS its counters on read."""
        return dict(self._level_observer_errors)

    def _notify_level(self, event: dict) -> None:
        """Deliver one level event, swallowing and counting any observer failure (contract 1)."""
        cb = self._level_observer
        if cb is None:
            return
        try:
            cb(event)
        except Exception as exc:                       # see set_level_observer
            key = f"{type(exc).__name__}: {str(exc)[:120]}"
            seen = self._level_observer_errors.get(key, 0)
            self._level_observer_errors[key] = seen + 1
            if seen == 0:                              # once per distinct fault, not per message
                log.warning(f"Kalshi WS: level observer raised — {key}")

    def set_trade_observer(self, callback: Optional[Callable]) -> None:
        """Register a callable that sees every PRINT on the venue's `trade` channel.

        ⛔ **OPT-IN, AND IT IS THE SUBSCRIPTION THAT IS OPT-IN.** The `trade` channel is added to its
        OWN `subscribe` command (id 2 → its own `sid`), sent only while an observer is registered, so
        a caller that never registers one sends exactly the frame it always sent. Register BEFORE
        `run_forever`, or call `resnapshot()` after. ⛔ **IT NEVER TOUCHES THE BOOK**: the handler
        builds one event and returns; a print that removed resting size arrives as its own delta.

        One dict per print: `{"kind": "trade", "ticker", "price_yes", "count", "taker_side",
        "venue_ts", "trade_id", "seq"}`.
          · `price_yes` / `count` — exact `Decimal` off `yes_price_dollars` / `count_fp`, or **None
            when the venue frame did not carry that spelling**. ⛔ Blank, never converted from a
            cents integer. ⛔ The size field on the wire is `count_fp`, NOT `count`.
          · `taker_side` — the venue's own token (`"yes"`/`"no"`), VERBATIM and never mapped onto
            BUY/SELL: the aggressor bought YES or bought NO, not "bought" in a slug's space.
          · `venue_ts` / `trade_id` — as sent, for de-duplication across a reconnect.
          · `seq` — THIS frame's own sequence number off ITS subscription. ⛔ Never the book's cursor.

        Same three contract points as `set_level_observer`.
        """
        self._trade_observer = callback

    @property
    def trade_observer_errors(self) -> dict[str, int]:
        """Fault → count for the trade observer. Cumulative; never reset on read."""
        return dict(self._trade_observer_errors)

    def _notify_trade(self, event: dict) -> None:
        """Deliver one trade event, swallowing and counting any observer failure."""
        cb = self._trade_observer
        if cb is None:
            return
        try:
            cb(event)
        except Exception as exc:                       # see set_trade_observer
            # ⛔ Keyed by exception TYPE, not by its message: a fault whose text carries a ticker would
            # mint a NEW key per print and turn a fault COUNTER into an unbounded dict.
            key = type(exc).__name__
            seen = self._trade_observer_errors.get(key, 0)
            self._trade_observer_errors[key] = seen + 1
            if seen == 0:                              # once per distinct fault, not per message
                log.warning(f"Kalshi WS: trade observer raised — {key}")

    def set_book_tickers(self, tickers: list[str]) -> None:
        """Set/refresh the orderbook subscription set (orderbook mode only). Resubscribes if the set
        changed and the socket is connected; no-op outside orderbook mode."""
        if not self._orderbook_mode:
            return
        new = sorted(set(tickers))
        if new == self._book_tickers:
            return
        self._book_tickers = new
        if self._ws is not None:
            asyncio.create_task(self._resubscribe())

    # ── THE EXACT GETTERS (Phase D step 2) ────────────────────────────────────────────────────────
    # `*_d` returns the internal Decimal verbatim and the float getters DELEGATE through them, so
    # there is ONE derivation path. Siblings rather than a flipped return type because the float
    # getters are shared with the STOPPED cross-arb bot, which cannot be smoke-tested.

    def get_depth_d(self, ticker: str, side: str = "yes") -> Optional[Decimal]:
        """Contracts available to BUY `side` at the touch (orderbook mode), EXACT, or None — lifted
        verbatim off the exact book, with no arithmetic to lose it in."""
        d = self._depth.get(ticker)
        if not d:
            return None
        return d.get(side)

    def get_depth(self, ticker: str, side: str = "yes") -> Optional[float]:
        """Contracts available to BUY `side` at the touch (orderbook mode), or None. FLOAT BOUNDARY
        (Phase D): a depth is compared to a size, never rounded across a money threshold, so the cast
        is safe here as it would not be for a price."""
        qty = self.get_depth_d(ticker, side)
        return None if qty is None else float(qty)

    def fillable_qty(self, ticker: str, side: str, limit_price: float) -> Optional[float]:
        """Contracts of `side` buyable at price <= limit_price from the maintained book (orderbook
        mode), or None if no book exists (ticker mode → caller skips the gate). Buying a side lifts
        the OPPOSING side's resting bids: a yes bid at p is a NO offer at (1-p), takeable for a NO buy
        when (1-p) <= limit. Depth AT OUR LIMIT, so it catches a best offer that moved above us."""
        book = self._books.get(ticker)
        if not book:
            return None
        opp = book.get("yes" if side == "no" else "no", {})
        # Exact threshold (Phase 2): a level priced exactly AT our limit must count as fillable.
        threshold = _ONE - from_float(limit_price)
        return float(sum((qty for px, qty in opp.items() if px >= threshold), _ZERO))

    def get_best_ask_d(self, ticker: str, side: str = "yes") -> Optional[Decimal]:
        """Best ask for the given side ("yes" or "no") as the EXACT cached Decimal, or None."""
        data = self._prices.get(ticker)
        if data is None:
            return None
        return data.yes_ask if side == "yes" else data.no_ask

    def get_best_bid_d(self, ticker: str, side: str = "yes") -> Optional[Decimal]:
        """Best bid for the given side ("yes" or "no") as the EXACT cached Decimal, or None — the
        venue's own number, with complements taken exactly (`1 − 0.56` = `0.44`), so a maker quote
        lands on the cent grid rather than a tick beside it."""
        data = self._prices.get(ticker)
        if data is None:
            return None
        return data.yes_bid if side == "yes" else data.no_bid

    def get_best_ask(self, ticker: str, side: str = "yes") -> Optional[float]:
        """Return best ask for the given side ("yes" or "no"), or None if unknown. FLOAT BOUNDARY
        (Phase D) — see get_best_bid for why this still returns float."""
        px = self.get_best_ask_d(ticker, side)
        return None if px is None else float(px)

    def get_best_bid(self, ticker: str, side: str = "yes") -> Optional[float]:
        """Return best bid for the given side ("yes" or "no"), or None if unknown. FLOAT BOUNDARY
        (Phase D): delegates to `get_best_bid_d` and floats, because the remaining external call sites
        are float end-to-end and Decimal would raise deep inside the arb fire path instead of here."""
        px = self.get_best_bid_d(ticker, side)
        return None if px is None else float(px)

    def get_age(self, ticker: str) -> Optional[float]:
        """Return seconds since this ticker's price was last written, or None if unknown."""
        data = self._prices.get(ticker)
        if data is None:
            return None
        return time.time() - data.last_updated

    def get_liquidity(
        self, ticker: str
    ) -> Optional[tuple[Optional[float], Optional[float], Optional[float], float]]:
        """Live (open_interest, volume, last_trade_px, age_s) from the ticker stream, or None if unseen
        (orderbook mode carries no ticker channel → blank, never stale). LOGGING-ONLY; OI only moves
        on trades, so a flat value can be current — stable ≠ frozen."""
        hit = self._liquidity.get(ticker)
        if hit is None:
            return None
        oi, vol, last_px, recv_ts = hit
        return oi, vol, last_px, time.time() - recv_ts

    def prime(self, ticker: str, yes_bid: float, yes_ask: float) -> None:
        """Seed the cache from REST for a ticker not yet seen on the WS; skips cached tickers."""
        if ticker in self._prices or yes_ask <= 0:
            return
        # from_float, not parse_wire: these genuinely arrive as floats — KalshiMarket.yes_bid/yes_ask
        # (scanner.py) are float-typed, so the wire string is already gone. A named lossy boundary.
        bid, ask = from_float(yes_bid), from_float(yes_ask)
        self._prices[ticker] = KalshiPriceData(
            yes_bid=bid,
            yes_ask=ask,
            no_bid=_ONE - ask,
            no_ask=_ONE - bid,
            last_updated=time.time(),
        )

    def refresh(self, ticker: str, yes_bid: float, yes_ask: float) -> None:
        """Update cache from REST data unless the WS has written within 10 seconds; WS is preferred."""
        if self._orderbook_mode:
            return  # orderbook book is the source of truth — don't clobber with the lagging
                    # markets-endpoint quote (the zombie watchdog + re-snapshot handle a dead book).
        if yes_ask <= 0:
            return
        existing = self._prices.get(ticker)
        if existing and (time.time() - existing.last_updated) < 10:
            return  # WS wrote recently — don't overwrite with stale REST price
        bid, ask = from_float(yes_bid), from_float(yes_ask)   # float in — see prime()
        self._prices[ticker] = KalshiPriceData(
            yes_bid=bid,
            yes_ask=ask,
            no_bid=_ONE - ask,
            no_ask=_ONE - bid,
            last_updated=time.time(),
        )

    # ── Message handling ──────────────────────────────────────────────────────

    def _matches_prefix(self, ticker: str) -> bool:
        return any(ticker.startswith(p) for p in self._prefixes)

    # ── Orderbook maintenance (orderbook mode) ────────────────────────────────

    @staticmethod
    def _levels_to_dict(levels) -> dict[Decimal, Decimal]:
        """Wire levels → an EXACT Decimal book (Phase 2): a price is its own exact key, so two quotes
        of "0.55" can never land in two dict slots, and a qty is exactly what the venue said.
        `is_zero` remains as a guard against genuinely sub-resolution venue data."""
        out: dict[Decimal, Decimal] = {}
        for lvl in levels or []:
            try:
                px, qty = parse_wire(lvl[0]), parse_wire(lvl[1])
            except (TypeError, ValueError, IndexError, InvalidOperation):
                continue
            # ⚠️ is_finite() FIRST. `Decimal("NaN") > 0` RAISES InvalidOperation rather than
            # returning False, outside the try above, cycling the WS connection.
            if not (px.is_finite() and qty.is_finite()):
                continue
            if qty > 0 and not is_zero(qty, _QTY_STEP):
                out[px] = qty
        return out

    def _check_seq(self, data: dict) -> None:
        """Track the seq PER SUBSCRIPTION (`sid`); a real forward gap (missed messages) means the book
        may be stale → throttled warn + resubscribe (resnapshot). Resets cleanly on (re)subscribe.

        ⛔ **KEYED BY `sid`, NOT BY THE SOCKET** [2026-09-03]. This process may hold TWO subscriptions
        on one socket; a shared counter would read every print as a MISSED BOOK MESSAGE, and
        `mark_all_suspect` is unthrottled, so the tape would go all-`suspect` all night.
        ⚠️ **UNRESOLVED FROM OUR OWN EVIDENCE:** no captured mixed-channel frame exists in this repo,
        so per-sid vs socket-wide is NOT verified. If socket-wide, the first night shows it
        unmistakably — do not read it as a venue outage. `_seq_global` stays the BOOK cursor."""
        seq = data.get("seq")
        if not isinstance(seq, int):
            return
        sid = data.get("sid")
        key = sid if isinstance(sid, int) else -1
        last = self._seq_by_sid.get(key)
        self._seq_by_sid[key] = seq
        if last is not None and seq > last + 1:
            self._gap_count += 1
            # A dropped message on the shared stream can corrupt ANY book → all suspect.
            self.mark_all_suspect(config.KALSHI_BOOK_SUSPECT_SECONDS)
            now = time.time()
            if now - self._last_gap_action > 15.0:
                self._last_gap_action = now
                # DEBUG (not WARNING): benign — the resnapshot self-heals it and the gap rate is
                # ~0.006%. Raise LOG_LEVEL=DEBUG to see gaps.
                log.debug(f"Kalshi book seq gap {last}→{seq} — resubscribing (resnapshot)")
                asyncio.create_task(self._resubscribe())
        self._seq_global = seq

    def _note_price_change(self, ticker: str, pd: KalshiPriceData) -> None:
        """Write a ticker's price (ALWAYS) and stamp _last_book_change_ts ONLY when the tradeable
        top-of-book actually moves — identical re-sends and liquidity-only ticks are NOT changes (the
        frozen-resend zombie signature). Keys on (yes_bid, yes_ask), which fully determine the book.
        REST writes bypass it on purpose."""
        old = self._prices.get(ticker)
        if old is None or (old.yes_bid, old.yes_ask) != (pd.yes_bid, pd.yes_ask):
            self._book_changes += 1
            self._last_book_change_ts = time.time()
        self._prices[ticker] = pd

    def _derive(self, ticker: str) -> None:
        """Recompute executable prices + touch depth from the maintained book and
        write them into _prices (same shape detection already reads)."""
        book = self._books.get(ticker)
        if not book:
            return
        yes = book.get("yes", {})
        no = book.get("no", {})
        # Decimal keys (Phase 2): max() is exact and 1 - Decimal("0.55") is exactly Decimal("0.45").
        best_yes_bid = max(yes) if yes else _ZERO
        best_no_bid = max(no) if no else _ZERO
        # To BUY a side you cross the opposing side's best bid: buy-yes hits the no
        # bids (no bid p ⇒ yes offered at 1-p); buy-no hits the yes bids.
        # Phase D: no float() here — the cache is Decimal, so the complement stays exact to the getter.
        self._note_price_change(ticker, KalshiPriceData(
            yes_bid=best_yes_bid,
            yes_ask=(_ONE - best_no_bid) if no else _ONE,
            no_bid=best_no_bid,
            no_ask=(_ONE - best_yes_bid) if yes else _ONE,
            last_updated=time.time(),
        ))
        self._depth[ticker] = {
            "yes": no.get(best_no_bid, _ZERO),   # size available to buy YES
            "no": yes.get(best_yes_bid, _ZERO),  # size available to buy NO
        }
        # DIAGNOSTIC: a CROSSED book (best_yes_bid + best_no_bid > 1) violates no-arb and makes the
        # asks above phantom-cheap. ENTER (raw top levels) + EXIT+duration; 1.01 skips flicker.
        _xsum = best_yes_bid + best_no_bid
        if yes and no and _xsum > 1.01:
            if ticker not in self._crossed:
                self._crossed[ticker] = time.time()
                # float() the top levels for the LOG only: the recorded (0.65, 5.68e-14) shape is
                # quoted verbatim in the docs that root-caused KALSHI_CROSSED.
                _yt = [(float(p), float(q)) for p, q in sorted(yes.items(), reverse=True)[:3]]
                _nt = [(float(p), float(q)) for p, q in sorted(no.items(), reverse=True)[:3]]
                log.info("KALSHI_CROSSED enter %s sum=%.3f yes_bid=%.3f no_bid=%.3f "
                         "yes_top=%s no_top=%s nyes=%d nno=%d",
                         ticker, _xsum, best_yes_bid, best_no_bid, _yt, _nt, len(yes), len(no))
        elif ticker in self._crossed:
            log.info("KALSHI_CROSSED exit  %s after %.1fs",
                     ticker, time.time() - self._crossed.pop(ticker))

    async def _handle_message(self, message: str) -> None:
        """Parse a WebSocket message and update the cache. Handles the ticker
        channel (default) and orderbook_snapshot/delta (orderbook mode)."""
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        self._last_msg_ts = time.time()  # any message proves the socket is alive

        mtype = data.get("type")
        msg = data.get("msg", {})
        ticker = msg.get("market_ticker")
        if not ticker or not self._matches_prefix(ticker):
            return

        if mtype == "orderbook_snapshot":
            self._check_seq(data)
            self._books[ticker] = {
                "yes": self._levels_to_dict(msg.get("yes_dollars_fp")),
                "no": self._levels_to_dict(msg.get("no_dollars_fp")),
            }
            self._derive(ticker)
            # A wholesale replacement, so any per-level total a consumer has been accumulating for
            # this ticker spans a hole it cannot see. Announced rather than inferred.
            if self._level_observer is not None:
                self._notify_level({"kind": "snapshot", "ticker": ticker})
        elif mtype == "orderbook_delta":
            self._check_seq(data)
            book = self._books.get(ticker)
            if book is None:
                return  # no snapshot yet — wait for it
            try:
                px = parse_wire(msg["price_dollars"])
                delta = parse_wire(msg["delta_fp"])
                side = msg["side"]
            except (KeyError, TypeError, ValueError, InvalidOperation):
                return
            # `delta` must be finite: a NaN propagates through the addition and RAISES on `qty > 0`,
            # outside any try; `Infinity` compares True and installs a level of infinite depth.
            if not (px.is_finite() and delta.is_finite()):
                return
            levels = book.get(side)
            if levels is None:
                return
            # ⚠️ THIS LINE IS THE CROSSED-BOOK BUG SITE. In float, a fully-removed level left ~1e-13
            # of dust; `qty > 0` kept the ghost, `max(price)` selected it as best-bid, the book
            # crossed → phantom fat edges (30/52 would-fires). In exact Decimal it lands on zero.
            qty_before = levels.get(px, _ZERO)
            qty = qty_before + delta
            # is_finite() guard as in _levels_to_dict: a NaN delta propagates through the addition and
            # then RAISES on the comparison, which would tear down the WS connection.
            if qty.is_finite() and qty > 0 and not is_zero(qty, _QTY_STEP):
                levels[px] = qty
            else:
                levels.pop(px, None)
                qty = _ZERO      # what the level actually IS now — report that, not the dust
            self._derive(ticker)
            if self._level_observer is not None:
                # `qty_before` as well as the raw venue `delta`, because they can DISAGREE: a level
                # falling below `_QTY_STEP` is popped to zero, so the book moved by the difference.
                self._notify_level({"kind": "delta", "ticker": ticker, "side": side,
                                    "price": px, "delta": delta,
                                    "qty_before": qty_before, "qty_after": qty})
        elif mtype == "trade":
            # ⛔ NO BOOK MUTATION HERE. A print is reported; the size it removed arrives as its own
            # `orderbook_delta`. This branch builds one event and returns.
            if self._trade_observer is not None:
                self._notify_trade({
                    "kind": "trade",
                    "ticker": ticker,
                    # ⛔ ONE KEY EACH, AND ONLY KEYS UNAMBIGUOUS IN YES SPACE. `price_dollars` is NOT
                    # accepted: on the trade channel a side-relative price is the TAKER's side, so it
                    # would write a NO price into a YES column on every no-taker print.
                    "price_yes": _wire_or_none(msg, ("yes_price_dollars",)),
                    # ⛔ `count_fp` IS THE VENUE'S SPELLING, and the plain `count` was never on the
                    # wire: reading it wrote `quantity` BLANK on all 37,722 rows of
                    # logs/kalshi_trades.csv (night one, 2026-09-08). Same spelling the trade tape
                    # feed reads [VERIFIED bot/kalshi/trade_feed.py:_handle], and the same `_fp`
                    # family as the fills API's `fill_count_fp`.
                    "count": _wire_or_none(msg, ("count_fp",)),
                    "taker_side": str(msg.get("taker_side") or ""),
                    "venue_ts": msg.get("ts"),
                    "trade_id": str(msg.get("trade_id") or ""),
                    # THIS frame's own seq, off its own subscription — never the book cursor.
                    "seq": data.get("seq"),
                })
        elif mtype == "ticker":
            # Liquidity/activity (LOGGING-ONLY) from the SAME ticker msg, captured FIRST and ISOLATED:
            # before the bid/ask early-return, in its own swallowing try writing only _liquidity, so a
            # malformed liquidity field can NEVER block the load-bearing bid/ask write.
            try:
                self._liquidity[ticker] = (
                    float(msg["open_interest_fp"]),
                    float(msg["volume_fp"]),
                    float(msg["price_dollars"]),
                    time.time(),
                )
            except (KeyError, TypeError, ValueError):
                pass  # absent/garbled → leave prior (or absent → get_liquidity blank); never raise

            # ── bid/ask price path (UNCHANGED — the fire decision rides on this) ──
            yes_bid_raw = msg.get("yes_bid_dollars")
            yes_ask_raw = msg.get("yes_ask_dollars")
            if yes_bid_raw is None or yes_ask_raw is None:
                return
            # parse_wire, not float(): these are decimal STRINGS ("0.4500"). Two guards come with it:
            # an unparseable field used to RAISE outside any try and cycle the WS connection
            # mid-game, and "NaN"/"Infinity" parsed happily into the cache the fire path reads.
            try:
                yes_bid = parse_wire(yes_bid_raw)
                yes_ask = parse_wire(yes_ask_raw)
            except (TypeError, ValueError, InvalidOperation):
                log.debug(f"ticker {ticker}: unparseable price dropped "
                          f"(bid={yes_bid_raw!r} ask={yes_ask_raw!r}) — prior quote stands")
                return
            if not (yes_bid.is_finite() and yes_ask.is_finite()
                    and _ZERO <= yes_bid <= _ONE and _ZERO <= yes_ask <= _ONE):
                # Non-finite OR outside [0,1]: not a price on this venue ("5.0000" is as wrong as
                # "NaN") — don't cache it; the prior quote stands and ages into the freshness gates.
                log.debug(f"ticker {ticker}: out-of-domain price dropped "
                          f"(bid={yes_bid} ask={yes_ask}) — prior quote stands")
                return
            self._note_price_change(ticker, KalshiPriceData(
                yes_bid=yes_bid,
                yes_ask=yes_ask,
                no_bid=_ONE - yes_ask,
                no_ask=_ONE - yes_bid,
                last_updated=time.time(),
            ))
        else:
            return

        if self._on_update_callback:
            self._on_update_callback()

    # ── WebSocket loop ────────────────────────────────────────────────────────

    # Force a reconnect only if NO data frame arrives this long. A genuinely dead socket is already
    # caught by the library's ping_interval=20/ping_timeout=20, so this guard exists only for a
    # silently-dead SUBSCRIPTION (socket alive, server stopped sending). 45s was too aggressive:
    # thin overnight markets go quiet for >45s, storming reconnects (TLS churn → RSS growth → OOM).
    _STALE_RECONNECT_S = 180

    # ── Frozen-book / data-freshness watchdog (the SECOND timer) ────────────────
    # _STALE_RECONNECT_S above = dead STREAM (no message at all). THIS one = frozen BOOK: messages
    # keep arriving while the tradeable top-of-book never moves — the zombie that feeds phantom
    # edges into detection. Keyed on _last_book_change_ts.
    # THRESHOLD — a PLACEHOLDER, not derived: the QUIET-SLATE false-positive floor. "Slow for an
    # in-game freeze" BY DESIGN, tolerable only because the fire path re-checks fresh REST. 240 =
    # the dead-stream 180s + margin (book-CHANGE gaps are strictly LONGER than message gaps).
    _FRESHNESS_RECONNECT_S = 240
    _FRESHNESS_RECONNECT_COOLDOWN_S = 60   # min gap between forced reconnects (anti-storm)

    async def _send_subscribe(self, ws) -> None:
        """Send the channel subscription for the active price source."""
        self._seq_global = None  # new subscription → seq stream restarts
        self._seq_by_sid.clear()
        # Drop cached liquidity on every (re)subscribe: a mode switch or resubscribe must blank
        # get_liquidity rather than serve a value frozen at the last ticker-mode tick.
        self._liquidity.clear()
        if self._orderbook_mode:
            params = {"channels": ["orderbook_delta"]}
            if self._book_tickers:
                params["market_tickers"] = self._book_tickers
            await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": params}))
            log.info(f"Kalshi WS: subscribed orderbook_delta ({len(self._book_tickers)} tickers)")
            # ⛔ THE TRADE CHANNEL IS ITS OWN `subscribe` COMMAND (id 2 → its own `sid`), never a
            # second channel on the book's frame, and only while an observer is registered — the book
            # seq cursor is keyed by sid, so prints cannot be read as missed book messages.
            if self._trade_observer is not None:
                trade_params: dict = {"channels": ["trade"]}
                if self._book_tickers:
                    trade_params["market_tickers"] = self._book_tickers
                await ws.send(json.dumps({"id": 2, "cmd": "subscribe", "params": trade_params}))
                log.info(f"Kalshi WS: subscribed trade ({len(self._book_tickers)} tickers)")
        else:
            await ws.send(json.dumps({"id": 1, "cmd": "subscribe",
                                      "params": {"channels": ["ticker"]}}))
            log.info("Kalshi WS: subscribed to ticker channel")

    def divergent_from_rest(
        self, rest_map: dict[str, tuple[float, float]], tol: float
    ) -> list[tuple[str, float, float]]:
        """Tickers whose WS-maintained best yes-bid disagrees with the REST book by more than `tol` —
        a sign the delta stream left a stale level behind. Returns (ticker, ws_yes_bid, rest_yes_bid)
        per offender; orderbook mode only, and the caller resnapshots when non-empty."""
        if not self._orderbook_mode:
            return []
        out: list[tuple[str, float, float]] = []
        for ticker, (rest_yes_bid, _rest_yes_ask) in rest_map.items():
            data = self._prices.get(ticker)
            if data is None:
                continue
            # data.yes_bid is Decimal, rest_yes_bid a float from the REST scanner. ⚠️ `>=`, not `>`:
            # the caller GATES a resnapshot on this, and at exact tolerance flagging is the cheap,
            # safe direction.
            if abs(data.yes_bid - from_float(rest_yes_bid)) >= from_float(tol):
                out.append((ticker, float(data.yes_bid), rest_yes_bid))
        return out

    async def resnapshot(self) -> bool:
        """Force a fresh orderbook snapshot for all book tickers (re-subscribe), flushing stale levels
        the delta stream left behind. THROTTLED to once per KALSHI_RESNAP_THROTTLE_SECONDS, so a
        chronically-divergent ticker cannot storm; it stays suspect per-ticker instead."""
        if not self._orderbook_mode:
            return False
        now = time.time()
        if now - self._last_resnap_ts < config.KALSHI_RESNAP_THROTTLE_SECONDS:
            return False
        self._last_resnap_ts = now
        self._resnap_count += 1
        # Book is being rebuilt — don't trade off it until the fresh snapshot lands.
        self.mark_all_suspect(config.KALSHI_BOOK_SUSPECT_SECONDS)
        await self._resubscribe()
        return True

    def mark_all_suspect(self, seconds: float) -> None:
        """Flag ALL tickers suspect for `seconds` (e.g. a global seq gap / resnapshot —
        a dropped message can corrupt any book on the shared stream)."""
        self._global_suspect_until = max(self._global_suspect_until, time.time() + seconds)

    def is_suspect(self, ticker: str) -> bool:
        """True if this ticker's book is currently untrustworthy (recent gap/divergence/
        resnapshot). The execution gate refuses to trade a suspect ticker."""
        now = time.time()
        return now < self._global_suspect_until

    def book_health(self) -> dict:
        """Compact orderbook-maintenance snapshot for the periodic health log. Reads and
        RESETS the receive-loop profiling counters, so each call reports its interval."""
        h = {
            "books": len(self._books),
            "seq": self._seq_global,
            "gaps": self._gap_count,
            "resnaps": self._resnap_count,
            "msgs": self._msg_count,
            "handler_s": self._handler_time,
            "changes": self._book_changes - self._book_changes_anchor,  # real top-of-book moves
        }                                                               # this interval (socket-wide)
        self._msg_count = 0
        self._handler_time = 0.0
        self._book_changes_anchor = self._book_changes
        return h

    async def _resubscribe(self) -> None:
        """Re-send the orderbook subscription after the ticker set changes."""
        if self._ws is None:
            return
        try:
            await self._send_subscribe(self._ws)
        except Exception as e:
            log.warning(f"Kalshi WS: resubscribe failed: {e}")

    async def run_forever(self) -> None:
        """Connect to Kalshi WebSocket and maintain the price cache indefinitely."""
        while True:
            self.subscriptions_ready = False
            self._ws = None
            try:
                ws_url = self._client.ws_url
                log.info(f"Kalshi WS: connecting to {ws_url}")
                async with websockets.connect(
                    ws_url,
                    additional_headers=self._client.ws_headers(),
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    log.info("Kalshi WS: connected")
                    self._ws = ws
                    await self._send_subscribe(ws)
                    self.subscriptions_ready = True
                    self._last_msg_ts = time.time()
                    self._last_book_change_ts = time.time()  # baseline so the freshness watchdog
                    # doesn't fire before the first book change on a freshly-connected socket

                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=10.0)
                            _t0 = time.perf_counter()
                            await self._handle_message(msg)
                            self._handler_time += time.perf_counter() - _t0
                            self._msg_count += 1
                            # Diagnostic only (default off): reuse _t0 (captured before
                            # parse+cache+callback) for the receive-loop timing CSV.
                            if ws_timer.enabled:
                                ws_timer.record_message("kalshi", _t0)
                        except asyncio.TimeoutError:
                            pass  # keepalive handled by ping_interval
                        # Dead-STREAM guard: SDK keeps is_connected True on a dead socket;
                        # force a reconnect if no message at all arrives for a while.
                        if time.time() - self._last_msg_ts > self._STALE_RECONNECT_S:
                            log.warning(
                                f"Kalshi WS: no messages for {self._STALE_RECONNECT_S}s "
                                f"— forcing reconnect"
                            )
                            break
                        # Frozen-BOOK guard: messages arriving but no real top-of-book change —
                        # the zombie the dead-stream check cannot see. Socket-wide, cooldown-bounded.
                        now = time.time()
                        if _stale_book_reconnect(now, self._last_book_change_ts, len(self._prices),
                                                 self._last_forced_reconnect,
                                                 self._FRESHNESS_RECONNECT_S,
                                                 self._FRESHNESS_RECONNECT_COOLDOWN_S):
                            log.warning(
                                f"Kalshi WS: no real book change for {self._FRESHNESS_RECONNECT_S}s "
                                f"across {len(self._prices)} ticker(s) — frozen/zombie, forcing reconnect"
                            )
                            self._last_forced_reconnect = now
                            break

            except websockets.exceptions.ConnectionClosed as e:
                # Routine server cycle (1001) / network drop (1006 no-close-frame) —
                # not a bug, reconnect quietly. ERROR stays for real failures below.
                self.subscriptions_ready = False
                log.info(f"Kalshi WS closed by server ({e}). Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                self.subscriptions_ready = False
                log.error(f"Kalshi WS error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
