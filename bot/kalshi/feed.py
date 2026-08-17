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
  yes_bid, yes_ask, no_bid, no_ask — all floats in [0.0, 1.0]
  no_bid = 1 - yes_ask   (complementary market identity)
  no_ask = 1 - yes_bid
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
from bot.core.money import complement, from_float, is_zero, parse_wire
from bot.core.feed_health import _stale_book_reconnect
from bot.core.logger import get_logger
from bot.core.ws_timing import ws_timer

log = get_logger(__name__)

# The book is maintained in EXACT Decimal, so delta arithmetic no
# longer CREATES the ~1e-13 dust that produced the KALSHI_CROSSED bug (a removed level left residue,
# `qty > 0` kept the ghost, max(price) selected it, the book crossed, and the phantom-cheap ask became a
# phantom fat edge). `_QTY_STEP` is retained not to sweep our own arithmetic — there is none to sweep —
# but as a resolution floor on VENUE data: anything below wire precision is not real depth. Kalshi qty is
# 2-dp on the wire (326.89), so 4dp is wire precision plus a two-decimal buffer.
_QTY_STEP = Decimal("0.0001")   # 4 dp = wire precision (2 dp) + a two-decimal buffer
_ZERO = Decimal(0)
_ONE = Decimal(1)


@dataclass
class KalshiPriceData:
    yes_bid: float = 0.0
    yes_ask: float = 1.0
    no_bid:  float = 0.0
    no_ask:  float = 1.0
    last_updated: float = field(default_factory=time.time)


class KalshiOrderBookCache:
    """
    Maintains real-time Kalshi prices from the WebSocket `ticker` channel (and, in
    orderbook mode, a full `orderbook_delta` book with the seq-gap resnapshot machinery).
    """

    def __init__(self, client, series_prefixes: list[str] | None = None) -> None:
        """
        Args:
            client:           KalshiClient instance (provides ws_url and ws_headers())
            series_prefixes:  List of ticker prefixes to cache, e.g. ["KXNBAGAME", "KXNHLGAME"].
                              If None, falls back to config.KALSHI_SERIES.
        """
        self._client = client
        self._prefixes = series_prefixes if series_prefixes is not None else config.KALSHI_SERIES
        self._prices: dict[str, KalshiPriceData] = {}
        # Liquidity/activity from the SAME ticker messages (open_interest_fp, volume_fp,
        # last-trade price) — LOGGING-ONLY (phantom-vs-real context), kept in its own dict so the
        # bid/ask price path above is never perturbed. ticker -> (oi, volume, last_trade, ts).
        # Cleared on (re)subscribe so a mode switch (ticker→orderbook, no ticker stream) blanks
        # rather than serving a frozen value. NOTE: only fed in ticker mode (the ticker channel).
        self._liquidity: dict[str, tuple[float | None, float | None, float | None, float]] = {}
        self._on_update_callback: Optional[Callable] = None
        self.subscriptions_ready = False

        # Orderbook mode (KALSHI_PRICE_SOURCE="orderbook"): maintain the real book
        # per ticker and derive executable prices/depth from it (kills the phantom
        # edges the lagging ticker quote produces). _books[ticker][side] = {px: qty}.
        self._orderbook_mode = config.KALSHI_PRICE_SOURCE == "orderbook"
        # Decimal keys AND values (migration Phase 2) — exact, so a price is its own key and delta
        # arithmetic cannot create dust. float() conversion happens only at the accessors.
        self._books: dict[str, dict[str, dict[Decimal, Decimal]]] = {}
        self._depth: dict[str, dict[str, float]] = {}   # ticker -> {yes,no} touch depth
        self._book_tickers: list[str] = []               # orderbook subscription set
        # DIAGNOSTIC: ticker -> ts it entered a CROSSED book (best_yes_bid +
        # best_no_bid > 1, a no-arb violation that makes _derive's yes_ask = 1-best_no_bid
        # phantom-cheap → the suspected phantom-edge source on the fire path). Root-causing
        # whether a stale level causes it; logs enter/exit + raw top levels. Observability only.
        self._crossed: dict[str, float] = {}
        # Kalshi seq is a SINGLE monotonic counter across the whole subscription
        # (not per-market) — track it globally; reset to None on (re)subscribe.
        self._seq_global: int | None = None
        self._last_gap_action: float = 0.0               # throttle gap warn/resnapshot
        self._ws = None                                   # live socket (for resubscribe)
        self._last_msg_ts: float = 0.0                    # dead-stream watchdog (ANY message)
        # Data-freshness / frozen-book watchdog — a SEPARATE concern from _last_msg_ts above.
        # Advances ONLY on a real WS top-of-book change (see _note_price_change), so it catches the
        # frozen-resend zombie (ticker frames arriving, tradeable price frozen) that _last_msg_ts
        # (any message — incl. OI/volume-only ticks + library heartbeats) cannot. Mirrors the
        # poly_us feed's _last_book_change_ts. REST writes (prime/refresh) deliberately do NOT touch
        # it — else the 10s REST refresh loop would mask a frozen WS book and defeat the watchdog.
        self._last_book_change_ts: float = 0.0
        self._last_forced_reconnect: float = 0.0          # anti-storm cooldown anchor for the above
        self._book_changes: int = 0                       # cumulative real WS top-of-book changes
        self._book_changes_anchor: int = 0                # book_health() reads the per-interval delta
        # to surface changes/min (the empirical basis for tuning _FRESHNESS_RECONNECT_S post-deploy)
        # Health counters (cumulative, for the periodic ORDERBOOK HEALTH log).
        self._gap_count: int = 0                          # seq gaps detected
        self._resnap_count: int = 0                       # divergence-triggered resnapshots
        # Book-trust gate: a ticker (or all of them, on a global seq gap) is "suspect"
        # — not safe to trade — until its book restabilizes. Set on gap/divergence/
        # resnapshot; read by the execution gate via is_suspect().
        self._global_suspect_until: float = 0.0
        self._last_resnap_ts: float = 0.0   # throttle divergence-triggered resnapshots
        # Receive-loop profiling (Phase 3): is the handler keeping up with the stream?
        # Reset each time book_health() reads them, so the health log shows the interval.
        self._msg_count: int = 0
        self._handler_time: float = 0.0     # cumulative seconds inside _handle_message
        # Per-LEVEL observer (orderbook mode). None by default, so every existing caller — the arb
        # bot included — pays nothing for it. See set_level_observer.
        self._level_observer: Optional[Callable] = None
        self._level_observer_errors: dict[str, int] = {}

    # ── Public price access ───────────────────────────────────────────────────

    def set_callback(self, callback: Callable) -> None:
        """Register a callable triggered on every meaningful price update."""
        self._on_update_callback = callback

    def set_level_observer(self, callback: Optional[Callable]) -> None:
        """Register a callable that sees every applied orderbook mutation, level by level.

        `set_callback` fires after a message with no argument — it says "something changed", which
        is all the arb path needs. The maker needs strictly more: *which* price level moved and by
        how much, because the queue ahead of a resting quote is a single level and the question
        "did that queue TRADE away or get CANCELLED away" is answered by pairing these deltas
        against the trade tape. A caller that only polls the derived book cannot
        answer it: an add and a remove between two polls cancel out.

        The callback receives one dict per event:
          · `{"kind": "delta", "ticker", "side", "price", "delta", "qty_after"}` — `side` is the
            LADDER ("yes"/"no", both BID ladders per feed._derive), `price` its own key on that
            ladder, `delta` signed, `qty_after` the level after applying. All Decimal.
          · `{"kind": "snapshot", "ticker"}` — the whole book was replaced. A consumer accumulating
            removals MUST treat its totals for that ticker as unreliable from here: a snapshot
            arrives on subscribe AND on every gap-triggered resnapshot, and the deltas it skipped
            are exactly the ones nobody saw.

        Contract, in the order it matters:

        1. **The observer can never break the book.** It is called after the level is applied, and
           any exception is caught and counted, never propagated. An escape here would reach the
           receive loop and cycle the WS connection — the same hazard the non-finite guards below
           exist to prevent, and this file feeds the arb fire path.
        2. **Observation only.** Nothing in this class reads what the observer does, so a wrong
           observer produces a wrong LOG and nothing else.
        3. **Called inline on the socket.** Keep it cheap; a slow observer drops tape.
        """
        self._level_observer = callback

    @property
    def level_observer_errors(self) -> dict[str, int]:
        """Fault → count for the registered observer. Empty when it has never raised.

        Exists so the swallow in `_notify_level` is REPORTABLE. A caught exception that nothing can
        read is the anti-pattern this codebase keeps re-learning: a health flag no caller checks is
        not a guard.
        Deliberately not folded into `book_health()`, which RESETS its counters on read — this is a
        cumulative fault record, and a reader should not have to clear it to see it."""
        return dict(self._level_observer_errors)

    def _notify_level(self, event: dict) -> None:
        """Deliver one level event, swallowing and counting any observer failure (contract 1)."""
        cb = self._level_observer
        if cb is None:
            return
        try:
            cb(event)
        except Exception as exc:                       # noqa: BLE001 — see set_level_observer
            key = f"{type(exc).__name__}: {str(exc)[:120]}"
            seen = self._level_observer_errors.get(key, 0)
            self._level_observer_errors[key] = seen + 1
            if seen == 0:                              # once per distinct fault, not per message
                log.warning(f"Kalshi WS: level observer raised — {key}")

    def set_book_tickers(self, tickers: list[str]) -> None:
        """Set/refresh the orderbook subscription set (orderbook mode only).

        Resubscribes if the set changed and the socket is connected — mirrors
        poly_us_feed.resubscribe(). No-op outside orderbook mode.
        """
        if not self._orderbook_mode:
            return
        new = sorted(set(tickers))
        if new == self._book_tickers:
            return
        self._book_tickers = new
        if self._ws is not None:
            asyncio.create_task(self._resubscribe())

    def get_depth(self, ticker: str, side: str = "yes") -> Optional[float]:
        """Contracts available to BUY `side` at the touch (orderbook mode), or None."""
        d = self._depth.get(ticker)
        if not d:
            return None
        return d.get(side)

    def fillable_qty(self, ticker: str, side: str, limit_price: float) -> Optional[float]:
        """Contracts of `side` buyable at price <= limit_price from the maintained
        book (orderbook mode), or None if no book exists (ticker mode → caller
        skips the gate).

        Buying a side lifts the OPPOSING side's resting bids: a yes bid at price p
        is a NO offer at (1-p), takeable for a NO buy when (1-p) <= limit i.e.
        p >= 1-limit. (Symmetric for a YES buy against no bids.) This is the depth
        actually available AT OUR LIMIT — not just at the touch — so it catches the
        case where the real best offer has moved above our price (0 fillable).
        """
        book = self._books.get(ticker)
        if not book:
            return None
        opp = book.get("yes" if side == "no" else "no", {})
        # Exact threshold (Phase 2): the old round(1.0 - limit_price, 6) guarded a boundary comparison —
        # a level priced exactly AT our limit must count as fillable, and float noise could drop it.
        threshold = _ONE - from_float(limit_price)
        return float(sum((qty for px, qty in opp.items() if px >= threshold), _ZERO))

    def get_best_ask(self, ticker: str, side: str = "yes") -> Optional[float]:
        """Return best ask for the given side ("yes" or "no"), or None if unknown."""
        data = self._prices.get(ticker)
        if data is None:
            return None
        return data.yes_ask if side == "yes" else data.no_ask

    def get_best_bid(self, ticker: str, side: str = "yes") -> Optional[float]:
        """Return best bid for the given side ("yes" or "no"), or None if unknown."""
        data = self._prices.get(ticker)
        if data is None:
            return None
        return data.yes_bid if side == "yes" else data.no_bid

    def get_age(self, ticker: str) -> Optional[float]:
        """Return seconds since this ticker's price was last written, or None if unknown."""
        data = self._prices.get(ticker)
        if data is None:
            return None
        return time.time() - data.last_updated

    def get_liquidity(
        self, ticker: str
    ) -> Optional[tuple[Optional[float], Optional[float], Optional[float], float]]:
        """Live (open_interest, volume, last_trade_px, age_s) from the ticker stream, or None if
        this ticker hasn't been seen (incl. orderbook mode, which carries no ticker channel → the
        cache stays empty → blank, never a stale value). LOGGING-ONLY; reads its own dict so it's
        independent of the price path. age_s = now − receive time of the last ticker update; the
        consumer judges staleness from it (OI only moves on trades, so a flat value can be
        genuinely current — stable ≠ frozen — which is why this stamps rather than hard-blanks)."""
        hit = self._liquidity.get(ticker)
        if hit is None:
            return None
        oi, vol, last_px, recv_ts = hit
        return oi, vol, last_px, time.time() - recv_ts

    def prime(self, ticker: str, yes_bid: float, yes_ask: float) -> None:
        """Seed the cache from REST data for a ticker not yet seen on the WS.
        Skips tickers already cached (WS data is more current than REST).
        """
        if ticker in self._prices or yes_ask <= 0:
            return
        self._prices[ticker] = KalshiPriceData(
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=complement(yes_ask),
            no_ask=complement(yes_bid),
            last_updated=time.time(),
        )

    def refresh(self, ticker: str, yes_bid: float, yes_ask: float) -> None:
        """Update cache from REST data unless the WS has written within 10 seconds.
        Used by the periodic price-refresh loop; WS data is always preferred.
        """
        if self._orderbook_mode:
            return  # orderbook book is the source of truth — don't clobber with
                    # the lagging markets-endpoint quote (the zombie watchdog +
                    # re-snapshot handle a dead book instead).
        if yes_ask <= 0:
            return
        existing = self._prices.get(ticker)
        if existing and (time.time() - existing.last_updated) < 10:
            return  # WS wrote recently — don't overwrite with stale REST price
        self._prices[ticker] = KalshiPriceData(
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=complement(yes_ask),
            no_ask=complement(yes_bid),
            last_updated=time.time(),
        )

    # ── Message handling ──────────────────────────────────────────────────────

    def _matches_prefix(self, ticker: str) -> bool:
        return any(ticker.startswith(p) for p in self._prefixes)

    # ── Orderbook maintenance (orderbook mode) ────────────────────────────────

    @staticmethod
    def _levels_to_dict(levels) -> dict[Decimal, Decimal]:
        """Wire levels → an EXACT Decimal book (Decimal Phase 2).

        Parsing straight to Decimal (rather than float-then-round) means a price is its own exact key —
        two quotes of "0.55" can never land in two dict slots — and a qty is exactly what the venue
        said. The old `round(qty, 4)` existed only to sweep float dust that exact arithmetic
        never creates; `is_zero` is kept as a cheap guard against genuinely sub-resolution venue data."""
        out: dict[Decimal, Decimal] = {}
        for lvl in levels or []:
            try:
                px, qty = parse_wire(lvl[0]), parse_wire(lvl[1])
            except (TypeError, ValueError, IndexError, InvalidOperation):
                continue
            # ⚠️ is_finite() FIRST. Decimal parses "NaN"/"Infinity" happily (a float would too), but
            # `Decimal("NaN") > 0` RAISES InvalidOperation rather than returning False — and this runs
            # outside the try above, so it would escape to the receive loop and cycle the whole Kalshi
            # WS connection mid-game. float("nan") > 0 was simply False, dropping the level. Preserve
            # that: a non-finite quantity is not depth, so drop it quietly.
            if not (px.is_finite() and qty.is_finite()):
                continue
            if qty > 0 and not is_zero(qty, _QTY_STEP):
                out[px] = qty
        return out

    def _check_seq(self, data: dict) -> None:
        """Track the subscription-wide seq; a real forward gap (missed messages)
        means the book may be stale → throttled warn + resubscribe (resnapshot).
        Resets cleanly on (re)subscribe (seq restarts) — no false alarms."""
        seq = data.get("seq")
        if not isinstance(seq, int):
            return
        last = self._seq_global
        if last is not None and seq > last + 1:
            self._gap_count += 1
            # A dropped message on the shared stream can corrupt ANY book → all suspect.
            self.mark_all_suspect(config.KALSHI_BOOK_SUSPECT_SECONDS)
            now = time.time()
            if now - self._last_gap_action > 15.0:
                self._last_gap_action = now
                # DEBUG (not WARNING): benign — the resnapshot self-heals it and the gap rate is
                # ~0.006%; kept off BOTH the default-INFO feed and the WARNING→Discord handler
                # (logger.py). Raise LOG_LEVEL=DEBUG to see gaps.
                log.debug(f"Kalshi book seq gap {last}→{seq} — resubscribing (resnapshot)")
                asyncio.create_task(self._resubscribe())
        self._seq_global = seq

    def _note_price_change(self, ticker: str, pd: KalshiPriceData) -> None:
        """Write a ticker's price (ALWAYS — byte-identical to the prior direct assignment) and stamp
        _last_book_change_ts ONLY when the tradeable top-of-book actually moves. Identical re-sends
        and liquidity-only ticks (OI/volume churn at a frozen price) are NOT changes — the
        frozen-resend zombie signature the freshness watchdog must catch. Keys on (yes_bid, yes_ask):
        no_bid/no_ask are exact complements (no_bid = 1−yes_ask, no_ask = 1−yes_bid) in both ticker
        and orderbook mode, so the pair fully determines the book with no info loss and no None risk.
        Mirrors poly_us _note_book. Used by the two WS write paths only (ticker handler + _derive);
        REST writes (prime/refresh) bypass it on purpose (see _last_book_change_ts in __init__)."""
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
        # Decimal keys (Phase 2) — max() is exact, and the complements below no longer need the
        # hand-placed round(): 1 - Decimal("0.55") is exactly Decimal("0.45"), never 0.44999999999999996.
        best_yes_bid = max(yes) if yes else _ZERO
        best_no_bid = max(no) if no else _ZERO
        # To BUY a side you cross the opposing side's best bid: buy-yes hits the no
        # bids (no bid p ⇒ yes offered at 1-p); buy-no hits the yes bids.
        # float() at this boundary ONLY: KalshiPriceData is float-typed and read by 38 external call
        # sites (D7 strangler-fig — the exact island stays inside the book).
        self._note_price_change(ticker, KalshiPriceData(
            yes_bid=float(best_yes_bid),
            yes_ask=float(_ONE - best_no_bid) if no else 1.0,
            no_bid=float(best_no_bid),
            no_ask=float(_ONE - best_yes_bid) if yes else 1.0,
            last_updated=time.time(),
        ))
        self._depth[ticker] = {
            "yes": float(no.get(best_no_bid, _ZERO)),   # size available to buy YES
            "no": float(yes.get(best_yes_bid, _ZERO)),  # size available to buy NO
        }
        # DIAGNOSTIC: a CROSSED book (best_yes_bid + best_no_bid > 1) violates no-arb and makes the
        # yes_ask/no_ask above phantom-cheap — the suspected phantom fire-path edge source. Log ENTER
        # (with raw top levels, to spot a stale level) + EXIT+duration, once per crossing episode.
        # Threshold 1.01 skips sub-cent rounding flicker. Reads only; nothing that fires changes.
        _xsum = best_yes_bid + best_no_bid
        if yes and no and _xsum > 1.01:
            if ticker not in self._crossed:
                self._crossed[ticker] = time.time()
                # float() the top levels for the LOG only: the book is Decimal, and "%s" of Decimal
                # tuples renders [(Decimal('0.65'), Decimal('5.68E-14'))] — unreadable at a glance,
                # and this diagnostic's whole job is that a human spots the dust level instantly.
                # Keep the plain (0.65, 5.68e-14) shape; it is what root-caused KALSHI_CROSSED.
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
            # `delta` must be finite: a NaN propagates through the addition below and then RAISES
            # InvalidOperation on `qty > 0` (verified), outside any try — that would escape to the
            # receive loop and cycle the WS connection. `Infinity` is worse-behaved still: it compares
            # True and would install a level of infinite depth. A non-finite `px` is meaningless as a
            # level key regardless. (hash(Decimal("NaN")) does NOT raise on this Python — the hazard is
            # the comparison, not the hashing.)
            if not (px.is_finite() and delta.is_finite()):
                return
            levels = book.get(side)
            if levels is None:
                return
            # ⚠️ THIS LINE IS THE CROSSED-BOOK BUG SITE. In float, `levels.get(px) + delta` on a level
            # being fully removed left ~1e-13 of dust; `qty > 0` kept the ghost, `max(price)` then
            # selected it as best-bid, and the book crossed → phantom-cheap ask → phantom fat edges
            # (30/52 would-fires crossed at fire time). It was patched with round(qty, 4).
            # In exact Decimal the removal lands on EXACTLY zero, so the dust cannot exist — the bug
            # is now unrepresentable rather than swept up afterwards.
            qty_before = levels.get(px, _ZERO)
            qty = qty_before + delta
            # is_finite() guard as in _levels_to_dict: a NaN delta propagates silently through the
            # addition and then RAISES on the comparison, which would tear down the WS connection.
            # Non-finite ⇒ not depth ⇒ drop the level (what the old float path did implicitly).
            if qty.is_finite() and qty > 0 and not is_zero(qty, _QTY_STEP):
                levels[px] = qty
            else:
                levels.pop(px, None)
                qty = _ZERO      # what the level actually IS now — report that, not the dust
            self._derive(ticker)
            if self._level_observer is not None:
                # `qty_before` as well as the raw venue `delta`, because the two can DISAGREE: a
                # level whose new qty falls below `_QTY_STEP` is popped to zero, so the book moved
                # by `qty_after - qty_before` while the wire said `delta`. A consumer accumulating
                # removals off the raw value over-counts, and for the maker's queue attribution that
                # direction favours the alarming verdict.
                self._notify_level({"kind": "delta", "ticker": ticker, "side": side,
                                    "price": px, "delta": delta,
                                    "qty_before": qty_before, "qty_after": qty})
        elif mtype == "ticker":
            # Liquidity/activity (LOGGING-ONLY) from the SAME ticker msg, captured FIRST and
            # ISOLATED: before the bid/ask early-return (so a tick missing bid/ask still records
            # liquidity), in its own swallowing try writing only _liquidity (so a malformed
            # liquidity field can NEVER prevent the bid/ask price write below — the load-bearing
            # path). Freshness anchor = receive time (mirrors KalshiPriceData.last_updated).
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
            yes_bid = float(yes_bid_raw)
            yes_ask = float(yes_ask_raw)
            self._note_price_change(ticker, KalshiPriceData(
                yes_bid=yes_bid,
                yes_ask=yes_ask,
                no_bid=complement(yes_ask),
                no_ask=complement(yes_bid),
                last_updated=time.time(),
            ))
        else:
            return

        if self._on_update_callback:
            self._on_update_callback()

    # ── WebSocket loop ────────────────────────────────────────────────────────

    # Force a reconnect only if NO data frame arrives this long. A genuinely dead
    # socket is already caught by the library's ping_interval=20/ping_timeout=20
    # (closes in ~40s → ConnectionClosed → reconnect), so this guard only exists to
    # catch a silently-dead SUBSCRIPTION (socket alive, server stopped sending).
    # 45s was too aggressive: thin overnight markets go quiet for >45s, tripping a
    # reconnect storm (repeated TLS handshakes → glibc RSS growth → OOM). 180s clears
    # the false positives while still recovering a stuck subscription within 3 min.
    _STALE_RECONNECT_S = 180

    # ── Frozen-book / data-freshness watchdog (the SECOND timer) ────────────────
    # _STALE_RECONNECT_S above = dead STREAM (no message at all). THIS one = frozen BOOK: messages
    # keep arriving (OI/volume ticks, re-sends — so _last_msg_ts stays fresh) while the tradeable
    # top-of-book never moves — the zombie that feeds phantom edges into detection. Keyed on
    # _last_book_change_ts (real WS changes only); see bot/core/feed_health._stale_book_reconnect.
    #
    # THRESHOLD — a PLACEHOLDER, not a derived value (see the measurement note). It is the
    # QUIET-SLATE false-positive floor: it only bites during DEAD periods (few/thin tickers
    # subscribed, between games). During an active game the socket-wide book changes sub-second, so
    # the timer is constantly reset and never approaches this. It is "slow for an in-game freeze" BY
    # DESIGN — a phase-blind socket-wide timer can't be both seconds-fast in-game AND quiet-slate-safe
    # (that needs per-game phase awareness, deferred). Tolerable only because the fire path re-checks
    # fresh REST before firing, so a frozen WS book costs wasted DETECTIONS, not a stale fire.
    #   Why 240, honestly: this is NOT derived — it's the dead-stream timer's 180s + margin. 180s was
    #   calibrated to MESSAGE gaps (45s stormed on thin overnight markets). Book-CHANGE gaps are
    #   strictly LONGER than message gaps (a frozen book still draws OI/heartbeat messages, so changes
    #   are rarer than messages), so a change-threshold should EXCEED 180s, not equal it — 180 would
    #   be too short here and re-court the dead-slate churn (bounded by the 60s cooldown, but
    #   wasteful). 240 is a conservative-ish starting point leaning the right way (up).
    #   PROVISIONAL: replace with a value tuned from the post-deploy dead-slate `changes/min` in
    #   ORDERBOOK HEALTH (the _book_changes hook). Expected direction of any retune: UP, not down.
    _FRESHNESS_RECONNECT_S = 240
    _FRESHNESS_RECONNECT_COOLDOWN_S = 60   # min gap between forced reconnects (anti-storm)

    async def _send_subscribe(self, ws) -> None:
        """Send the channel subscription for the active price source."""
        self._seq_global = None  # new subscription → seq stream restarts
        # Drop cached liquidity on every (re)subscribe: a mode switch (ticker→orderbook, which
        # carries no ticker channel) or any resubscribe must blank get_liquidity rather than serve
        # a value frozen at the last ticker-mode tick — blank-not-stale.
        self._liquidity.clear()
        if self._orderbook_mode:
            params = {"channels": ["orderbook_delta"]}
            if self._book_tickers:
                params["market_tickers"] = self._book_tickers
            await ws.send(json.dumps({"id": 1, "cmd": "subscribe", "params": params}))
            log.info(f"Kalshi WS: subscribed orderbook_delta ({len(self._book_tickers)} tickers)")
        else:
            await ws.send(json.dumps({"id": 1, "cmd": "subscribe",
                                      "params": {"channels": ["ticker"]}}))
            log.info("Kalshi WS: subscribed to ticker channel")

    def divergent_from_rest(
        self, rest_map: dict[str, tuple[float, float]], tol: float
    ) -> list[tuple[str, float, float]]:
        """Tickers whose WS-maintained best yes-bid disagrees with the REST book
        by more than `tol` — a sign the delta stream left a stale level behind.

        Returns (ticker, ws_yes_bid, rest_yes_bid) per offender. Orderbook mode
        only ([] otherwise); the caller resnapshots when this is non-empty.
        """
        if not self._orderbook_mode:
            return []
        out: list[tuple[str, float, float]] = []
        for ticker, (rest_yes_bid, _rest_yes_ask) in rest_map.items():
            data = self._prices.get(ticker)
            if data is None:
                continue
            if abs(data.yes_bid - rest_yes_bid) > tol:
                out.append((ticker, data.yes_bid, rest_yes_bid))
        return out

    async def resnapshot(self) -> bool:
        """Force a fresh orderbook snapshot for all book tickers (re-subscribe),
        flushing stale levels the delta stream left behind. THROTTLED: at most once per
        KALSHI_RESNAP_THROTTLE_SECONDS, so a chronically-divergent ticker can't trigger
        a resnapshot storm (it stays suspect via per-ticker marking instead). Returns
        True if it actually resnapshotted. No-op outside orderbook mode."""
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
                        # the zombie the dead-stream check (any message) can't see. Socket-wide,
                        # cooldown-bounded; mirrors the poly_us split.
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
